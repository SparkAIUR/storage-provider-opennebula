package driver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func newWebhookTestDriver(objects ...runtime.Object) *Driver {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.LastNodePreferenceEnabledVar, true)
	cfg.OverrideVal(config.LastNodePreferenceWebhookEnabledVar, true)
	runtime := &KubeRuntime{client: fake.NewSimpleClientset(objects...), enabled: true}
	driver := &Driver{
		name:         DefaultDriverName,
		PluginConfig: cfg,
		kubeRuntime:  runtime,
		metrics:      NewDriverMetrics("test", "test"),
	}
	driver.featureGates = loadFeatureGates(cfg)
	driver.stickyAttachments = NewStickyAttachmentManager(runtime, "default")
	driver.volumeHistory = NewVolumeHistoryManager(runtime, "default")
	driver.volumeRepairState = NewVolumeRepairStateManager(runtime, "default")
	return driver
}

func TestLastNodePreferenceWebhookInjectsSoftAffinity(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-a"
	pvc.Spec.VolumeName = pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"}}}
	driver := newWebhookTestDriver(pv, pvc, node)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.NotEmpty(t, patch)

	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), jsonPointerEscape(annotationRequiredNodeInjected))
	assert.Contains(t, string(raw), jsonPointerEscape(annotationPlacementSource))
	assert.Contains(t, string(raw), jsonPointerEscape(annotationPlacementDecision))
	assert.Contains(t, string(raw), "requiredDuringSchedulingIgnoredDuringExecution")
	assert.Contains(t, string(raw), "host-a")
	assert.NotContains(t, string(raw), "preferredDuringSchedulingIgnoredDuringExecution")
}

func TestLastNodePreferenceWebhookInjectsHardAffinityDuringMaintenance(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-a"
	pvc.Spec.VolumeName = pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"}}}
	driver := newWebhookTestDriver(pv, pvc, node)
	driver.maintenanceMode = NewMaintenanceModeManager(driver, "default")
	driver.maintenanceMode.setState(true, true, false)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.NotEmpty(t, patch)

	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), "requiredDuringSchedulingIgnoredDuringExecution")
	assert.Contains(t, string(raw), "host-a")
	assert.NotContains(t, string(raw), "preferredDuringSchedulingIgnoredDuringExecution")
}

func TestLastNodePreferenceWebhookInjectsHardAffinityForProtectedStickyVolume(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"}}}
	driver := newWebhookTestDriver(pv, pvc, node)
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
		VolumeID:     "vol-1",
		NodeID:       "node-a",
		Backend:      "local",
		PVCNamespace: "default",
		PVCName:      pvc.Name,
		StartedAt:    time.Now().Add(-10 * time.Second),
		ExpiresAt:    time.Now().Add(90 * time.Second),
		GraceSeconds: 90,
		Reason:       "stateful_restart",
	}))
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "minio-0",
			Namespace:   "default",
			Annotations: map[string]string{annotationLastNodePref: lastNodePreferenceDisabledValue},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.NotEmpty(t, patch)

	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), "requiredDuringSchedulingIgnoredDuringExecution")
	assert.Contains(t, string(raw), "host-a")
	assert.NotContains(t, string(raw), "preferredDuringSchedulingIgnoredDuringExecution")
}

func TestLastNodePreferenceWebhookRejectsConflictingNodesDuringMaintenance(t *testing.T) {
	pv1, pvc1 := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv2, pvc2 := newLocalPVAndPVC("vol-2", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv1.Annotations[annotationLastAttachedNode] = "node-a"
	pv2.Annotations[annotationLastAttachedNode] = "node-b"
	pvc1.Spec.VolumeName = pv1.Name
	pvc2.Spec.VolumeName = pv2.Name
	nodeA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: hostLabel("a")}}}
	nodeB := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: hostLabel("b")}}}
	driver := newWebhookTestDriver(pv1, pvc1, pv2, pvc2, nodeA, nodeB)
	driver.maintenanceMode = NewMaintenanceModeManager(driver, "default")
	driver.maintenanceMode.setState(true, true, false)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{Name: "data-a", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc1.Name}}},
				{Name: "data-b", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name}}},
			},
		},
	})
	require.Error(t, err)
	assert.Empty(t, patch)
	assert.NotEmpty(t, warnings)
}

func TestLastNodePreferenceWebhookSkipsConflictingPVCs(t *testing.T) {
	pv1, pvc1 := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv2, pvc2 := newLocalPVAndPVC("vol-2", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv1.Annotations[annotationLastAttachedNode] = "node-a"
	pv2.Annotations[annotationLastAttachedNode] = "node-b"
	pvc1.Spec.VolumeName = pv1.Name
	pvc2.Spec.VolumeName = pv2.Name
	driver := newWebhookTestDriver(pv1, pvc1, pv2, pvc2)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{Name: "data-a", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc1.Name}}},
				{Name: "data-b", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name}}},
			},
		},
	})
	require.Error(t, err)
	assert.Empty(t, patch)
	assert.NotEmpty(t, warnings)
	assert.Contains(t, warnings[len(warnings)-1], "conflicting local RWO volumes require different manual or protected nodes")
}

func TestLastNodePreferenceWebhookHonorsOptOut(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-a"
	pvc.Spec.VolumeName = pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: hostLabel("a")}}}
	driver := newWebhookTestDriver(pv, pvc, node)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "minio-0",
			Namespace:   "default",
			Annotations: map[string]string{annotationLastNodePref: lastNodePreferenceDisabledValue},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, patch)
	assert.Empty(t, warnings)

	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), jsonPointerEscape(annotationRequiredNodeInjected))
	assert.Contains(t, string(raw), "requiredDuringSchedulingIgnoredDuringExecution")
	assert.Contains(t, string(raw), hostLabel("a"))
	assert.NotContains(t, string(raw), "preferredDuringSchedulingIgnoredDuringExecution")
}

func TestLastNodePreferenceWebhookInjectsExplicitRequiredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-required", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationRequiredNode] = "node-b"
	pvc.Annotations[annotationPlacementReason] = "manual-recovery"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-b",
		Labels: map[string]string{
			corev1.LabelHostname:  hostLabel("b"),
			topologySystemDSLabel: "104",
		},
	}}
	driver := newWebhookTestDriver(pv, pvc, node)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaNode{
			ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
			Status: inventoryv1alpha1.OpenNebulaNodeStatus{
				Phase: inventoryv1alpha1.NodePhaseReady,
				OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{
					SystemDatastoreID: 104,
				},
			},
		},
	)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), jsonPointerEscape(annotationRequiredNodeInjected))
	assert.Contains(t, string(raw), jsonPointerEscape(annotationPlacementSource))
	assert.Contains(t, string(raw), jsonPointerEscape(annotationPlacementDecision))
	assert.Contains(t, string(raw), `"required"`)
	assert.Contains(t, string(raw), hostLabel("b"))
}

func TestLastNodePreferenceWebhookRejectsConflictingExplicitRequiredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-conflict", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationRequiredNode] = "node-b"
	nodeA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-a",
		Labels: map[string]string{
			corev1.LabelHostname:  hostLabel("a"),
			topologySystemDSLabel: "104",
		},
	}}
	nodeB := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-b",
		Labels: map[string]string{
			corev1.LabelHostname:  hostLabel("b"),
			topologySystemDSLabel: "104",
		},
	}}
	driver := newWebhookTestDriver(pv, pvc, nodeA, nodeB)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
	)
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
		VolumeID:     "vol-conflict",
		NodeID:       "node-a",
		Backend:      "local",
		PVCNamespace: "default",
		PVCName:      pvc.Name,
		StartedAt:    time.Now().Add(-10 * time.Second),
		ExpiresAt:    time.Now().Add(90 * time.Second),
		GraceSeconds: 90,
		Reason:       "stateful_restart",
	}))
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.Error(t, err)
	assert.Empty(t, patch)
	assert.NotEmpty(t, warnings)
	assert.Contains(t, err.Error(), "conflicts with protected node")
}

func TestLastNodePreferenceWebhookInjectsExplicitPreferredNodeWhenSoftPreferenceDisabled(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-preferred", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationPreferredNode] = "node-b"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-b",
		Labels: map[string]string{
			corev1.LabelHostname:  hostLabel("b"),
			topologySystemDSLabel: "104",
		},
	}}
	driver := newWebhookTestDriver(pv, pvc, node)
	driver.PluginConfig.OverrideVal(config.LastNodePreferenceEnabledVar, false)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
	)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "minio-0",
			Namespace:   "default",
			Annotations: map[string]string{annotationLastNodePref: lastNodePreferenceDisabledValue},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), jsonPointerEscape(annotationPreferredNodeInjected))
	assert.Contains(t, string(raw), hostLabel("b"))
}

func TestLastNodePreferenceWebhookSkipsMissingHistoricalLastAttachedNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-missing-last", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-missing"
	pvc.Spec.VolumeName = pv.Name
	driver := newWebhookTestDriver(pv, pvc)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.Error(t, err)
	assert.Empty(t, patch)
	assert.Empty(t, warnings)
	assert.Contains(t, err.Error(), "node-missing")
}

func TestLastNodePreferenceWebhookSkipsTombstonedHistoricalLastAttachedNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-tombstoned-last", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pv.Annotations[annotationLastAttachedNode] = "node-a"
	pvc.Spec.VolumeName = pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-a",
		Labels: map[string]string{
			corev1.LabelHostname:  hostLabel("a"),
			topologySystemDSLabel: "104",
		},
	}}
	driver := newWebhookTestDriver(pv, pvc, node)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaNode{
			ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
			Status: inventoryv1alpha1.OpenNebulaNodeStatus{
				Phase: inventoryv1alpha1.NodePhaseNotFound,
				OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{
					SystemDatastoreID: 104,
				},
			},
		},
	)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), jsonPointerEscape(annotationRequiredNodeInjected))
	assert.Contains(t, string(raw), "requiredDuringSchedulingIgnoredDuringExecution")
	assert.NotContains(t, string(raw), "preferredDuringSchedulingIgnoredDuringExecution")
}

func TestLastNodePreferenceWebhookSkipsMissingExplicitPreferredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-missing-preferred", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationPreferredNode] = "node-missing"
	driver := newWebhookTestDriver(pv, pvc)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, patch)
	assert.NotEmpty(t, warnings)
	assert.Contains(t, warnings[0], "soft preferred-node was ignored")
}

func TestLastNodePreferenceWebhookRejectsMissingExplicitRequiredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-missing-required", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationRequiredNode] = "node-missing"
	driver := newWebhookTestDriver(pv, pvc)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.Error(t, err)
	assert.Empty(t, patch)
	assert.NotEmpty(t, warnings)
	assert.Contains(t, err.Error(), "required-node node-missing does not exist in Kubernetes")
}

func TestLastNodePreferenceWebhookIgnoresExpiredExplicitRequiredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-expired", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-a"
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationRequiredNode] = "node-b"
	pvc.Annotations[annotationRequiredNodeUntil] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: hostLabel("a")}}}
	driver := newWebhookTestDriver(pv, pvc, node)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			}},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, warnings)
	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), jsonPointerEscape(annotationRequiredNodeInjected))
	assert.Contains(t, string(raw), "requiredDuringSchedulingIgnoredDuringExecution")
	assert.Contains(t, string(raw), hostLabel("a"))
	assert.NotContains(t, string(raw), "preferredDuringSchedulingIgnoredDuringExecution")
}

func hostLabel(suffix string) string {
	return "host-" + suffix
}

func TestLastNodePreferenceWebhookInjectsRequiredSystemDatastoreAffinity(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	driver := newWebhookTestDriver(pv, pvc)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t, &inventoryv1alpha1.OpenNebulaDatastore{
		ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
		Spec: inventoryv1alpha1.OpenNebulaDatastoreSpec{
			Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111},
		},
		Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
			ID:      111,
			Backend: "local",
			OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
				CompatibleSystemDatastores: []int{104, 100},
			},
		},
	})
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-1", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}},
			}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.NotEmpty(t, patch)

	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), jsonPointerEscape(annotationSystemDSAffinity))
	assert.Contains(t, string(raw), jsonPointerEscape(annotationSystemDSInjected))
	assert.Contains(t, string(raw), topologySystemDSLabel)
	assert.Contains(t, string(raw), "100")
	assert.Contains(t, string(raw), "104")
	assert.Contains(t, string(raw), "requiredDuringSchedulingIgnoredDuringExecution")
}

func TestLastNodePreferenceWebhookIntersectsSystemDatastoreAffinity(t *testing.T) {
	pv1, pvc1 := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pv2, pvc2 := newLocalPVAndPVC("vol-2", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "124",
	})
	pvc1.Spec.VolumeName = pv1.Name
	pvc2.Spec.VolumeName = pv2.Name
	driver := newWebhookTestDriver(pv1, pvc1, pv2, pvc2)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{100, 104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-124"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 124}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      124,
				Backend: "ceph-rbd",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104, 123},
				},
			},
		},
	)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, warnings, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-1", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{Name: "data-a", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc1.Name}}},
				{Name: "data-b", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name}}},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"104"`)
	assert.NotContains(t, string(raw), `"100"`)
	assert.NotContains(t, string(raw), `"123"`)
}

func TestLastNodePreferenceWebhookRejectsDisjointSystemDatastoreAffinity(t *testing.T) {
	pv1, pvc1 := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pv2, pvc2 := newLocalPVAndPVC("vol-2", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "124",
	})
	pvc1.Spec.VolumeName = pv1.Name
	pvc2.Spec.VolumeName = pv2.Name
	driver := newWebhookTestDriver(pv1, pvc1, pv2, pvc2)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{100},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-124"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 124}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      124,
				Backend: "ceph-rbd",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{123},
				},
			},
		},
	)
	webhook := NewLastNodePreferenceWebhook(driver)

	patch, _, err := webhook.buildPatch(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-1", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{Name: "data-a", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc1.Name}}},
				{Name: "data-b", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name}}},
			},
		},
	})
	require.Error(t, err)
	assert.Empty(t, patch)
}
