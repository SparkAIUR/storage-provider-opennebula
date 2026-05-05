package driver

import (
	"context"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func newMaintenanceTestDriver(t *testing.T, objects ...runtime.Object) *Driver {
	t.Helper()
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.MaintenanceReleaseMinSecondsVar, 10)
	cfg.OverrideVal(config.MaintenanceReleaseMaxSecondsVar, 20)
	runtime := &KubeRuntime{client: fake.NewSimpleClientset(objects...), enabled: true}
	driver := &Driver{
		name:              DefaultDriverName,
		PluginConfig:      cfg,
		kubeRuntime:       runtime,
		metrics:           NewDriverMetrics("test", "test"),
		stickyAttachments: NewStickyAttachmentManager(runtime, "default"),
	}
	driver.maintenanceMode = NewMaintenanceModeManager(driver, "default")
	return driver
}

func TestMaintenanceModeReconcilePreparesLocalRWOPV(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-maint", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	pvName := pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: hostLabel("a")}}}
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-maint"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: hotplugStateConfigMapName, Namespace: "default"},
		Data:       map[string]string{maintenanceModeKey: "true"},
	}
	driver := newMaintenanceTestDriver(t, pv, pvc, node, va, cm)

	require.NoError(t, driver.maintenanceMode.Reconcile(context.Background()))
	assert.True(t, driver.maintenanceMode.Active())
	assert.True(t, driver.maintenanceMode.Ready())

	updatedCM, err := driver.kubeRuntime.client.CoreV1().ConfigMaps("default").Get(context.Background(), hotplugStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "true", updatedCM.Data[maintenanceReadyKey])

	updatedPV, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "node-a", updatedPV.Annotations[annotationLastAttachedNode])
}

func TestMaintenanceModePrepareHonorsExplicitPreferredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-maint-override", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	pv.Annotations[annotationPreferredNode] = "node-operator"
	pvName := pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-operator", Labels: map[string]string{corev1.LabelHostname: hostLabel("operator")}}}
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-maint-override"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-stale",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: hotplugStateConfigMapName, Namespace: "default"},
		Data:       map[string]string{maintenanceModeKey: "true"},
	}
	driver := newMaintenanceTestDriver(t, pv, pvc, node, va, cm)

	require.NoError(t, driver.maintenanceMode.Reconcile(context.Background()))

	updatedPV, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "node-operator", updatedPV.Annotations[annotationLastAttachedNode])
}

func TestMaintenanceModePrepareHonorsDeprecatedPreferredLastNodeFallback(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-maint-legacy", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	pv.Annotations[annotationPreferredLastNode] = "node-legacy"
	pvName := pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-legacy", Labels: map[string]string{corev1.LabelHostname: hostLabel("legacy")}}}
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-maint-legacy"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-stale",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: hotplugStateConfigMapName, Namespace: "default"},
		Data:       map[string]string{maintenanceModeKey: "true"},
	}
	driver := newMaintenanceTestDriver(t, pv, pvc, node, va, cm)

	require.NoError(t, driver.maintenanceMode.Reconcile(context.Background()))

	updatedPV, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "node-legacy", updatedPV.Annotations[annotationLastAttachedNode])
}

func TestMaintenanceModePrepareIgnoresInvalidExplicitPreferredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-maint-invalid-preferred", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	pv.Annotations[annotationPreferredNode] = "node-missing"
	pvName := pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-a",
		Labels: map[string]string{
			corev1.LabelHostname:  hostLabel("a"),
			topologySystemDSLabel: "104",
		},
	}}
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-maint-invalid-preferred"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: hotplugStateConfigMapName, Namespace: "default"},
		Data:       map[string]string{maintenanceModeKey: "true"},
	}
	driver := newMaintenanceTestDriver(t, pv, pvc, node, va, cm)
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
	)

	require.NoError(t, driver.maintenanceMode.Reconcile(context.Background()))

	updatedPV, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "node-a", updatedPV.Annotations[annotationLastAttachedNode])
}

func TestMaintenanceModePrepareIgnoresStaleLastAttachedNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-maint-stale-last", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	pv.Annotations[annotationLastAttachedNode] = "node-stale"
	pvName := pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-a",
		Labels: map[string]string{
			corev1.LabelHostname:  hostLabel("a"),
			topologySystemDSLabel: "104",
		},
	}}
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-maint-stale-last"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: hotplugStateConfigMapName, Namespace: "default"},
		Data:       map[string]string{maintenanceModeKey: "true"},
	}
	driver := newMaintenanceTestDriver(t, pv, pvc, node, va, cm)
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
	)

	require.NoError(t, driver.maintenanceMode.Reconcile(context.Background()))

	updatedPV, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "node-a", updatedPV.Annotations[annotationLastAttachedNode])
}

func TestMaintenanceLastNodeForVolumeSkipsInvalidHistoricalHint(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-maint-invalid-last", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-missing"
	pvc.Spec.VolumeName = pv.Name
	driver := newMaintenanceTestDriver(t, pv, pvc)

	lastNode := maintenanceLastNodeForVolume(context.Background(), driver, volumeRuntimeContextFromPVAndPVC(pv, pvc), "vol-maint-invalid-last")
	assert.Empty(t, lastNode)
}

func TestMaintenanceBlocksPublishIgnoresInvalidHistoricalHint(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-maint-block", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-missing"
	pvc.Spec.VolumeName = pv.Name
	driver := newMaintenanceTestDriver(t, pv, pvc)
	driver.maintenanceMode.setState(true, true, false)
	server := &ControllerServer{driver: driver}

	blocked, message := server.maintenanceBlocksPublish(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "vol-maint-block",
		NodeId:   "node-b",
	})
	assert.False(t, blocked)
	assert.Empty(t, message)
}

func TestMaintenanceModeReconcileClearsReadyAndStaggersRelease(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: hotplugStateConfigMapName, Namespace: "default"},
		Data: map[string]string{
			maintenanceModeKey:       "false",
			maintenanceReadyKey:      "true",
			maintenanceReadyAliasKey: "true",
		},
	}
	driver := newMaintenanceTestDriver(t, cm)
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
		VolumeID:     "vol-held",
		NodeID:       "node-a",
		Backend:      "local",
		PVCNamespace: "default",
		PVCName:      "pvc-held",
		StartedAt:    time.Now().Add(-time.Minute),
		ExpiresAt:    time.Now().Add(time.Hour),
		GraceSeconds: 3600,
		Reason:       maintenanceStickyReason,
	}))

	require.NoError(t, driver.maintenanceMode.Reconcile(context.Background()))
	assert.False(t, driver.maintenanceMode.Active())

	updatedCM, err := driver.kubeRuntime.client.CoreV1().ConfigMaps("default").Get(context.Background(), hotplugStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, updatedCM.Data, maintenanceReadyKey)
	assert.NotContains(t, updatedCM.Data, maintenanceReadyAliasKey)

	state, ok := driver.stickyAttachments.Get("vol-held")
	require.True(t, ok)
	assert.Equal(t, maintenanceReleaseReason, state.Reason)
	assert.GreaterOrEqual(t, state.GraceSeconds, 10)
	assert.LessOrEqual(t, state.GraceSeconds, 20)
}

func TestMaintenanceModeActiveSupportsAlias(t *testing.T) {
	active, alias := maintenanceModeActive(map[string]string{maintenanceModeAliasKey: "true"})
	assert.True(t, active)
	assert.True(t, alias)
}
