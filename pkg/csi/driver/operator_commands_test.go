package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"strings"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestValidationSummaryForCommandPrefersDisplaySummary(t *testing.T) {
	status := inventoryv1alpha1.OpenNebulaDatastoreStatus{
		LastValidationSummary: "R:12k W:8k p99:4ms",
		Validation: inventoryv1alpha1.OpenNebulaDatastoreValidationStatus{
			Phase:   inventoryv1alpha1.ValidationPhaseSucceeded,
			Message: "validation completed",
		},
	}
	if got := validationSummaryForCommand(status); got != "R:12k W:8k p99:4ms" {
		t.Fatalf("unexpected validation summary: %q", got)
	}
}

func TestBenchmarkSummaryForCommandPrefersDisplaySummary(t *testing.T) {
	status := inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRunStatus{
		Phase:   inventoryv1alpha1.ValidationPhaseSucceeded,
		Summary: "R:12k W:8k p99:4ms",
		Message: "benchmark completed",
	}
	if got := benchmarkSummaryForCommand(status); got != "R:12k W:8k p99:4ms" {
		t.Fatalf("unexpected benchmark summary: %q", got)
	}
}

func TestFlattenStorageClassDetails(t *testing.T) {
	datastores := []inventoryv1alpha1.OpenNebulaDatastore{
		{
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				StorageClassDetails: []inventoryv1alpha1.StorageClassDetail{{
					Name:              "opennebula-local",
					VolumeBindingMode: string(storagev1.VolumeBindingImmediate),
					Warnings:          []string{"local datastore uses Immediate binding"},
				}},
			},
		},
	}
	classes := []storagev1.StorageClass{{ObjectMeta: metav1.ObjectMeta{Name: "opennebula-local"}}}
	flattened := flattenStorageClassDetails(datastores, classes)
	if len(flattened) != 1 {
		t.Fatalf("expected 1 flattened storage class detail, got %d", len(flattened))
	}
	if len(flattened[0].Warnings) != 1 {
		t.Fatalf("expected warnings to be preserved, got %#v", flattened[0].Warnings)
	}
}

func TestSupportBundleConfigIncludesInventorySettings(t *testing.T) {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.InventoryControllerEnabledVar, true)
	cfg.OverrideVal(config.InventoryDatastoreAuthorityModeVar, "strict")
	cfg.OverrideVal(config.PreflightLocalImmediateBindingPolicyVar, "warn")
	cfg.OverrideVal(config.LocalDeviceRecoveryEnabledVar, true)
	cfg.OverrideVal(config.LocalDeviceRecoveryMinAttemptsVar, 3)
	cfg.OverrideVal(config.HostArtifactQuarantineEnabledVar, true)
	cfg.OverrideVal(config.HostArtifactQuarantineFailureThresholdVar, 1)

	bundleCfg := supportBundleConfig(cfg)
	if enabled, ok := bundleCfg["inventoryControllerEnabled"].(bool); !ok || !enabled {
		t.Fatalf("expected inventory controller enabled in support bundle config, got %#v", bundleCfg["inventoryControllerEnabled"])
	}
	if bundleCfg["inventoryAuthorityMode"] != "strict" {
		t.Fatalf("unexpected authority mode: %#v", bundleCfg["inventoryAuthorityMode"])
	}
	if enabled, ok := bundleCfg["localDeviceRecoveryEnabled"].(bool); !ok || !enabled {
		t.Fatalf("expected local device recovery enabled in support bundle config, got %#v", bundleCfg["localDeviceRecoveryEnabled"])
	}
	if bundleCfg["localDeviceRecoveryMinAttempts"] != 3 {
		t.Fatalf("unexpected local device recovery min attempts: %#v", bundleCfg["localDeviceRecoveryMinAttempts"])
	}
	if enabled, ok := bundleCfg["hostArtifactQuarantineEnabled"].(bool); !ok || !enabled {
		t.Fatalf("expected host artifact quarantine enabled in support bundle config, got %#v", bundleCfg["hostArtifactQuarantineEnabled"])
	}
	if bundleCfg["hostArtifactQuarantineFailureThreshold"] != 1 {
		t.Fatalf("unexpected host artifact quarantine threshold: %#v", bundleCfg["hostArtifactQuarantineFailureThreshold"])
	}
}

func TestSupportBundleLocalRWOSnapshotMarksBootstrappedHistory(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-support-bootstrap", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-old"
	client := fake.NewSimpleClientset(pv, pvc, newReadyNode("node-old", true))
	cfg := config.LoadConfiguration()

	history, _, _, _, _ := collectSupportBundleLocalRWOSnapshot(context.Background(), cfg, client, []corev1.PersistentVolume{*pv})
	record, ok := history["vol-support-bootstrap"]
	if !ok {
		t.Fatalf("expected bootstrapped support history for volume")
	}
	if !record.Bootstrapped {
		t.Fatalf("expected support history to be marked bootstrapped: %#v", record)
	}
	if record.EvidenceSource != volumeHistoryEvidenceSourceBootstrap {
		t.Fatalf("expected bootstrap evidence source, got %q", record.EvidenceSource)
	}
	if record.LastSuccessfulNodeName != "node-old" {
		t.Fatalf("expected last attached node to be preserved, got %q", record.LastSuccessfulNodeName)
	}
	if !record.LastSuccessfulPublishTime.IsZero() || !record.LastSuccessfulStageTime.IsZero() {
		t.Fatalf("support bootstrap must not fabricate successful publish/stage timestamps: %#v", record)
	}
}

func TestCollectVolumeHealthReportsFlagsLegacyLastAttachedNodeAnnotation(t *testing.T) {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv-legacy",
			Annotations: map[string]string{
				annotationLegacyLastAttachedNode: "node-old",
				annotationBackend:                "local",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			ClaimRef: &corev1.ObjectReference{
				Namespace: "default",
				Name:      "pvc-legacy",
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       DefaultDriverName,
					VolumeHandle: "vol-legacy",
				},
			},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pvc-legacy",
			Annotations: map[string]string{
				annotationLegacyLastAttachedNode: "node-old",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
	}
	kubeClient := fake.NewSimpleClientset(pv, pvc)
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.OpenNebulaCredentialsVar, "")

	reports, err := collectVolumeHealthReports(context.Background(), kubeClient, cfg, VolumeHealthOptions{VolumeID: "vol-legacy"})
	if err != nil {
		t.Fatalf("collectVolumeHealthReports returned error: %v", err)
	}
	if len(reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(reports))
	}
	if len(reports[0].AnnotationAudit) != 2 {
		t.Fatalf("expected PV and PVC legacy annotation audit findings, got %#v", reports[0].AnnotationAudit)
	}
	for _, finding := range reports[0].AnnotationAudit {
		if finding.CanonicalKey != annotationLastAttachedNode {
			t.Fatalf("expected canonical annotation key %q, got %#v", annotationLastAttachedNode, finding)
		}
	}
}

func TestCollectVolumeHealthReportsFlagsStaleLastAttachedNodeAnnotation(t *testing.T) {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv-stale-last",
			Annotations: map[string]string{
				annotationLastAttachedNode: "node-missing",
				annotationBackend:          "local",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			ClaimRef: &corev1.ObjectReference{
				Namespace: "default",
				Name:      "pvc-stale-last",
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       DefaultDriverName,
					VolumeHandle: "vol-stale-last",
				},
			},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pvc-stale-last",
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
	}
	kubeClient := fake.NewSimpleClientset(pv, pvc)
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.OpenNebulaCredentialsVar, "")

	reports, err := collectVolumeHealthReports(context.Background(), kubeClient, cfg, VolumeHealthOptions{VolumeID: "vol-stale-last"})
	if err != nil {
		t.Fatalf("collectVolumeHealthReports returned error: %v", err)
	}
	if len(reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(reports))
	}
	found := false
	for _, finding := range reports[0].AnnotationAudit {
		if finding.Key == annotationLastAttachedNode {
			found = true
			if !strings.Contains(finding.Message, "stale soft-placement hint") {
				t.Fatalf("expected stale soft-placement audit message, got %#v", finding)
			}
		}
	}
	if !found {
		t.Fatalf("expected stale last-attached-node audit finding, got %#v", reports[0].AnnotationAudit)
	}
}

func TestSupportBundleLocalRWOSnapshotIsReadOnly(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprintf("maintenance=%v", active), func(t *testing.T) {
			pv, pvc := newLocalPVAndPVC("vol-diagnostics", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.Annotations[annotationLastAttachedNode] = "node-old"
			node := newReadyNode("node-old", true)
			node.UID = "node-owned"
			state := StickyAttachmentState{VolumeID: "vol-diagnostics", NodeID: "node-old", Backend: "local", Reason: maintenanceStickyReason, StartedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Hour)}
			payload, err := json.Marshal(state)
			require.NoError(t, err)
			held := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: stickyAttachmentStateConfigMapName, Namespace: namespaceFromServiceAccount()}, Data: map[string]string{"vol-diagnostics": string(payload)}}
			maintenance := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: hotplugStateConfigMapName, Namespace: namespaceFromServiceAccount()}, Data: map[string]string{maintenanceModeKey: fmt.Sprint(active), maintenanceReadyKey: "true"}}
			client := fake.NewSimpleClientset(pv, pvc, node, held, maintenance)
			client.PrependReactor("*", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
				switch action.GetVerb() {
				case "get", "list":
					return false, nil, nil
				default:
					t.Errorf("diagnostic collection attempted %s %s", action.GetVerb(), action.GetResource().Resource)
					return true, nil, fmt.Errorf("diagnostics must be read-only")
				}
			})
			history, _, _, demand, protection := collectSupportBundleLocalRWOSnapshot(context.Background(), config.LoadConfiguration(), client, []corev1.PersistentVolume{*pv})
			require.True(t, history["vol-diagnostics"].Bootstrapped)
			require.Equal(t, "node-old", history["vol-diagnostics"].LastSuccessfulNodeName)
			require.Contains(t, demand, "vol-diagnostics")
			require.True(t, protection["vol-diagnostics"].Protected)
			require.Equal(t, "node-old", protection["vol-diagnostics"].RequiredNode)
			current, err := client.CoreV1().ConfigMaps(held.Namespace).Get(context.Background(), held.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, held.Data, current.Data)
		})
	}
}

func TestSupportBundleProjectsLiveMaintenanceAttachmentWithoutWrites(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-live-diagnostics", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	node := newReadyNode("node-old", true)
	va := &storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-live", UID: "va-live-uid"}, Spec: storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}}, Status: storagev1.VolumeAttachmentStatus{Attached: true}}
	maintenance := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: hotplugStateConfigMapName, Namespace: namespaceFromServiceAccount()}, Data: map[string]string{maintenanceModeKey: "true"}}
	secondPV, secondPVC := newLocalPVAndPVC("vol-diagnostics-second", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	secondPV.Annotations[annotationPreferredNode] = "invalid-explicit"
	secondPV.Annotations[annotationPreferredLastNode] = "legacy-node"
	legacy := newReadyNode("legacy-node", true)
	secondVA := va.DeepCopy()
	secondVA.Name = "va-second"
	secondVA.Spec.Source.PersistentVolumeName = &secondPV.Name
	client := fake.NewSimpleClientset(pv, pvc, secondPV, secondPVC, node, legacy, va, secondVA, maintenance)
	censuses := 0
	client.PrependReactor("list", "volumeattachments", func(ktesting.Action) (bool, kruntime.Object, error) {
		censuses++
		return false, nil, nil
	})
	client.PrependReactor("*", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetVerb() == "get" || action.GetVerb() == "list" {
			return false, nil, nil
		}
		t.Errorf("diagnostics attempted %s %s", action.GetVerb(), action.GetResource().Resource)
		return true, nil, fmt.Errorf("unexpected diagnostic write")
	})
	history, _, _, demand, protection := collectSupportBundleLocalRWOSnapshot(context.Background(), config.LoadConfiguration(), client, []corev1.PersistentVolume{*pv, *secondPV})
	require.Equal(t, 1, censuses)
	require.Len(t, demand, 2)
	require.Equal(t, legacy.Name, history["vol-diagnostics-second"].LastSuccessfulNodeName)
	require.Equal(t, legacy.Name, protection["vol-diagnostics-second"].RequiredNode)
	require.Equal(t, node.Name, history["vol-live-diagnostics"].LastSuccessfulNodeName)
	require.True(t, history["vol-live-diagnostics"].Bootstrapped)
	require.Equal(t, history["vol-live-diagnostics"], protection["vol-live-diagnostics"].History)
	require.Equal(t, history["vol-diagnostics-second"], protection["vol-diagnostics-second"].History)
	require.Equal(t, node.Name, protection["vol-live-diagnostics"].RequiredNode)
	require.True(t, protection["vol-live-diagnostics"].Protected)
	current, err := client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, pv.Annotations, current.Annotations)
}

func TestSupportBundlePreservesPodDemandWhenAttachmentListFails(t *testing.T) {
	modes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	pv, pvc := newLocalPVAndPVC("vol-active", modes, nil)
	idlePV, idlePVC := newLocalPVAndPVC("vol-no-pod", modes, nil)
	pod := activePodForPVC("consumer", pvc.Name, "node-a")
	client := fake.NewSimpleClientset(pv, pvc, idlePV, idlePVC, pod)
	attachmentLists := 0
	client.PrependReactor("*", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
		switch action.GetVerb() {
		case "list", "get":
			if action.GetResource().Resource == "volumeattachments" {
				attachmentLists++
				return true, nil, fmt.Errorf("collector may not list VolumeAttachments")
			}
			return false, nil, nil
		default:
			t.Errorf("diagnostic collection attempted %s %s", action.GetVerb(), action.GetResource().Resource)
			return true, nil, fmt.Errorf("diagnostics must be read-only")
		}
	})
	_, _, _, demand, _ := collectSupportBundleLocalRWOSnapshot(context.Background(), config.LoadConfiguration(), client, []corev1.PersistentVolume{*pv, *idlePV})
	require.Equal(t, 1, attachmentLists)
	require.Equal(t, "active_pod", demand["vol-active"].DemandSource)
	require.Equal(t, "node-a", demand["vol-active"].NodeName)
	require.NotContains(t, demand, "vol-no-pod")
}
