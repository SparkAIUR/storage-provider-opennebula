package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	mount "k8s.io/mount-utils"
)

func TestResolvedRecoveryControlStateHandlesExpiryAndInvalidAdoption(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	future := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)

	expired := resolvedRecoveryControlState("vol-expired", &VolumeRuntimeContext{
		PVName:        "pv-vol-expired",
		PVCNamespace:  "default",
		PVCName:       "pvc-vol-expired",
		PVAnnotations: map[string]string{annotationRecoveryMode: recoveryModeManual, annotationRecoveryModeUntil: past},
	}, VolumeRecoveryControlState{})
	if expired.Active() {
		t.Fatalf("expired recovery control should not remain active: %#v", expired)
	}
	if !expired.Expired || expired.Warning == "" {
		t.Fatalf("expected expired recovery control warning, got %#v", expired)
	}

	invalid := resolvedRecoveryControlState("vol-invalid", &VolumeRuntimeContext{
		PVName:        "pv-vol-invalid",
		PVCNamespace:  "default",
		PVCName:       "pvc-vol-invalid",
		PVAnnotations: map[string]string{annotationRecoveryMode: recoveryModeManual, annotationRecoveryModeUntil: future, annotationAdoptAttachedDevice: "true"},
	}, VolumeRecoveryControlState{})
	if !invalid.Invalid {
		t.Fatalf("expected adopt-attached-device without serial/name to be invalid")
	}
	if invalid.Message == "" {
		t.Fatalf("expected invalid recovery control to explain the failure")
	}
}

func TestVolumeQuarantineRefreshEntryClearsDeletedConfigMapKey(t *testing.T) {
	now := time.Now().UTC()
	state := VolumeQuarantineState{
		VolumeID:       "vol-1",
		Reason:         metadataDriftReason,
		Classification: metadataDriftClassification,
		Message:        "stale metadata",
		ExpiresAt:      now.Add(time.Hour),
	}
	payload, err := json.Marshal(state)
	require.NoError(t, err)

	runtime := &KubeRuntime{
		client:  fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: volumeQuarantineStateConfigMapName, Namespace: "default"}, Data: map[string]string{"vol-1": string(payload)}}),
		enabled: true,
	}
	manager := NewVolumeQuarantineManager(runtime, "default")
	require.NoError(t, manager.LoadFromConfigMap(context.Background()))
	if _, ok := manager.GetActive("vol-1", now); !ok {
		t.Fatalf("expected initial quarantine state to be active")
	}
	require.NoError(t, runtime.DeleteConfigMapKey(context.Background(), "default", volumeQuarantineStateConfigMapName, "vol-1"))
	require.NoError(t, manager.RefreshEntry(context.Background(), "vol-1"))
	if _, ok := manager.GetActive("vol-1", now); ok {
		t.Fatalf("expected refresh to drop deleted quarantine state from memory")
	}
}

func TestHostArtifactRefreshVolumeClearsDeletedConfigMapKey(t *testing.T) {
	now := time.Now().UTC()
	state := HostArtifactQuarantineState{
		Key:            "artifact-1",
		Reason:         hostArtifactReason,
		Classification: opennebula.HostArtifactConflictClassification,
		VolumeID:       "vol-1",
		Message:        "stale LV",
		ExpiresAt:      now.Add(time.Hour),
	}
	payload, err := json.Marshal(state)
	require.NoError(t, err)

	runtime := &KubeRuntime{
		client:  fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: hostArtifactStateConfigMapName, Namespace: "default"}, Data: map[string]string{"artifact-1": string(payload)}}),
		enabled: true,
	}
	manager := NewHostArtifactQuarantineManager(runtime, "default")
	require.NoError(t, manager.LoadFromConfigMap(context.Background()))
	if _, ok := manager.GetActive("artifact-1", now); !ok {
		t.Fatalf("expected initial host artifact quarantine state to be active")
	}
	require.NoError(t, runtime.DeleteConfigMapKey(context.Background(), "default", hostArtifactStateConfigMapName, "artifact-1"))
	require.NoError(t, manager.RefreshVolume(context.Background(), "vol-1"))
	if _, ok := manager.GetActive("artifact-1", now); ok {
		t.Fatalf("expected refresh to drop deleted host artifact state from memory")
	}
}

func TestValidateHotplugQueueRequestPausesOnlySelectedManualRecoveryVolume(t *testing.T) {
	until := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	pvManual, pvcManual := newLocalPVAndPVC("vol-manual", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationRecoveryMode:      recoveryModeManual,
		annotationRecoveryModeUntil: until,
	})
	pvNormal, pvcNormal := newLocalPVAndPVC("vol-normal", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvcManual.Spec.VolumeName = pvManual.Name
	pvcNormal.Spec.VolumeName = pvNormal.Name

	driver := newStickyTestDriver(t,
		pvManual,
		pvcManual,
		pvNormal,
		pvcNormal,
		newReadyNode("node-a", true),
		activePodForPVC("pod-manual", pvcManual.Name, "node-a"),
		activePodForPVC("pod-normal", pvcNormal.Name, "node-a"),
	)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-manual", "node-a").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  "vol-manual",
		ImageID:       11,
		RequestedNode: "node-a",
	}, nil).Once()
	provider.On("InspectVolumeLookup", mock.Anything, "vol-normal", "node-a").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  "vol-normal",
		ImageID:       12,
		RequestedNode: "node-a",
	}, nil).Once()
	provider.On("NodeExists", mock.Anything, "node-a").Return(101, nil).Twice()
	provider.On("GetVolumeInNode", mock.Anything, 11, 101).Return("", fmt.Errorf("not attached")).Once()
	provider.On("GetVolumeInNode", mock.Anything, 12, 101).Return("", fmt.Errorf("not attached")).Once()

	server := NewControllerServer(driver, provider, nil)
	manualValidation := server.validateHotplugQueueRequest(context.Background(), "node-a", "attach", "vol-manual")
	if manualValidation.Decision != HotplugQueueValidationPaused || manualValidation.Reason != queueReasonRecoveryModeManual {
		t.Fatalf("expected manual recovery volume to pause queue churn, got %#v", manualValidation)
	}
	normalValidation := server.validateHotplugQueueRequest(context.Background(), "node-a", "attach", "vol-normal")
	if normalValidation.Decision != HotplugQueueValidationValid {
		t.Fatalf("expected unrelated volume to remain valid, got %#v", normalValidation)
	}
	provider.AssertExpectations(t)
}

func TestManualRecoveryAdoptionTargetValidation(t *testing.T) {
	until := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	pv, pvc := newLocalPVAndPVC("vol-adopt", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationRecoveryMode:          recoveryModeManual,
		annotationRecoveryModeUntil:     until,
		annotationAdoptAttachedDevice:   "true",
		annotationConfirmedDeviceSerial: "onecsi-11",
		annotationConfirmedVolumeName:   "sdf",
	})
	pvc.Spec.VolumeName = pv.Name

	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-a", true), activePodForPVC("pod-adopt", pvc.Name, "node-a"))
	server := NewControllerServer(driver, &MockOpenNebulaVolumeProviderTestify{}, nil)
	runtimeCtx, err := driver.kubeRuntime.ResolveVolumeRuntimeContext(context.Background(), "vol-adopt")
	require.NoError(t, err)
	control := resolvedRecoveryControlState("vol-adopt", runtimeCtx, VolumeRecoveryControlState{})
	lookup := &opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  "vol-adopt",
		ImageID:       11,
		RequestedNode: "node-a",
		AttachmentMetadata: &opennebula.VolumeAttachmentMetadata{
			VolumeHandle:    "vol-adopt",
			ImageID:         11,
			RequestedNode:   "node-a",
			RequestedNodeID: 101,
		},
	}

	target, err := server.manualRecoveryAdoptionTarget(context.Background(), "vol-adopt", "node-a", VolumeHistoryRecord{}, runtimeCtx, lookup, control)
	require.NoError(t, err)
	if target != "sdf" {
		t.Fatalf("unexpected manual adoption target %q", target)
	}

	badSerial := control
	badSerial.ConfirmedDeviceSerial = "onecsi-12"
	if _, err := server.manualRecoveryAdoptionTarget(context.Background(), "vol-adopt", "node-a", VolumeHistoryRecord{}, runtimeCtx, lookup, badSerial); err == nil {
		t.Fatalf("expected wrong serial to fail closed")
	}

	conflictingLookup := *lookup
	conflictingLookup.AttachmentMetadata = &opennebula.VolumeAttachmentMetadata{
		VolumeHandle:    "vol-adopt",
		ImageID:         11,
		RequestedNode:   "node-a",
		RequestedNodeID: 101,
		ImageVMIDs:      []int{999},
	}
	if _, err := server.manualRecoveryAdoptionTarget(context.Background(), "vol-adopt", "node-a", VolumeHistoryRecord{}, runtimeCtx, &conflictingLookup, control); err == nil {
		t.Fatalf("expected conflicting OpenNebula owner to fail closed")
	}

	if _, err := server.manualRecoveryAdoptionTarget(context.Background(), "vol-adopt", "node-b", VolumeHistoryRecord{}, runtimeCtx, lookup, control); err == nil {
		t.Fatalf("expected mismatched desired node to fail closed")
	}
}

func TestEvaluateLocalDiskPathDetectsDuplicateDeviceMount(t *testing.T) {
	originalRoot := localDiskSessionRootPath
	localDiskSessionRootPath = t.TempDir()
	t.Cleanup(func() {
		localDiskSessionRootPath = originalRoot
	})

	stageOne := filepath.Join(t.TempDir(), "stage-one")
	stageTwo := filepath.Join(t.TempDir(), "stage-two")
	require.NoError(t, os.MkdirAll(stageOne, 0o755))
	require.NoError(t, os.MkdirAll(stageTwo, 0o755))

	ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: stageOne, Device: "/dev/null"}})
	require.NoError(t, ns.localDiskSessions.Save(localDiskSession{VolumeID: "vol-1", StagingTargetPath: stageOne, DevicePath: "/dev/null"}))
	require.NoError(t, ns.localDiskSessions.Save(localDiskSession{VolumeID: "vol-2", StagingTargetPath: stageTwo, DevicePath: "/dev/null"}))

	health, err := ns.evaluateLocalDiskPath("vol-1", stageOne)
	require.NoError(t, err)
	if !health.Stale || health.Reason != "duplicate_device_mount" {
		t.Fatalf("expected duplicate-device mount to be treated as stale, got %#v", health)
	}
}

func activePodForPVC(name, pvcName, nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			}},
			Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}
