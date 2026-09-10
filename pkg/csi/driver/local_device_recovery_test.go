package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func newLocalDeviceRecoveryTestDriver(t *testing.T, objects ...runtime.Object) *Driver {
	t.Helper()
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.LocalDeviceRecoveryEnabledVar, true)
	cfg.OverrideVal(config.LocalDeviceRecoveryMinAttemptsVar, 3)
	cfg.OverrideVal(config.LocalDeviceRecoveryMinAgeSecondsVar, 0)
	cfg.OverrideVal(config.LocalDeviceRecoveryCooldownSecondsVar, 0)
	cfg.OverrideVal(config.LocalDeviceRecoveryMaxAttemptsVar, 2)
	cfg.OverrideVal(config.HotplugQueueEnabledVar, false)
	runtime := &KubeRuntime{client: fake.NewSimpleClientset(objects...), enabled: true}
	driver := &Driver{
		name:           DefaultDriverName,
		PluginConfig:   cfg,
		kubeRuntime:    runtime,
		metrics:        NewDriverMetrics("test", "test"),
		operationLocks: NewOperationLocks(),
		hotplugGuard:   NewHotplugGuard(5 * time.Minute),
	}
	driver.volumeHistory = NewVolumeHistoryManager(runtime, namespaceFromServiceAccount())
	driver.volumeRepairState = NewVolumeRepairStateManager(runtime, namespaceFromServiceAccount())
	driver.volumeRecoveryControl = NewVolumeRecoveryControlManager(runtime, namespaceFromServiceAccount())
	driver.hostArtifactQuarantine = NewHostArtifactQuarantineManager(runtime, namespaceFromServiceAccount())
	return driver
}

func TestNodeRecordsAndClearsLocalDeviceMissingReport(t *testing.T) {
	driver := newLocalDeviceRecoveryTestDriver(t)
	driver.nodeID = "node-a"
	ns := &NodeServer{Driver: driver}
	publishContext := map[string]string{
		annotationBackend:                   "local",
		paramPVCNamespace:                   "default",
		paramPVCName:                        "pvc-vol-1",
		paramPVName:                         "pv-vol-1",
		publishContextDeviceSerial:          "onecsi-439",
		publishContextOpenNebulaImageID:     "439",
		publishContextHotplugTimeoutSeconds: "120",
	}

	ns.recordLocalDeviceMissing(context.Background(), "vol-1", "/dev/sdd", "/stage/vol-1", publishContext, errors.New("device not found"))
	ns.recordLocalDeviceMissing(context.Background(), "vol-1", "/dev/sdd", "/stage/vol-1", publishContext, errors.New("device not found"))

	cm, err := driver.kubeRuntime.client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(context.Background(), localDeviceStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	key := localDeviceReportKey("node-a", "vol-1")
	raw := cm.Data[key]
	require.NotEmpty(t, raw)
	var report LocalDeviceMissingReport
	require.NoError(t, json.Unmarshal([]byte(raw), &report))
	assert.Equal(t, "node-a", report.Node)
	assert.Equal(t, "vol-1", report.VolumeID)
	assert.Equal(t, localDeviceFailureClassRuntimeAttachmentMissing, report.FailureClass)
	assert.Equal(t, "/dev/sdd", report.ExpectedTarget)
	assert.Equal(t, "onecsi-439", report.DeviceSerial)
	assert.Equal(t, 2, report.Attempts)

	require.NoError(t, ns.confirmLocalDeviceRecovery(context.Background(), &report, "vol-1", "/dev/sdd", nil, publishContext))
	cm, err = driver.kubeRuntime.client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(context.Background(), localDeviceStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, cm.Data, key)
}

func TestNodeRecordsLocalDeviceMountFailureReport(t *testing.T) {
	driver := newLocalDeviceRecoveryTestDriver(t)
	driver.nodeID = "node-a"
	ns := &NodeServer{Driver: driver}
	publishContext := map[string]string{
		annotationBackend:                   "local",
		paramPVCNamespace:                   "default",
		paramPVCName:                        "pvc-vol-1",
		paramPVName:                         "pv-vol-1",
		publishContextDeviceSerial:          "onecsi-439",
		publishContextOpenNebulaImageID:     "439",
		publishContextHotplugTimeoutSeconds: "120",
	}

	ns.recordLocalDeviceMountFailure(
		context.Background(),
		"vol-1",
		"sdd",
		"/dev/sdd",
		"/stage/vol-1",
		"xfs",
		deviceResolutionResult{ResolvedBy: "cache", Latency: time.Millisecond},
		publishContext,
		errors.New("can't read superblock"),
	)

	cm, err := driver.kubeRuntime.client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(context.Background(), localDeviceStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	key := localDeviceReportKey("node-a", "vol-1")
	raw := cm.Data[key]
	require.NotEmpty(t, raw)
	var report LocalDeviceMissingReport
	require.NoError(t, json.Unmarshal([]byte(raw), &report))
	assert.Equal(t, "node-a", report.Node)
	assert.Equal(t, "vol-1", report.VolumeID)
	assert.Equal(t, localDeviceFailureClassMountFailed, report.FailureClass)
	assert.Equal(t, "/dev/sdd", report.DevicePath)
	assert.Equal(t, "onecsi-439", report.DeviceSerial)
	assert.Equal(t, "439", report.OpenNebulaImageID)
	assert.Equal(t, "xfs", report.FsType)
	assert.Equal(t, "cache", report.ResolvedBy)
	assert.Equal(t, "can't read superblock", report.LastError)
	assert.Equal(t, 1, report.Attempts)
}

func TestLocalDeviceRecoveryRequiresProvenAttachmentAbsence(t *testing.T) {
	for _, outcome := range []string{"metadata-attached", "unknown", "inspection-error", "other-owner", "absent", "confirmed-before-begin"} {
		t.Run(outcome, func(t *testing.T) {
			_, ns, server, provider, report := recoveryReviewFixture(t)
			ctx := context.Background()
			key := localDeviceReportKey(report.Node, report.VolumeID)
			provider.On("NodeReady", mock.Anything, report.Node).Return(true, nil).Once()
			provider.On("VolumeExists", mock.Anything, report.VolumeID).Return(394, 1, nil).Once()
			provider.On("NodeExists", mock.Anything, report.Node).Return(208, nil).Once()
			if outcome == "metadata-attached" {
				provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("sdd", nil).Once()
			} else {
				provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("", errors.New("lookup failed")).Once()
				var metadata *opennebula.VolumeAttachmentMetadata
				var inspectErr error
				if outcome == "inspection-error" {
					inspectErr = errors.New("provider unavailable")
				} else if outcome != "unknown" {
					metadata = &opennebula.VolumeAttachmentMetadata{VolumeHandle: report.VolumeID, ImageID: 394, RequestedNodeID: 208}
					if outcome == "other-owner" {
						metadata.ImageVMIDs = []int{209}
					}
				}
				provider.On("InspectVolumeAttachment", mock.Anything, report.VolumeID, report.Node).Run(func(mock.Arguments) {
					if outcome == "confirmed-before-begin" {
						ns.confirmLocalDeviceRecovery(ctx, &report, report.VolumeID, "/dev/sdd", nil, nil)
					}
				}).Return(metadata, inspectErr).Once()
			}
			if outcome == "absent" {
				provider.On("AttachVolume", mock.Anything, report.VolumeID, report.Node, false, mock.Anything).Run(func(mock.Arguments) {
					inProgress, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
					require.True(t, exists)
					require.Equal(t, localDeviceConfirmationStateInProgress, inProgress.ConfirmationState)
					ns.confirmLocalDeviceRecovery(ctx, &report, report.VolumeID, "/dev/sdd", nil, nil)
					ns.confirmLocalDeviceRecovery(ctx, &inProgress, report.VolumeID, "/dev/sdd", nil, nil)
					current, _ := ns.currentLocalDeviceReport(ctx, report.VolumeID)
					require.Equal(t, inProgress, current)
				}).Return(nil).Once()
				provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("sdd", nil).Once()
			}
			require.NoError(t, server.recoverLocalDeviceReport(ctx, key, report))
			current, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
			if outcome == "confirmed-before-begin" {
				require.False(t, exists)
			} else if outcome == "metadata-attached" {
				require.Equal(t, localDeviceConfirmationStateRepairRequired, current.ConfirmationState)
				require.Contains(t, current.LastRecoveryError, "drain consumers")
				require.Error(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
				ns.confirmLocalDeviceRecovery(ctx, &current, report.VolumeID, "/dev/sdd", nil, nil)
				require.NoError(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
			} else if outcome == "absent" {
				require.Equal(t, localDeviceConfirmationStatePending, current.ConfirmationState)
				require.Equal(t, localDeviceRecoveryMethodRuntimeRepublish, current.RecoveryMethod)
				require.NotEmpty(t, current.RecoveryToken)
				ns.confirmLocalDeviceRecovery(ctx, &current, report.VolumeID, "/dev/sdd", nil, nil)
				_, exists = ns.currentLocalDeviceReport(ctx, report.VolumeID)
				require.False(t, exists)
			} else {
				require.Equal(t, "failed", current.LastRecoveryOutcome)
			}
			provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
			provider.AssertExpectations(t)
		})
	}
}

func TestLocalDeviceRecoveryPersistsHostArtifactQuarantineOnReattachConflict(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-device-artifact", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	pvName := pv.Name
	report := LocalDeviceMissingReport{
		Node:              "node-a",
		VolumeID:          "vol-device-artifact",
		VolumeName:        "sdb",
		ExpectedTarget:    "sdb",
		PVCNamespace:      "default",
		PVCName:           pvc.Name,
		PVName:            pv.Name,
		FirstObservedAt:   time.Now().Add(-2 * time.Minute),
		LastObservedAt:    time.Now().Add(-time.Minute),
		Attempts:          3,
		DeviceSerial:      "onecsi-575",
		OpenNebulaImageID: "575",
	}
	key := localDeviceReportKey(report.Node, report.VolumeID)
	payload, err := json.Marshal(report)
	require.NoError(t, err)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: localDeviceStateConfigMapName, Namespace: namespaceFromServiceAccount()},
		Data:       map[string]string{key: string(payload)},
	}
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "va-device-artifact",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-30 * time.Second)),
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	driver := newLocalDeviceRecoveryTestDriver(t, pv, pvc, newReadyNode("node-a", true), va, cm)
	conflict, ok := opennebula.HostArtifactConflictFromMessage(
		`ATTACHDISK: transfer manager failed; see more details in VM log`,
		opennebula.HostArtifactAttachmentTarget{VolumeHandle: report.VolumeID, ImageID: 575, NodeName: "node-a", VMID: 160, DiskID: 2, Target: "sdb"},
		errors.New("attach failed"),
	)
	require.True(t, ok)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("NodeReady", mock.Anything, "node-a").Return(true, nil).Once()
	mockProvider.On("VolumeExists", mock.Anything, "vol-device-artifact").Return(575, 1, nil).Once()
	mockProvider.On("NodeExists", mock.Anything, "node-a").Return(160, nil).Once()
	mockProvider.On("GetVolumeInNode", mock.Anything, 575, 160).Return("", errors.New("not attached")).Once()
	mockProvider.On("InspectVolumeAttachment", mock.Anything, report.VolumeID, "node-a").Return(&opennebula.VolumeAttachmentMetadata{VolumeHandle: report.VolumeID, ImageID: 575, RequestedNodeID: 160}, nil).Once()
	mockProvider.On("AttachVolume", mock.Anything, "vol-device-artifact", "node-a", false, mock.Anything).Return(conflict).Once()
	server := NewControllerServer(driver, mockProvider, nil)

	require.NoError(t, server.recoverLocalDeviceReport(context.Background(), key, report))
	artifactCM, err := driver.kubeRuntime.client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(context.Background(), hostArtifactStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Contains(t, artifactCM.Data, "vm-160")
	assert.NotContains(t, artifactCM.Data, "vm-160.disk-2")
	updated, err := driver.kubeRuntime.client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(context.Background(), localDeviceStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(updated.Data[key]), &report))
	assert.Equal(t, "failed", report.LastRecoveryOutcome)
	assert.Contains(t, report.LastRecoveryError, "host artifact")
	mockProvider.AssertExpectations(t)
}

func TestLocalDeviceRecoveryClearsIneligibleReportWithoutOpenNebulaMutation(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-rwx", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, map[string]string{
		annotationBackend: "cephfs",
	})
	pvc.Spec.VolumeName = pv.Name
	report := LocalDeviceMissingReport{
		Node:            "node-a",
		VolumeID:        "vol-rwx",
		PVCNamespace:    "default",
		PVCName:         pvc.Name,
		PVName:          pv.Name,
		FirstObservedAt: time.Now().Add(-2 * time.Minute),
		LastObservedAt:  time.Now().Add(-time.Minute),
		Attempts:        3,
	}
	key := localDeviceReportKey(report.Node, report.VolumeID)
	payload, err := json.Marshal(report)
	require.NoError(t, err)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: localDeviceStateConfigMapName, Namespace: namespaceFromServiceAccount()},
		Data:       map[string]string{key: string(payload)},
	}
	driver := newLocalDeviceRecoveryTestDriver(t, pv, pvc, cm)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	server := NewControllerServer(driver, mockProvider, nil)

	require.NoError(t, server.recoverLocalDeviceReport(context.Background(), key, report))
	updated, err := driver.kubeRuntime.client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(context.Background(), localDeviceStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, updated.Data, key)
	mockProvider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	mockProvider.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestLocalDeviceReportReadyStopsSameFailureAfterMaxAttempts(t *testing.T) {
	driver := newLocalDeviceRecoveryTestDriver(t)
	server := NewControllerServer(driver, &MockOpenNebulaVolumeProviderTestify{}, nil)
	now := time.Now().UTC()
	recoveredAt := now.Add(-10 * time.Minute)
	report := LocalDeviceMissingReport{
		Node:                  "node-a",
		VolumeID:              "vol-1",
		VolumeName:            "sdg",
		DeviceSerial:          "onecsi-439",
		FirstObservedAt:       now.Add(-30 * time.Minute),
		LastObservedAt:        now.Add(-time.Minute),
		Attempts:              3,
		RecoveryAttempts:      2,
		LastRecoveryAt:        &recoveredAt,
		LastRecoverySignature: localDeviceRecoverySignature(LocalDeviceMissingReport{VolumeName: "sdg", DeviceSerial: "onecsi-439"}),
	}

	ready, reason := server.localDeviceReportReady(report, now)
	assert.False(t, ready)
	assert.Equal(t, "max_recovery_attempts_same_failure", reason)

	report.LastObservedAt = recoveredAt.Add(-time.Minute)
	ready, reason = server.localDeviceReportReady(report, now)
	assert.False(t, ready)
	assert.Equal(t, "max_recovery_attempts", reason)
}

func TestLocalDeviceReportReadyRetriesAfterFailureSignatureChanges(t *testing.T) {
	driver := newLocalDeviceRecoveryTestDriver(t)
	server := NewControllerServer(driver, &MockOpenNebulaVolumeProviderTestify{}, nil)
	now := time.Now().UTC()
	recoveredAt := now.Add(-10 * time.Minute)
	report := LocalDeviceMissingReport{
		Node:                  "node-a",
		VolumeID:              "vol-1",
		VolumeName:            "sdg",
		DeviceSerial:          "onecsi-439",
		FirstObservedAt:       now.Add(-30 * time.Minute),
		LastObservedAt:        now.Add(-time.Minute),
		Attempts:              3,
		RecoveryAttempts:      2,
		LastRecoveryAt:        &recoveredAt,
		LastRecoverySignature: localDeviceRecoverySignature(LocalDeviceMissingReport{VolumeName: "sdf", DeviceSerial: "onecsi-439"}),
	}

	ready, reason := server.localDeviceReportReady(report, now)
	assert.True(t, ready)
	assert.Empty(t, reason)
}

func TestLocalDeviceRecoveryTimeoutPreservesConcurrentConfirmation(t *testing.T) {
	for _, outcome := range []string{"confirmed", "removed", "new-episode", "timeout"} {
		for _, attempts := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/%d", outcome, attempts), func(t *testing.T) {
				ctx := context.Background()
				now := time.Now().UTC()
				deadline := now.Add(-time.Minute)
				report := LocalDeviceMissingReport{Node: "node-a", VolumeID: "vol-1", FirstObservedAt: now.Add(-time.Hour), LastObservedAt: now.Add(-time.Minute), Attempts: 3, RecoveryAttempts: attempts, RecoveryToken: "episode-1", ConfirmationState: localDeviceConfirmationStatePending, ConfirmationDeadline: &deadline}
				key := localDeviceReportKey(report.Node, report.VolumeID)
				driver := newLocalDeviceRecoveryTestDriver(t)
				require.NoError(t, updateLocalDeviceReport(ctx, driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = report }))
				client := driver.kubeRuntime.client.(*fake.Clientset)
				conflicted := false
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					if conflicted || cm.Name != localDeviceStateConfigMapName || outcome == "timeout" {
						return false, nil, nil
					}
					conflicted = true
					current := report
					switch outcome {
					case "confirmed":
						current.ConfirmationState = localDeviceConfirmationStateConfirmed
						current.RecoveryToken = ""
						current.ConfirmationDeadline = nil
					case "new-episode":
						current.RecoveryToken = "episode-2"
						future := now.Add(time.Hour)
						current.ConfirmationDeadline = &future
					}
					resource := corev1.SchemeGroupVersion.WithResource("configmaps")
					stored, err := client.Tracker().Get(resource, namespaceFromServiceAccount(), localDeviceStateConfigMapName)
					require.NoError(t, err)
					latest := stored.(*corev1.ConfigMap)
					if outcome == "removed" {
						delete(latest.Data, key)
					} else {
						payload, err := json.Marshal(current)
						require.NoError(t, err)
						latest.Data[key] = string(payload)
					}
					require.NoError(t, client.Tracker().Update(resource, latest, namespaceFromServiceAccount()))
					return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("concurrent node update"))
				})
				server := NewControllerServer(driver, &MockOpenNebulaVolumeProviderTestify{}, nil)
				_, changed, err := server.refreshLocalDeviceReportConfirmationState(ctx, key, report, now)
				require.NoError(t, err)
				require.True(t, changed)
				cm, err := driver.kubeRuntime.GetConfigMap(ctx, namespaceFromServiceAccount(), localDeviceStateConfigMapName)
				require.NoError(t, err)
				if outcome == "removed" {
					require.NotContains(t, cm.Data, key)
				} else {
					var current LocalDeviceMissingReport
					require.NoError(t, json.Unmarshal([]byte(cm.Data[key]), &current))
					switch outcome {
					case "confirmed":
						require.Equal(t, localDeviceConfirmationStateConfirmed, current.ConfirmationState)
					case "new-episode":
						require.Equal(t, "episode-2", current.RecoveryToken)
						require.Equal(t, localDeviceConfirmationStatePending, current.ConfirmationState)
					case "timeout":
						want := localDeviceConfirmationStateTimedOut
						if attempts == 2 {
							want = localDeviceConfirmationStateRepairRequired
						}
						require.Equal(t, want, current.ConfirmationState)
					}
				}
				_, repairExists, repairErr := driver.volumeRepairState.GetCurrent(ctx, report.VolumeID)
				require.NoError(t, repairErr)
				require.Equal(t, outcome == "timeout" && attempts == 2, repairExists)
			})
		}
	}
}

func TestNodeRecordsNewMissingDeviceEpisodeAfterConfirmation(t *testing.T) {
	ctx := context.Background()
	driver := newLocalDeviceRecoveryTestDriver(t)
	driver.nodeID = "node-a"
	ns := &NodeServer{Driver: driver}
	publishContext := map[string]string{annotationBackend: "local", paramPVCNamespace: "default", paramPVCName: "pvc-vol-1", paramPVName: "pv-vol-1"}
	ns.recordLocalDeviceMissing(ctx, "vol-1", "sdd", "/stage/vol-1", publishContext, errors.New("device missing"))
	report, exists := ns.currentLocalDeviceReport(ctx, "vol-1")
	require.True(t, exists)
	client := driver.kubeRuntime.client.(*fake.Clientset)
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if cm.Data[localDeviceReportKey(driver.nodeID, "vol-1")] == "" {
			return true, nil, errors.New("report deletion unavailable")
		}
		return false, nil, nil
	})
	ns.confirmLocalDeviceRecovery(ctx, &report, "vol-1", "/dev/sdd", nil, publishContext)
	confirmed, exists := ns.currentLocalDeviceReport(ctx, "vol-1")
	require.True(t, exists)
	require.Equal(t, localDeviceConfirmationStateConfirmed, confirmed.ConfirmationState)
	ns.recordLocalDeviceMissing(ctx, "vol-1", "sdd", "/stage/vol-1", publishContext, errors.New("device disappeared again"))
	current, exists := ns.currentLocalDeviceReport(ctx, "vol-1")
	require.True(t, exists)
	require.Empty(t, current.ConfirmationState)
	require.Equal(t, localDeviceAttachmentStateRuntimeUnconfirmed, current.AttachmentState)
	require.True(t, current.FirstObservedAt.After(report.FirstObservedAt))
	require.Equal(t, 1, current.Attempts)
}
