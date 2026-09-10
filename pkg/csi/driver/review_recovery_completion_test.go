package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	mount "k8s.io/mount-utils"
)

func reviewRecoveryCompletionRPC(t *testing.T, ns *NodeServer, stage *csi.NodeStageVolumeRequest, endpoint string) func(context.Context) error {
	t.Helper()
	if endpoint == "stage" {
		return func(ctx context.Context) error {
			_, err := ns.NodeStageVolume(ctx, stage)
			return err
		}
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	request := &csi.NodePublishVolumeRequest{
		VolumeId: stage.VolumeId, StagingTargetPath: stage.StagingTargetPath,
		TargetPath: filepath.Join(root, "block"), PublishContext: stage.PublishContext,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	}
	return func(ctx context.Context) error {
		_, err := ns.NodePublishVolume(ctx, request)
		return err
	}
}

func TestReviewNodeStageProviderCompletionAfterConcurrentWrongIdentity(t *testing.T) {
	for _, endpoint := range []string{"stage", "block"} {
		for _, scenario := range []string{"identity", "deadline", "deadline-on-conflict", "marker-write-failed"} {
			t.Run(endpoint+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				driver, _, controller, provider, report := recoveryReviewFixture(t)
				ns, stage := reviewLocalStageFixture(t, driver, report, false)
				device := filepath.Join(defaultDiskPath, report.VolumeName)
				require.NoError(t, os.Remove(filepath.Join(defaultDiskPath, "disk", "by-id", "virtio-onecsi-42")))
				serial := "onecsi-99"
				exec := reviewDeviceSerialFixture(t, func(string) string { return serial })
				ns.mounter.Exec, ns.deviceResolver.exec = exec, exec
				reviewDeviceResolutionClock(t)
				stage.PublishContext[publishContextDeviceDiscoveryTimeoutSeconds] = "1"
				rpc := reviewRecoveryCompletionRPC(t, ns, stage, endpoint)
				require.NoError(t, ns.localDiskSessions.Save(localDiskSession{
					VolumeID: report.VolumeID, VolumeName: report.VolumeName, DevicePath: device,
					DeviceSerial: "onecsi-42", FSType: "ext4", StagingTargetPath: stage.StagingTargetPath,
					Identity: &LocalDiskIdentity{LegacyDeviceSerial: "onecsi-42"},
				}))
				key := localDeviceReportKey(report.Node, report.VolumeID)
				client := driver.kubeRuntime.client.(*fake.Clientset)
				if scenario == "marker-write-failed" {
					client.PrependReactor("patch", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
						if action.(ktesting.PatchAction).GetName() == volumeRepairStateConfigMapName {
							return true, nil, errors.New("repair marker API unavailable")
						}
						return false, nil, nil
					})
				}
				provider.On("NodeReady", mock.Anything, report.Node).Return(true, nil).Once()
				provider.On("VolumeExists", mock.Anything, report.VolumeID).Return(394, 1, nil).Once()
				provider.On("NodeExists", mock.Anything, report.Node).Return(208, nil).Once()
				provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("", errors.New("not attached")).Once()
				provider.On("InspectVolumeAttachment", mock.Anything, report.VolumeID, report.Node).Return(&opennebula.VolumeAttachmentMetadata{VolumeHandle: report.VolumeID, ImageID: 394, RequestedNodeID: 208}, nil).Once()
				attachEntered := make(chan LocalDeviceMissingReport, 1)
				finishAttach := make(chan struct{})
				var release sync.Once
				provider.On("AttachVolume", mock.Anything, report.VolumeID, report.Node, false, mock.Anything).Run(func(mock.Arguments) {
					current, _ := ns.currentLocalDeviceReport(ctx, report.VolumeID)
					attachEntered <- current
					select {
					case <-finishAttach:
					case <-ctx.Done():
					}
				}).Return(nil).Once()
				provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("sdd", nil).Once()
				var episode LocalDeviceMissingReport
				var recoveryErr error
				started := false
				controllerDone := make(chan struct{})
				defer func() {
					release.Do(func() { close(finishAttach) })
					if started {
						<-controllerDone
					}
				}()
				stat := nodeVolumePathStat
				nodeVolumePathStat = func(path string) (os.FileInfo, error) {
					if path == device && !started {
						started = true
						go func() {
							recoveryErr = controller.recoverLocalDeviceReport(ctx, key, report)
							close(controllerDone)
						}()
						select {
						case episode = <-attachEntered:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return stat(path)
				}
				mounter := ns.mounter.Interface.(*mount.FakeMounter)
				before := mounter.GetLog()
				_, err := ns.NodeStageVolume(ctx, stage)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.True(t, started)
				require.NotEmpty(t, episode.RecoveryToken)
				fault, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
				require.True(t, exists)
				require.Equal(t, episode.RecoveryToken, fault.RecoveryToken)
				require.Equal(t, localDeviceFailureClassWrongIdentity, fault.FailureClass)
				require.Empty(t, fault.RecoveryMethod)
				require.Equal(t, before, mounter.GetLog())
				if scenario == "deadline" {
					var changed bool
					fault, changed, err = controller.refreshLocalDeviceReportConfirmationState(ctx, key, fault, fault.ConfirmationDeadline.Add(time.Second))
					require.NoError(t, err)
					require.True(t, changed)
				}
				marker, markerErr := client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(ctx, volumeRepairStateConfigMapName, metav1.GetOptions{})
				if scenario == "marker-write-failed" {
					require.True(t, apierrors.IsNotFound(markerErr))
				} else {
					require.NoError(t, markerErr)
					require.NotEmpty(t, marker.Data[report.VolumeID])
				}
				writes := 0
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					if cm.Name != localDeviceStateConfigMapName {
						return false, nil, nil
					}
					var proposed LocalDeviceMissingReport
					if json.Unmarshal([]byte(cm.Data[key]), &proposed) != nil || proposed.RecoveryMethod == "" {
						return false, nil, nil
					}
					writes++
					if scenario != "deadline-on-conflict" || writes != 1 {
						return false, nil, nil
					}
					fault.ConfirmationState = localDeviceConfirmationStateRepairRequired
					fault.ConfirmationDeadline = nil
					fault.LastRecoveryOutcome = localDeviceConfirmationStateRepairRequired
					fault.LastRecoveryError = "deadline elapsed during provider completion commit"
					observedAt := time.Now().UTC()
					fault.ConfirmationObservedAt = &observedAt
					payload, err := json.Marshal(fault)
					if err != nil {
						return true, nil, err
					}
					cm.Data[key] = string(payload)
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
						return true, nil, err
					}
					return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("deadline observer advanced state"))
				})
				release.Do(func() { close(finishAttach) })
				<-controllerDone
				require.NoError(t, recoveryErr)
				if scenario == "deadline-on-conflict" {
					require.Equal(t, 2, writes)
				} else {
					require.Equal(t, 1, writes)
				}
				completed, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
				require.True(t, exists)
				expected := fault
				if expected.ConfirmationState == localDeviceConfirmationStateInProgress {
					expected.ConfirmationState = localDeviceConfirmationStatePending
					expected.LastRecoveryOutcome = localDeviceConfirmationStatePending
					expected.ConfirmationDeadline = completed.ConfirmationDeadline
					require.NotNil(t, expected.ConfirmationDeadline)
				}
				expected.RecoveryMethod = localDeviceRecoveryMethodRuntimeRepublish
				expected.MetadataAttachedToNode = true
				expected.MetadataNode = report.Node
				expected.MetadataTarget = "sdd"
				require.Equal(t, expected, completed)
				if markerErr == nil {
					retained, err := client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(ctx, volumeRepairStateConfigMapName, metav1.GetOptions{})
					require.NoError(t, err)
					require.Equal(t, marker.Data, retained.Data)
				}
				require.Error(t, rpc(ctx))
				retained, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
				require.True(t, exists)
				require.Equal(t, completed.RecoveryToken, retained.RecoveryToken)
				require.Equal(t, completed.RecoveryMethod, retained.RecoveryMethod)
				require.Equal(t, completed.ExpectedIdentity, retained.ExpectedIdentity)
				require.Equal(t, localDeviceFailureClassWrongIdentity, retained.FailureClass)
				require.Equal(t, before, mounter.GetLog())
				serial = "onecsi-42"
				require.NoError(t, rpc(ctx))
				_, exists = ns.currentLocalDeviceReport(ctx, report.VolumeID)
				require.False(t, exists)
				_, exists, err = driver.volumeRepairState.GetCurrent(ctx, report.VolumeID)
				require.NoError(t, err)
				require.False(t, exists)
				require.NotEqual(t, before, mounter.GetLog())
				require.NoError(t, rpc(ctx))
				provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
				provider.AssertExpectations(t)
			})
		}
	}
}

func TestReviewNodeStageProviderCompletionWriteFailure(t *testing.T) {
	for _, endpoint := range []string{"stage", "block"} {
		for _, failure := range []string{"token-replaced", "token-replaced-on-conflict", "conflicts-exhausted", "api-unavailable"} {
			t.Run(endpoint+"/"+failure, func(t *testing.T) {
				ctx := context.Background()
				driver, _, controller, provider, report := recoveryReviewFixture(t)
				ns, stage := reviewLocalStageFixture(t, driver, report, false)
				exec := reviewDeviceSerialFixture(t, func(string) string { return "onecsi-42" })
				ns.mounter.Exec, ns.deviceResolver.exec = exec, exec
				rpc := reviewRecoveryCompletionRPC(t, ns, stage, endpoint)
				key := localDeviceReportKey(report.Node, report.VolumeID)
				provider.On("NodeReady", mock.Anything, report.Node).Return(true, nil).Once()
				provider.On("VolumeExists", mock.Anything, report.VolumeID).Return(394, 1, nil).Once()
				provider.On("NodeExists", mock.Anything, report.Node).Return(208, nil).Once()
				provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("", errors.New("not attached")).Once()
				provider.On("InspectVolumeAttachment", mock.Anything, report.VolumeID, report.Node).Return(&opennebula.VolumeAttachmentMetadata{VolumeHandle: report.VolumeID, ImageID: 394, RequestedNodeID: 208}, nil).Once()
				var unfinished LocalDeviceMissingReport
				provider.On("AttachVolume", mock.Anything, report.VolumeID, report.Node, false, mock.Anything).Run(func(mock.Arguments) {
					var exists bool
					unfinished, exists = ns.currentLocalDeviceReport(ctx, report.VolumeID)
					require.True(t, exists)
					if failure == "token-replaced" {
						unfinished.RecoveryToken = "replacement-T2"
						require.NoError(t, updateLocalDeviceReport(ctx, driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = unfinished }))
					}
				}).Return(nil).Once()
				provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("sdd", nil).Once()
				client := driver.kubeRuntime.client.(*fake.Clientset)
				writes := 0
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					if cm.Name != localDeviceStateConfigMapName {
						return false, nil, nil
					}
					var proposed LocalDeviceMissingReport
					require.NoError(t, json.Unmarshal([]byte(cm.Data[key]), &proposed))
					if proposed.RecoveryMethod == "" {
						return false, nil, nil
					}
					writes++
					if failure == "api-unavailable" {
						return true, nil, errors.New("completion API unavailable")
					}
					if failure == "token-replaced-on-conflict" {
						unfinished.RecoveryToken = "replacement-T2"
						payload, err := json.Marshal(unfinished)
						require.NoError(t, err)
						cm.Data[key] = string(payload)
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					}
					return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("completion conflict"))
				})
				err := controller.recoverLocalDeviceReport(ctx, key, report)
				switch failure {
				case "token-replaced":
					require.NoError(t, err)
					require.Zero(t, writes)
				case "token-replaced-on-conflict":
					require.NoError(t, err)
					require.Equal(t, 1, writes)
				case "conflicts-exhausted":
					require.True(t, apierrors.IsConflict(err))
					require.Greater(t, writes, 1)
				case "api-unavailable":
					require.EqualError(t, err, "completion API unavailable")
					require.Equal(t, 1, writes)
				}
				retained, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
				require.True(t, exists)
				require.Equal(t, unfinished, retained)
				mounter := ns.mounter.Interface.(*mount.FakeMounter)
				before := mounter.GetLog()
				require.Equal(t, codes.Unavailable, status.Code(rpc(ctx)))
				require.Equal(t, before, mounter.GetLog())
				retained, exists = ns.currentLocalDeviceReport(ctx, report.VolumeID)
				require.True(t, exists)
				require.Equal(t, unfinished, retained)
				provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
				provider.AssertExpectations(t)
			})
		}
	}
}

func TestLocalDeviceRecoveryCompletionOwnership(t *testing.T) {
	for _, changed := range []string{"node", "volume", "episode", "attempt", "recovery-time", "tokenless"} {
		t.Run(changed, func(t *testing.T) {
			ctx := context.Background()
			driver, _, controller, _, report := recoveryReviewFixture(t)
			key := localDeviceReportKey(report.Node, report.VolumeID)
			episode, started, err := controller.beginLocalDeviceRecovery(ctx, key, report)
			require.NoError(t, err)
			require.True(t, started)
			replacement := episode
			switch changed {
			case "node":
				replacement.Node = "node-b"
			case "volume":
				replacement.VolumeID = "volume-b"
			case "episode":
				replacement.FirstObservedAt = replacement.FirstObservedAt.Add(time.Second)
			case "attempt":
				replacement.RecoveryAttempts++
			case "recovery-time":
				later := replacement.LastRecoveryAt.Add(time.Second)
				replacement.LastRecoveryAt = &later
			case "tokenless":
				episode.RecoveryToken = ""
				replacement.RecoveryToken = ""
			}
			require.NoError(t, updateLocalDeviceReport(ctx, driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = replacement }))
			updated, err := controller.markLocalDeviceRecoveryPendingConfirmation(ctx, key, episode, &localDeviceRecoveryAttempt{Method: localDeviceRecoveryMethodRuntimeRepublish, MetadataNode: report.Node, MetadataAttached: true, MetadataTarget: "sdd"}, time.Now().Add(time.Minute))
			require.NoError(t, err)
			require.False(t, updated)
			cm, err := driver.kubeRuntime.GetConfigMap(ctx, namespaceFromServiceAccount(), localDeviceStateConfigMapName)
			require.NoError(t, err)
			var retained LocalDeviceMissingReport
			require.NoError(t, json.Unmarshal([]byte(cm.Data[key]), &retained))
			require.Equal(t, replacement, retained)
		})
	}
}
