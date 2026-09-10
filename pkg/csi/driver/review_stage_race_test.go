package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
	testingexec "k8s.io/utils/exec/testing"
)

func reviewLocalStageFixture(t *testing.T, driver *Driver, report LocalDeviceMissingReport, mounted bool) (*NodeServer, *csi.NodeStageVolumeRequest) {
	t.Helper()
	disk := withTestDiskPath(t)
	device := filepath.Join(disk, report.VolumeName)
	require.NoError(t, os.WriteFile(device, []byte("fixture"), 0600))
	byID := filepath.Join(disk, "disk", "by-id", "virtio-onecsi-42")
	require.NoError(t, os.MkdirAll(filepath.Dir(byID), 0750))
	require.NoError(t, os.Symlink(device, byID))
	stage := t.TempDir()
	var points []mount.MountPoint
	if mounted {
		points = []mount.MountPoint{{Path: stage, Device: device, Type: "ext4"}}
	}
	ns := getTestNodeServerWithMountPoints(points)
	ns.Driver = driver
	ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
	fakeExec := ns.mounter.Exec.(*testingexec.FakeExec)
	commands := append([]testingexec.FakeCommandAction(nil), fakeExec.CommandScript...)
	for i := 0; i < 5; i++ {
		fakeExec.CommandScript = append(fakeExec.CommandScript, commands...)
	}
	return ns, &csi.NodeStageVolumeRequest{VolumeId: report.VolumeID, StagingTargetPath: stage, PublishContext: map[string]string{"volumeName": report.VolumeName}, VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}
}

func TestReviewNodeStagePreservesFailedProviderRecoveryAuthority(t *testing.T) {
	for _, failure := range []string{"attach-timeout", "attach-unknown", "confirmation-lookup"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			driver, _, server, provider, report := recoveryReviewFixture(t)
			ns, request := reviewLocalStageFixture(t, driver, report, false)
			provider.On("NodeReady", mock.Anything, report.Node).Return(true, nil).Once()
			provider.On("VolumeExists", mock.Anything, report.VolumeID).Return(394, 1, nil).Once()
			provider.On("NodeExists", mock.Anything, report.Node).Return(208, nil).Once()
			provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("", errors.New("not attached")).Once()
			provider.On("InspectVolumeAttachment", mock.Anything, report.VolumeID, report.Node).Return(&opennebula.VolumeAttachmentMetadata{VolumeHandle: report.VolumeID, ImageID: 394, RequestedNodeID: 208}, nil).Once()
			var attachErr error
			switch failure {
			case "attach-timeout":
				attachErr = context.DeadlineExceeded
			case "attach-unknown":
				attachErr = errors.New("provider connection lost")
			case "confirmation-lookup":
				provider.On("GetVolumeInNode", mock.Anything, 394, 208).Return("", errors.New("provider lookup unavailable")).Once()
			}
			var started LocalDeviceMissingReport
			provider.On("AttachVolume", mock.Anything, report.VolumeID, report.Node, false, mock.Anything).Run(func(mock.Arguments) {
				var exists bool
				started, exists = ns.currentLocalDeviceReport(ctx, report.VolumeID)
				require.True(t, exists)
				require.NotEmpty(t, started.RecoveryToken)
			}).Return(attachErr).Once()
			key := localDeviceReportKey(report.Node, report.VolumeID)
			require.NoError(t, server.recoverLocalDeviceReport(ctx, key, report))
			current, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
			require.True(t, exists)
			require.Equal(t, started.RecoveryToken, current.RecoveryToken)
			require.Equal(t, started.ConfirmationDeadline, current.ConfirmationDeadline)
			require.Equal(t, localDeviceConfirmationStateInProgress, current.ConfirmationState)
			require.Empty(t, current.RecoveryMethod)
			_, err := ns.NodeStageVolume(ctx, request)
			require.Equal(t, codes.Unavailable, status.Code(err))
			terminal, updated, err := server.refreshLocalDeviceReportConfirmationState(ctx, key, current, current.ConfirmationDeadline.Add(time.Second))
			require.NoError(t, err)
			require.True(t, updated)
			require.Equal(t, localDeviceConfirmationStateRepairRequired, terminal.ConfirmationState)
			_, err = ns.NodeStageVolume(ctx, request)
			require.Equal(t, codes.Unavailable, status.Code(err))
			current, exists = ns.currentLocalDeviceReport(ctx, report.VolumeID)
			require.True(t, exists)
			require.Equal(t, terminal, current)
			ready, _ := server.localDeviceReportReady(current, time.Now().Add(time.Hour))
			require.False(t, ready)
			require.Error(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
			provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
			provider.AssertExpectations(t)
		})
	}
}

func TestReviewNodeStageRetiresReleasedLegacyRuntimeMarker(t *testing.T) {
	const legacy = `{"version":1,"volumeID":"vol-device","classification":"same_node_runtime_attach_unconfirmed","reason":"node_device_missing","message":"runtime attachment unconfirmed","requestedNode":"node-a","lastKnownNodeName":"node-a","evidenceSource":"local_device_report","failureCount":2,"firstObservedAt":"2026-09-09T10:00:00Z","lastObservedAt":"2026-09-09T10:05:00Z"}`
	for _, scenario := range []string{"legacy-only", "legacy-and-report", "replacement-on-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			driver, _, server, _, report := recoveryReviewFixture(t)
			ns, request := reviewLocalStageFixture(t, driver, report, true)
			require.NoError(t, driver.kubeRuntime.UpsertConfigMapData(ctx, namespaceFromServiceAccount(), volumeRepairStateConfigMapName, map[string]string{report.VolumeID: legacy}))
			if scenario == "legacy-and-report" {
				report.ConfirmationState = localDeviceConfirmationStateRepairRequired
				report.RecoveryToken = "completed-episode"
				report.RecoveryMethod = localDeviceRecoveryMethodRuntimeRepublish
				require.NoError(t, updateLocalDeviceReport(ctx, driver.kubeRuntime, namespaceFromServiceAccount(), localDeviceReportKey(report.Node, report.VolumeID), func(current *LocalDeviceMissingReport) { *current = report }))
			}
			client := driver.kubeRuntime.client.(*fake.Clientset)
			injected := false
			var replacement string
			if scenario == "replacement-on-conflict" {
				var newer VolumeRepairState
				require.NoError(t, json.Unmarshal([]byte(legacy), &newer))
				newer.Message = "new repair evidence with unchanged timestamps"
				payload, err := json.Marshal(newer)
				require.NoError(t, err)
				replacement = string(payload)
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					if injected || cm.Name != volumeRepairStateConfigMapName || cm.Data[report.VolumeID] != "" {
						return false, nil, nil
					}
					injected = true
					cm.Data[report.VolumeID] = replacement
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("new legacy repair evidence"))
				})
			}
			require.Error(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
			_, err := ns.NodeStageVolume(ctx, request)
			if scenario != "legacy-only" {
				require.Equal(t, codes.Unavailable, status.Code(err))
				cm, readErr := client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(ctx, volumeRepairStateConfigMapName, metav1.GetOptions{})
				require.NoError(t, readErr)
				if scenario == "replacement-on-conflict" {
					require.True(t, injected)
					require.Equal(t, replacement, cm.Data[report.VolumeID])
				} else {
					require.Equal(t, legacy, cm.Data[report.VolumeID])
				}
				_, err = ns.NodeStageVolume(ctx, request)
			}
			require.NoError(t, err)
			current := NewVolumeRepairStateManager(driver.kubeRuntime, namespaceFromServiceAccount())
			_, exists, err := current.GetCurrent(ctx, report.VolumeID)
			require.NoError(t, err)
			require.False(t, exists)
			require.NoError(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
		})
	}
}

func TestReviewNodeStageMustNotAcknowledgeUnconfirmableRecovery(t *testing.T) {
	for _, mode := range []string{"in-progress", "advanced-during-discovery", "created-during-discovery", "terminalized-during-discovery", "fresh-pending"} {
		for _, mounted := range []bool{false, true} {
			name := mode
			if mounted {
				name += "/already-mounted"
			} else {
				name += "/new-mount"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				disk := withTestDiskPath(t)
				device := filepath.Join(disk, "zero")
				stage := t.TempDir()
				require.NoError(t, os.WriteFile(device, []byte("fixture"), 0600))
				byID := filepath.Join(disk, "disk", "by-id", "virtio-onecsi-42")
				require.NoError(t, os.MkdirAll(filepath.Dir(byID), 0750))
				require.NoError(t, os.Symlink(device, byID))
				var points []mount.MountPoint
				if mounted {
					points = []mount.MountPoint{{Path: stage, Device: device, Type: "ext4"}}
				}
				ns := getTestNodeServerWithMountPoints(points)
				fakeExec := ns.mounter.Exec.(*testingexec.FakeExec)
				commands := append([]testingexec.FakeCommandAction(nil), fakeExec.CommandScript...)
				for i := 0; i < 5; i++ {
					fakeExec.CommandScript = append(fakeExec.CommandScript, commands...)
				}
				ns.Driver.kubeRuntime = &KubeRuntime{client: fake.NewSimpleClientset(), enabled: true}
				ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
				now := time.Now().UTC()
				deadline := now.Add(time.Minute)
				report := LocalDeviceMissingReport{Node: ns.Driver.nodeID, VolumeID: "review-volume", VolumeName: "zero", FirstObservedAt: now, LastObservedAt: now, FailureClass: localDeviceFailureClassMissingDevice, ConfirmationState: localDeviceConfirmationStateInProgress, RecoveryToken: "episode-T", RecoveryAttempts: 1, ConfirmationDeadline: &deadline}
				if mode == "fresh-pending" || mode == "terminalized-during-discovery" {
					report.ConfirmationState = localDeviceConfirmationStatePending
					report.RecoveryMethod = localDeviceRecoveryMethodRuntimeRepublish
				}
				if mode == "advanced-during-discovery" {
					report.ConfirmationState = ""
					report.RecoveryToken = ""
					report.RecoveryAttempts = 0
				}
				key := localDeviceReportKey(report.Node, report.VolumeID)
				if mode != "created-during-discovery" {
					require.NoError(t, updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = report }))
				}
				if mode == "advanced-during-discovery" || mode == "created-during-discovery" || mode == "terminalized-during-discovery" {
					originalStat := nodeVolumePathStat
					injected := false
					nodeVolumePathStat = func(path string) (os.FileInfo, error) {
						if path == device && !injected {
							injected = true
							require.NoError(t, updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) {
								*current = report
								current.RecoveryMethod = localDeviceRecoveryMethodRuntimeRepublish
								current.RecoveryToken = "episode-new"
								current.RecoveryAttempts = 1
								current.ConfirmationState = localDeviceConfirmationStatePending
								if mode == "terminalized-during-discovery" {
									current.RecoveryToken = report.RecoveryToken
									current.ConfirmationState = localDeviceConfirmationStateRepairRequired
									current.ConfirmationDeadline = nil
								}
							}))
						}
						return originalStat(path)
					}
				}
				req := &csi.NodeStageVolumeRequest{VolumeId: report.VolumeID, StagingTargetPath: stage, PublishContext: map[string]string{"volumeName": "zero"}, VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}
				response, err := ns.NodeStageVolume(ctx, req)
				current, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
				t.Logf("NodeStage response=%v error=%v report_exists=%v confirmation=%s", response, err, exists, current.ConfirmationState)
				if mode == "fresh-pending" {
					require.NoError(t, err)
					require.False(t, exists)
					return
				}
				require.Error(t, err)
				if mode == "terminalized-during-discovery" {
					_, err = ns.NodeStageVolume(ctx, req)
					require.NoError(t, err)
					_, exists = ns.currentLocalDeviceReport(ctx, report.VolumeID)
					require.False(t, exists, "a fresh retry must finish the same completed episode")
					return
				}
				if err == nil && exists && (current.ConfirmationState == localDeviceConfirmationStateInProgress || current.ConfirmationState == localDeviceConfirmationStatePending) {
					t.Fatal("NodeStage acknowledged success while recovery remains unconfirmed; kubelet will not retry staging when attachment completes")
				}
			})
		}
	}
}
