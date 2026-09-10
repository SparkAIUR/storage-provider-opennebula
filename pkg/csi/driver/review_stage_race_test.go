package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	mount "k8s.io/mount-utils"
	testingexec "k8s.io/utils/exec/testing"
)

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
