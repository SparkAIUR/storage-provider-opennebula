package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	mount "k8s.io/mount-utils"
)

func reviewBlockPublishFixture(t *testing.T, bound bool) (*NodeServer, *ControllerServer, LocalDeviceMissingReport, *csi.NodePublishVolumeRequest) {
	t.Helper()
	driver, _, controller, _, report := recoveryReviewFixture(t)
	ns, stage := reviewLocalStageFixture(t, driver, report, false)
	targetRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	request := &csi.NodePublishVolumeRequest{
		VolumeId: report.VolumeID, StagingTargetPath: stage.StagingTargetPath,
		TargetPath: filepath.Join(targetRoot, "block"), PublishContext: stage.PublishContext,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	}
	if bound {
		require.NoError(t, os.WriteFile(request.TargetPath, nil, 0600))
		require.NoError(t, ns.mounter.Mount(filepath.Join(defaultDiskPath, report.VolumeName), request.TargetPath, "", []string{"bind"}))
	}
	return ns, controller, report, request
}

func TestReviewBlockPublishRecoveryAuthority(t *testing.T) {
	for _, bound := range []bool{false, true} {
		for _, scenario := range []string{"in-progress", "unfinished-timeout", "pending-completed", "terminal-completed", "no-report", "created-during-discovery", "advanced-during-discovery", "confirmation-conflict", "deletion-conflict"} {
			name := scenario + "/new-bind"
			if bound {
				name = scenario + "/already-bound"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				ns, controller, report, request := reviewBlockPublishFixture(t, bound)
				key := localDeviceReportKey(report.Node, report.VolumeID)
				episode, started, err := controller.beginLocalDeviceRecovery(ctx, key, report)
				require.NoError(t, err)
				require.True(t, started)
				if scenario == "unfinished-timeout" {
					require.NoError(t, controller.updateLocalDeviceRecoveryFailure(ctx, key, episode, context.DeadlineExceeded))
					episode, _ = ns.currentLocalDeviceReport(ctx, report.VolumeID)
					episode, _, err = controller.refreshLocalDeviceReportConfirmationState(ctx, key, episode, episode.ConfirmationDeadline.Add(time.Second))
					require.NoError(t, err)
					require.Equal(t, localDeviceConfirmationStateRepairRequired, episode.ConfirmationState)
				} else if scenario != "in-progress" {
					updated, err := controller.markLocalDeviceRecoveryPendingConfirmation(ctx, key, episode, &localDeviceRecoveryAttempt{Method: localDeviceRecoveryMethodRuntimeRepublish, MetadataNode: report.Node}, time.Now().Add(time.Minute))
					require.NoError(t, err)
					require.True(t, updated)
					episode, _ = ns.currentLocalDeviceReport(ctx, report.VolumeID)
				}
				if scenario == "terminal-completed" {
					episode.ConfirmationState = localDeviceConfirmationStateRepairRequired
					require.NoError(t, updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = episode }))
				}
				if scenario == "created-during-discovery" || scenario == "no-report" {
					require.NoError(t, clearLocalDeviceReportIf(ctx, ns.Driver.kubeRuntime, namespaceFromServiceAccount(), key, episode))
				}
				newer := episode
				newer.RecoveryToken = "replacement-episode"
				newer.RecoveryAttempts++
				injected := false
				originalStat := nodeVolumePathStat
				nodeVolumePathStat = func(path string) (os.FileInfo, error) {
					if path == filepath.Join(defaultDiskPath, report.VolumeName) && !injected && strings.HasSuffix(scenario, "during-discovery") {
						injected = true
						require.NoError(t, updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = newer }))
					}
					return originalStat(path)
				}
				client := ns.Driver.kubeRuntime.client.(*fake.Clientset)
				if strings.HasSuffix(scenario, "conflict") {
					client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
						cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
						if injected || cm.Name != localDeviceStateConfigMapName || (scenario == "deletion-conflict" && cm.Data[key] != "") {
							return false, nil, nil
						}
						injected = true
						payload, err := json.Marshal(newer)
						require.NoError(t, err)
						cm.Data[key] = string(payload)
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
						return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("new recovery episode"))
					})
				}
				mounter := ns.mounter.Interface.(*mount.FakeMounter)
				before, err := mounter.List()
				require.NoError(t, err)
				beforeLog := mounter.GetLog()
				response, err := ns.NodePublishVolume(ctx, request)
				current, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
				if scenario == "pending-completed" || scenario == "terminal-completed" || scenario == "no-report" {
					require.NoError(t, err)
					require.NotNil(t, response)
					require.False(t, exists)
					point, mounted, err := ns.mountPointForPath(request.TargetPath)
					require.NoError(t, err)
					require.True(t, mounted)
					require.Equal(t, filepath.Join(defaultDiskPath, report.VolumeName), point.Device)
					require.Empty(t, point.Type)
					require.NoError(t, controller.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
					if bound {
						require.Equal(t, beforeLog, mounter.GetLog())
					}
					return
				}
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, response)
				require.True(t, exists)
				after, err := mounter.List()
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.Equal(t, beforeLog, mounter.GetLog())
				if scenario == "in-progress" || scenario == "unfinished-timeout" {
					require.Equal(t, episode, current)
				} else {
					require.True(t, injected)
					require.Equal(t, newer, current)
					response, err = ns.NodePublishVolume(ctx, request)
					require.NoError(t, err)
					require.NotNil(t, response)
					_, exists = ns.currentLocalDeviceReport(ctx, report.VolumeID)
					require.False(t, exists)
				}
			})
		}
	}
}

func TestReviewBlockPublishRepairAuthority(t *testing.T) {
	for _, bound := range []bool{false, true} {
		for _, scenario := range []string{"wrong-report-identity", "matching-report-identity", "wrong-repair-identity", "legacy-repair", "new-repair-during-discovery", "changed-repair-during-discovery", "report-read-error", "repair-read-error"} {
			name := scenario + "/new-bind"
			if bound {
				name = scenario + "/already-bound"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				ns, controller, report, request := reviewBlockPublishFixture(t, bound)
				identity := &LocalDiskIdentity{ObservedFromDevice: &LocalDiskObservedIdentity{Block: &LocalDiskObservedBlockIdentity{ByIDPath: filepath.Join(defaultDiskPath, "disk", "by-id", "virtio-onecsi-42")}}}
				key := localDeviceReportKey(report.Node, report.VolumeID)
				if scenario == "wrong-report-identity" || scenario == "matching-report-identity" {
					report.FailureClass = localDeviceFailureClassWrongIdentity
					report.ExpectedIdentity = identity
					if scenario == "wrong-report-identity" {
						report.ExpectedIdentity = &LocalDiskIdentity{LegacyDeviceSerial: "onecsi-wrong"}
					}
					require.NoError(t, updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = report }))
				}
				if scenario == "legacy-repair" {
					const legacy = `{"version":1,"volumeID":"vol-device","classification":"same_node_runtime_attach_unconfirmed","requestedNode":"node-a","evidenceSource":"local_device_report","failureCount":2,"firstObservedAt":"2026-09-09T10:00:00Z","lastObservedAt":"2026-09-09T10:05:00Z"}`
					require.NoError(t, ns.Driver.kubeRuntime.UpsertConfigMapData(ctx, namespaceFromServiceAccount(), volumeRepairStateConfigMapName, map[string]string{report.VolumeID: legacy}))
				}
				if scenario == "wrong-repair-identity" || scenario == "changed-repair-during-discovery" {
					expected := identity
					if scenario == "wrong-repair-identity" {
						expected = &LocalDiskIdentity{LegacyDeviceSerial: "onecsi-wrong"}
					}
					_, err := ns.Driver.volumeRepairState.Mark(ctx, VolumeRepairState{VolumeID: report.VolumeID, RequestedNode: report.Node, Classification: repairClassificationWrongDeviceIdentity, LastHealthyIdentity: expected})
					require.NoError(t, err)
				}
				injected := false
				var newer VolumeRepairState
				originalStat := nodeVolumePathStat
				nodeVolumePathStat = func(path string) (os.FileInfo, error) {
					if path == filepath.Join(defaultDiskPath, report.VolumeName) && !injected && strings.HasSuffix(scenario, "during-discovery") {
						injected = true
						var err error
						newer, err = ns.Driver.volumeRepairState.Mark(ctx, VolumeRepairState{VolumeID: report.VolumeID, RequestedNode: report.Node, Classification: repairClassificationWrongDeviceIdentity, LastHealthyIdentity: identity, Message: "new repair evidence"})
						require.NoError(t, err)
					}
					return originalStat(path)
				}
				client := ns.Driver.kubeRuntime.client.(*fake.Clientset)
				readFailure := strings.HasSuffix(scenario, "read-error")
				client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					name := action.(ktesting.GetAction).GetName()
					if readFailure && ((scenario == "report-read-error" && name == localDeviceStateConfigMapName) || (scenario == "repair-read-error" && name == volumeRepairStateConfigMapName)) {
						return true, nil, errors.New("API unavailable")
					}
					return false, nil, nil
				})
				mounter := ns.mounter.Interface.(*mount.FakeMounter)
				before, err := mounter.List()
				require.NoError(t, err)
				beforeLog := mounter.GetLog()
				response, err := ns.NodePublishVolume(ctx, request)
				readFailure = false
				if scenario == "matching-report-identity" || scenario == "legacy-repair" {
					require.NoError(t, err)
					require.NotNil(t, response)
					require.NoError(t, controller.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
					_, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
					require.False(t, exists)
					_, mounted, err := ns.mountPointForPath(request.TargetPath)
					require.NoError(t, err)
					require.True(t, mounted)
					return
				}
				if strings.HasPrefix(scenario, "wrong-") {
					require.Equal(t, codes.FailedPrecondition, status.Code(err))
					require.Error(t, controller.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
				}
				require.Nil(t, response)
				after, err := mounter.List()
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.Equal(t, beforeLog, mounter.GetLog())
				if strings.HasSuffix(scenario, "during-discovery") {
					require.True(t, injected)
					current, exists, err := ns.Driver.volumeRepairState.GetCurrent(ctx, report.VolumeID)
					require.NoError(t, err)
					require.True(t, exists)
					require.Equal(t, newer, current)
				} else {
					_, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
					require.True(t, exists)
				}
			})
		}
	}
}
