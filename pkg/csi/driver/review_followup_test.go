package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	ktesting "k8s.io/client-go/testing"
	mount "k8s.io/mount-utils"
)

type reviewConfigMaps struct {
	typedcore.ConfigMapInterface
	update func(context.Context, *corev1.ConfigMap, metav1.UpdateOptions) (*corev1.ConfigMap, error)
	patch  func(context.Context, string, types.PatchType, []byte, metav1.PatchOptions, ...string) (*corev1.ConfigMap, error)
}

func (c reviewConfigMaps) Update(ctx context.Context, cm *corev1.ConfigMap, opts metav1.UpdateOptions) (*corev1.ConfigMap, error) {
	if c.update != nil {
		return c.update(ctx, cm, opts)
	}
	return c.ConfigMapInterface.Update(ctx, cm, opts)
}

func (c reviewConfigMaps) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, sub ...string) (*corev1.ConfigMap, error) {
	if c.patch != nil {
		return c.patch(ctx, name, pt, data, opts, sub...)
	}
	return c.ConfigMapInterface.Patch(ctx, name, pt, data, opts, sub...)
}

type reviewCore struct {
	typedcore.CoreV1Interface
	maps typedcore.ConfigMapInterface
}

func (c reviewCore) ConfigMaps(string) typedcore.ConfigMapInterface { return c.maps }

type reviewClient struct {
	kubernetes.Interface
	core typedcore.CoreV1Interface
}

func (c reviewClient) CoreV1() typedcore.CoreV1Interface { return c.core }

func reviewRuntime(client kubernetes.Interface, maps reviewConfigMaps) *KubeRuntime {
	return &KubeRuntime{enabled: true, client: reviewClient{Interface: client, core: reviewCore{CoreV1Interface: client.CoreV1(), maps: maps}}}
}

func recoveryReviewFixture(t *testing.T) (*Driver, *NodeServer, *ControllerServer, *MockOpenNebulaVolumeProviderTestify, LocalDeviceMissingReport) {
	t.Helper()
	pv, pvc := newLocalPVAndPVC("vol-device", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	report := LocalDeviceMissingReport{Node: "node-a", VolumeID: "vol-device", VolumeName: "sdd", ExpectedTarget: "sdd", PVCNamespace: "default", PVCName: pvc.Name, PVName: pv.Name, FirstObservedAt: time.Now().UTC().Add(-time.Hour), LastObservedAt: time.Now().UTC().Add(-time.Minute), Attempts: 3}
	va := &storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-device", CreationTimestamp: metav1.NewTime(time.Now().Add(-30 * time.Second))}, Spec: storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: report.Node, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}}, Status: storagev1.VolumeAttachmentStatus{Attached: true}}
	driver := newLocalDeviceRecoveryTestDriver(t, pv, pvc, newReadyNode(report.Node, true), va)
	driver.nodeID = report.Node
	require.NoError(t, updateLocalDeviceReport(context.Background(), driver.kubeRuntime, namespaceFromServiceAccount(), localDeviceReportKey(report.Node, report.VolumeID), func(current *LocalDeviceMissingReport) { *current = report }))
	provider := &MockOpenNebulaVolumeProviderTestify{}
	return driver, &NodeServer{Driver: driver}, NewControllerServer(driver, provider, nil), provider, report
}

func TestLocalDeviceRecoveryPreservesWrongIdentityAndManualRepair(t *testing.T) {
	for _, classification := range []string{localDeviceFailureClassWrongIdentity, localDeviceConfirmationStateRepairRequired} {
		t.Run(classification, func(t *testing.T) {
			driver, ns, server, _, report := recoveryReviewFixture(t)
			ctx := context.Background()
			key := localDeviceReportKey(report.Node, report.VolumeID)
			report.RecoveryAttempts = 2
			report.RecoveryToken = "repair-episode"
			report.ExpectedIdentity = &LocalDiskIdentity{LegacyFilesystemUUID: "expected"}
			report.ObservedIdentity = &LocalDiskIdentity{LegacyFilesystemUUID: "wrong"}
			if classification == localDeviceFailureClassWrongIdentity {
				report.FailureClass = classification
				report.ConfirmationState = localDeviceConfirmationStateConfirmed
			} else {
				report.ConfirmationState = classification
			}
			require.NoError(t, updateLocalDeviceReport(ctx, driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = report }))
			ns.recordLocalDeviceMissing(ctx, report.VolumeID, "sde", "/stage", nil, errors.New("not visible"))
			current, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
			require.True(t, exists)
			require.Equal(t, report, current)
			ready, _ := server.localDeviceReportReady(current, time.Now().Add(time.Hour))
			require.False(t, ready)
		})
	}
}

func TestLocalDeviceConfirmationRejectsSupersededEpisodeOnConflict(t *testing.T) {
	for _, operation := range []string{"confirm", "delete"} {
		t.Run(operation, func(t *testing.T) {
			driver, ns, _, _, report := recoveryReviewFixture(t)
			ctx := context.Background()
			client := driver.kubeRuntime.client.(*fake.Clientset)
			key := localDeviceReportKey(report.Node, report.VolumeID)
			newer := report
			newer.RecoveryToken = "new-episode"
			newer.ConfirmationState = localDeviceConfirmationStateInProgress
			newer.RecoveryAttempts++
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				if injected || cm.Name != localDeviceStateConfigMapName || (operation == "delete" && cm.Data[key] != "") {
					return false, nil, nil
				}
				injected = true
				payload, err := json.Marshal(newer)
				require.NoError(t, err)
				cm.Data[key] = string(payload)
				require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("new recovery episode"))
			})
			ns.confirmLocalDeviceRecovery(ctx, &report, report.VolumeID, "/dev/sdd", nil, nil)
			require.True(t, injected)
			current, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
			require.True(t, exists)
			require.Equal(t, newer, current)
		})
	}
}

func TestLocalDeviceTimeoutRepairIsAtomicAndClearedByLateConfirmation(t *testing.T) {
	driver, ns, server, _, report := recoveryReviewFixture(t)
	ctx := context.Background()
	report.RecoveryToken = "episode-timeout"
	report.RecoveryMethod = localDeviceRecoveryMethodRuntimeRepublish
	report.RecoveryAttempts = 2
	report.ConfirmationState = localDeviceConfirmationStatePending
	deadline := time.Now().UTC().Add(-time.Minute)
	report.ConfirmationDeadline = &deadline
	key := localDeviceReportKey(report.Node, report.VolumeID)
	require.NoError(t, updateLocalDeviceReport(ctx, driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) { *current = report }))
	client := driver.kubeRuntime.client.(*fake.Clientset)
	fail := true
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			fail = false
			return true, nil, errors.New("API unavailable")
		}
		return false, nil, nil
	})
	_, _, err := server.refreshLocalDeviceReportConfirmationState(ctx, key, report, time.Now())
	require.Error(t, err)
	current, _ := ns.currentLocalDeviceReport(ctx, report.VolumeID)
	require.Equal(t, localDeviceConfirmationStatePending, current.ConfirmationState)
	current, _, err = server.refreshLocalDeviceReportConfirmationState(ctx, key, current, time.Now())
	require.NoError(t, err)
	fresh := NewVolumeRepairStateManager(driver.kubeRuntime, namespaceFromServiceAccount())
	state, exists, err := fresh.GetCurrent(ctx, report.VolumeID)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, report.RecoveryToken, state.RecoveryToken)
	require.Error(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
	ns.confirmLocalDeviceRecovery(ctx, &current, report.VolumeID, "/dev/sdd", nil, nil)
	require.NoError(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
	_, _, err = server.refreshLocalDeviceReportConfirmationState(ctx, key, report, time.Now())
	require.NoError(t, err)
	_, exists, err = fresh.GetCurrent(ctx, report.VolumeID)
	require.NoError(t, err)
	require.False(t, exists)
	_, err = fresh.Mark(ctx, VolumeRepairState{VolumeID: report.VolumeID, Classification: repairClassificationWrongDeviceIdentity})
	require.NoError(t, err)
	_, err = fresh.Mark(ctx, state)
	require.Error(t, err)
	currentRepair, exists, err := fresh.GetCurrent(ctx, report.VolumeID)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, repairClassificationWrongDeviceIdentity, currentRepair.Classification)
}

func TestVolumeHistoryBlockedWriteDoesNotBlockUnrelatedCacheRead(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	base := client.CoreV1().ConfigMaps("default")
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	manager := NewVolumeHistoryManager(reviewRuntime(client, reviewConfigMaps{ConfigMapInterface: base, update: func(ctx context.Context, cm *corev1.ConfigMap, opts metav1.UpdateOptions) (*corev1.ConfigMap, error) {
		if cm.Data["blocked"] != "" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return base.Update(ctx, cm, opts)
	}}), "default")
	_, err := manager.Upsert(ctx, "healthy", func(state *VolumeHistoryRecord) { state.LastSuccessfulDiskID = 7 })
	require.NoError(t, err)
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := manager.Upsert(writeCtx, "blocked", func(*VolumeHistoryRecord) {}); done <- err }()
	<-entered
	read := make(chan VolumeHistoryRecord, 1)
	go func() { state, _ := manager.Get("healthy"); read <- state }()
	select {
	case state := <-read:
		require.Equal(t, 7, state.LastSuccessfulDiskID)
	case <-time.After(time.Second):
		t.Fatal("unrelated cache read blocked behind I/O")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRepairSuccessUsesDurableStateAndConditionalClear(t *testing.T) {
	driver, ns, server, _, report := recoveryReviewFixture(t)
	ctx := context.Background()
	controller := driver.volumeRepairState
	ns.Driver.volumeRepairState = NewVolumeRepairStateManager(driver.kubeRuntime, namespaceFromServiceAccount())
	old, err := controller.Mark(ctx, VolumeRepairState{VolumeID: report.VolumeID, Classification: repairClassificationWrongDeviceIdentity})
	require.NoError(t, err)
	require.Error(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
	ns.clearRepairStateOnSuccess(ctx, report.VolumeID)
	require.NoError(t, server.rejectIfActiveRepairState(ctx, report.VolumeID, nil))
	newer, err := controller.Mark(ctx, VolumeRepairState{VolumeID: report.VolumeID, Classification: repairClassificationWrongDeviceIdentity, RequestedNode: "node-b"})
	require.NoError(t, err)
	require.NoError(t, controller.ClearObserved(ctx, old))
	state, exists, err := controller.GetCurrent(ctx, report.VolumeID)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, newer, state)
}

func TestLocalDiskForcedReprobeUsesCurrentBoundedManualMode(t *testing.T) {
	for _, scenario := range []string{"set-on-pv", "set-on-pvc", "removed", "expired", "malformed", "missing-deadline", "pv-unavailable", "pvc-unavailable", "no-client"} {
		t.Run(scenario, func(t *testing.T) {
			original := localDiskSessionRootPath
			localDiskSessionRootPath = t.TempDir()
			t.Cleanup(func() { localDiskSessionRootPath = original })
			stage := filepath.Join(t.TempDir(), "stage")
			require.NoError(t, os.MkdirAll(stage, 0750))
			session := localDiskSession{VolumeID: "vol-reprobe", StagingTargetPath: stage, RecoveryMode: recoveryModeManual, PublishedTargets: []localDiskPublishedTarget{{TargetPath: "/var/lib/kubelet/pods/example/volumes/kubernetes.io~csi/pvc/mount"}}}
			annotations := map[string]string{annotationRecoveryMode: recoveryModeManual, annotationRecoveryModeUntil: time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}
			switch scenario {
			case "set-on-pv", "set-on-pvc":
				session.RecoveryMode = ""
			case "removed":
				annotations = nil
			case "expired":
				annotations[annotationRecoveryModeUntil] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
			case "malformed":
				annotations[annotationRecoveryModeUntil] = "invalid"
			case "missing-deadline":
				delete(annotations, annotationRecoveryModeUntil)
			}
			store := newLocalDiskSessionStore(localDiskSessionRootPath)
			require.NoError(t, store.Save(session))
			pv, pvc := newLocalPVAndPVC(session.VolumeID, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, annotations)
			if scenario == "set-on-pvc" {
				pv.Annotations = nil
				pvc.Annotations = annotations
			}
			if scenario == "set-on-pv" {
				pv.Annotations = annotations
				pvc.Annotations = nil
			}
			client := fake.NewSimpleClientset(pv, pvc)
			if scenario == "pv-unavailable" {
				client.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("API unavailable") })
			}
			if scenario == "pvc-unavailable" {
				client.PrependReactor("get", "persistentvolumeclaims", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("API unavailable") })
			}
			var kube kubernetes.Interface = client
			if scenario == "no-client" {
				kube = nil
			}
			report, err := runLocalDiskReprobeCommandWithMounter(context.Background(), kube, LocalDiskReprobeOptions{VolumeID: session.VolumeID, AllowPublished: true}, mount.NewFakeMounter(nil))
			if scenario == "set-on-pv" || scenario == "set-on-pvc" {
				require.NoError(t, err)
				require.True(t, report.CleanupPerformed)
				_, exists, err := store.Load(session.VolumeID)
				require.NoError(t, err)
				require.False(t, exists)
			} else {
				require.Error(t, err)
				require.False(t, report.CleanupPerformed)
				_, err = os.Stat(stage)
				require.NoError(t, err)
				_, exists, err := store.Load(session.VolumeID)
				require.NoError(t, err)
				require.True(t, exists)
			}
		})
	}
}

func TestLocalDeviceRecoveryCannotDetachConcurrentSuccessfulStage(t *testing.T) {
	disk := withTestDiskPath(t)
	require.NoError(t, os.WriteFile(filepath.Join(disk, "sdd"), nil, 0600))
	original := localDiskSessionRootPath
	localDiskSessionRootPath = t.TempDir()
	t.Cleanup(func() { localDiskSessionRootPath = original })
	driver, _, server, provider, report := recoveryReviewFixture(t)
	stage := filepath.Join(t.TempDir(), "stage")
	require.NoError(t, os.MkdirAll(stage, 0750))
	ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: stage, Device: filepath.Join(disk, "sdd"), Type: "ext4"}})
	ns.Driver = driver
	ctx := context.Background()
	provider.On("NodeReady", mock.Anything, report.Node).Return(true, nil).Once()
	provider.On("VolumeExists", mock.Anything, report.VolumeID).Return(394, 1, nil).Once()
	provider.On("NodeExists", mock.Anything, report.Node).Return(208, nil).Once()
	provider.On("GetVolumeInNode", mock.Anything, 394, 208).Run(func(mock.Arguments) {
		response, err := ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: report.VolumeID, StagingTargetPath: stage, PublishContext: map[string]string{"volumeName": "sdd", annotationBackend: "local"}, VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}})
		require.NoError(t, err)
		require.NotNil(t, response)
	}).Return("sdd", nil).Once()
	require.NoError(t, server.recoverLocalDeviceReport(ctx, localDeviceReportKey(report.Node, report.VolumeID), report))
	provider.AssertExpectations(t)
	_, mounted, err := ns.mountPointForPath(stage)
	require.NoError(t, err)
	require.True(t, mounted)
	_, exists := ns.currentLocalDeviceReport(ctx, report.VolumeID)
	require.False(t, exists)
	provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	provider.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}
