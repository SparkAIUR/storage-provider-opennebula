package driver

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	mount "k8s.io/mount-utils"
)

func TestVolumeHistoryRefreshEntryClearsDeletedConfigMapKey(t *testing.T) {
	record := VolumeHistoryRecord{
		Version:                   stateObjectVersion,
		VolumeID:                  "vol-1",
		Backend:                   "local",
		LastSuccessfulNodeName:    "node-a",
		LastSuccessfulPublishTime: time.Now().UTC(),
	}
	payload, err := json.Marshal(record)
	require.NoError(t, err)

	runtime := &KubeRuntime{
		client: fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: volumeHistoryStateConfigMapName, Namespace: "default"},
			Data:       map[string]string{"vol-1": string(payload)},
		}),
		enabled: true,
	}
	manager := NewVolumeHistoryManager(runtime, "default")
	require.NoError(t, manager.LoadFromConfigMap(context.Background()))
	_, ok := manager.Get("vol-1")
	require.True(t, ok)

	require.NoError(t, runtime.DeleteConfigMapKey(context.Background(), "default", volumeHistoryStateConfigMapName, "vol-1"))
	require.NoError(t, manager.RefreshEntry(context.Background(), "vol-1"))

	_, ok = manager.Get("vol-1")
	require.False(t, ok)
}

func TestLocalRWOProtectionDecisionRefreshesDeletedHistoryEntry(t *testing.T) {
	record := VolumeHistoryRecord{
		Version:                   stateObjectVersion,
		VolumeID:                  "vol-1",
		Backend:                   "local",
		LastSuccessfulNodeName:    "node-a",
		LastSuccessfulPublishTime: time.Now().UTC(),
	}
	payload, err := json.Marshal(record)
	require.NoError(t, err)

	runtime := &KubeRuntime{
		client: fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: volumeHistoryStateConfigMapName, Namespace: "default"},
			Data:       map[string]string{"vol-1": string(payload)},
		}),
		enabled: true,
	}
	driver := &Driver{
		kubeRuntime:   runtime,
		volumeHistory: NewVolumeHistoryManager(runtime, "default"),
	}
	require.NoError(t, driver.volumeHistory.LoadFromConfigMap(context.Background()))
	require.NoError(t, runtime.DeleteConfigMapKey(context.Background(), "default", volumeHistoryStateConfigMapName, "vol-1"))

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-1", "node-b")
	require.NoError(t, err)
	require.False(t, decision.Protected)
	require.Empty(t, decision.RequiredNode)
	require.True(t, decision.History.LastSuccessfulPublishTime.IsZero())
}

func TestLookupVolumeForPublishBootstrapsLegacyLocalHistoryForTombstone(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-legacy-tombstone", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-gone"
	driver := newStickyTestDriver(t, pv, pvc)

	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-legacy-tombstone", "node-new").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupNotFound,
		VolumeHandle:  "vol-legacy-tombstone",
		RequestedNode: "node-new",
	}, nil).Once()

	server := NewControllerServer(driver, provider, nil)
	result, history, _, err := server.lookupVolumeForPublish(context.Background(), "vol-legacy-tombstone", "node-new")
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.NotNil(t, result)
	require.Equal(t, opennebula.VolumeLookupImageRecordMissing, result.Status)
	require.True(t, history.Bootstrapped)
	require.Equal(t, volumeHistoryEvidenceSourceBootstrap, history.EvidenceSource)
	require.Equal(t, "node-gone", history.LastSuccessfulNodeName)
	require.True(t, history.LastSuccessfulPublishTime.IsZero())
	require.True(t, history.LastSuccessfulStageTime.IsZero())

	state, ok := driver.volumeRepairState.Get("vol-legacy-tombstone")
	require.True(t, ok)
	require.Equal(t, repairClassificationHistoricalNodeTombstone, state.Classification)
	require.Contains(t, state.EvidenceSource, "bootstrap")
	provider.AssertExpectations(t)
}

func TestLookupVolumeForPublishBootstrapsLegacyLocalHistoryForMissingImageRecord(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-legacy-missing", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-old"
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))

	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-legacy-missing", "node-new").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupImageRecordMissing,
		VolumeHandle:  "vol-legacy-missing",
		RequestedNode: "node-new",
	}, nil).Once()

	server := NewControllerServer(driver, provider, nil)
	_, history, _, err := server.lookupVolumeForPublish(context.Background(), "vol-legacy-missing", "node-new")
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.True(t, history.Bootstrapped)
	require.Equal(t, "node-old", history.LastSuccessfulNodeName)
	require.True(t, history.LastSuccessfulPublishTime.IsZero())
	require.True(t, history.LastSuccessfulStageTime.IsZero())

	state, ok := driver.volumeRepairState.Get("vol-legacy-missing")
	require.True(t, ok)
	require.Equal(t, repairClassificationMissingImageRecord, state.Classification)
	require.Contains(t, state.EvidenceSource, "bootstrap")
	provider.AssertExpectations(t)
}

func TestLookupVolumeForPublishObservedHistoryWinsOverBootstrap(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-observed-wins", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-from-annotation"
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-observed", true), newReadyNode("node-from-annotation", true))
	_, err := driver.volumeHistory.Upsert(context.Background(), "vol-observed-wins", func(state *VolumeHistoryRecord) {
		state.Backend = "local"
		state.LastSuccessfulNodeName = "node-observed"
		state.LastSuccessfulPublishTime = time.Now().UTC()
	})
	require.NoError(t, err)

	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-observed-wins", "node-new").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupImageRecordMissing,
		VolumeHandle:  "vol-observed-wins",
		RequestedNode: "node-new",
	}, nil).Once()

	server := NewControllerServer(driver, provider, nil)
	_, history, _, err := server.lookupVolumeForPublish(context.Background(), "vol-observed-wins", "node-new")
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.False(t, history.Bootstrapped)
	require.Equal(t, volumeHistoryEvidenceSourceObserved, history.EvidenceSource)
	require.Equal(t, "node-observed", history.LastSuccessfulNodeName)
	require.False(t, history.LastSuccessfulPublishTime.IsZero())
	provider.AssertExpectations(t)
}

func TestLookupVolumeForPublishUnknownVolumeRemainsNotFound(t *testing.T) {
	driver := newStickyTestDriver(t)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-unknown", "node-new").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupNotFound,
		VolumeHandle:  "vol-unknown",
		RequestedNode: "node-new",
	}, nil).Once()

	server := NewControllerServer(driver, provider, nil)
	_, history, _, err := server.lookupVolumeForPublish(context.Background(), "vol-unknown", "node-new")
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.False(t, history.Bootstrapped)
	_, ok := driver.volumeRepairState.Get("vol-unknown")
	require.False(t, ok)
	provider.AssertExpectations(t)
}

func TestVolumeHistoryBootstrapUsesPlacementSummaryFallback(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-summary", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationPlacementSummary] = `{"backend":"local","lastAttachedNode":"node-summary"}`
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-summary", true))
	runtimeCtx, err := driver.kubeRuntime.ResolveVolumeRuntimeContext(context.Background(), "vol-summary")
	require.NoError(t, err)

	record, seeded, err := driver.volumeHistory.SeedFromRuntimeContext(context.Background(), "vol-summary", runtimeCtx)
	require.NoError(t, err)
	require.True(t, seeded)
	require.True(t, record.Bootstrapped)
	require.Equal(t, "node-summary", record.LastSuccessfulNodeName)
	require.Contains(t, record.BootstrappedFields, "pv.placementSummary.lastAttachedNode")
	require.True(t, record.LastSuccessfulPublishTime.IsZero())
	require.True(t, record.LastSuccessfulStageTime.IsZero())
}

func TestValidateHotplugQueueRequestAllowsOrphanDetachAfterHistoryRefresh(t *testing.T) {
	record := VolumeHistoryRecord{
		Version:                   stateObjectVersion,
		VolumeID:                  "vol-orphan",
		Backend:                   "local",
		LastSuccessfulNodeName:    "node-a",
		LastSuccessfulPublishTime: time.Now().UTC(),
	}
	payload, err := json.Marshal(record)
	require.NoError(t, err)

	pv, pvc := newLocalPVAndPVC("vol-orphan", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	pv.Status.Phase = corev1.VolumeReleased
	pvName := pv.Name
	now := metav1.NewTime(time.Now().UTC().Add(-time.Minute))
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "va-orphan",
			DeletionTimestamp: &now,
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	historyCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: volumeHistoryStateConfigMapName, Namespace: "default"},
		Data:       map[string]string{"vol-orphan": string(payload)},
	}
	driver := newStickyTestDriver(t, pv, pvc, va, newReadyNode("node-a", true), historyCM)
	require.NoError(t, driver.kubeRuntime.DeleteConfigMapKey(context.Background(), "default", volumeHistoryStateConfigMapName, "vol-orphan"))

	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-orphan", "node-a").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  "vol-orphan",
		ImageID:       11,
		RequestedNode: "node-a",
	}, nil).Once()
	provider.On("NodeExists", mock.Anything, "node-a").Return(101, nil).Once()
	provider.On("GetVolumeInNode", mock.Anything, 11, 101).Return("vdb", nil).Once()

	server := NewControllerServer(driver, provider, nil)
	validation := server.validateHotplugQueueRequest(context.Background(), "node-a", "detach", "vol-orphan")
	require.Equal(t, HotplugQueueValidationValid, validation.Decision)
	provider.AssertExpectations(t)
}

func TestValidateHotplugQueueRequestKeepsLiveDesiredDetachPaused(t *testing.T) {
	record := VolumeHistoryRecord{
		Version:                   stateObjectVersion,
		VolumeID:                  "vol-live",
		Backend:                   "local",
		LastSuccessfulNodeName:    "node-a",
		LastSuccessfulPublishTime: time.Now().UTC(),
	}
	payload, err := json.Marshal(record)
	require.NoError(t, err)

	pv, pvc := newLocalPVAndPVC("vol-live", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	historyCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: volumeHistoryStateConfigMapName, Namespace: "default"},
		Data:       map[string]string{"vol-live": string(payload)},
	}
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-a", true), activePodForPVC("pod-live", pvc.Name, "node-a"), historyCM)

	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-live", "node-a").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  "vol-live",
		ImageID:       12,
		RequestedNode: "node-a",
	}, nil).Once()
	provider.On("NodeExists", mock.Anything, "node-a").Return(101, nil).Once()
	provider.On("GetVolumeInNode", mock.Anything, 12, 101).Return("vdb", nil).Once()

	server := NewControllerServer(driver, provider, nil)
	validation := server.validateHotplugQueueRequest(context.Background(), "node-a", "detach", "vol-live")
	require.Equal(t, HotplugQueueValidationPaused, validation.Decision)
	require.Equal(t, queueReasonSameNodeReuseRequired, validation.Reason)
	provider.AssertExpectations(t)
}

func TestValidateHotplugQueueRequestManualRecoveryStillWinsOverOrphanTeardown(t *testing.T) {
	until := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	pv, pvc := newLocalPVAndPVC("vol-manual-orphan", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationRecoveryMode:      recoveryModeManual,
		annotationRecoveryModeUntil: until,
	})
	pvc.Spec.VolumeName = pv.Name
	pv.Status.Phase = corev1.VolumeReleased

	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-a", true))
	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-manual-orphan", "node-a").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  "vol-manual-orphan",
		ImageID:       13,
		RequestedNode: "node-a",
	}, nil).Once()
	provider.On("NodeExists", mock.Anything, "node-a").Return(101, nil).Once()
	provider.On("GetVolumeInNode", mock.Anything, 13, 101).Return("vdb", nil).Once()

	server := NewControllerServer(driver, provider, nil)
	validation := server.validateHotplugQueueRequest(context.Background(), "node-a", "detach", "vol-manual-orphan")
	require.Equal(t, HotplugQueueValidationPaused, validation.Decision)
	require.Equal(t, queueReasonRecoveryModeManual, validation.Reason)
	provider.AssertExpectations(t)
}

func TestRunLocalDiskReprobeCommandCleansOnlyRequestedVolume(t *testing.T) {
	originalRoot := localDiskSessionRootPath
	localDiskSessionRootPath = t.TempDir()
	t.Cleanup(func() {
		localDiskSessionRootPath = originalRoot
	})

	stageOne := filepath.Join(t.TempDir(), "stage-one")
	stageTwo := filepath.Join(t.TempDir(), "stage-two")
	require.NoError(t, os.MkdirAll(stageOne, 0o755))
	require.NoError(t, os.MkdirAll(stageTwo, 0o755))

	store := newLocalDiskSessionStore(localDiskSessionRootPath)
	require.NoError(t, store.Save(localDiskSession{VolumeID: "vol-1", StagingTargetPath: stageOne}))
	require.NoError(t, store.Save(localDiskSession{VolumeID: "vol-2", StagingTargetPath: stageTwo}))

	report, err := runLocalDiskReprobeCommandWithMounter(context.Background(), nil, LocalDiskReprobeOptions{VolumeID: "vol-1"}, mount.NewFakeMounter(nil))
	require.NoError(t, err)
	require.True(t, report.CleanupPerformed)
	require.NotNil(t, report.After)
	require.Equal(t, "missing", report.After.CurrentStageMountState)

	_, exists, err := store.Load("vol-1")
	require.NoError(t, err)
	require.False(t, exists)

	_, exists, err = store.Load("vol-2")
	require.NoError(t, err)
	require.True(t, exists)

	_, err = os.Stat(stageOne)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(stageTwo)
	require.NoError(t, err)
}

func TestRunLocalDiskReprobeCommandRequiresRecoveryModeManualForPublishedTargets(t *testing.T) {
	originalRoot := localDiskSessionRootPath
	localDiskSessionRootPath = t.TempDir()
	t.Cleanup(func() {
		localDiskSessionRootPath = originalRoot
	})

	stagePath := filepath.Join(t.TempDir(), "stage-published")
	require.NoError(t, os.MkdirAll(stagePath, 0o755))

	store := newLocalDiskSessionStore(localDiskSessionRootPath)
	require.NoError(t, store.Save(localDiskSession{
		VolumeID:          "vol-published",
		StagingTargetPath: stagePath,
		PublishedTargets: []localDiskPublishedTarget{{
			TargetPath: "/var/lib/kubelet/pods/example/volumes/kubernetes.io~csi/mount",
		}},
	}))

	_, err := runLocalDiskReprobeCommandWithMounter(context.Background(), nil, LocalDiskReprobeOptions{VolumeID: "vol-published"}, mount.NewFakeMounter(nil))
	require.Error(t, err)

	_, err = runLocalDiskReprobeCommandWithMounter(context.Background(), nil, LocalDiskReprobeOptions{
		VolumeID:       "vol-published",
		AllowPublished: true,
	}, mount.NewFakeMounter(nil))
	require.Error(t, err)

	require.NoError(t, store.Save(localDiskSession{
		VolumeID:          "vol-published",
		StagingTargetPath: stagePath,
		RecoveryMode:      recoveryModeManual,
		PublishedTargets: []localDiskPublishedTarget{{
			TargetPath: "/var/lib/kubelet/pods/example/volumes/kubernetes.io~csi/mount",
		}},
	}))

	report, err := runLocalDiskReprobeCommandWithMounter(context.Background(), nil, LocalDiskReprobeOptions{
		VolumeID:       "vol-published",
		AllowPublished: true,
	}, mount.NewFakeMounter(nil))
	require.NoError(t, err)
	require.True(t, report.PublishedGuardBypassed)
}
