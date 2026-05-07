package driver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func newAttachmentTestDriver(objects ...runtime.Object) *Driver {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, true)
	runtime := &KubeRuntime{client: fake.NewSimpleClientset(objects...), enabled: true}
	driver := &Driver{
		name:           DefaultDriverName,
		PluginConfig:   cfg,
		kubeRuntime:    runtime,
		metrics:        NewDriverMetrics("test", "test"),
		operationLocks: NewOperationLocks(),
		hotplugGuard:   NewHotplugGuard(time.Minute),
	}
	driver.stickyAttachments = NewStickyAttachmentManager(runtime, "default")
	driver.volumeHistory = NewVolumeHistoryManager(runtime, "default")
	driver.volumeRepairState = NewVolumeRepairStateManager(runtime, "default")
	return driver
}

func TestAttachmentReconcilerDetachesOrphanAttachment(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	driver := newAttachmentTestDriver(pv, pvc)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{
		VolumeHandle: "vol-1",
		ImageID:      1,
		NodeName:     "node-a",
		NodeID:       101,
		Backend:      "local",
	}}, nil).Once()
	mockProvider.On("NodeExists", mock.Anything, "node-a").Return(101, nil).Once()
	mockProvider.On("GetVolumeInNode", mock.Anything, 1, 101).Return("vdb", nil).Twice()
	mockProvider.On("DetachVolume", mock.Anything, "vol-1", "node-a").Return(nil).Once()
	mockProvider.On("ResolveVolumeSizeBytes", mock.Anything, "vol-1").Return(int64(1024), nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.orphanSeen["vol-1@node-a"] = time.Now().Add(-2 * reconciler.orphanGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))
	mockProvider.AssertExpectations(t)
}

func TestAttachmentReconcilerDeletesStaleVolumeAttachment(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-1"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pv.Name,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	driver := newAttachmentTestDriver(pv, pvc, va)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.staleVASeen["va-1"] = time.Now().Add(-2 * reconciler.staleVAGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))

	_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(context.Background(), "va-1", metav1.GetOptions{})
	assert.Error(t, err)
	mockProvider.AssertExpectations(t)
}

func TestAttachmentReconcilerPrunesDeletedVolumeState(t *testing.T) {
	driver := newAttachmentTestDriver()
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
		VolumeID:     "vol-gone",
		NodeID:       "node-a",
		Backend:      "local",
		PVCNamespace: "default",
		PVCName:      "pvc-vol-gone",
		StartedAt:    time.Now().Add(-10 * time.Second),
		ExpiresAt:    time.Now().Add(90 * time.Second),
		GraceSeconds: 90,
		Reason:       "stateful_restart",
	}))
	_, err := driver.volumeHistory.Upsert(context.Background(), "vol-gone", func(state *VolumeHistoryRecord) {
		state.VolumeID = "vol-gone"
		state.Backend = "local"
		state.LastSuccessfulNodeName = "node-a"
		state.LastSuccessfulPublishTime = time.Now().UTC()
	})
	require.NoError(t, err)
	_, err = driver.volumeRepairState.Mark(context.Background(), VolumeRepairState{
		VolumeID:        "vol-gone",
		Version:         stateObjectVersion,
		Classification:  repairClassificationMissingImageRecord,
		Message:         "missing image",
		FirstObservedAt: time.Now().UTC(),
		LastObservedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	report := LocalDeviceMissingReport{
		Node:            "node-a",
		VolumeID:        "vol-gone",
		FailureClass:    localDeviceFailureClassMissingDevice,
		FirstObservedAt: time.Now().UTC(),
		LastObservedAt:  time.Now().UTC(),
	}
	payload, err := json.Marshal(report)
	require.NoError(t, err)
	require.NoError(t, driver.kubeRuntime.UpsertConfigMapData(context.Background(), "default", localDeviceStateConfigMapName, map[string]string{
		localDeviceReportKey("node-a", "vol-gone"): string(payload),
	}))

	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))

	_, ok := driver.stickyAttachments.Get("vol-gone")
	assert.False(t, ok)
	assert.Empty(t, driver.volumeHistory.Snapshot())
	assert.Empty(t, driver.volumeRepairState.Snapshot())

	cm, getErr := driver.kubeRuntime.client.CoreV1().ConfigMaps("default").Get(context.Background(), localDeviceStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Empty(t, cm.Data)
	mockProvider.AssertExpectations(t)
}
