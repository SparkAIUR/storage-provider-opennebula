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

func TestAttachmentReconcilerSkipsSharedFilesystemVolumeAttachments(t *testing.T) {
	sharedID, err := opennebula.EncodeSharedVolumeID(opennebula.SharedVolumeMetadata{
		DatastoreID:    125,
		Mode:           opennebula.SharedVolumeModeDynamic,
		FSName:         "cephfs",
		SubvolumeGroup: "csi",
		Subpath:        "/volumes/csi/test",
		Backend:        "cephfs",
		SubvolumeName:  "test",
	})
	require.NoError(t, err)

	sharedPrefixPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-cephfs-prefix"},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			ClaimRef: &corev1.ObjectReference{
				Namespace: "default",
				Name:      "pvc-cephfs-prefix",
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       DefaultDriverName,
					VolumeHandle: sharedID,
				},
			},
		},
	}
	sharedPrefixPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pvc-cephfs-prefix",
		},
	}

	metadataPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv-cephfs-metadata",
			Annotations: map[string]string{
				annotationBackend: "cephfs",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			ClaimRef: &corev1.ObjectReference{
				Namespace: "default",
				Name:      "pvc-cephfs-metadata",
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       DefaultDriverName,
					VolumeHandle: "legacy-sharedfs-handle",
					VolumeAttributes: map[string]string{
						annotationBackend: "cephfs",
					},
				},
			},
		},
	}
	metadataPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pvc-cephfs-metadata",
		},
	}

	vaPrefixA := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-cephfs-prefix-a"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &sharedPrefixPV.Name,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	vaPrefixB := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-cephfs-prefix-b"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-b",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &sharedPrefixPV.Name,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	vaMetadata := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-cephfs-metadata"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-c",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &metadataPV.Name,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "cephfs-user",
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: sharedPrefixPVC.Name,
					},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	driver := newAttachmentTestDriver(sharedPrefixPV, sharedPrefixPVC, metadataPV, metadataPVC, vaPrefixA, vaPrefixB, vaMetadata, pod)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.staleVASeen["va-cephfs-prefix-a"] = time.Now().Add(-2 * reconciler.staleVAGrace)
	reconciler.staleVASeen["va-cephfs-prefix-b"] = time.Now().Add(-2 * reconciler.staleVAGrace)
	reconciler.staleVASeen["va-cephfs-metadata"] = time.Now().Add(-2 * reconciler.staleVAGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))

	for _, name := range []string{"va-cephfs-prefix-a", "va-cephfs-prefix-b", "va-cephfs-metadata"} {
		_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(context.Background(), name, metav1.GetOptions{})
		assert.NoError(t, err, "shared filesystem VolumeAttachment %s should not be deleted", name)
		assert.NotContains(t, reconciler.staleVASeen, name, "shared filesystem VolumeAttachment %s should not be tracked as stale", name)
	}
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
