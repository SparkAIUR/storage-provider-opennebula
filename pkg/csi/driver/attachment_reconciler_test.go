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
	ktesting "k8s.io/client-go/testing"
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
	driver.volumeQuarantine = NewVolumeQuarantineManager(runtime, "default")
	driver.hostArtifactQuarantine = NewHostArtifactQuarantineManager(runtime, "default")
	return driver
}

func TestAttachmentReconcilerDetachesOrphanAttachment(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.UID = "pv-owned"
	pvc.UID = "pvc-owned"
	pv.Spec.ClaimRef.UID = pvc.UID
	pvc.Spec.VolumeName = pv.Name
	node := newReadyNode("node-a", true)
	node.UID = "node-owned"
	driver := newAttachmentTestDriver(pv, pvc, node)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{
		VolumeHandle: "vol-1",
		ImageID:      1,
		NodeName:     "node-a",
		NodeID:       101,
		Backend:      "local",
	}}, opennebula.VMNameInventory{"node-a": {101}}, nil).Once()
	mockProvider.On("NodeExists", mock.Anything, "node-a").Return(101, nil)

	physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: mockProvider, attached: true, imageID: 1, vmID: 101, diskID: 0}
	mockProvider.On("DetachVolume", mock.Anything, "vol-1", "node-a").Run(func(mock.Arguments) { physical.attached = false }).Return(nil).Once()
	mockProvider.On("ResolveVolumeSizeBytes", mock.Anything, "vol-1").Return(int64(1024), nil).Once()

	server := NewControllerServer(driver, physical, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.orphanSeen["vol-1@node-a@pv-owned"] = time.Now().Add(-2 * reconciler.orphanGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))
	mockProvider.AssertExpectations(t)
}

func TestAttachmentReconcilerDeletesStaleVolumeAttachment(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-1", UID: "va-owned", ResourceVersion: "1"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pv.Name,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	pv.UID = "pv-owned"
	pv.ResourceVersion = "1"
	node := newReadyNode("node-a", true)
	node.UID = "node-owned"
	driver := newAttachmentTestDriver(pv, pvc, va, node)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("VolumeExists", mock.Anything, "vol-1").Return(1, 1024, nil)
	mockProvider.On("NodeExists", mock.Anything, "node-a").Return(101, nil)
	mockProvider.On("GetVolumeInNode", mock.Anything, 1, 101).Return("", opennebula.ErrAttachmentNotFound)
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil, nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.staleVASeen["va-owned"] = time.Now().Add(-2 * reconciler.staleVAGrace)

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
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil, nil).Once()

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
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil, nil).Once()

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

func TestAttachmentReconcilerPreservesMalformedAndReplacedDeviceReports(t *testing.T) {
	for _, mode := range []string{"null", "wrong-volume", "unknown-state", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC(cephRestartVolume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			node := newReadyNode("node-a", true)
			key := localDeviceReportKey(node.Name, cephRestartVolume)
			current := `{"node":"node-a","volumeID":"pvc-ceph-restart","failureClass":"wrong_device_identity"}`
			raw := current
			switch mode {
			case "null":
				raw = "null"
			case "wrong-volume":
				raw = `{"node":"node-a","volumeID":"gone","failureClass":"wrong_device_identity"}`
			case "unknown-state":
				raw = `{"node":"node-a","volumeID":"pvc-ceph-restart","confirmationState":"unknown"}`
			case "replaced":
				key = localDeviceReportKey("gone-node", cephRestartVolume)
				raw = `{"node":"gone-node","volumeID":"pvc-ceph-restart","failureClass":"missing_device"}`
				current = "null"
			}
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: localDeviceStateConfigMapName, UID: "report-map", ResourceVersion: "1"},
				Data:       map[string]string{key: raw},
			}
			driver := newAttachmentTestDriver(pv, pvc, node, cm)
			client := driver.kubeRuntime.client.(*fake.Clientset)
			if mode == "replaced" {
				swapped := false
				client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					if swapped || action.(ktesting.GetAction).GetName() != cm.Name {
						return false, nil, nil
					}
					swapped = true
					replaced := cm.DeepCopy()
					replaced.ResourceVersion = "2"
					replaced.Data[key] = current
					err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), replaced, cm.Namespace)
					return true, cm.DeepCopy(), err
				})
			} else {
				current = raw
			}
			base := new(MockOpenNebulaVolumeProviderTestify)
			base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{VolumeHandle: cephRestartVolume, ImageID: 3137, NodeName: node.Name, NodeID: 575, DiskID: 1, Backend: "ceph"}}, opennebula.VMNameInventory{node.Name: {575}}, nil)
			base.On("NodeExists", mock.Anything, node.Name).Return(575, nil)
			base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1_073_741_824, nil)
			physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: base, attached: true, imageID: 3137, vmID: 575, diskID: 1}
			r := NewAttachmentReconciler(NewControllerServer(driver, physical, nil))
			r.orphanGrace = time.Nanosecond
			require.NoError(t, r.ReconcileOnce(ctx))
			require.NoError(t, r.ReconcileOnce(ctx))
			after, err := client.CoreV1().ConfigMaps(cm.Namespace).Get(ctx, cm.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, current, after.Data[key])
			base.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}
