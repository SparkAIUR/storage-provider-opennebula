package driver

import (
	"context"
	"encoding/json"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	unusable := newReadyNode("node-unusable", true)
	driver := newAttachmentTestDriver(pv, pvc, node, unusable)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{
		VolumeHandle: "vol-1",
		ImageID:      1,
		NodeName:     "node-a",
		NodeID:       101,
		Backend:      "local",
	}}, opennebula.VMNameInventory{"node-a": {101}, "node-unusable": {0}}, nil).Once()
	mockProvider.On("NodeExists", mock.Anything, "node-a").Return(101, nil)

	physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: mockProvider, attached: true, imageID: 1, vmID: 101, diskID: 0}
	mockProvider.On("DetachVolume", mock.Anything, "vol-1", "node-a").Run(func(mock.Arguments) { physical.attached = false }).Return(nil).Once()
	mockProvider.On("ResolveVolumeSizeBytes", mock.Anything, "vol-1").Return(int64(1024), nil).Once()

	kube := driver.kubeRuntime.client.(*fake.Clientset)
	replacement := pvc.DeepCopy()
	replacement.UID = "replacement"
	replacement.Spec.VolumeName = ""
	pvcResource := corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
	replaced := false
	kube.PrependReactor("get", "persistentvolumeclaims", func(ktesting.Action) (bool, runtime.Object, error) {
		if !physical.attached && !replaced {
			require.NoError(t, kube.Tracker().Update(pvcResource, replacement, pvc.Namespace))
			replaced = true
			return true, pvc.DeepCopy(), nil
		}
		return false, nil, nil
	})

	server := NewControllerServer(driver, physical, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.orphanSeen["vol-1@node-a@pv-owned"] = time.Now().Add(-2 * reconciler.orphanGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))
	mockProvider.AssertExpectations(t)
	require.True(t, replaced)
	events, err := kube.CoreV1().Events(pvc.Namespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, events.Items, 1)
	require.Equal(t, eventReasonStuckAttachmentDetached, events.Items[0].Reason)
	require.Equal(t, pvc.UID, events.Items[0].InvolvedObject.UID)
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
		ObjectMeta: metav1.ObjectMeta{Name: "pv-cephfs-prefix", Annotations: map[string]string{annotationBackend: "local"}},
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
	for _, binding := range []struct {
		pv  *corev1.PersistentVolume
		pvc *corev1.PersistentVolumeClaim
	}{{sharedPrefixPV, sharedPrefixPVC}, {metadataPV, metadataPVC}} {
		binding.pv.UID = types.UID(binding.pv.Name)
		binding.pv.ResourceVersion = "1"
		binding.pvc.UID = types.UID(binding.pvc.Name)
		binding.pvc.Spec.VolumeName = binding.pv.Name
		binding.pv.Spec.ClaimRef.UID = binding.pvc.UID
	}
	vas := []*storagev1.VolumeAttachment{vaPrefixA, vaPrefixB, vaMetadata}
	for _, va := range vas {
		va.UID = types.UID(va.Name)
		va.ResourceVersion = "1"
	}
	driver := newAttachmentTestDriver(sharedPrefixPV, sharedPrefixPVC, metadataPV, metadataPVC, vaPrefixA, vaPrefixB, vaMetadata, pod,
		newReadyNode("node-a", true), newReadyNode("node-b", true), newReadyNode("node-c", true))
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil, nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	for _, va := range vas {
		reconciler.staleVASeen[string(va.UID)] = time.Now().Add(-2 * reconciler.staleVAGrace)
	}

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))

	for _, va := range vas {
		_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(context.Background(), va.Name, metav1.GetOptions{})
		assert.NoError(t, err, "shared filesystem VolumeAttachment %s should not be deleted", va.Name)
		assert.NotContains(t, reconciler.staleVASeen, string(va.UID), "shared filesystem VolumeAttachment %s should not be tracked as stale", va.Name)
	}
	mockProvider.AssertExpectations(t)
}

func TestAttachmentReconcilerPrunesDeletedVolumeState(t *testing.T) {
	for _, live := range []bool{false, true} {
		t.Run(fmt.Sprintf("foreign-live=%t", live), func(t *testing.T) {
			const volume = "vol-state"
			ctx := context.Background()
			objects := []runtime.Object{newReadyNode("node-a", true)}
			if live {
				pv, _ := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				pv.Spec.CSI.Driver = "foreign.csi.example"
				objects = append(objects, pv)
			}
			driver := newAttachmentTestDriver(objects...)
			driver.volumeRecoveryControl = NewVolumeRecoveryControlManager(driver.kubeRuntime, "default")
			_, err := driver.volumeRecoveryControl.Upsert(ctx, volume, func(state *VolumeRecoveryControlState) {
				state.Mode = recoveryModeManual
			})
			require.NoError(t, err)
			require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
				VolumeID:     volume,
				NodeID:       "node-a",
				Backend:      "local",
				PVCNamespace: "default",
				PVCName:      "pvc-vol-gone",
				StartedAt:    time.Now().Add(-10 * time.Second),
				ExpiresAt:    time.Now().Add(90 * time.Second),
				GraceSeconds: 90,
				Reason:       "stateful_restart",
			}))
			_, err = driver.volumeHistory.Upsert(context.Background(), volume, func(state *VolumeHistoryRecord) {
				state.VolumeID = volume
				state.Backend = "local"
				state.LastSuccessfulNodeName = "node-a"
				state.LastSuccessfulPublishTime = time.Now().UTC()
			})
			require.NoError(t, err)
			_, err = driver.volumeRepairState.Mark(context.Background(), VolumeRepairState{
				VolumeID:        volume,
				Version:         stateObjectVersion,
				Classification:  repairClassificationMissingImageRecord,
				Message:         "missing image",
				FirstObservedAt: time.Now().UTC(),
				LastObservedAt:  time.Now().UTC(),
			})
			require.NoError(t, err)
			report := LocalDeviceMissingReport{
				Node:            "node-a",
				VolumeID:        volume,
				FailureClass:    localDeviceFailureClassMissingDevice,
				FirstObservedAt: time.Now().UTC(),
				LastObservedAt:  time.Now().UTC(),
				Attempts:        1,
			}
			payload, err := json.Marshal(report)
			require.NoError(t, err)
			require.NoError(t, driver.kubeRuntime.UpsertConfigMapData(context.Background(), "default", localDeviceStateConfigMapName, map[string]string{
				localDeviceReportKey("node-a", volume): string(payload),
			}))

			mockProvider := &MockOpenNebulaVolumeProviderTestify{}
			mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil, nil).Once()

			server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
			reconciler := NewAttachmentReconciler(server)

			require.NoError(t, reconciler.ReconcileOnce(context.Background()))

			_, sticky := driver.stickyAttachments.Get(volume)
			_, history := driver.volumeHistory.Get(volume)
			_, repair := driver.volumeRepairState.Get(volume)
			_, control := driver.volumeRecoveryControl.Get(volume)
			cm, getErr := driver.kubeRuntime.client.CoreV1().ConfigMaps("default").Get(ctx, localDeviceStateConfigMapName, metav1.GetOptions{})
			require.NoError(t, getErr)
			_, reportPresent := cm.Data[localDeviceReportKey("node-a", volume)]
			assert.Equal(t, live, sticky, "sticky hold")
			assert.Equal(t, live, history, "volume history")
			assert.Equal(t, live, repair, "repair state")
			assert.Equal(t, live, control, "recovery control")
			assert.Equal(t, live, reportPresent, "local-device report")
			mockProvider.AssertExpectations(t)
		})
	}
}

func TestAttachmentReconcilerPreservesMalformedAndReplacedDeviceReports(t *testing.T) {
	for _, mode := range []string{"null", "wrong-volume", "unknown-state", "incomplete-stale", "legacy-pruned", "pruned", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC(cephRestartVolume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			node := newReadyNode("node-a", true)
			key := localDeviceReportKey(node.Name, cephRestartVolume)
			now := time.Now().UTC()
			report := LocalDeviceMissingReport{Node: node.Name, VolumeID: cephRestartVolume, FailureClass: localDeviceFailureClassWrongIdentity,
				FirstObservedAt: now, LastObservedAt: now, Attempts: 2}
			switch mode {
			case "wrong-volume":
				report.VolumeID = "gone"
			case "unknown-state":
				report.ConfirmationState = "unknown"
			case "incomplete-stale", "legacy-pruned", "pruned", "replaced":
				report.Node = "gone-node"
				key = localDeviceReportKey(report.Node, cephRestartVolume)
				report.FailureClass = localDeviceFailureClassMissingDevice
				if mode == "legacy-pruned" || mode == "incomplete-stale" {
					report.FailureClass = ""
				}
				if mode == "incomplete-stale" {
					report = LocalDeviceMissingReport{Node: report.Node, VolumeID: report.VolumeID}
				}
			}
			payload, err := json.Marshal(report)
			require.NoError(t, err)
			raw := string(payload)
			switch mode {
			case "null":
				raw = "null"
			case "incomplete-stale":
				raw = `{"node":"gone-node","volumeID":"pvc-ceph-restart"}`
			}
			current := raw
			if mode == "replaced" {
				report.FailureClass = localDeviceFailureClassWrongIdentity
				report.LastObservedAt = now.Add(time.Second)
				report.Attempts++
				payload, err := json.Marshal(report)
				require.NoError(t, err)
				current = string(payload)
			}
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: localDeviceStateConfigMapName, UID: "report-map", ResourceVersion: "1"},
				Data:       map[string]string{key: raw},
			}
			driver := newAttachmentTestDriver(pv, pvc, node, cm)
			client := driver.kubeRuntime.client.(*fake.Clientset)
			deletable := mode == "pruned" || mode == "legacy-pruned" || mode == "replaced"
			reportReads := 0
			if deletable {
				client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					if action.(ktesting.GetAction).GetName() != cm.Name {
						return false, nil, nil
					}
					reportReads++
					if reportReads == 2 && mode == "replaced" {
						replaced := cm.DeepCopy()
						replaced.ResourceVersion = "2"
						replaced.Data[key] = current
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), replaced, cm.Namespace))
					}
					return false, nil, nil
				})
			}
			base := new(MockOpenNebulaVolumeProviderTestify)
			base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{VolumeHandle: cephRestartVolume, ImageID: 3137, NodeName: node.Name, NodeID: 575, DiskID: 1, Backend: "ceph"}}, opennebula.VMNameInventory{node.Name: {575}}, nil)
			base.On("NodeExists", mock.Anything, node.Name).Return(575, nil)
			base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1_073_741_824, nil)
			physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: base, attached: true, imageID: 3137, vmID: 575, diskID: 1}
			r := NewAttachmentReconciler(NewControllerServer(driver, physical, nil))
			r.orphanGrace = time.Nanosecond
			require.NoError(t, r.ReconcileOnce(ctx))
			if deletable {
				require.Equal(t, 2, reportReads)
			} else {
				require.NoError(t, r.ReconcileOnce(ctx))
			}
			after, err := client.CoreV1().ConfigMaps(cm.Namespace).Get(ctx, cm.Name, metav1.GetOptions{})
			require.NoError(t, err)
			if mode == "pruned" || mode == "legacy-pruned" {
				require.NotContains(t, after.Data, key)
			} else {
				require.Equal(t, current, after.Data[key])
			}
			base.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}
