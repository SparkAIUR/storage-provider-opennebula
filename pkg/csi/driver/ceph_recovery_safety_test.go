package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type cleanupReadbackProvider struct {
	*MockOpenNebulaVolumeProviderTestify
	attached   bool
	readError  error
	beforeRead func()
}

func (p *cleanupReadbackProvider) GetVolumeInNode(context.Context, int, int) (string, error) {
	if p.beforeRead != nil {
		p.beforeRead()
	}
	if p.readError != nil {
		return "", p.readError
	}
	if !p.attached {
		return "", opennebula.ErrAttachmentNotFound
	}
	return "vdb", nil
}

func TestControllerUnpublishCephObservedHistoryAuthority(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"publish", "stage-only", "no-success", "bootstrap", "bootstrap-source", "bootstrap-with-timestamp"} {
			t.Run(fmt.Sprintf("%v/%s", queued, mode), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				record, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				if mode != "publish" && mode != "bootstrap-with-timestamp" {
					record.LastSuccessfulPublishTime = time.Time{}
				}
				if mode == "stage-only" {
					record.LastSuccessfulStageTime = time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
				}
				if mode == "bootstrap" || mode == "bootstrap-with-timestamp" {
					record.Bootstrapped = true
				}
				if mode == "bootstrap-source" {
					record.EvidenceSource = volumeHistoryEvidenceSourceBootstrap
				}
				payload, err := json.Marshal(record)
				require.NoError(t, err)
				require.NoError(t, f.server.driver.kubeRuntime.UpsertConfigMapData(context.Background(), "default", volumeHistoryStateConfigMapName, map[string]string{cephRestartVolume: string(payload)}))
				response, err := f.unpublish(t)
				if mode == "publish" || mode == "stage-only" {
					require.NoError(t, err)
					require.NotNil(t, response)
				} else {
					require.Error(t, err)
					require.Nil(t, response)
				}
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestControllerUnpublishCephRejectsBackendAndRepairUnknowns(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"missing-backend", "changed-backend", "empty-repair", "missing-repair-id", "wrong-repair-id", "unknown-repair-schema"} {
			t.Run(fmt.Sprintf("%v/%s", queued, mode), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				before, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				switch mode {
				case "missing-backend":
					delete(f.pv.Annotations, annotationBackend)
				case "changed-backend":
					f.pv.Annotations[annotationBackend] = "local"
				default:
					raw := ""
					if mode == "missing-repair-id" {
						raw = `{"classification":"same_node_runtime_attach_unconfirmed"}`
					}
					if mode == "wrong-repair-id" {
						raw = `{"version":1,"volumeID":"another","classification":"same_node_runtime_attach_unconfirmed"}`
					}
					if mode == "unknown-repair-schema" {
						raw = `{"version":2,"volumeID":"pvc-ceph-restart","classification":"same_node_runtime_attach_unconfirmed"}`
					}
					require.NoError(t, f.server.driver.kubeRuntime.UpsertConfigMapData(context.Background(), "default", volumeRepairStateConfigMapName, map[string]string{cephRestartVolume: raw}))
				}
				f.syncObjects(t)
				response, err := f.unpublish(t)
				require.Error(t, err)
				require.Nil(t, response)
				after, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				require.Equal(t, before, after)
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestControllerUnpublishCephDelayedRPCWithoutActiveDemand(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"removed", "terminating", "terminal"} {
			t.Run(fmt.Sprintf("%v/%s", queued, mode), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				_, err := f.unpublish(t)
				require.NoError(t, err)
				f.va.UID = "new-publication"
				f.va.DeletionTimestamp = nil
				f.va.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
				f.syncObjects(t)
				client := f.server.driver.kubeRuntime.client
				if mode == "removed" {
					require.NoError(t, client.CoreV1().Pods(f.pod.Namespace).Delete(context.Background(), f.pod.Name, metav1.DeleteOptions{}))
				} else {
					if mode == "terminating" {
						at := metav1.NewTime(time.Now().Add(-time.Hour))
						f.pod.DeletionTimestamp = &at
					} else {
						f.pod.Status.Phase = corev1.PodSucceeded
					}
					_, err := client.CoreV1().Pods(f.pod.Namespace).Update(context.Background(), f.pod, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				response, err := f.unpublish(t)
				require.Error(t, err)
				require.Nil(t, response)
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestCephRestartCleanupReleaseGuards(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"released", "claim-removed", "terminating", "node-in-use", "repair", "volume-quarantine", "host-quarantine", "observe", "desired-unknown", "provider-unknown", "late-pod", "node-replaced", "already-absent", "readback-unknown", "readback-attached"} {
			t.Run(fmt.Sprintf("%v/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				f := newCephRestartTestFixture(t, queued)
				_, err := f.unpublish(t)
				require.NoError(t, err)
				persisted, err := f.server.driver.kubeRuntime.client.CoreV1().ConfigMaps("default").List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				objects := []kruntime.Object{f.pv.DeepCopy(), f.pvc.DeepCopy(), f.node.DeepCopy()}
				for i := range persisted.Items {
					objects = append(objects, persisted.Items[i].DeepCopy())
				}
				if mode == "terminating" {
					pod := f.pod.DeepCopy()
					at := metav1.NewTime(time.Now().Add(-time.Hour))
					pod.DeletionTimestamp = &at
					objects = append(objects, pod)
				}
				driver := newStickyTestDriver(t, objects...)
				base := new(MockOpenNebulaVolumeProviderTestify)
				physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: base, attached: true}
				base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{VolumeHandle: cephRestartVolume, ImageID: 3137, NodeName: "node-a", NodeID: 575, DiskID: 1, Backend: "ceph"}}, nil)
				base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1024*1024*1024, nil)
				base.On("NodeExists", mock.Anything, "node-a").Return(575, nil)
				if mode == "released" || mode == "claim-removed" || mode == "readback-unknown" || mode == "readback-attached" {
					base.On("DetachVolume", mock.Anything, cephRestartVolume, "node-a").Run(func(mock.Arguments) {
						if mode == "released" || mode == "claim-removed" {
							physical.attached = false
						}
						if mode == "readback-unknown" {
							physical.readError = errors.New("readback unavailable")
						}
					}).Return(nil).Once()
				}
				if mode == "already-absent" {
					physical.attached = false
				}
				switch mode {
				case "claim-removed":
					require.NoError(t, driver.kubeRuntime.client.CoreV1().PersistentVolumeClaims(f.pvc.Namespace).Delete(ctx, f.pvc.Name, metav1.DeleteOptions{}))
				case "node-in-use", "node-replaced":
					node := f.node.DeepCopy()
					if mode == "node-in-use" {
						node.Status.VolumesInUse = []corev1.UniqueVolumeName{"kubernetes.io/csi/" + DefaultDriverName + "^" + cephRestartVolume}
					} else {
						node.UID = "new-node"
					}
					_, err := driver.kubeRuntime.client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
					require.NoError(t, err)
				case "repair":
					_, err := driver.volumeRepairState.Mark(ctx, VolumeRepairState{VolumeID: cephRestartVolume, Classification: "metadata_drift"})
					require.NoError(t, err)
				case "volume-quarantine":
					_, _, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: cephRestartVolume, Reason: "metadata_drift"}, 1, time.Hour)
					require.NoError(t, err)
				case "host-quarantine":
					_, _, err := driver.hostArtifactQuarantine.MarkFailure(ctx, HostArtifactQuarantineState{Key: cephRestartVolume + "@host", VolumeID: cephRestartVolume}, 1, time.Hour)
					require.NoError(t, err)
				case "observe":
					pvc := f.pvc.DeepCopy()
					pvc.Annotations[annotationRecoveryMode] = recoveryModeObserve
					_, err := driver.kubeRuntime.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
					require.NoError(t, err)
				case "desired-unknown":
					driver.kubeRuntime.client.(*fake.Clientset).PrependReactor("list", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
						if action.GetNamespace() == "" {
							return false, nil, nil
						}
						return true, nil, errors.New("Pod observation unavailable")
					})
				case "provider-unknown":
					physical.readError = errors.New("provider observation unavailable")
				case "late-pod":
					physical.beforeRead = func() {
						physical.beforeRead = nil
						_, err := driver.kubeRuntime.client.CoreV1().Pods(f.pod.Namespace).Create(ctx, f.pod.DeepCopy(), metav1.CreateOptions{})
						require.NoError(t, err)
					}
				}
				server := NewControllerServer(driver, physical, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				r := NewAttachmentReconciler(server)
				r.orphanGrace = time.Nanosecond
				firstErr := r.ReconcileOnce(ctx)
				secondErr := r.ReconcileOnce(ctx)
				require.NoError(t, firstErr)
				require.NoError(t, secondErr)
				if mode == "released" || mode == "claim-removed" {
					base.AssertNumberOfCalls(t, "DetachVolume", 1)
					require.False(t, physical.attached)
				} else {
					if mode == "readback-unknown" || mode == "readback-attached" {
						base.AssertNumberOfCalls(t, "DetachVolume", 1)
					} else {
						base.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
					}
					require.Equal(t, mode != "already-absent", physical.attached)
					events, err := driver.kubeRuntime.client.CoreV1().Events("default").List(ctx, metav1.ListOptions{})
					require.NoError(t, err)
					for _, event := range events.Items {
						require.NotEqual(t, eventReasonStuckAttachmentDetached, event.Reason)
					}
				}
			})
		}
	}
}

func TestAttachmentReconcilerStaleIdentityAndAbsence(t *testing.T) {
	for _, mode := range []string{"absent", "foreign-driver", "foreign-attacher", "replacement-before-read", "replacement-before-list", "replacement-at-delete", "replaced-pv", "unknown", "reattached"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := newCephRestartTestFixture(t, false)
			f.va.ResourceVersion = "1"
			if mode == "foreign-driver" {
				f.pv.Spec.CSI.Driver = "foreign.csi.example"
			}
			if mode == "foreign-attacher" {
				f.va.Spec.Attacher = "foreign.csi.example"
			}
			f.syncObjects(t)
			client := f.server.driver.kubeRuntime.client.(*fake.Clientset)
			base := new(MockOpenNebulaVolumeProviderTestify)
			base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil)
			base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1024, nil)
			base.On("NodeExists", mock.Anything, "node-a").Return(575, nil)
			physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: base}
			if mode == "unknown" {
				physical.readError = errors.New("lookup unavailable")
			}
			if mode == "reattached" {
				physical.attached = true
			}
			replace := func() {
				replacement := f.va.DeepCopy()
				replacement.UID = "replacement"
				replacement.ResourceVersion = "2"
				require.NoError(t, client.Tracker().Update(storagev1.SchemeGroupVersion.WithResource("volumeattachments"), replacement, ""))
			}
			if mode == "replacement-before-read" {
				physical.beforeRead = replace
			}
			if mode == "replacement-before-list" {
				replace()
			}
			if mode == "replaced-pv" {
				physical.beforeRead = func() {
					replacement := f.pv.DeepCopy()
					replacement.UID = "replacement-pv"
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), replacement, ""))
				}
			}
			deletes := 0
			client.PrependReactor("delete", "volumeattachments", func(action ktesting.Action) (bool, kruntime.Object, error) {
				deletes++
				options := action.(ktesting.DeleteAction).GetDeleteOptions()
				require.NotNil(t, options.Preconditions)
				require.Equal(t, f.va.UID, *options.Preconditions.UID)
				require.Equal(t, "1", *options.Preconditions.ResourceVersion)
				if mode == "replacement-at-delete" {
					replace()
					return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "storage.k8s.io", Resource: "volumeattachments"}, f.va.Name, errors.New("UID precondition failed"))
				}
				return false, nil, nil
			})
			r := NewAttachmentReconciler(NewControllerServer(f.server.driver, physical, nil))
			r.staleVASeen[string(f.va.UID)] = time.Now().Add(-2 * r.staleVAGrace)
			require.NoError(t, r.ReconcileOnce(ctx))
			va, err := client.StorageV1().VolumeAttachments().Get(ctx, f.va.Name, metav1.GetOptions{})
			if mode == "absent" {
				require.True(t, apierrors.IsNotFound(err))
				require.Equal(t, 1, deletes)
			} else {
				require.NoError(t, err)
				if mode == "replacement-before-read" || mode == "replacement-before-list" || mode == "replacement-at-delete" {
					require.EqualValues(t, "replacement", va.UID)
				}
				if mode != "replacement-at-delete" {
					require.Zero(t, deletes)
				}
			}
		})
	}
}

func TestAttachmentReconcilerDoesNotCombineForeignHandleOwnership(t *testing.T) {
	ctx := context.Background()
	f := newCephRestartTestFixture(t, false)
	f.va.ResourceVersion = "1"
	f.syncObjects(t)
	foreignPV := f.pv.DeepCopy()
	foreignPV.Name = "aa-foreign-pv"
	foreignPV.UID = "foreign-pv"
	foreignPV.Spec.CSI.Driver = "foreign.csi.example"
	foreignVA := f.va.DeepCopy()
	foreignVA.Name = "foreign-va"
	foreignVA.UID = "foreign-va-uid"
	foreignVA.Spec.Attacher = "foreign.csi.example"
	foreignVA.Spec.Source.PersistentVolumeName = &foreignPV.Name
	client := f.server.driver.kubeRuntime.client
	_, err := client.CoreV1().PersistentVolumes().Create(ctx, foreignPV, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = client.StorageV1().VolumeAttachments().Create(ctx, foreignVA, metav1.CreateOptions{})
	require.NoError(t, err)
	base := new(MockOpenNebulaVolumeProviderTestify)
	base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil)
	base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1024, nil)
	base.On("NodeExists", mock.Anything, "node-a").Return(575, nil)
	physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: base}
	r := NewAttachmentReconciler(NewControllerServer(f.server.driver, physical, nil))
	r.staleVASeen[string(f.va.UID)] = time.Now().Add(-2 * r.staleVAGrace)
	r.staleVASeen[string(foreignVA.UID)] = time.Now().Add(-2 * r.staleVAGrace)
	require.NoError(t, r.ReconcileOnce(ctx))
	_, err = client.StorageV1().VolumeAttachments().Get(ctx, f.va.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	observed, err := client.StorageV1().VolumeAttachments().Get(ctx, foreignVA.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, foreignVA.UID, observed.UID)
	base.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
}
