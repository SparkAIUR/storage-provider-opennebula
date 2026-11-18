package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestControllerMarkedReplacementWithRenewedDemand(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, distinctVM := range []bool{false, true} {
			for _, mode := range []string{"retained", "released-reused", "released-reused-no-va", "released-rebound", "released-unbound", "released-old-pod"} {
				t.Run(fmt.Sprintf("queued=%t/distinctVM=%t/%s", queued, distinctVM, mode), func(t *testing.T) {
					ctx := context.Background()
					const volume = "volume-test"
					pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
					pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
					pvc.Spec.AccessModes = pv.Spec.AccessModes
					pv.Spec.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("128Mi")}
					pvc.Spec.Resources.Requests = pv.Spec.Capacity.DeepCopy()
					pvc.Status.Capacity = pv.Spec.Capacity.DeepCopy()
					original := newReadyNode("node-old", true)
					original.UID = "original-node-uid"
					driver := newStickyTestDriver(t, pv, pvc, original)
					provider, wire := nodeAttachmentProvider(t)
					wire.mu.Lock()
					wire.disks[41], wire.marker = 128, 128*1024*1024
					wire.ceph, wire.version = true, "7.4.0"
					wire.mu.Unlock()
					wire.requirePersistedAttachNonce(t, driver, volume)
					makeServer := func(d *Driver) *ControllerServer {
						d.PluginConfig.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, true)
						server := NewControllerServer(d, provider, nil)
						if queued {
							d.hotplugQueue = NewHotplugQueueManager(nil, "default", d.metrics, 5*time.Second, time.Second)
							d.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
						}
						return server
					}
					server := makeServer(driver)
					client := reviewControllerClient(t, server)
					publish := newPublishReq(volume, original.Name)
					originalReply, err := client.ControllerPublishVolume(ctx, publish)
					require.NoError(t, err)
					require.Equal(t, "vdb", originalReply.GetPublishContext()["volumeName"])
					before, err := server.readAttachmentHistory(ctx, volume)
					require.NoError(t, err)
					require.Equal(t, string(original.UID), before.LastSuccessfulNodeUID)
					setReplacementHistoryVersion(t, driver)

					kube := driver.kubeRuntime.client
					core := kube.CoreV1()
					nodes := core.Nodes()
					require.NoError(t, nodes.Delete(ctx, original.Name, metav1.DeleteOptions{}))
					node := original.DeepCopy()
					node.UID = "replacement-node-uid"
					node.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
					_, err = nodes.Create(ctx, node, metav1.CreateOptions{})
					require.NoError(t, err)
					pods := core.Pods(pvc.Namespace)
					consumer := activePodForPVC("first-consumer", pvc.Name, node.Name)
					consumer.UID, consumer.Status.Phase, consumer.CreationTimestamp = "first-consumer-uid", corev1.PodPending, metav1.Now()
					_, err = pods.Create(ctx, consumer, metav1.CreateOptions{})
					require.NoError(t, err)
					vas := kube.StorageV1().VolumeAttachments()
					va := replacementPublishVA(pv, node)
					_, err = vas.Create(ctx, va, metav1.CreateOptions{})
					require.NoError(t, err)
					wire.mu.Lock()
					delete(wire.disks, 41)
					target := 41
					if distinctVM {
						target = 42
						wire.vmNames = map[int]string{41: "retired-vm", 42: node.Name}
					}
					wire.loseAttachReply = true
					wire.mu.Unlock()
					_, err = client.ControllerPublishVolume(ctx, publish)
					require.Error(t, err)
					assertWire := func(detached bool) {
						t.Helper()
						wire.mu.Lock()
						defer wire.mu.Unlock()
						expected := []string{fmt.Sprintf("attach:%d:false", target)}
						if detached {
							expected = append(expected, fmt.Sprintf("detach:%d", target))
						}
						require.Equal(t, expected, wire.mutations)
						require.Equal(t, 1, wire.attachAttempts)
						require.Equal(t, len(expected)-1, wire.detachAttempts)
					}
					assertWire(false)
					lost, err := server.readAttachmentHistory(ctx, volume)
					require.NoError(t, err)
					require.NotNil(t, lost.PendingReplacementAttach)
					require.Equal(t, string(va.UID), lost.PendingReplacementAttach.VAUID)
					require.Equal(t, before.LastSuccessfulPublishTime, lost.LastSuccessfulPublishTime)
					require.Equal(t, before.LastSuccessfulStageTime, lost.LastSuccessfulStageTime)
					require.Equal(t, before.LastSuccessfulNodeUID, lost.LastSuccessfulNodeUID)
					require.Equal(t, before.LastSuccessfulOpenNebulaVMID, lost.LastSuccessfulOpenNebulaVMID)

					if mode != "released-old-pod" {
						require.NoError(t, pods.Delete(ctx, consumer.Name, metav1.DeleteOptions{}))
					}
					restarted := newStickyTestDriver(t)
					restarted.kubeRuntime.client = kube
					require.NoError(t, restarted.volumeHistory.LoadFromConfigMap(ctx))
					server = makeServer(restarted)
					client = reviewControllerClient(t, server)
					unpublish := &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name}

					if mode != "retained" {
						now := metav1.Now()
						va.DeletionTimestamp = &now
						va.Status.Attached = true
						_, err = vas.Update(ctx, va, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.NoError(t, core.PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
						pv.Status.Phase = corev1.VolumeReleased
						_, err = core.PersistentVolumes().UpdateStatus(ctx, pv, metav1.UpdateOptions{})
						require.NoError(t, err)
						reused := pvc.DeepCopy()
						reused.UID, reused.CreationTimestamp = "reused-claim-uid", metav1.Now()
						if mode != "released-rebound" {
							other, _ := newLocalPVAndPVC("different-provider-handle", pv.Spec.AccessModes, nil)
							other.Status.Phase = corev1.VolumeBound
							other.Spec.ClaimRef = &corev1.ObjectReference{Name: reused.Name, Namespace: reused.Namespace, UID: reused.UID}
							_, err = core.PersistentVolumes().Create(ctx, other, metav1.CreateOptions{})
							require.NoError(t, err)
							reused.Spec.VolumeName = other.Name
						}
						if mode == "released-unbound" {
							reused.Spec.VolumeName = ""
							reused.Status.Phase = corev1.ClaimPending
						}
						_, err = core.PersistentVolumeClaims(pvc.Namespace).Create(ctx, reused, metav1.CreateOptions{})
						require.NoError(t, err)
						if mode == "released-reused-no-va" {
							require.NoError(t, vas.Delete(ctx, va.Name, metav1.DeleteOptions{}))
							reconciler := NewAttachmentReconciler(server)
							reconciler.orphanGrace = 0
							require.NoError(t, reconciler.ReconcileOnce(ctx))
							require.NoError(t, reconciler.ReconcileOnce(ctx))
							assertWire(true)
						} else {
							_, err = client.ControllerUnpublishVolume(ctx, unpublish)
							if mode == "released-reused" {
								require.NoError(t, err)
								assertWire(true)
								_, err = client.ControllerUnpublishVolume(ctx, unpublish)
								require.NoError(t, err)
								assertWire(true)
							} else {
								require.Error(t, err)
								assertWire(false)
							}
						}
						current, err := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, before.LastSuccessfulNodeUID, current.LastSuccessfulNodeUID)
						if mode == "released-reused-no-va" {
							require.Nil(t, current.PendingReplacementAttach)
						}
						return
					}

					waiting := activePodForPVC("waiting-consumer", pvc.Name, node.Name)
					waiting.UID, waiting.Status.Phase, waiting.CreationTimestamp = "waiting-consumer-uid", corev1.PodPending, metav1.Now()
					waiting.Spec.Containers = []corev1.Container{{Name: "app", Image: "app"}}
					setPremountKubeletFixtureStatus(waiting, "ContainerCreating")
					_, err = pods.Create(ctx, waiting, metav1.CreateOptions{})
					require.NoError(t, err)
					node.Status.VolumesInUse = []corev1.UniqueVolumeName{corev1.UniqueVolumeName("kubernetes.io/csi/" + DefaultDriverName + "^" + volume)}
					_, err = nodes.UpdateStatus(ctx, node, metav1.UpdateOptions{})
					require.NoError(t, err)
					_, err = client.ControllerUnpublishVolume(ctx, unpublish)
					require.Error(t, err, "a non-deleting VA cannot authorize retention")
					now := metav1.Now()
					va.DeletionTimestamp = &now
					va.Status.Attached = distinctVM
					_, err = vas.Update(ctx, va, metav1.UpdateOptions{})
					require.NoError(t, err)

					wire.mu.Lock()
					nonce := wire.attachAttempt
					wire.attachAttempt = ""
					wire.mu.Unlock()
					_, err = client.ControllerUnpublishVolume(ctx, unpublish)
					require.Error(t, err, "an unmarked disk cannot authorize retention")
					wire.mu.Lock()
					wire.attachAttempt = nonce
					wire.pending = true
					wire.mu.Unlock()
					_, err = client.ControllerUnpublishVolume(ctx, unpublish)
					require.Error(t, err, "an unfinished resize cannot authorize retention")
					wire.mu.Lock()
					wire.pending, wire.ownerCount = false, 2
					wire.mu.Unlock()
					_, err = client.ControllerUnpublishVolume(ctx, unpublish)
					require.Error(t, err, "ambiguous ownership cannot authorize retention")
					wire.mu.Lock()
					wire.ownerCount = 0
					wire.mu.Unlock()

					pv.Annotations[annotationRecoveryMode] = recoveryModeObserve
					_, err = core.PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
					require.NoError(t, err)
					_, err = client.ControllerUnpublishVolume(ctx, unpublish)
					require.Error(t, err, "observe mode blocks automatic retention")
					delete(pv.Annotations, annotationRecoveryMode)
					_, err = core.PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
					require.NoError(t, err)

					for range 2 {
						_, err = client.ControllerUnpublishVolume(ctx, unpublish)
						require.NoError(t, err)
						current, err := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, lost, current)
						assertWire(false)
					}

					successor := replacementPublishVA(pv, node)
					successor.Name, successor.UID = "successor-va", "successor-uid"
					_, err = vas.Create(ctx, successor, metav1.CreateOptions{})
					require.NoError(t, err)
					_, err = client.ControllerPublishVolume(ctx, publish)
					require.Error(t, err, "the original pinned VA has not retired")
					require.NoError(t, vas.Delete(ctx, va.Name, metav1.DeleteOptions{}))
					_, err = client.ControllerUnpublishVolume(ctx, unpublish)
					require.Error(t, err, "a delayed Unpublish cannot acknowledge the successor")
					reply, err := client.ControllerPublishVolume(ctx, publish)
					require.NoError(t, err)
					require.Equal(t, "vdb", reply.GetPublishContext()["volumeName"])
					require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
					current, err := server.readAttachmentHistory(ctx, volume)
					require.NoError(t, err)
					require.Nil(t, current.PendingReplacementAttach)
					require.Equal(t, string(node.UID), current.LastSuccessfulNodeUID)
					require.Equal(t, target, current.LastSuccessfulOpenNebulaVMID)
					require.True(t, current.LastSuccessfulPublishTime.After(before.LastSuccessfulPublishTime))
					require.Equal(t, before.LastSuccessfulStageTime, current.LastSuccessfulStageTime)
					_, err = client.ControllerUnpublishVolume(ctx, unpublish)
					require.Error(t, err, "a late Unpublish cannot use the newly published successor")
					assertWire(false)
				})
			}
		}
	}
}
