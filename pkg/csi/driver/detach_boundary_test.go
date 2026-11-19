package driver

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type detachAuthorizationFixture struct {
	*opennebula.PersistentDiskVolumeProvider
	beforeAuthorize func()
}

func (p detachAuthorizationFixture) DetachVolumeWithAuthority(ctx context.Context, volume, node string, vm int, backend string, owner opennebula.DetachOwnerCheck, authorize opennebula.DetachAuthorization) (bool, error) {
	return p.PersistentDiskVolumeProvider.DetachVolumeWithAuthority(ctx, volume, node, vm, backend, owner, func(ctx context.Context, image, vm, disk int) error {
		p.beforeAuthorize()
		return authorize(ctx, image, vm, disk)
	})
}

func TestDetachRechecksPlacementDeadlineAtDispatch(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, background := range []bool{false, true} {
			for _, expire := range []bool{false, true} {
				t.Run(fmt.Sprintf("queued=%t/background=%t/expire=%t", queued, background, expire), func(t *testing.T) {
					ctx := context.Background()
					const volume = "volume-test"
					pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
					pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
					node, destination := newReadyNode("node-old", true), newReadyNode("node-new", true)
					deadline := time.Now().UTC().Add(time.Minute)
					if expire {
						deadline = time.Now().UTC().Add(2 * time.Second)
					}
					pv.Annotations[annotationRequiredNode] = destination.Name
					pv.Annotations[annotationAllowCrossNodeUntil] = deadline.Format(time.RFC3339Nano)
					objects := []kruntime.Object{pv, pvc, node, destination}
					if !background {
						deleting := metav1.Now()
						objects = append(objects, &storagev1.VolumeAttachment{
							ObjectMeta: metav1.ObjectMeta{Name: "source-va", UID: "source-uid", DeletionTimestamp: &deleting, Finalizers: []string{"external-attacher/" + DefaultDriverName}},
							Spec: storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name,
								Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
							Status: storagev1.VolumeAttachmentStatus{Attached: true},
						})
					}
					driver := newStickyTestDriver(t, objects...)
					driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
					_, err := driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
						record.Backend, record.LastSuccessfulNodeName = "local", node.Name
						record.LastSuccessfulPublishTime = time.Now().UTC().Add(-time.Minute)
					})
					require.NoError(t, err)
					var inCallback, expired atomic.Bool
					var podReads atomic.Int32
					if expire {
						driver.kubeRuntime.client.(*fake.Clientset).PrependReactor("list", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
							if inCallback.Load() && action.GetNamespace() == pvc.Namespace && podReads.Add(1) == 2 {
								require.True(t, time.Now().UTC().Before(deadline))
								for remaining := deadline.Sub(time.Now().UTC()); remaining > 0; remaining = deadline.Sub(time.Now().UTC()) {
									time.Sleep(remaining)
								}
								expired.Store(true)
							}
							return false, nil, nil
						})
					}
					provider, wire := nodeAttachmentProvider(t)
					server := NewControllerServer(driver, detachAuthorizationFixture{provider, func() { inCallback.Store(true) }}, nil)
					if queued {
						driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
						driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
					}
					if background {
						r := NewAttachmentReconciler(server)
						r.orphanGrace = 0
						require.NoError(t, r.ReconcileOnce(ctx))
						require.NoError(t, r.ReconcileOnce(ctx))
					} else {
						reply, err := reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
						if expire {
							require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
							require.ErrorContains(t, err, "detach placement grant expired before dispatch")
							require.Nil(t, reply)
						} else {
							require.NoError(t, err)
							require.NotNil(t, reply)
						}
					}
					require.True(t, inCallback.Load())
					wire.mu.Lock()
					defer wire.mu.Unlock()
					if expire {
						require.True(t, expired.Load())
						require.Empty(t, wire.mutations)
						require.Contains(t, wire.disks, 41)
					} else {
						require.Equal(t, []string{"detach:41"}, wire.mutations)
					}
				})
			}
		}
	}
}

func TestProviderDetachRequiresDurableCompletedCapacity(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			state.marker = 0
			state.pending = pending
			state.mu.Unlock()
			require.Error(t, provider.DetachVolume(context.Background(), "volume-test", "node-old"))
			state.mu.Lock()
			defer state.mu.Unlock()
			require.Empty(t, state.mutations)
			require.Equal(t, 256, state.disks[41])
			require.Zero(t, state.marker)
		})
	}
}

func TestProviderLegacyDetachAfterVerifiedExpansion(t *testing.T) {
	provider, state := nodeAttachmentProvider(t)
	state.mu.Lock()
	state.marker = 0
	state.ignoreMarkerWrite = true
	state.mu.Unlock()
	_, err := provider.ExpandVolume(context.Background(), "volume-test", 256*1024*1024, false)
	require.ErrorContains(t, err, "readback failed")
	require.Error(t, provider.DetachVolume(context.Background(), "volume-test", "node-old"))
	state.mu.Lock()
	require.NotContains(t, state.mutations, "detach:41")
	require.Zero(t, state.marker)
	state.ignoreMarkerWrite = false
	state.mu.Unlock()
	size, err := provider.ExpandVolume(context.Background(), "volume-test", 256*1024*1024, false)
	require.NoError(t, err)
	require.EqualValues(t, 256*1024*1024, size)
	require.NoError(t, provider.DetachVolume(context.Background(), "volume-test", "node-old"))
	state.mu.Lock()
	defer state.mu.Unlock()
	require.EqualValues(t, 256*1024*1024, state.marker)
	require.Contains(t, state.mutations, "detach:41")
	require.NotContains(t, state.mutations, "resize:41:256")
}

func TestProviderDetachRejectsLateConflictingImageID(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%t", retry), func(t *testing.T) {
			var imageReads int
			var protocol *nodeAttachmentProtocol
			provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if method != "one.image.info" {
					return
				}
				protocol.mu.Lock()
				defer protocol.mu.Unlock()
				imageReads++
				if (!retry && imageReads == 3) || (retry && protocol.detachAttempts == 1) {
					protocol.prependImageID = true
				}
			})
			protocol = wire
			wire.mu.Lock()
			wire.rejectFirstDetach = retry
			wire.mu.Unlock()

			accepted, err := provider.DetachVolumeWithAuthority(context.Background(), "volume-test", "node-old", 41, "local", nil, nil)
			require.ErrorContains(t, err, "detach disk identity or completed capacity is unverified")
			require.False(t, accepted)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.True(t, wire.prependImageID)
			require.Empty(t, wire.mutations)
			require.Contains(t, wire.disks, 41)
			if retry {
				require.Equal(t, 1, wire.detachAttempts)
			} else {
				require.Zero(t, wire.detachAttempts)
			}
		})
	}
}

func TestProviderLocalAttachmentBackendRejectsConflictingImageID(t *testing.T) {
	provider, wire := nodeAttachmentProvider(t)
	wire.mu.Lock()
	wire.prependImageID = true
	wire.mu.Unlock()
	require.ErrorIs(t, provider.VerifyAttachmentBackend(context.Background(), "volume-test", "node-old", 7, 41, "local"), opennebula.ErrConflictingAttachmentEvidence)
	wire.mu.Lock()
	defer wire.mu.Unlock()
	require.Empty(t, wire.mutations)
}

func TestControllerUnpublishFilesystemDatastoreTransfer(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, tc := range []struct {
			datastore, disk string
			allowed         bool
		}{
			{"shared", "shared", true},
			{"qcow2", "qcow2", true},
			{"qcow2", "shared", false},
		} {
			t.Run(fmt.Sprintf("queued=%t/%s/%s", queued, tc.datastore, tc.disk), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "local"})
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
				provider, wire := nodeAttachmentProvider(t)
				wire.mu.Lock()
				wire.imageTransfer, wire.diskTransfer = tc.datastore, tc.disk
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				reply, err := reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if tc.allowed {
					require.NoError(t, err)
					require.NotNil(t, reply)
					require.Equal(t, []string{"detach:41"}, wire.mutations)
				} else {
					require.Error(t, err)
					require.Nil(t, reply)
					require.Empty(t, wire.mutations)
					require.Contains(t, wire.disks, 41)
				}
			})
		}
	}
}

func TestControllerLegacyDetachBoundaries(t *testing.T) {
	for _, action := range []string{"unpublish", "transfer", "expiry"} {
		t.Run(action, func(t *testing.T) {
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			state.marker = 0
			state.mu.Unlock()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().UTC().Add(time.Minute).Format(time.RFC3339)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			_, err := driver.volumeHistory.Upsert(context.Background(), "volume-test", func(record *VolumeHistoryRecord) { record.Backend = "local" })
			require.NoError(t, err)
			if action != "unpublish" {
				expires := time.Now().UTC().Add(time.Minute)
				if action == "expiry" {
					expires = time.Now().UTC().Add(-time.Minute)
				}
				require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{VolumeID: "volume-test", NodeID: "node-old", Backend: "local", StartedAt: time.Now().UTC().Add(-2 * time.Minute), ExpiresAt: expires, GraceSeconds: 60}))
			}
			controller := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
			client := reviewControllerClient(t, controller)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			switch action {
			case "unpublish":
				response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Nil(t, response)
			case "transfer":
				response, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-new"))
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.ErrorContains(t, err, "detach disk identity or completed capacity is unverified")
				require.Nil(t, response)
				current, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, pv.Annotations[annotationAllowCrossNodeUntil], current.Annotations[annotationAllowCrossNodeUntil])
				require.NotContains(t, current.Annotations, annotationPublishMoveReceipt)
			case "expiry":
				before, _ := driver.stickyAttachments.Get("volume-test")
				controller.expireStickyDetach(ctx, before)
			}
			if action != "unpublish" {
				_, exists := driver.stickyAttachments.Get("volume-test")
				require.True(t, exists)
			}
			record, exists := driver.volumeHistory.Get("volume-test")
			require.True(t, exists)
			require.True(t, record.LastSafeDetachTime.IsZero())
			_, quarantined, err := controller.activeVolumeQuarantine(ctx, "volume-test")
			require.NoError(t, err)
			require.False(t, quarantined)
			state.mu.Lock()
			defer state.mu.Unlock()
			require.Empty(t, state.mutations)
			require.Equal(t, 256, state.disks[41])
			require.Zero(t, state.marker)
		})
	}
}

func TestOrdinaryDetachReadbackSurvivesHistoryWriteTimeout(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"unpublish", "expiry", "unreadable"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume, node = "volume-test", "node-old"
				provider, wire := nodeAttachmentProvider(t)
				wire.mu.Lock()
				delete(wire.disks, 41)
				wire.mu.Unlock()
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				driver := newStickyTestDriver(t, pv, pvc, newReadyNode(node, true))
				initial, err := driver.volumeHistory.Upsert(ctx, volume, func(history *VolumeHistoryRecord) {
					history.Backend, history.LastSuccessfulNodeName = "local", node
					history.LastSuccessfulImageID, history.LastSuccessfulOpenNebulaVMID = 7, 41
					history.LastSuccessfulPublishTime = time.Now().UTC().Add(-time.Minute)
				})
				require.NoError(t, err)
				kube := driver.kubeRuntime.client.(*fake.Clientset)
				var attempted atomic.Bool
				kube.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					if cm.Name == volumeHistoryStateConfigMapName {
						attempted.Store(true)
						return true, nil, context.DeadlineExceeded
					}
					return false, nil, nil
				})
				if mode == "unreadable" {
					kube.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
						if attempted.Load() && action.(ktesting.GetAction).GetName() == volumeHistoryStateConfigMapName {
							return true, nil, context.DeadlineExceeded
						}
						return false, nil, nil
					})
				}
				controller := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, controller.validateHotplugQueueRequest)
				}
				if mode == "expiry" {
					grace := StickyAttachmentState{VolumeID: volume, NodeID: node, Backend: "local",
						StartedAt: time.Now().Add(-2 * time.Minute), ExpiresAt: time.Now().Add(-time.Minute), GraceSeconds: 60}
					require.NoError(t, driver.stickyAttachments.StartGrace(grace))
					controller.expireStickyDetach(ctx, grace)
					_, held := driver.stickyAttachments.Get(volume)
					require.False(t, held)
				} else {
					reply, err := reviewControllerClient(t, controller).ControllerUnpublishVolume(ctx,
						&csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node})
					if mode == "unreadable" {
						require.Error(t, err)
						require.Nil(t, reply)
					} else {
						require.NoError(t, err)
						require.NotNil(t, reply)
					}
				}
				require.True(t, attempted.Swap(false))
				stored, err := controller.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, initial, stored)
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Empty(t, wire.mutations)
			})
		}
	}
}

func TestStickyExpiryDoesNotQuarantineDetachAuthorityDenial(t *testing.T) {
	ctx := context.Background()
	pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	node := newReadyNode("node-old", true)
	node.Status.VolumesInUse = []corev1.UniqueVolumeName{"kubernetes.io/csi/" + DefaultDriverName + "^volume-test"}
	driver := newStickyTestDriver(t, pv, pvc, node)
	driver.PluginConfig.OverrideVal(config.MetadataDriftQuarantineEnabledVar, true)
	driver.PluginConfig.OverrideVal(config.MetadataDriftQuarantineFailureThresholdVar, 1)
	state := StickyAttachmentState{
		VolumeID: "volume-test", NodeID: node.Name, Backend: "local", PVCName: pvc.Name, PVCNamespace: pvc.Namespace,
		StartedAt: time.Now().Add(-2 * time.Minute), ExpiresAt: time.Now().Add(-time.Minute), GraceSeconds: 60,
	}
	require.NoError(t, driver.stickyAttachments.StartGrace(state))
	provider, protocol := nodeAttachmentProvider(t)
	controller := NewControllerServer(driver, provider, nil)
	controller.expireStickyDetach(ctx, state)

	_, quarantined, err := controller.activeVolumeQuarantine(ctx, "volume-test")
	require.NoError(t, err)
	require.False(t, quarantined)
	_, held := driver.stickyAttachments.Get("volume-test")
	require.True(t, held)
	require.True(t, driver.volumeHistory.Snapshot()["volume-test"].LastSafeDetachTime.IsZero())
	events, err := driver.kubeRuntime.client.CoreV1().Events(pvc.Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, events.Items)
	protocol.mu.Lock()
	defer protocol.mu.Unlock()
	require.Zero(t, protocol.detachAttempts)
	require.Contains(t, protocol.disks, 41)
}

func TestProviderDetachRejectsContradictoryNoDiskReread(t *testing.T) {
	provider, state := nodeAttachmentProvider(t)
	state.mu.Lock()
	state.hideDiskInfo = true
	state.mu.Unlock()
	require.Error(t, provider.DetachVolume(context.Background(), "volume-test", "node-old"))
	state.mu.Lock()
	defer state.mu.Unlock()
	require.Empty(t, state.mutations)
	require.Equal(t, 256, state.disks[41])
}

func TestControllerUnpublishPreservesLookupRejections(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, kind := range []string{"inconsistent", "image-missing", "unknown-state", "generic-not-found", "nil-result"} {
			t.Run(fmt.Sprintf("queued=%v/%s", queued, kind), func(t *testing.T) {
				controller, provider, before := attachmentEvidenceFixture(t, queued, false)
				provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil).Once()
				result := &opennebula.VolumeLookupResult{VolumeHandle: before.VolumeID, RequestedNode: before.NodeID}
				var lookupErr error
				code := codes.Unavailable
				switch kind {
				case "inconsistent":
					result.Status = opennebula.VolumeLookupProviderInconsistent
					code = codes.FailedPrecondition
				case "image-missing":
					result.Status = opennebula.VolumeLookupImageRecordMissing
					code = codes.FailedPrecondition
				case "generic-not-found":
					lookupErr = status.Error(codes.NotFound, "provider lookup failed")
				case "nil-result":
					result = nil
				}
				provider.On("InspectVolumeLookup", mock.Anything, before.VolumeID, before.NodeID).Return(result, lookupErr).Once()
				client := reviewControllerClient(t, controller)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: before.VolumeID, NodeId: before.NodeID})
				require.Equal(t, code, status.Code(err))
				require.Nil(t, response)
				requireAttachmentHoldPreserved(t, controller, before)
				provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
				provider.AssertExpectations(t)
			})
		}
	}
}

type nonInspectingVolumeProvider struct {
	opennebula.OpenNebulaVolumeProvider
}

func TestControllerUnpublishRejectsFallbackLookupFailure(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			controller, _, before := attachmentEvidenceFixture(t, queued, false)
			provider := &MockOpenNebulaVolumeProviderTestify{}
			provider.On("VolumeExists", mock.Anything, before.VolumeID).Return(7, 128*1024*1024, nil).Once()
			provider.On("VolumeExists", mock.Anything, before.VolumeID).Return(-1, -1, status.Error(codes.NotFound, "lookup unavailable")).Once()
			provider.On("NodeExists", mock.Anything, before.NodeID).Return(11, nil)
			provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil).Once()
			controller.volumeProvider = nonInspectingVolumeProvider{provider}
			client := reviewControllerClient(t, controller)
			response, err := client.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: before.VolumeID, NodeId: before.NodeID})
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Nil(t, response)
			requireAttachmentHoldPreserved(t, controller, before)
			provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
			provider.AssertExpectations(t)
		})
	}
}

func TestControllerUnpublishRejectsFailedNodeLookup(t *testing.T) {
	provider := &MockOpenNebulaVolumeProviderTestify{}
	provider.On("VolumeExists", mock.Anything, "volume").Return(7, 128*1024*1024, nil)
	provider.On("NodeExists", mock.Anything, "node").Return(-1, errors.New("VM lookup unavailable"))
	provider.On("VolumeReadyWithTimeout", mock.Anything, 7).Return(true, nil).Maybe()
	client := reviewControllerClient(t, getTestControllerServer(provider))
	response, err := client.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume", NodeId: "node"})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Nil(t, response)
	provider.AssertNotCalled(t, "VolumeReadyWithTimeout", mock.Anything, mock.Anything)
	provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}
