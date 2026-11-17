package driver

import (
	"context"
	"encoding/json"
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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const cephRestartVolume = "pvc-ceph-restart"

type cephRetentionTestProvider struct {
	*MockOpenNebulaVolumeProviderTestify
	observe          func(context.Context) (*opennebula.SameNodeRetentionEvidence, error)
	inspections      atomic.Int32
	attachmentAbsent atomic.Bool
}

func (p *cephRetentionTestProvider) InspectSameNodeRetention(ctx context.Context, volume, node string) (*opennebula.SameNodeRetentionEvidence, error) {
	p.inspections.Add(1)
	return p.observe(ctx)
}

func (p *cephRetentionTestProvider) GetVolumeInNode(ctx context.Context, imageID, nodeID int) (string, error) {
	if p.attachmentAbsent.Load() {
		return "", opennebula.ErrAttachmentNotFound
	}
	return p.MockOpenNebulaVolumeProviderTestify.GetVolumeInNode(ctx, imageID, nodeID)
}

func (p *cephRetentionTestProvider) DetachVolumeWithAuthority(ctx context.Context, volume, node, expectedBackend string, verifyOwner opennebula.DetachOwnerCheck, authorize opennebula.DetachAuthorization) (bool, error) {
	if verifyOwner != nil {
		if _, err := verifyOwner(ctx, 3137, 575, 1); err != nil {
			return false, err
		}
	}
	if err := authorize(ctx, 3137, 575, 1); err != nil {
		return false, err
	}
	err := p.DetachVolume(ctx, volume, node)
	return err == nil, err
}

type cephRestartTestFixture struct {
	server   *ControllerServer
	provider *cephRetentionTestProvider
	pv       *corev1.PersistentVolume
	pvc      *corev1.PersistentVolumeClaim
	pod      *corev1.Pod
	node     *corev1.Node
	va       *storagev1.VolumeAttachment
	detach   *mock.Call
}

func newCephRestartTestFixture(t *testing.T, queued bool) *cephRestartTestFixture {
	t.Helper()
	pv, pvc := newLocalPVAndPVC(cephRestartVolume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph", annotationDatastoreID: "124"})
	pv.UID = types.UID("pv-owned")
	pvc.UID = types.UID("pvc-owned")
	pv.Spec.ClaimRef.UID = pvc.UID
	pv.Spec.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	pv.Status.Phase = corev1.VolumeBound
	pvc.Spec.VolumeName = pv.Name
	pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	pvc.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	pod := activePodForPVC("replacement", pvc.Name, "node-a")
	pod.UID = types.UID("replacement-owned")
	pod.Status.Phase = corev1.PodPending
	node := newReadyNode("node-a", true)
	node.UID = types.UID("node-owned")
	deletedAt := metav1.NewTime(time.Now().UTC().Add(-time.Minute))
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-ceph-restart", UID: types.UID("va-owned"), DeletionTimestamp: &deletedAt, Finalizers: []string{"external-attacher/" + DefaultDriverName}},
		Spec:       storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: "node-a", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
		Status:     storagev1.VolumeAttachmentStatus{Attached: true, AttachmentMetadata: map[string]string{publishContextOpenNebulaImageID: "3137", publishContextDeviceSerial: "onecsi-3137", "volumeName": "vdb", paramPVName: pv.Name, paramPVCName: pvc.Name, paramPVCNamespace: pvc.Namespace}},
	}
	driver := newStickyTestDriver(t, pv, pvc, pod, node, va)
	_, err := driver.volumeHistory.Upsert(context.Background(), cephRestartVolume, func(record *VolumeHistoryRecord) {
		record.Backend = "ceph"
		record.LastSuccessfulPublishTime = time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
		record.PVName = pv.Name
		record.PVCName = pvc.Name
		record.PVCNamespace = pvc.Namespace
		record.LastSuccessfulImageID = 3137
		record.LastSuccessfulNodeName = node.Name
		record.LastSuccessfulNodeUID = string(node.UID)
		record.LastSuccessfulDeviceSerial = "onecsi-3137"
	})
	require.NoError(t, err)
	base := new(MockOpenNebulaVolumeProviderTestify)
	detach := base.On("DetachVolume", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("unexpected physical detach")).Maybe()
	base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1024*1024*1024, nil)
	base.On("NodeExists", mock.Anything, "node-a").Return(575, nil)
	base.On("GetVolumeInNode", mock.Anything, 3137, 575).Return("vdb", nil)
	base.On("InspectVolumeLookup", mock.Anything, cephRestartVolume, "node-a").Return(&opennebula.VolumeLookupResult{
		Status: opennebula.VolumeLookupPresent, VolumeHandle: cephRestartVolume, ImageID: 3137, SizeBytes: 1024 * 1024 * 1024, RequestedNode: "node-a", RequestedNodeID: 575,
		AttachmentMetadata: &opennebula.VolumeAttachmentMetadata{VolumeHandle: cephRestartVolume, ImageID: 3137, ImageName: cephRestartVolume, ImageState: "USED_PERS", ImageRunningVMs: 1, ImageVMIDs: []int{575}, RequestedNode: "node-a", RequestedNodeID: 575, AttachedToRequestedNode: true, DiskRecords: []opennebula.VolumeDiskRecord{{NodeName: "node-a", NodeID: 575, DiskID: 1, Target: "vdb", Serial: "onecsi-3137"}}},
	}, nil)
	provider := &cephRetentionTestProvider{MockOpenNebulaVolumeProviderTestify: base, observe: func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
		return validCephRetentionTestEvidence(), nil
	}}
	server := NewControllerServer(driver, provider, nil)
	if queued {
		driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
		driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
	}
	return &cephRestartTestFixture{server: server, provider: provider, pv: pv, pvc: pvc, pod: pod, node: node, va: va, detach: detach}
}

func validCephRetentionTestEvidence() *opennebula.SameNodeRetentionEvidence {
	return &opennebula.SameNodeRetentionEvidence{VolumeID: cephRestartVolume, ImageID: 3137, NodeID: 575, DiskID: 1, Target: "vdb", DeviceSerial: "onecsi-3137", SizeBytes: 1024 * 1024 * 1024}
}

func (f *cephRestartTestFixture) syncObjects(t *testing.T) {
	t.Helper()
	client := f.server.driver.kubeRuntime.client
	_, err := client.CoreV1().PersistentVolumes().Update(context.Background(), f.pv, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().PersistentVolumeClaims(f.pvc.Namespace).Update(context.Background(), f.pvc, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().Nodes().Update(context.Background(), f.node, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = client.StorageV1().VolumeAttachments().Update(context.Background(), f.va, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func (f *cephRestartTestFixture) unpublish(t *testing.T) (*csi.ControllerUnpublishVolumeResponse, error) {
	t.Helper()
	client := reviewControllerClient(t, f.server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: cephRestartVolume, NodeId: "node-a"})
}

func (f *cephRestartTestFixture) requireNoProviderMutation(t *testing.T) {
	t.Helper()
	f.provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	f.provider.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	f.provider.AssertNotCalled(t, "RestoreVolumeCapacity", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	f.provider.AssertNotCalled(t, "ExpandVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	f.provider.AssertNotCalled(t, "DeleteVolume", mock.Anything, mock.Anything)
}

// This reproduces the hplsvc overlap: Pending replacement, deleting attached VA,
// and positive same-VM disk evidence. Pending is not proof that a disk is unmounted.
func TestControllerUnpublishCephDeletingAttachmentSameNode(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%v", queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			before, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
			response, err := f.unpublish(t)
			require.NoError(t, err, "same-node deleting Ceph VA must complete logically without detaching the desired disk")
			require.NotNil(t, response)
			f.requireNoProviderMutation(t)
			require.Positive(t, f.provider.inspections.Load(), "success must consume positive typed provider proof")
			after, ok := f.server.driver.volumeHistory.Get(cephRestartVolume)
			require.True(t, ok)
			require.Equal(t, before, after, "physical retention must preserve history and LastSafeDetachTime")
		})
	}
}

func TestControllerUnpublishCephRejectsUnverifiedRetention(t *testing.T) {
	cases := map[string]func(*testing.T, *cephRestartTestFixture){
		"missing PV UID": func(t *testing.T, f *cephRestartTestFixture) { f.pv.UID = "" },
		"VA image pin contradicts current PV": func(t *testing.T, f *cephRestartTestFixture) {
			f.va.Status.AttachmentMetadata[publishContextOpenNebulaImageID] = "3138"
		},
		"VA serial contradicts current disk": func(t *testing.T, f *cephRestartTestFixture) {
			f.va.Status.AttachmentMetadata[publishContextDeviceSerial] = "onecsi-3138"
		},
		"VA target contradicts current disk": func(t *testing.T, f *cephRestartTestFixture) { f.va.Status.AttachmentMetadata["volumeName"] = "vdc" },
		"VA claim metadata contradicts current PVC": func(t *testing.T, f *cephRestartTestFixture) {
			f.va.Status.AttachmentMetadata[paramPVCName] = "different-claim"
		},
		"same name replaced PV and contradictory VA pin": func(t *testing.T, f *cephRestartTestFixture) {
			f.pv.UID = "replaced-before-first-read"
			f.va.Status.AttachmentMetadata[publishContextOpenNebulaImageID] = "3138"
		},
		"durable cleanup disabled": func(t *testing.T, f *cephRestartTestFixture) {
			f.server.driver.PluginConfig.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, false)
		},
		"repair state active": func(t *testing.T, f *cephRestartTestFixture) {
			_, err := f.server.driver.volumeRepairState.Mark(context.Background(), VolumeRepairState{VolumeID: cephRestartVolume, Classification: "metadata_drift"})
			require.NoError(t, err)
		},
		"host quarantine active": func(t *testing.T, f *cephRestartTestFixture) {
			_, active, err := f.server.driver.hostArtifactQuarantine.MarkFailure(context.Background(), HostArtifactQuarantineState{Key: "vm-575", VMID: 575, ImageID: 3137, VolumeID: cephRestartVolume}, 1, time.Hour)
			require.NoError(t, err)
			require.True(t, active)
		},
		"multiple desired nodes": func(t *testing.T, f *cephRestartTestFixture) {
			other := activePodForPVC("other", f.pvc.Name, "node-b")
			other.UID = "other-consumer"
			_, err := f.server.driver.kubeRuntime.client.CoreV1().Pods(f.pvc.Namespace).Create(context.Background(), other, metav1.CreateOptions{})
			require.NoError(t, err)
		},
		"unknown provider": func(t *testing.T, f *cephRestartTestFixture) {
			f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
				return nil, errors.New("owner census unavailable")
			}
		},
		"missing provider proof": func(t *testing.T, f *cephRestartTestFixture) {
			f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) { return nil, nil }
		},
		"changed image identity": func(t *testing.T, f *cephRestartTestFixture) {
			f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
				e := validCephRetentionTestEvidence()
				e.ImageID++
				return e, nil
			}
		},
		"changed VM identity": func(t *testing.T, f *cephRestartTestFixture) {
			f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
				e := validCephRetentionTestEvidence()
				e.NodeID++
				return e, nil
			}
		},
		"changed device serial": func(t *testing.T, f *cephRestartTestFixture) {
			f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
				e := validCephRetentionTestEvidence()
				e.DeviceSerial = "other-device"
				return e, nil
			}
		},
		"capacity disagreement": func(t *testing.T, f *cephRestartTestFixture) {
			f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
				e := validCephRetentionTestEvidence()
				e.SizeBytes *= 2
				return e, nil
			}
		},
		"pending PVC expansion": func(t *testing.T, f *cephRestartTestFixture) {
			f.pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")
		},
		"PV claim UID changed": func(t *testing.T, f *cephRestartTestFixture) { f.pv.Spec.ClaimRef.UID = "other-pvc" },
		"node UID changed":     func(t *testing.T, f *cephRestartTestFixture) { f.node.UID = "replacement-node" },
		"node not Ready": func(t *testing.T, f *cephRestartTestFixture) {
			f.node.Status.Conditions[0].Status = corev1.ConditionFalse
		},
		"unsupported multiwriter access": func(t *testing.T, f *cephRestartTestFixture) {
			f.pv.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
			f.pvc.Spec.AccessModes = f.pv.Spec.AccessModes
		},
		"different attacher owns VA": func(t *testing.T, f *cephRestartTestFixture) { f.va.Spec.Attacher = "other.driver" },
		"VA not deleting":            func(t *testing.T, f *cephRestartTestFixture) { f.va.DeletionTimestamp = nil },
		"observe recovery active": func(t *testing.T, f *cephRestartTestFixture) {
			f.pvc.Annotations[annotationRecoveryMode] = recoveryModeObserve
		},
		"volume quarantine active": func(t *testing.T, f *cephRestartTestFixture) {
			_, active, err := f.server.driver.volumeQuarantine.MarkFailure(context.Background(), VolumeQuarantineState{VolumeID: cephRestartVolume, Reason: "metadata_drift", Classification: "metadata_drift"}, 1, time.Hour)
			require.NoError(t, err)
			require.True(t, active)
		},
		"API PV lookup unknown": func(t *testing.T, f *cephRestartTestFixture) {
			f.server.driver.kubeRuntime.client.(*fake.Clientset).PrependReactor("get", "persistentvolumes", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, errors.New("API unavailable") })
		},
	}
	for _, queued := range []bool{false, true} {
		for name, setup := range cases {
			t.Run(fmt.Sprintf("queued=%v/%s", queued, name), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				setup(t, f)
				f.syncObjects(t)
				response, err := f.unpublish(t)
				require.Error(t, err, "uncertain/ineligible Ceph overlap must not fall through to permissive no-queue ACK")
				require.Nil(t, response)
				require.Contains(t, []codes.Code{codes.Unavailable, codes.FailedPrecondition}, status.Code(err))
				f.requireNoProviderMutation(t)
				history, ok := f.server.driver.volumeHistory.Get(cephRestartVolume)
				require.True(t, ok)
				require.True(t, history.LastSafeDetachTime.IsZero(), "denial cannot record physical detach")
			})
		}
	}
}

func TestControllerUnpublishCephRechecksLateAuthorityChanges(t *testing.T) {
	changes := map[string]func(context.Context, *cephRestartTestFixture) error{
		"PV UID": func(ctx context.Context, f *cephRestartTestFixture) error {
			f.pv.UID = "replaced-pv"
			_, err := f.server.driver.kubeRuntime.client.CoreV1().PersistentVolumes().Update(ctx, f.pv, metav1.UpdateOptions{})
			return err
		},
		"PVC UID": func(ctx context.Context, f *cephRestartTestFixture) error {
			f.pvc.UID = "replaced-pvc"
			_, err := f.server.driver.kubeRuntime.client.CoreV1().PersistentVolumeClaims(f.pvc.Namespace).Update(ctx, f.pvc, metav1.UpdateOptions{})
			return err
		},
		"VA UID": func(ctx context.Context, f *cephRestartTestFixture) error {
			f.va.UID = "replaced-va"
			_, err := f.server.driver.kubeRuntime.client.StorageV1().VolumeAttachments().Update(ctx, f.va, metav1.UpdateOptions{})
			return err
		},
		"Pod demand disappears": func(ctx context.Context, f *cephRestartTestFixture) error {
			return f.server.driver.kubeRuntime.client.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{})
		},
		"recovery control": func(ctx context.Context, f *cephRestartTestFixture) error {
			f.pvc.Annotations[annotationRecoveryMode] = recoveryModeManual
			_, err := f.server.driver.kubeRuntime.client.CoreV1().PersistentVolumeClaims(f.pvc.Namespace).Update(ctx, f.pvc, metav1.UpdateOptions{})
			return err
		},
		"quarantine": func(ctx context.Context, f *cephRestartTestFixture) error {
			_, _, err := f.server.driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: cephRestartVolume, Reason: "metadata_drift", Classification: "metadata_drift"}, 1, time.Hour)
			return err
		},
	}
	for _, queued := range []bool{false, true} {
		for name, change := range changes {
			t.Run(fmt.Sprintf("queued=%v/%s", queued, name), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				f.provider.observe = func(ctx context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
					if f.provider.inspections.Load() == 1 {
						if err := change(ctx, f); err != nil {
							return nil, err
						}
					}
					return validCephRetentionTestEvidence(), nil
				}
				response, err := f.unpublish(t)
				require.Error(t, err)
				require.Nil(t, response)
				f.requireNoProviderMutation(t)
				history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				require.True(t, history.LastSafeDetachTime.IsZero())
			})
		}
	}
}

func TestControllerUnpublishCephRejectsProviderRecheckDrift(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, unknown := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%v/unknown=%v", queued, unknown), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
					evidence := validCephRetentionTestEvidence()
					if f.provider.inspections.Load() > 1 {
						if unknown {
							return nil, errors.New("ownership changed or unavailable")
						}
						evidence.DiskID++
					}
					return evidence, nil
				}
				response, err := f.unpublish(t)
				require.Error(t, err)
				require.Nil(t, response)
				require.EqualValues(t, 2, f.provider.inspections.Load())
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestControllerUnpublishCephRejectsUnknownProtectionReads(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, name := range []string{volumeQuarantineStateConfigMapName, hostArtifactStateConfigMapName, volumeRepairStateConfigMapName} {
			t.Run(fmt.Sprintf("queued=%v/%s", queued, name), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				f.server.driver.kubeRuntime.client.(*fake.Clientset).PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
					if action.(ktesting.GetAction).GetName() == name {
						return true, nil, errors.New("protection read unavailable")
					}
					return false, nil, nil
				})
				response, err := f.unpublish(t)
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, response)
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestControllerUnpublishCephOrdinaryDetachAfterDemandDisappears(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, detach := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%v/confirmed=%v", queued, detach), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				ctx := context.Background()
				response, err := f.unpublish(t)
				require.NoError(t, err)
				require.NotNil(t, response)
				f.requireNoProviderMutation(t)
				f.pod.Status.Phase = corev1.PodSucceeded
				_, err = f.server.driver.kubeRuntime.client.CoreV1().Pods(f.pod.Namespace).Update(ctx, f.pod, metav1.UpdateOptions{})
				require.NoError(t, err)
				f.detach.Return(nil).Run(func(mock.Arguments) {
					if detach {
						f.provider.attachmentAbsent.Store(true)
					}
				})
				response, err = f.unpublish(t)
				f.provider.AssertNumberOfCalls(t, "DetachVolume", 1)
				history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				if detach {
					require.NoError(t, err)
					require.NotNil(t, response)
					require.False(t, history.LastSafeDetachTime.IsZero())
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, response)
					require.True(t, history.LastSafeDetachTime.IsZero())
				}
			})
		}
	}
}

func TestControllerUnpublishCephCancellationAfterLockWait(t *testing.T) {
	f := newCephRestartTestFixture(t, true)
	release := f.server.driver.operationLocks.Acquire(controllerVolumeLockKey(cephRestartVolume))
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := f.server.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: cephRestartVolume, NodeId: "node-a"})
		done <- err
	}()
	<-started
	// Wait for the request to register as a second holder of the volume lock,
	// proving cancellation happens during the lock wait rather than before RPC.
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	waiting := false
	for !waiting {
		locks := f.server.driver.operationLocks
		locks.mu.Lock()
		ref := locks.locks[controllerVolumeLockKey(cephRestartVolume)]
		waiting = ref != nil && ref.refs == 2
		locks.mu.Unlock()
		if waiting {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			cancel()
			release()
			t.Fatal("request did not reach volume-lock wait")
		}
	}
	cancel()
	release()
	select {
	case err := <-done:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled request did not leave operation lock")
	}
	require.Zero(t, f.provider.inspections.Load())
	f.requireNoProviderMutation(t)
}

// The explicit operator recovery contract precedes automatic Ceph retention.
// Its existing bookkeeping is intentionally not redefined by this correction.
func TestControllerUnpublishCephPreservesExplicitManualRecovery(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, conflicting := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%v/conflicting=%v", queued, conflicting), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				if conflicting {
					f.pv.Annotations[annotationBackend] = "local"
					f.pv.Spec.CSI.VolumeAttributes = map[string]string{annotationBackend: "ceph"}
				}
				f.pvc.Annotations[annotationRecoveryMode] = recoveryModeManual
				f.syncObjects(t)
				response, err := f.unpublish(t)
				require.NoError(t, err)
				require.NotNil(t, response)
				require.Zero(t, f.provider.inspections.Load(), "explicit manual contract must not invoke automatic retention")
				history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				require.False(t, history.LastSafeDetachTime.IsZero(), "manual acknowledgement retains historical bookkeeping")
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestCephLegacyStaticUnpublishRequiresBackendEvidence(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, evidence := range []struct{ annotation, attribute string }{{}, {attribute: "local"}, {annotation: "cehp"}, {attribute: "cehp"}, {annotation: "unknown", attribute: "unknown"}} {
			t.Run(fmt.Sprintf("queued=%v/%s/%s", queued, evidence.annotation, evidence.attribute), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				delete(f.pv.Annotations, annotationBackend)
				if evidence.annotation != "" {
					f.pv.Annotations[annotationBackend] = evidence.annotation
				}
				if evidence.attribute != "" {
					f.pv.Spec.CSI.VolumeAttributes = map[string]string{annotationBackend: evidence.attribute}
				}
				f.syncObjects(t)
				_, err := f.server.driver.volumeHistory.Upsert(context.Background(), cephRestartVolume, func(record *VolumeHistoryRecord) {
					record.Backend = "local"
				})
				require.NoError(t, err)
				runtimeCtx, err := f.server.driver.kubeRuntime.ResolveVolumeRuntimeContext(context.Background(), cephRestartVolume)
				require.NoError(t, err)
				require.False(t, runtimeContextEligibleForMaintenance(runtimeCtx, cephRestartVolume))
				response, err := f.unpublish(t)
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, response)
				history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				require.True(t, history.LastSafeDetachTime.IsZero())
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func cephRestartOwnerMetadata() *opennebula.VolumeAttachmentMetadata {
	return &opennebula.VolumeAttachmentMetadata{
		VolumeHandle: cephRestartVolume, ImageID: 3137, ImageName: cephRestartVolume, RequestedNode: "node-a", RequestedNodeID: 575,
		ImageRunningVMs: 1, ImageVMIDs: []int{575}, AttachedToRequestedNode: true,
		DiskRecords: []opennebula.VolumeDiskRecord{{NodeID: 575, DiskID: 1, Target: "vdb"}},
	}
}

func TestCephRetentionSurvivesControllerRestartAndOrphanCleanup(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, demand := range []string{"none", "remote-orphan", "remote-divergent", "remote-in-use", "remote-multiple-owners"} {
			t.Run(fmt.Sprintf("queued=%v/%s", queued, demand), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				response, err := f.unpublish(t)
				require.NoError(t, err)
				require.NotNil(t, response)
				f.requireNoProviderMutation(t)
				// External-attacher has consumed the ACK and removed its VA. The replacement
				// never publishes and its consumer disappears before a new VA is created.
				oldClient := f.server.driver.kubeRuntime.client
				require.NoError(t, oldClient.StorageV1().VolumeAttachments().Delete(context.Background(), f.va.Name, metav1.DeleteOptions{}))
				require.NoError(t, oldClient.CoreV1().Pods(f.pod.Namespace).Delete(context.Background(), f.pod.Name, metav1.DeleteOptions{}))
				persisted, err := oldClient.CoreV1().ConfigMaps("default").List(context.Background(), metav1.ListOptions{})
				require.NoError(t, err)
				objects := []kruntime.Object{f.pv.DeepCopy(), f.pvc.DeepCopy(), f.node.DeepCopy()}
				for i := range persisted.Items {
					objects = append(objects, persisted.Items[i].DeepCopy())
				}
				if demand != "none" {
					objects[2].(*corev1.Node).Spec.Unschedulable = true
					pod := activePodForPVC("remote-replacement", f.pvc.Name, "node-b")
					objects = append(objects, newReadyNode("node-b", true), pod)
					if demand != "remote-orphan" {
						objects = append(objects, &storagev1.VolumeAttachment{
							ObjectMeta: metav1.ObjectMeta{Name: "pending-remote-va", UID: "pending-remote-uid"},
							Spec: storagev1.VolumeAttachmentSpec{NodeName: "node-b", Attacher: DefaultDriverName,
								Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &f.pv.Name}},
						})
					}
					if demand == "remote-in-use" {
						objects[2].(*corev1.Node).Status.VolumesInUse = []corev1.UniqueVolumeName{"kubernetes.io/csi/" + DefaultDriverName + "^" + cephRestartVolume}
					}
				}
				// A new runtime/client/managers/controller receives only durable API state;
				// no in-memory retention record or another Unpublish call is available.
				driver := newStickyTestDriver(t, objects...)
				base := new(MockOpenNebulaVolumeProviderTestify)
				base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{VolumeHandle: cephRestartVolume, ImageID: 3137, NodeName: "node-a", NodeID: 575, DiskID: 1, Backend: "ceph"}}, opennebula.VMNameInventory{"node-a": {575}}, nil).Twice()
				base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1024*1024*1024, nil)
				base.On("NodeExists", mock.Anything, "node-a").Return(575, nil)
				physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: base, attached: true, imageID: 3137, vmID: 575, diskID: 1}
				detachAllowed := demand != "remote-in-use" && demand != "remote-multiple-owners"
				if detachAllowed {
					base.On("DetachVolume", mock.Anything, cephRestartVolume, "node-a").Run(func(mock.Arguments) { physical.attached = false }).Return(nil).Once()
				}
				metadata := cephRestartOwnerMetadata()
				if demand == "remote-multiple-owners" {
					metadata.DiskRecords = append(metadata.DiskRecords, opennebula.VolumeDiskRecord{NodeID: 576, DiskID: 1})
				}
				base.On("InspectVolumeAttachment", mock.Anything, cephRestartVolume, "node-a").Return(metadata, nil).Maybe()
				server := NewControllerServer(driver, physical, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, func(ctx context.Context, node, operation, volume string) HotplugQueueValidation {
						if node == "node-b" {
							return HotplugQueueValidation{Decision: HotplugQueueValidationValid}
						}
						return server.validateHotplugQueueRequest(ctx, node, operation, volume)
					})
					if demand != "none" {
						blockerCtx, cancelBlocker := context.WithCancel(context.Background())
						publishCtx, cancelPublish := context.WithCancel(context.Background())
						blocked := make(chan struct{})
						blockerDone, publishDone := make(chan error, 1), make(chan error, 1)
						go func() {
							blockerDone <- driver.hotplugQueue.Run(blockerCtx, "node-b", "test", "other-volume", hotplugQueuePriorityNormal, func(ctx context.Context) error {
								close(blocked)
								<-ctx.Done()
								return ctx.Err()
							})
						}()
						t.Cleanup(func() {
							cancelPublish()
							defer cancelBlocker()
							select {
							case err := <-publishDone:
								var canceledWait *HotplugQueueTimeoutError
								if !errors.Is(err, context.Canceled) {
									require.ErrorAs(t, err, &canceledWait)
									require.Equal(t, cephRestartVolume, canceledWait.Volume)
								}
							case <-time.After(time.Second):
								t.Error("pending Publish did not exit after cancellation")
							}
							cancelBlocker()
							select {
							case err := <-blockerDone:
								require.ErrorIs(t, err, context.Canceled)
							case <-time.After(time.Second):
								t.Error("active queue blocker did not exit after cancellation")
							}
						})
						go func() {
							select {
							case <-blocked:
							case <-publishCtx.Done():
								publishDone <- publishCtx.Err()
								return
							}
							publishDone <- server.withQueuedHotplug(publishCtx, "node-b", "attach", cephRestartVolume, hotplugQueuePriorityNormal, func(context.Context) error {
								return fmt.Errorf("pending Publish ran before the destination queue was released")
							})
						}()
						require.Eventually(t, func() bool {
							return len(driver.hotplugQueue.Snapshot()["node-b"].Queued) == 1
						}, time.Second, time.Millisecond)
					}
				}
				reconciler := NewAttachmentReconciler(server)
				reconciler.orphanGrace = time.Nanosecond
				require.NoError(t, reconciler.ReconcileOnce(context.Background()))
				base.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
				require.NoError(t, reconciler.ReconcileOnce(context.Background()))
				if detachAllowed {
					base.AssertNumberOfCalls(t, "DetachVolume", 1)
					require.False(t, physical.attached)
				} else {
					base.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
					require.True(t, physical.attached)
				}
				base.AssertNotCalled(t, "DeleteVolume", mock.Anything, mock.Anything)
				base.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				base.AssertExpectations(t)
			})
		}
	}
}

func TestControllerUnpublishCephDelayedRPCProtectsNewAttachment(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%v", queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			response, err := f.unpublish(t)
			require.NoError(t, err)
			require.NotNil(t, response)
			initialInspections := f.provider.inspections.Load()
			// A new VA is published on the same node. An old RPC must not inherit that
			// new object's authority merely because image/node names still match.
			f.va.UID = "new-published-va"
			f.va.DeletionTimestamp = nil
			f.syncObjects(t)
			response, err = f.unpublish(t)
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Nil(t, response)
			require.Equal(t, initialInspections, f.provider.inspections.Load())
			f.requireNoProviderMutation(t)
			history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
			require.True(t, history.LastSafeDetachTime.IsZero())
		})
	}
}

func TestControllerUnpublishCephRejectsMalformedProtectionState(t *testing.T) {
	for _, name := range []string{volumeQuarantineStateConfigMapName, hostArtifactStateConfigMapName} {
		for _, raw := range []string{"broken", "{}", "", " ", `{"volumeID":"pvc-ceph-restart","classification":"metadata_drift","failureCount":1}`, `{"volumeID":"pvc-ceph-restart","classification":"metadata_drift","failureCount":1,"expiresAt":"2000-01-01T00:00:00Z","futureSchema":true}`} {
			t.Run(name+"/"+raw, func(t *testing.T) {
				f := newCephRestartTestFixture(t, true)
				_, err := f.server.driver.kubeRuntime.client.CoreV1().ConfigMaps("default").Create(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Data: map[string]string{cephRestartVolume: raw}}, metav1.CreateOptions{})
				require.NoError(t, err)
				response, err := f.unpublish(t)
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, response)
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestControllerUnpublishCephCancellationDuringFinalAuthorityRead(t *testing.T) {
	f := newCephRestartTestFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
		if f.provider.inspections.Load() == 2 {
			f.server.driver.kubeRuntime.client.(*fake.Clientset).PrependReactor("get", "nodes", func(ktesting.Action) (bool, kruntime.Object, error) { cancel(); return true, nil, context.Canceled })
		}
		return validCephRetentionTestEvidence(), nil
	}
	response, err := f.server.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: cephRestartVolume, NodeId: "node-a"})
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Nil(t, response)
	require.EqualValues(t, 2, f.provider.inspections.Load())
	f.requireNoProviderMutation(t)
}

func TestControllerUnpublishCephPreservesKnownExpiredQuarantine(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%v", queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			state, _, err := f.server.driver.volumeQuarantine.MarkFailure(context.Background(), VolumeQuarantineState{VolumeID: cephRestartVolume, Reason: "metadata_drift", Classification: "metadata_drift"}, 1, time.Hour)
			require.NoError(t, err)
			state.ExpiresAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			state.FirstObservedAt = state.ExpiresAt.Add(-time.Hour)
			state.LastObservedAt = state.FirstObservedAt
			raw, err := json.Marshal(state)
			require.NoError(t, err)
			client := f.server.driver.kubeRuntime.client
			cm, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), volumeQuarantineStateConfigMapName, metav1.GetOptions{})
			require.NoError(t, err)
			cm.Data[cephRestartVolume] = string(raw)
			_, err = client.CoreV1().ConfigMaps("default").Update(context.Background(), cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			response, err := f.unpublish(t)
			require.NoError(t, err)
			require.NotNil(t, response)
			f.requireNoProviderMutation(t)
			after, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), volumeQuarantineStateConfigMapName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, string(raw), after.Data[cephRestartVolume], "expired receipt is read, never cleared or rewritten")
		})
	}
}

func TestControllerUnpublishCephCurrentBindingRetainsPinnedSameImage(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			ctx := context.Background()
			client := f.server.driver.kubeRuntime.client
			before, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
			require.NoError(t, client.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
			require.NoError(t, client.StorageV1().VolumeAttachments().Delete(ctx, f.va.Name, metav1.DeleteOptions{}))
			require.NoError(t, client.CoreV1().PersistentVolumes().Delete(ctx, f.pv.Name, metav1.DeleteOptions{}))
			require.NoError(t, client.CoreV1().PersistentVolumeClaims(f.pvc.Namespace).Delete(ctx, f.pvc.Name, metav1.DeleteOptions{}))

			pv, pvc := f.pv.DeepCopy(), f.pvc.DeepCopy()
			pv.Name, pv.UID = "rebound-pv", "rebound-pv-uid"
			pvc.Name, pvc.UID = "rebound-pvc", "rebound-pvc-uid"
			pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.UID = pvc.Name, pvc.UID
			pv.Spec.CSI.VolumeAttributes = map[string]string{annotationBackend: "Ceph"}
			pvc.Spec.VolumeName = pv.Name
			pv.Spec.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")}
			pvc.Status.Capacity = pv.Spec.Capacity.DeepCopy()
			_, err := client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
			require.NoError(t, err)
			_, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
			require.NoError(t, err)

			pod := activePodForPVC("rebound-consumer", pvc.Name, f.node.Name)
			pod.UID = "rebound-pod-uid"
			_, err = client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			require.NoError(t, err)
			va := f.va.DeepCopy()
			va.Name, va.UID = "rebound-attachment", "rebound-va-uid"
			va.DeletionTimestamp = nil
			va.Spec.Source.PersistentVolumeName = &pv.Name
			va.Status.AttachmentMetadata[paramPVName] = pv.Name
			va.Status.AttachmentMetadata[paramPVCName] = pvc.Name
			attachedStatus := va.Status
			va.Status = storagev1.VolumeAttachmentStatus{}
			va, err = client.StorageV1().VolumeAttachments().Create(ctx, va, metav1.CreateOptions{})
			require.NoError(t, err)
			va.Status = attachedStatus
			va, err = client.StorageV1().VolumeAttachments().UpdateStatus(ctx, va, metav1.UpdateOptions{})
			require.NoError(t, err)
			fakeClient := client.(*fake.Clientset)
			fakeClient.PrependReactor("delete", "volumeattachments", func(action ktesting.Action) (bool, kruntime.Object, error) {
				require.Equal(t, va.Name, action.(ktesting.DeleteAction).GetName())
				require.NotEmpty(t, va.Finalizers)
				deleting := metav1.Now()
				va.DeletionTimestamp = &deleting
				return true, nil, fakeClient.Tracker().Update(storagev1.SchemeGroupVersion.WithResource("volumeattachments"), va, "")
			})
			require.NoError(t, client.StorageV1().VolumeAttachments().Delete(ctx, va.Name, metav1.DeleteOptions{}))

			f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
				disk := validCephRetentionTestEvidence()
				disk.SizeBytes = 2 * 1024 * 1024 * 1024
				return disk, nil
			}
			response, err := f.unpublish(t)
			require.NoError(t, err)
			require.NotNil(t, response)
			after, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
			require.Equal(t, before, after)
			f.requireNoProviderMutation(t)
		})
	}
}

func TestControllerUnpublishCephLegacyVAMetadataNeedsHistoryPin(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprintf("knownHistoryImage=%v", known), func(t *testing.T) {
			f := newCephRestartTestFixture(t, true)
			f.va.Status.AttachmentMetadata = nil
			f.syncObjects(t)
			if !known {
				_, err := f.server.driver.volumeHistory.Upsert(context.Background(), cephRestartVolume, func(record *VolumeHistoryRecord) { record.LastSuccessfulImageID = 0 })
				require.NoError(t, err)
			}
			response, err := f.unpublish(t)
			if known {
				require.NoError(t, err)
				require.NotNil(t, response)
			} else {
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, response)
			}
			f.requireNoProviderMutation(t)
		})
	}
}
