package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

type cleanupReadbackProvider struct {
	*MockOpenNebulaVolumeProviderTestify
	attached              bool
	readError             error
	beforeRead            func()
	beforeAuthorize       func()
	imageID, vmID, diskID int
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

func (p *cleanupReadbackProvider) DetachVolumeWithAuthority(ctx context.Context, volume, node string, expectedVMID int, expectedBackend string, verifyOwner opennebula.DetachOwnerCheck, authorize opennebula.DetachAuthorization) (bool, error) {
	if verifyOwner != nil {
		if _, err := verifyOwner(ctx, p.imageID, p.vmID, p.diskID); err != nil {
			return false, err
		}
	}
	if p.beforeAuthorize != nil {
		p.beforeAuthorize()
	}
	if err := authorize(ctx, p.imageID, p.vmID, p.diskID); err != nil {
		return false, err
	}
	err := p.DetachVolume(ctx, volume, node)
	return err == nil, err
}

func TestControllerUnpublishCephObservedHistoryAuthority(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"publish", "stage-only", "stage-only-without-vm", "no-success", "bootstrap", "bootstrap-source", "bootstrap-with-timestamp", "history-backend", "history-schema", "history-vm", "history-vm-replaced", "history-vm-missing", "history-disk", "va-backend"} {
			t.Run(fmt.Sprintf("%v/%s", queued, mode), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				record, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				if mode != "publish" && mode != "bootstrap-with-timestamp" && mode != "bootstrap-source" {
					record.LastSuccessfulPublishTime = time.Time{}
				}
				if mode == "stage-only" || mode == "stage-only-without-vm" {
					record.LastSuccessfulStageTime = time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
				}
				if mode == "bootstrap" || mode == "bootstrap-with-timestamp" {
					record.Bootstrapped = true
				}
				if mode == "bootstrap-source" {
					record.EvidenceSource = volumeHistoryEvidenceSourceBootstrap
				}
				switch mode {
				case "history-backend", "history-schema", "history-vm", "history-vm-replaced", "history-vm-missing", "history-disk", "va-backend":
					record.LastSuccessfulPublishTime = time.Now().UTC()
				}
				switch mode {
				case "history-backend":
					record.Backend = "local"
				case "history-schema":
					record.Version = 42
				case "history-vm":
					record.LastSuccessfulOpenNebulaVMID = -1
				case "history-vm-replaced":
					record.LastSuccessfulOpenNebulaVMID = 574
				case "history-vm-missing", "stage-only-without-vm":
					record.LastSuccessfulOpenNebulaVMID = 0
					f.va.Status.AttachmentMetadata = nil
					f.syncObjects(t)
				case "history-disk":
					record.LastSuccessfulDiskID = new(-1)
				case "va-backend":
					f.va.Status.AttachmentMetadata[annotationBackend] = "local"
					f.syncObjects(t)
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

func TestControllerManualUnpublishPreservesUnobservedProvenance(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, source := range []string{volumeHistoryEvidenceSourceBootstrap, "unknown-provenance"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, source), func(t *testing.T) {
				ctx := context.Background()
				f := newCephRestartTestFixture(t, queued)
				f.pvc.Annotations[annotationRecoveryMode] = recoveryModeManual
				f.syncObjects(t)
				driver := f.server.driver
				record, _ := driver.volumeHistory.Get(cephRestartVolume)
				record.EvidenceSource = source
				record.Bootstrapped = source == volumeHistoryEvidenceSourceBootstrap
				if record.Bootstrapped {
					record.BootstrappedAt = time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
					record.BootstrappedFields = []string{"lastSuccessfulNodeName"}
				}
				raw, err := json.Marshal(record)
				require.NoError(t, err)
				require.NoError(t, driver.kubeRuntime.UpsertConfigMapData(ctx, "default", volumeHistoryStateConfigMapName, map[string]string{cephRestartVolume: string(raw)}))

				_, err = f.unpublish(t)
				require.NoError(t, err)
				detached, err := f.server.readAttachmentHistory(ctx, cephRestartVolume)
				require.NoError(t, err)
				require.False(t, detached.LastSafeDetachTime.IsZero())
				require.Equal(t, f.node.Name, detached.LastSafeDetachNodeName)
				record.LastSafeDetachTime, record.LastSafeDetachNodeName = detached.LastSafeDetachTime, detached.LastSafeDetachNodeName
				require.Equal(t, record, detached)

				delete(f.pvc.Annotations, annotationRecoveryMode)
				f.syncObjects(t)
				response, err := f.unpublish(t)
				require.Error(t, err)
				require.Nil(t, response)
				require.Zero(t, f.provider.inspections.Load())

				client := reviewControllerClient(t, f.server)
				published, err := client.ControllerPublishVolume(ctx, newPublishReq(cephRestartVolume, f.node.Name))
				require.NoError(t, err)
				require.Equal(t, "vdb", published.GetPublishContext()["volumeName"])
				observed, err := f.server.readAttachmentHistory(ctx, cephRestartVolume)
				require.NoError(t, err)
				require.Equal(t, volumeHistoryEvidenceSourceObserved, observed.EvidenceSource)
				require.False(t, observed.Bootstrapped)
				require.Empty(t, observed.BootstrappedFields)
				require.True(t, observed.LastSuccessfulPublishTime.After(detached.LastSuccessfulPublishTime))

				response, err = client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: cephRestartVolume, NodeId: f.node.Name})
				require.NoError(t, err)
				require.NotNil(t, response)
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestControllerUnpublishCephRejectsBackendAndRepairUnknowns(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"missing-backend", "changed-backend", "conflicting-backend-attribute", "empty-repair", "missing-repair-id", "wrong-repair-id", "unknown-repair-schema"} {
			t.Run(fmt.Sprintf("%v/%s", queued, mode), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				before, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				switch mode {
				case "missing-backend":
					delete(f.pv.Annotations, annotationBackend)
				case "changed-backend":
					f.pv.Annotations[annotationBackend] = "local"
				case "conflicting-backend-attribute":
					f.pv.Spec.CSI.VolumeAttributes = map[string]string{annotationBackend: "cephfs"}
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
		for _, mode := range []string{"released", "claim-removed", "terminal-released", "terminal-in-use", "terminal-terminating", "terminating", "ephemeral-terminating", "ephemeral-unverified", "node-in-use", "repair", "incomplete-device-report", "volume-quarantine", "host-quarantine", "observe", "desired-unknown", "provider-unknown", "late-pod", "late-observe-at-rpc", "pv-replaced-after-grace", "node-replaced", "already-absent", "readback-unknown", "readback-attached"} {
			t.Run(fmt.Sprintf("%v/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				f := newCephRestartTestFixture(t, queued)
				_, err := f.unpublish(t)
				require.NoError(t, err)
				persisted, err := f.server.driver.kubeRuntime.client.CoreV1().ConfigMaps("default").List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				pvc := f.pvc.DeepCopy()
				objects := []kruntime.Object{f.pv.DeepCopy(), pvc, f.node.DeepCopy()}
				for i := range persisted.Items {
					objects = append(objects, persisted.Items[i].DeepCopy())
				}
				if mode == "terminating" || strings.HasPrefix(mode, "terminal-") {
					pod := f.pod.DeepCopy()
					if mode != "terminating" {
						pod.Status.Phase = corev1.PodSucceeded
					}
					if mode == "terminating" || mode == "terminal-terminating" {
						at := metav1.NewTime(time.Now().Add(-time.Hour))
						pod.DeletionTimestamp = &at
					}
					objects = append(objects, pod)
				}
				if strings.HasPrefix(mode, "ephemeral-") {
					pod := f.pod.DeepCopy()
					pod.Name = "pvc"
					pod.Spec.Volumes = []corev1.Volume{{
						Name: cephRestartVolume,
						VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{
							VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{},
						}},
					}}
					deleting := metav1.NewTime(time.Now().Add(-time.Hour))
					pod.DeletionTimestamp = &deleting
					if mode == "ephemeral-terminating" {
						pvc.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(pod, corev1.SchemeGroupVersion.WithKind("Pod"))}
					}
					objects = append(objects, pod)
				}
				driver := newStickyTestDriver(t, objects...)
				base := new(MockOpenNebulaVolumeProviderTestify)
				physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: base, attached: true, imageID: 3137, vmID: 575, diskID: 1}
				base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{VolumeHandle: cephRestartVolume, ImageID: 3137, NodeName: "node-a", NodeID: 575, DiskID: 1, Backend: "ceph"}}, opennebula.VMNameInventory{"node-a": {575}}, nil)
				base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1024*1024*1024, nil)
				base.On("NodeExists", mock.Anything, "node-a").Return(575, nil)
				base.On("InspectVolumeAttachment", mock.Anything, cephRestartVolume, "node-a").Return(cephRestartOwnerMetadata(), nil).Maybe()
				if mode == "released" || mode == "claim-removed" || mode == "terminal-released" || mode == "readback-unknown" || mode == "readback-attached" {
					base.On("DetachVolume", mock.Anything, cephRestartVolume, "node-a").Run(func(mock.Arguments) {
						if mode == "released" || mode == "claim-removed" || mode == "terminal-released" {
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
				reportKey := localDeviceReportKey(f.node.Name, cephRestartVolume)
				incompleteReport := fmt.Sprintf(`{"node":%q,"volumeID":%q}`, f.node.Name, cephRestartVolume)
				switch mode {
				case "claim-removed":
					require.NoError(t, driver.kubeRuntime.client.CoreV1().PersistentVolumeClaims(f.pvc.Namespace).Delete(ctx, f.pvc.Name, metav1.DeleteOptions{}))
				case "node-in-use", "terminal-in-use", "node-replaced":
					node := f.node.DeepCopy()
					if mode == "node-in-use" || mode == "terminal-in-use" {
						node.Status.VolumesInUse = []corev1.UniqueVolumeName{"kubernetes.io/csi/" + DefaultDriverName + "^" + cephRestartVolume}
					} else {
						node.UID = "new-node"
					}
					_, err := driver.kubeRuntime.client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
					require.NoError(t, err)
				case "repair":
					_, err := driver.volumeRepairState.Mark(ctx, VolumeRepairState{VolumeID: cephRestartVolume, Classification: "metadata_drift"})
					require.NoError(t, err)
				case "incomplete-device-report":
					require.NoError(t, driver.kubeRuntime.UpsertConfigMapData(ctx, "default", localDeviceStateConfigMapName, map[string]string{reportKey: incompleteReport}))
				case "volume-quarantine":
					_, _, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: cephRestartVolume, Reason: "metadata_drift"}, 1, time.Hour)
					require.NoError(t, err)
				case "host-quarantine":
					_, _, err := driver.hostArtifactQuarantine.MarkFailure(ctx, HostArtifactQuarantineState{Key: "vm-575", VMID: 575, ImageID: 3137, VolumeID: cephRestartVolume}, 1, time.Hour)
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
				case "pv-replaced-after-grace":
					physical.beforeRead = func() {
						physical.beforeRead = nil
						pv := f.pv.DeepCopy()
						pv.UID = "replacement-pv"
						_, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
						require.NoError(t, err)
					}
				case "late-observe-at-rpc":
					physical.beforeAuthorize = func() {
						enableObserveOnConfigMapRead(t, driver.kubeRuntime.client.(*fake.Clientset), f.pvc, 1)
					}
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
				if mode == "incomplete-device-report" {
					cm, err := driver.kubeRuntime.GetConfigMap(ctx, "default", localDeviceStateConfigMapName)
					require.NoError(t, err)
					require.Equal(t, incompleteReport, cm.Data[reportKey])
				}
				if mode == "released" || mode == "claim-removed" || mode == "terminal-released" {
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

type staleCleanupTestProvider struct {
	*cleanupReadbackProvider
}

func (p staleCleanupTestProvider) InspectVolumeDatastore(ctx context.Context, volume string, imageID int) (opennebula.Datastore, error) {
	return (staleAttachmentTestProvider{p.MockOpenNebulaVolumeProviderTestify}).InspectVolumeDatastore(ctx, volume, imageID)
}

func TestAttachmentReconcilerStaleIdentityAndAbsence(t *testing.T) {
	for _, mode := range []string{"absent", "foreign-driver", "foreign-attacher", "replacement-before-read", "replacement-before-list", "replacement-at-delete", "replaced-pv", "unknown", "reattached", "replacement-vm", "va-image", "va-serial"} {
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
			vmID := 575
			switch mode {
			case "replacement-vm":
				vmID = 576
				_, err := f.server.driver.volumeHistory.Upsert(ctx, cephRestartVolume, func(h *VolumeHistoryRecord) {
					h.LastSuccessfulOpenNebulaVMID = 575
				})
				require.NoError(t, err)
			case "va-image":
				f.va.Status.AttachmentMetadata[publishContextOpenNebulaImageID] = "3138"
			case "va-serial":
				f.va.Status.AttachmentMetadata[publishContextDeviceSerial] = "onecsi-3138"
			}
			f.syncObjects(t)
			client := f.server.driver.kubeRuntime.client.(*fake.Clientset)
			base := new(MockOpenNebulaVolumeProviderTestify)
			base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil, nil)
			base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1024, nil)
			base.On("InspectVolumeDatastore", mock.Anything, cephRestartVolume, 3137).Return(opennebula.Datastore{Backend: "ceph"}, nil).Maybe()
			base.On("NodeExists", mock.Anything, "node-a").Return(vmID, nil)
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
			r := NewAttachmentReconciler(NewControllerServer(f.server.driver, staleCleanupTestProvider{physical}, nil))
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
	base.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil, nil)
	base.On("VolumeExists", mock.Anything, cephRestartVolume).Return(3137, 1024, nil)
	base.On("InspectVolumeDatastore", mock.Anything, cephRestartVolume, 3137).Return(opennebula.Datastore{Backend: "ceph"}, nil).Once()
	base.On("NodeExists", mock.Anything, "node-a").Return(575, nil)
	physical := &cleanupReadbackProvider{MockOpenNebulaVolumeProviderTestify: base}
	r := NewAttachmentReconciler(NewControllerServer(f.server.driver, staleCleanupTestProvider{physical}, nil))
	r.staleVASeen[string(f.va.UID)] = time.Now().Add(-2 * r.staleVAGrace)
	r.staleVASeen[string(foreignVA.UID)] = time.Now().Add(-2 * r.staleVAGrace)
	require.NoError(t, r.ReconcileOnce(ctx))
	_, err = client.StorageV1().VolumeAttachments().Get(ctx, f.va.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	observed, err := client.StorageV1().VolumeAttachments().Get(ctx, foreignVA.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, foreignVA.UID, observed.UID)
	base.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	base.AssertExpectations(t)
}

func TestControllerUnpublishCephLateDemandCannotAcknowledge(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			ctx := context.Background()
			client := f.server.driver.kubeRuntime.client
			require.NoError(t, client.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
			var calls int
			for _, expectation := range f.provider.ExpectedCalls {
				if expectation.Method == "InspectVolumeLookup" {
					expectation.Run(func(mock.Arguments) {
						calls++
						if calls == 1 {
							_, err := client.CoreV1().Pods(f.pod.Namespace).Create(ctx, f.pod, metav1.CreateOptions{})
							require.NoError(t, err)
						}
					})
				}
			}
			response, err := f.unpublish(t)
			require.Error(t, err)
			require.Nil(t, response)
			require.Positive(t, calls)
			require.Zero(t, f.provider.inspections.Load())
			history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
			require.True(t, history.LastSafeDetachTime.IsZero())
			f.requireNoProviderMutation(t)
		})
	}
}

func TestControllerUnpublishCephRejectsMalformedSecondaryProtection(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"null", "unrelated-null", "unrelated-valid", "unrelated-dotted", "colliding-dotted", "ambiguous-null", "invalid-key-null", "identity-only", "missing-count", "missing-last-observation", "reversed-observation", "unrelated-reversed-observation", "unrelated-invalid-schema", "wrong-report-key", "miskeyed-relevant-report", "unknown-confirmation", "unknown-failure", "host-key", "host-owner"} {
			t.Run(fmt.Sprintf("%v/%s", queued, mode), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				key := localDeviceReportKey(f.node.Name, cephRestartVolume)
				cm := localDeviceStateConfigMapName
				var raw string
				switch mode {
				case "null":
					raw = "null"
				case "unrelated-null":
					key, raw = localDeviceReportKey(f.node.Name, "other-volume"), "null"
				case "ambiguous-null":
					key, raw = localDeviceReportKey(f.node.Name, "other."+cephRestartVolume), "null"
				case "invalid-key-null":
					key, raw = "unattributable", "null"
				case "identity-only":
					raw = fmt.Sprintf(`{"node":%q,"volumeID":%q}`, f.node.Name, cephRestartVolume)
				case "unrelated-invalid-schema":
					key = localDeviceReportKey(f.node.Name, "other-volume")
					raw = fmt.Sprintf(`{"node":%q,"volumeID":"other-volume","firstObservedAt":17}`, f.node.Name)
				case "missing-count", "missing-last-observation", "reversed-observation", "unrelated-reversed-observation", "unrelated-valid", "unrelated-dotted", "colliding-dotted", "wrong-report-key", "miskeyed-relevant-report", "unknown-confirmation", "unknown-failure":
					now := time.Now().UTC()
					report := LocalDeviceMissingReport{Node: f.node.Name, VolumeID: cephRestartVolume, FailureClass: localDeviceFailureClassMissingDevice,
						FirstObservedAt: now, LastObservedAt: now, Attempts: 1}
					switch mode {
					case "missing-count":
						report.Attempts = 0
					case "missing-last-observation":
						report.LastObservedAt = time.Time{}
					case "reversed-observation":
						report.FirstObservedAt = now.Add(time.Minute)
					case "unrelated-reversed-observation", "unrelated-valid":
						if mode == "unrelated-reversed-observation" {
							report.FirstObservedAt = now.Add(time.Minute)
						}
						report.VolumeID = "other-volume"
						report.FailureClass = localDeviceFailureClassWrongIdentity
						key = localDeviceReportKey(report.Node, report.VolumeID)
					case "wrong-report-key":
						report.VolumeID = "another-volume"
					case "unrelated-dotted", "colliding-dotted":
						report.VolumeID = "archive." + cephRestartVolume
						report.FailureClass = localDeviceFailureClassWrongIdentity
						key = localDeviceReportKey(report.Node, report.VolumeID)
						if mode == "colliding-dotted" {
							candidate := newReadyNode(f.node.Name+".archive", true)
							_, err := f.server.driver.kubeRuntime.client.CoreV1().Nodes().Create(context.Background(), candidate, metav1.CreateOptions{})
							require.NoError(t, err)
						}
					case "miskeyed-relevant-report":
						key = localDeviceReportKey(report.Node, "other-volume")
						report.FailureClass = localDeviceFailureClassWrongIdentity
					case "unknown-confirmation":
						report.ConfirmationState = "future"
					case "unknown-failure":
						report.FailureClass = "future"
					}
					payload, err := json.Marshal(report)
					require.NoError(t, err)
					raw = string(payload)
				case "host-key", "host-owner":
					cm = hostArtifactStateConfigMapName
					key = "vm-575"
					now := time.Now().UTC()
					state := HostArtifactQuarantineState{Key: key, VMID: 575, ImageID: 3137, VolumeID: cephRestartVolume, FailureCount: 1, FirstObservedAt: now, LastObservedAt: now, ExpiresAt: now.Add(time.Hour)}
					if mode == "host-key" {
						state.Key = "vm-576"
					} else {
						state.VolumeID = "another-volume"
					}
					payload, err := json.Marshal(state)
					require.NoError(t, err)
					raw = string(payload)
				}
				require.NoError(t, f.server.driver.kubeRuntime.UpsertConfigMapData(context.Background(), "default", cm, map[string]string{key: raw}))
				reply, err := f.unpublish(t)
				if strings.HasPrefix(mode, "unrelated-") {
					require.NoError(t, err)
					require.NotNil(t, reply)
					require.EqualValues(t, 2, f.provider.inspections.Load())
				} else {
					require.Error(t, err)
					require.Nil(t, reply)
					require.Zero(t, f.provider.inspections.Load())
				}
				f.requireNoProviderMutation(t)
				persisted, err := f.server.driver.kubeRuntime.GetConfigMap(context.Background(), "default", cm)
				require.NoError(t, err)
				require.Equal(t, raw, persisted.Data[key])
			})
		}
	}
}

func TestControllerUnpublishCephEventUsesVerifiedClaimWithoutLateReads(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			client := f.server.driver.kubeRuntime.client.(*fake.Clientset)
			var finalPodRead atomic.Bool
			var finalAuthorityRead atomic.Bool
			var lateActions atomic.Int32
			client.PrependReactor("*", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
				if finalAuthorityRead.Load() {
					lateActions.Add(1)
				}
				if action.Matches("list", "pods") && f.provider.inspections.Load() == 2 {
					finalPodRead.Store(true)
				}
				if action.Matches("list", "volumeattachments") && finalPodRead.Load() {
					finalAuthorityRead.Store(true)
				}
				return false, nil, nil
			})
			broadcaster := record.NewBroadcaster()
			t.Cleanup(broadcaster.Shutdown)
			events := make(chan *corev1.Event, 1)
			watcher := broadcaster.StartEventWatcher(func(event *corev1.Event) { events <- event })
			t.Cleanup(watcher.Stop)
			f.server.driver.kubeRuntime.recorder = broadcaster.NewRecorder(clientgoscheme.Scheme, corev1.EventSource{Component: DefaultDriverName})

			reply, err := f.unpublish(t)
			require.NoError(t, err)
			require.NotNil(t, reply)
			require.True(t, finalAuthorityRead.Load())
			require.Zero(t, lateActions.Load(), "acknowledgement must follow the final authority read without another synchronous API call")
			select {
			case event := <-events:
				require.Equal(t, f.pvc.UID, event.InvolvedObject.UID)
				require.Equal(t, f.pvc.Namespace, event.InvolvedObject.Namespace)
				require.Equal(t, f.pvc.Name, event.InvolvedObject.Name)
				require.Equal(t, eventReasonSameNodeDetachRescued, event.Reason)
				require.Contains(t, event.Message, "logical attachment deletion")
				require.NotContains(t, event.Message, "3137")
				require.NotContains(t, event.Message, "575")
				require.NotContains(t, event.Message, "disk 1")
			case <-time.After(time.Second):
				t.Fatal("retention event was not queued")
			}
		})
	}
}

func TestControllerUnpublishCephPreservesPersistedRecoveryControlsForAUTO(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"auto", "observe", "manual"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				f := newCephRestartTestFixture(t, queued)
				client := f.server.driver.kubeRuntime.client.(*fake.Clientset)
				if mode != "auto" {
					f.pvc.Annotations[annotationRecoveryMode] = mode
					f.syncObjects(t)
				}
				_, err := f.server.driver.volumeRecoveryControl.Upsert(ctx, cephRestartVolume, func(state *VolumeRecoveryControlState) {
					state.Mode = recoveryModeObserve
					state.SuppressRepairActions = true
				})
				require.NoError(t, err)
				configMaps := client.CoreV1().ConfigMaps("default")
				before, err := configMaps.Get(ctx, volumeRecoveryControlStateConfigMapName, metav1.GetOptions{})
				require.NoError(t, err)
				historyBefore, err := f.server.readAttachmentHistory(ctx, cephRestartVolume)
				require.NoError(t, err)
				client.ClearActions()

				reply, err := f.unpublish(t)
				requestActions := client.Actions()
				if mode == "observe" {
					require.Equal(t, codes.FailedPrecondition, status.Code(err))
					require.Nil(t, reply)
				} else {
					require.NoError(t, err)
					require.NotNil(t, reply)
				}
				after, err := configMaps.Get(ctx, volumeRecoveryControlStateConfigMapName, metav1.GetOptions{})
				require.NoError(t, err)
				historyAfter, err := f.server.readAttachmentHistory(ctx, cephRestartVolume)
				require.NoError(t, err)
				if mode == "manual" {
					var control VolumeRecoveryControlState
					require.NoError(t, json.Unmarshal([]byte(after.Data[cephRestartVolume]), &control))
					require.Equal(t, recoveryModeManual, control.Mode)
					require.False(t, historyAfter.LastSafeDetachTime.IsZero())
					require.Zero(t, f.provider.inspections.Load())
				} else {
					require.Equal(t, before.Data, after.Data)
					require.Equal(t, historyBefore, historyAfter)
					for _, action := range requestActions {
						require.Contains(t, []string{"get", "list"}, action.GetVerb(), "automatic eligibility must be read-only")
					}
				}
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func enableObserveOnConfigMapRead(t *testing.T, client *fake.Clientset, pvc *corev1.PersistentVolumeClaim, after int) {
	t.Helper()
	client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.(ktesting.GetAction).GetName() != hostArtifactStateConfigMapName {
			return false, nil, nil
		}
		after--
		if after == 0 {
			replacement := pvc.DeepCopy()
			replacement.Annotations[annotationRecoveryMode] = recoveryModeObserve
			require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), replacement, pvc.Namespace))
		}
		return false, nil, nil
	})
}

func TestControllerUnpublishCephRechecksDemandAfterProtectionReads(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			client := f.server.driver.kubeRuntime.client.(*fake.Clientset)
			f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
				if f.provider.inspections.Load() == 2 {
					fired := false
					client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
						if !fired && action.(ktesting.GetAction).GetName() == hostArtifactStateConfigMapName {
							fired = true
							pod := activePodForPVC("late-remote-consumer", f.pvc.Name, "node-b")
							pod.UID = "late-remote-pod"
							err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace)
							require.NoError(t, err)
						}
						return false, nil, nil
					})
				}
				return validCephRetentionTestEvidence(), nil
			}
			response, err := f.unpublish(t)
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Nil(t, response)
			history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
			require.True(t, history.LastSafeDetachTime.IsZero())
			f.requireNoProviderMutation(t)
		})
	}
}

func TestControllerUnpublishCephDeniesObserveDuringFinalRead(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, duringPods := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%t/pods=%t", queued, duringPods), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				client := f.server.driver.kubeRuntime.client.(*fake.Clientset)
				var installed bool
				var protectionReads int
				f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
					if f.provider.inspections.Load() == 2 {
						if duringPods {
							client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
								if action.(ktesting.GetAction).GetName() == hostArtifactStateConfigMapName {
									protectionReads++
								}
								return false, nil, nil
							})
							client.PrependReactor("list", "pods", func(ktesting.Action) (bool, kruntime.Object, error) {
								if !installed && protectionReads >= 2 {
									observed := f.pvc.DeepCopy()
									observed.Annotations[annotationRecoveryMode] = recoveryModeObserve
									require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), observed, f.pvc.Namespace))
									installed = true
								}
								return false, nil, nil
							})
						} else {
							enableObserveOnConfigMapRead(t, client, f.pvc, 1)
						}
					}
					return validCephRetentionTestEvidence(), nil
				}
				response, err := f.unpublish(t)
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, response)
				require.EqualValues(t, 2, f.provider.inspections.Load())
				if duringPods {
					require.True(t, installed)
					require.GreaterOrEqual(t, protectionReads, 3)
				}
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func persistProtectionAfterHostRead(t *testing.T, client *fake.Clientset, name, key, raw string) {
	t.Helper()
	client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.(ktesting.GetAction).GetName() != hostArtifactStateConfigMapName {
			return false, nil, nil
		}
		configMaps := corev1.SchemeGroupVersion.WithResource("configmaps")
		namespace := action.GetNamespace()
		current, readErr := client.Tracker().Get(configMaps, namespace, hostArtifactStateConfigMapName)
		_, installedErr := client.Tracker().Get(configMaps, namespace, name)
		if apierrors.IsNotFound(installedErr) {
			require.NoError(t, client.Tracker().Create(configMaps, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Data:       map[string]string{key: raw},
			}, namespace))
		}
		return true, current, readErr
	})
}

func TestControllerUnpublishCephRechecksPersistedProtection(t *testing.T) {
	now := time.Now().UTC()
	const volume = cephRestartVolume
	cases := []struct {
		name, key string
		record    any
	}{
		{volumeRepairStateConfigMapName, volume, VolumeRepairState{Version: 1, VolumeID: volume, Classification: repairClassificationWrongDeviceIdentity}},
		{localDeviceStateConfigMapName, localDeviceReportKey("node-a", volume), LocalDeviceMissingReport{Node: "node-a", VolumeID: volume, FailureClass: localDeviceFailureClassWrongIdentity, FirstObservedAt: now, LastObservedAt: now, Attempts: 1}},
		{volumeQuarantineStateConfigMapName, volume, VolumeQuarantineState{VolumeID: volume, Reason: "metadata drift", FailureCount: 1, FirstObservedAt: now, LastObservedAt: now, ExpiresAt: now.Add(time.Hour)}},
		{hostArtifactStateConfigMapName, "vm-575", HostArtifactQuarantineState{Key: "vm-575", VMID: 575, VolumeID: volume, FailureCount: 1, FirstObservedAt: now, LastObservedAt: now, ExpiresAt: now.Add(time.Hour)}},
	}
	for _, queued := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v/%s", queued, tc.name), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				raw, err := json.Marshal(tc.record)
				require.NoError(t, err)
				f.provider.observe = func(context.Context) (*opennebula.SameNodeRetentionEvidence, error) {
					if f.provider.inspections.Load() == 2 {
						persistProtectionAfterHostRead(t, f.server.driver.kubeRuntime.client.(*fake.Clientset), tc.name, tc.key, string(raw))
					}
					return validCephRetentionTestEvidence(), nil
				}
				response, err := f.unpublish(t)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Nil(t, response)
				require.EqualValues(t, 2, f.provider.inspections.Load())
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestControllerUnpublishCephRejectsContradictoryQuarantineTimes(t *testing.T) {
	now := time.Now().UTC()
	hourAgo := now.Add(-time.Hour)
	for _, host := range []bool{false, true} {
		for _, tc := range []struct {
			name                string
			first, last, expiry time.Time
		}{
			{name: "missing observations", expiry: hourAgo},
			{name: "expiry precedes last failure", first: hourAgo, last: now, expiry: hourAgo},
			{name: "unordered pending observations", first: now, last: hourAgo},
			{name: "expiry equals last failure", first: hourAgo, last: hourAgo, expiry: hourAgo},
		} {
			t.Run(fmt.Sprintf("host=%v/%s", host, tc.name), func(t *testing.T) {
				f := newCephRestartTestFixture(t, false)
				cm, key := volumeQuarantineStateConfigMapName, cephRestartVolume
				var record any = VolumeQuarantineState{VolumeID: cephRestartVolume, Reason: "metadata drift", FailureCount: 1, FirstObservedAt: tc.first, LastObservedAt: tc.last, ExpiresAt: tc.expiry}
				if host {
					cm, key = hostArtifactStateConfigMapName, "vm-575"
					record = HostArtifactQuarantineState{Key: key, VMID: 575, VolumeID: cephRestartVolume, FailureCount: 1, FirstObservedAt: tc.first, LastObservedAt: tc.last, ExpiresAt: tc.expiry}
				}
				raw, err := json.Marshal(record)
				require.NoError(t, err)
				require.NoError(t, f.server.driver.kubeRuntime.UpsertConfigMapData(context.Background(), "default", cm, map[string]string{key: string(raw)}))
				reply, err := f.unpublish(t)
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, reply)
				require.Zero(t, f.provider.inspections.Load())
				f.requireNoProviderMutation(t)
			})
		}
	}
}

func TestControllerUnpublishRejectsDuplicateProtectionEvidence(t *testing.T) {
	now := time.Now().UTC()
	observed := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	future := now.Add(time.Hour).Format(time.RFC3339Nano)
	expired := now.Add(-time.Hour).Format(time.RFC3339Nano)
	times := fmt.Sprintf(`"firstObservedAt":%q,"lastObservedAt":%q,"expiresAt":%q`, observed, observed, future)
	for _, tc := range []struct {
		name, configMap, key, record, message string
	}{
		{
			"duplicate volume expiry", volumeQuarantineStateConfigMapName, cephRestartVolume,
			fmt.Sprintf(`{"volumeID":%q,"reason":"metadata drift","failureCount":1,%s,"expiresAt":%q}`, cephRestartVolume, times, expired),
			"volume quarantine is malformed",
		},
		{
			"aliased host expiry", hostArtifactStateConfigMapName, "vm-575",
			fmt.Sprintf(`{"key":"vm-575","vmID":575,"volumeID":%q,"failureCount":1,%s,"expire\u017fAt":%q}`, cephRestartVolume, times, expired),
			"host artifact quarantine is malformed",
		},
		{
			"nested repair identity", volumeRepairStateConfigMapName, cephRestartVolume,
			fmt.Sprintf(`{"version":1,"volumeID":%q,"classification":"wrong_device_identity","lastObservedIdentity":{"observedFromDevice":{"block":{"deviceSerial":"onecsi-7","DeviceSerial":"onecsi-8"}}}}`, cephRestartVolume),
			"cannot verify current volume repair state",
		},
		{
			"aliased device routing identity", localDeviceStateConfigMapName, localDeviceReportKey("node-a", "other-volume"),
			fmt.Sprintf(`{"node":"node-a","volumeID":%q,"VolumeId":"other-volume","failureClass":"wrong_device_identity","firstObservedAt":%q,"lastObservedAt":%q,"attempts":1}`, cephRestartVolume, observed, observed),
			"cannot verify current volume repair state",
		},
		{
			"aliased host routing identity", hostArtifactStateConfigMapName, "vm-999",
			fmt.Sprintf(`{"key":"vm-999","vmID":575,"VMId":999,"volumeID":"other-volume","failureCount":1,%s}`, times),
			"host artifact quarantine is malformed",
		},
		{
			"duplicate matching device route", localDeviceStateConfigMapName, localDeviceReportKey("node-a", cephRestartVolume),
			fmt.Sprintf(`{"node":"node-a","volumeID":%q,"VolumeID":%q,"firstObservedAt":%q,"lastObservedAt":%q,"attempts":1}`, cephRestartVolume, cephRestartVolume, observed, observed),
			"cannot verify current volume repair state",
		},
		{
			"duplicate matching host route", hostArtifactStateConfigMapName, "vm-575",
			fmt.Sprintf(`{"key":"vm-575","vmID":575,"VMID":575,"volumeID":%q,"failureCount":1,%s}`, cephRestartVolume, times),
			"host artifact quarantine is malformed",
		},
	} {
		for _, tcPath := range []struct{ queued, physical bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
			t.Run(fmt.Sprintf("%s/queued=%t/physical=%t", tc.name, tcPath.queued, tcPath.physical), func(t *testing.T) {
				ctx := context.Background()
				f := newCephRestartTestFixture(t, tcPath.queued)
				runtime := f.server.driver.kubeRuntime
				if tcPath.physical {
					require.NoError(t, runtime.client.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
				}
				require.NoError(t, runtime.UpsertConfigMapData(ctx, "default", tc.configMap, map[string]string{tc.key: tc.record}))
				before, err := f.server.readAttachmentHistory(ctx, cephRestartVolume)
				require.NoError(t, err)
				response, err := f.unpublish(t)
				require.ErrorContains(t, err, tc.message)
				require.Nil(t, response)
				require.Zero(t, f.provider.inspections.Load())
				f.requireNoProviderMutation(t)
				after, err := f.server.readAttachmentHistory(ctx, cephRestartVolume)
				require.NoError(t, err)
				require.Equal(t, before, after)
				persisted, err := runtime.GetConfigMap(ctx, "default", tc.configMap)
				require.NoError(t, err)
				require.Equal(t, tc.record, persisted.Data[tc.key])
			})
		}
	}
}

func TestAttachmentProtectionRoutesUnrelatedMalformedRecords(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, auto := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%t/auto=%t", queued, auto), func(t *testing.T) {
				ctx := context.Background()
				const report = `{"node":"node-b","volumeID":"volume-b","VolumeID":"volume-b","attempts":5,"Attempts":0}`
				const quarantine = `{"key":"vm-999","vmID":999,"VMID":999,"volumeID":"volume-b","expiresAt":"invalid","ExpiresAt":null}`
				reportKey := localDeviceReportKey("node-b", "volume-b")
				var driver *Driver
				var check func()
				if auto {
					f := newCephRestartTestFixture(t, queued)
					driver = f.server.driver
					check = func() {
						reply, err := f.unpublish(t)
						require.NoError(t, err)
						require.NotNil(t, reply)
						require.EqualValues(t, 2, f.provider.inspections.Load())
						f.requireNoProviderMutation(t)
					}
				} else {
					const volume = "volume-test"
					pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "local"})
					node := newReadyNode("node-old", true)
					driver = newStickyTestDriver(t, pv, pvc, node)
					driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
					provider, wire := nodeAttachmentProvider(t)
					server := NewControllerServer(driver, provider, nil)
					if queued {
						driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
						driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
					}
					check = func() {
						client := reviewControllerClient(t, server)
						published, err := client.ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
						require.NoError(t, err)
						require.Equal(t, "vdb", published.GetPublishContext()["volumeName"])
						reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
						require.NoError(t, err)
						require.NotNil(t, reply)
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, []string{"detach:41"}, wire.mutations)
					}
				}
				runtime := driver.kubeRuntime
				require.NoError(t, runtime.UpsertConfigMapData(ctx, "default", localDeviceStateConfigMapName, map[string]string{reportKey: report}))
				require.NoError(t, runtime.UpsertConfigMapData(ctx, "default", hostArtifactStateConfigMapName, map[string]string{"vm-999": quarantine}))
				check()
				reports, err := runtime.GetConfigMap(ctx, "default", localDeviceStateConfigMapName)
				require.NoError(t, err)
				require.Equal(t, map[string]string{reportKey: report}, reports.Data)
				quarantines, err := runtime.GetConfigMap(ctx, "default", hostArtifactStateConfigMapName)
				require.NoError(t, err)
				require.Equal(t, map[string]string{"vm-999": quarantine}, quarantines.Data)
			})
		}
	}
}

func TestControllerUnpublishCephRequiresSerializedQuarantineExpiry(t *testing.T) {
	for _, host := range []bool{false, true} {
		for _, expiry := range []string{"omitted", "null", "zero"} {
			t.Run(fmt.Sprintf("host=%v/%s", host, expiry), func(t *testing.T) {
				f := newCephRestartTestFixture(t, false)
				cm, key := volumeQuarantineStateConfigMapName, cephRestartVolume
				now := time.Now().UTC()
				var pending any = VolumeQuarantineState{VolumeID: cephRestartVolume, Reason: "metadata drift", FailureCount: 1, FirstObservedAt: now, LastObservedAt: now}
				if host {
					cm, key = hostArtifactStateConfigMapName, "vm-575"
					pending = HostArtifactQuarantineState{Key: key, VMID: 575, VolumeID: cephRestartVolume, FailureCount: 1, FirstObservedAt: now, LastObservedAt: now}
				}
				raw, err := json.Marshal(pending)
				require.NoError(t, err)
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(raw, &fields))
				switch expiry {
				case "omitted":
					delete(fields, "expiresAt")
				case "null":
					fields["expiresAt"] = json.RawMessage("null")
				case "zero":
					require.JSONEq(t, `"0001-01-01T00:00:00Z"`, string(fields["expiresAt"]))
				}
				raw, err = json.Marshal(fields)
				require.NoError(t, err)
				require.NoError(t, f.server.driver.kubeRuntime.UpsertConfigMapData(context.Background(), "default", cm, map[string]string{key: string(raw)}))
				if expiry != "zero" {
					defer func() {
						ctx := context.Background()
						require.NoError(t, f.server.driver.kubeRuntime.client.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
						response, err := f.unpublish(t)
						require.Equal(t, codes.Unavailable, status.Code(err))
						require.Nil(t, response)
						f.requireNoProviderMutation(t)
					}()
				}
				response, err := f.unpublish(t)
				if expiry == "zero" {
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
}

func TestControllerUnpublishCephQuarantineMatchesActualTarget(t *testing.T) {
	for _, mode := range []string{"pending", "unrelated-disk", "same-disk", "same-vm", "same-node"} {
		t.Run(mode, func(t *testing.T) {
			f := newCephRestartTestFixture(t, false)
			ctx := context.Background()
			target := HostArtifactQuarantineState{VolumeID: "different-volume", ImageID: 999, VMID: 575, NodeName: f.node.Name}
			switch mode {
			case "unrelated-disk":
				target.Key, target.DiskID = "vm-575.disk-2", 2
			case "same-disk":
				target.Key, target.DiskID = "vm-575.disk-1", 1
			case "same-node":
				target.Key, target.VMID = f.node.Name, 0
			default:
				target.Key = "vm-575"
			}
			threshold := 1
			if mode == "pending" {
				threshold = 2
				_, active, err := f.server.driver.volumeQuarantine.MarkFailure(ctx,
					VolumeQuarantineState{VolumeID: cephRestartVolume, Reason: "metadata drift"}, threshold, time.Hour)
				require.NoError(t, err)
				require.False(t, active)
			}
			_, active, err := f.server.driver.hostArtifactQuarantine.MarkFailure(ctx, target, threshold, time.Hour)
			require.NoError(t, err)
			require.Equal(t, mode != "pending", active)
			response, err := f.unpublish(t)
			if mode == "pending" || mode == "unrelated-disk" {
				require.NoError(t, err)
				require.NotNil(t, response)
			} else {
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Nil(t, response)
			}
			f.requireNoProviderMutation(t)
		})
	}
}

func TestHostArtifactQuarantineFollowsVolumeAcrossVMs(t *testing.T) {
	for _, op := range []string{"publish", "retention", "detach"} {
		for _, malformed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/malformed=%t", op, malformed), func(t *testing.T) {
				ctx := context.Background()
				wantCode := codes.FailedPrecondition
				if malformed {
					wantCode = codes.Unavailable
				}
				volume, node := cephRestartVolume, "node-a"
				var server *ControllerServer
				var checkProvider func()
				if op == "publish" {
					volume, node = "volume-test", "node-new"
					pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
					driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true), newReadyNode(node, true))
					provider, wire := nodeAttachmentProvider(t)
					wire.mu.Lock()
					wire.disks[42] = wire.disks[41]
					delete(wire.disks, 41)
					wire.mu.Unlock()
					server = NewControllerServer(driver, provider, nil)
					checkProvider = func() {
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Empty(t, wire.mutations)
						require.Equal(t, map[int]int{42: 256}, wire.disks)
					}
				} else {
					f := newCephRestartTestFixture(t, op == "retention")
					server = f.server
					if op == "detach" {
						require.NoError(t, server.driver.kubeRuntime.client.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
					}
					checkProvider = func() {
						f.requireNoProviderMutation(t)
						require.Zero(t, f.provider.inspections.Load())
					}
				}
				now := time.Now().UTC()
				const key = "vm-41"
				record := HostArtifactQuarantineState{Key: key, VMID: 41, VolumeID: " " + volume + " ", FailureCount: 1, FirstObservedAt: now, LastObservedAt: now, ExpiresAt: now.Add(time.Hour)}
				data, err := json.Marshal(record)
				require.NoError(t, err)
				raw := string(data)
				if malformed {
					raw = strings.TrimSuffix(raw, "}") + `,"FailureCount":0}`
				}
				runtime := server.driver.kubeRuntime
				require.NoError(t, runtime.UpsertConfigMapData(ctx, "default", hostArtifactStateConfigMapName, map[string]string{key: raw}))
				client := reviewControllerClient(t, server)
				if op == "publish" {
					reply, err := client.ControllerPublishVolume(ctx, newPublishReq(volume, node))
					require.Equal(t, wantCode, status.Code(err), "%v", err)
					require.ErrorContains(t, err, "host artifact quarantine")
					require.Nil(t, reply)
				} else {
					reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node})
					require.Equal(t, wantCode, status.Code(err), "%v", err)
					require.ErrorContains(t, err, "host artifact quarantine")
					require.Nil(t, reply)
				}
				checkProvider()
				history, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.True(t, history.LastSafeDetachTime.IsZero())
				if op == "publish" {
					require.True(t, history.LastSuccessfulPublishTime.IsZero())
				}
				cm, err := runtime.GetConfigMap(ctx, "default", hostArtifactStateConfigMapName)
				require.NoError(t, err)
				require.Equal(t, map[string]string{key: raw}, cm.Data)
			})
		}
	}
}

func TestControllerUnpublishHidesStoredRepairDetails(t *testing.T) {
	for _, mode := range []string{"auto", "inline-detach", "queued-detach"} {
		t.Run(mode, func(t *testing.T) {
			f := newCephRestartTestFixture(t, mode == "queued-detach")
			ctx := context.Background()
			kube := f.server.driver.kubeRuntime.client
			if mode != "auto" {
				require.NoError(t, kube.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
			}
			const privateDetail = "privileged-serial-and-filesystem-uuid"
			_, err := f.server.driver.volumeRepairState.Mark(ctx,
				VolumeRepairState{VolumeID: cephRestartVolume, Classification: repairClassificationWrongDeviceIdentity, Message: privateDetail})
			require.NoError(t, err)
			resp, err := f.unpublish(t)
			require.ErrorContains(t, err, "volume repair is required")
			require.NotContains(t, err.Error(), privateDetail)
			require.Nil(t, resp)
			events, err := kube.CoreV1().Events(f.pvc.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, events.Items)
			for _, event := range events.Items {
				require.NotContains(t, event.Message, privateDetail)
			}
			f.requireNoProviderMutation(t)
		})
	}
}

func replaceDeletingVolumeAttachment(t *testing.T, kube *fake.Clientset, va *storagev1.VolumeAttachment) {
	t.Helper()
	resource := storagev1.SchemeGroupVersion.WithResource("volumeattachments")
	replacement := va.DeepCopy()
	replacement.UID = "replacement-attachment-uid"
	require.NoError(t, kube.Tracker().Delete(resource, "", va.Name))
	require.NoError(t, kube.Tracker().Create(resource, replacement, ""))
}

func TestControllerUnpublishCephRejectsVAReplacementDuringFinalProtectionRead(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			first, err := f.unpublish(t)
			require.NoError(t, err)
			require.NotNil(t, first)

			kube := f.server.driver.kubeRuntime.client.(*fake.Clientset)
			f.provider.inspections.Store(0)
			var replaced atomic.Bool
			var reads int
			kube.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
				if action.(ktesting.GetAction).GetName() == hostArtifactStateConfigMapName &&
					f.provider.inspections.Load() == 2 {
					reads++
					if reads == 3 {
						replacement := f.va.DeepCopy()
						replacement.UID = "current-nondeleting-successor"
						replacement.DeletionTimestamp = nil
						require.NoError(t, kube.Tracker().Update(storagev1.SchemeGroupVersion.WithResource("volumeattachments"), replacement, ""))
						replaced.Store(true)
					}
				}
				return false, nil, nil
			})
			reply, err := f.unpublish(t)
			require.True(t, replaced.Load())
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.ErrorContains(t, err, "VolumeAttachment deletion authority changed")
			require.Nil(t, reply)
			history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
			require.True(t, history.LastSafeDetachTime.IsZero())
			f.requireNoProviderMutation(t)
		})
	}
}

func TestControllerUnpublishCephRechecksNodeAfterProtectionReads(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, change := range []string{"uid", "deleting", "readiness"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, change), func(t *testing.T) {
				f := newCephRestartTestFixture(t, queued)
				kube := f.server.driver.kubeRuntime.client.(*fake.Clientset)
				var readNode, changed atomic.Bool
				kube.PrependReactor("get", "nodes", func(ktesting.Action) (bool, kruntime.Object, error) {
					if f.provider.inspections.Load() == 2 {
						readNode.Store(true)
					}
					return false, nil, nil
				})
				kube.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
					if action.(ktesting.GetAction).GetName() != hostArtifactStateConfigMapName ||
						!readNode.Load() || !changed.CompareAndSwap(false, true) {
						return false, nil, nil
					}
					node := f.node.DeepCopy()
					nodes := corev1.SchemeGroupVersion.WithResource("nodes")
					switch change {
					case "uid":
						node.UID = "replacement-node"
						require.NoError(t, kube.Tracker().Delete(nodes, "", node.Name))
						require.NoError(t, kube.Tracker().Create(nodes, node, ""))
					case "deleting":
						now := metav1.Now()
						node.DeletionTimestamp = &now
						require.NoError(t, kube.Tracker().Update(nodes, node, ""))
					case "readiness":
						node.Status.Conditions[0].Status = corev1.ConditionFalse
						require.NoError(t, kube.Tracker().Update(nodes, node, ""))
					}
					return false, nil, nil
				})
				resp, err := f.unpublish(t)
				require.True(t, changed.Load())
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "node identity or readiness changed")
				require.Nil(t, resp)
				history, _ := f.server.driver.volumeHistory.Get(cephRestartVolume)
				require.True(t, history.LastSafeDetachTime.IsZero())
				f.requireNoProviderMutation(t)
			})
		}
	}
}
