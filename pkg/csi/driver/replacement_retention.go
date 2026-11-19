package driver

import (
	"context"
	"reflect"
	"slices"
	"strings"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type markedReplacementObservation struct {
	disk       opennebula.VolumeDiskRecord
	capacity   int64
	va         detachAttachmentAuthority
	vaName     string
	requested  string
	pvVersion  string
	pvcVersion string
}

func (s *ControllerServer) replacementVAGone(ctx context.Context, name string, uids ...string) (bool, error) {
	va, err := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, ctx.Err()
	}
	if err != nil || va.UID == "" {
		return false, attachmentEvidenceUnavailable("pinned replacement VolumeAttachment identity is unverified", err)
	}
	return !slices.Contains(uids, string(va.UID)), ctx.Err()
}

func (s *ControllerServer) checkReplacementVAOrigin(ctx context.Context, attempt replacementAttachAttempt, pvName, name string, observed detachAttachmentAuthority) error {
	if name == "" {
		gone, err := s.replacementVAGone(ctx, attempt.VAName, attempt.VAUID)
		if err != nil || !gone {
			return attachmentEvidenceUnavailable("pinned replacement VolumeAttachment retirement is unverified", err)
		}
		return nil
	}
	if name != attempt.VAName {
		return attachmentEvidenceUnavailable("replacement VolumeAttachment name differs from the pinned name", nil)
	}
	if string(observed.uid) == attempt.VAUID {
		return ctx.Err()
	}
	return s.recheckReplacementVolumeAttachment(ctx, pvName, name, observed)
}

func (s *ControllerServer) recheckReplacementVolumeAttachment(ctx context.Context, pvName, name string, observed detachAttachmentAuthority) error {
	current, err := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(ctx, name, metav1.GetOptions{})
	if err != nil || current == nil || current.Name != name || !matchesVolumeAttachment(pvName, current) ||
		!reflect.DeepEqual(observed, detachAttachmentSnapshot(current)) {
		return attachmentEvidenceUnavailable("replacement VolumeAttachment identity is unverified", err)
	}
	return ctx.Err()
}

func validateMarkedReplacementMetadata(va detachAttachmentAuthority, pv *corev1.PersistentVolume, target string, imageID int) error {
	if backend, present := va.metadata[annotationBackend]; present && !isCephBackend(backend) {
		return attachmentEvidenceUnavailable("marked replacement VolumeAttachment backend changed", nil)
	}
	if value, present := va.metadata["volumeName"]; present && strings.TrimPrefix(strings.TrimSpace(value), "/dev/") != target {
		return attachmentEvidenceUnavailable("marked replacement VolumeAttachment target changed", nil)
	}
	if err := validateAttachmentImageAssertions(va.metadata, imageID); err != nil {
		return attachmentEvidenceUnavailable("marked replacement VolumeAttachment image or serial changed", err)
	}
	for key, expected := range map[string]string{
		paramPVName: pv.Name, paramPVCName: pv.Spec.ClaimRef.Name, paramPVCNamespace: pv.Spec.ClaimRef.Namespace,
	} {
		if value, present := va.metadata[key]; present && strings.TrimSpace(value) != expected {
			return attachmentEvidenceUnavailable("marked replacement VolumeAttachment metadata changed", nil)
		}
	}
	return nil
}

func markedFilesystemResizePending(pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) bool {
	if (pv.Spec.VolumeMode != nil && *pv.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) ||
		(pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) {
		return false
	}
	completed, requested := pv.Spec.Capacity.Storage(), pvc.Spec.Resources.Requests.Storage()
	observed := pvc.Status.Capacity.Storage()
	if requested.Sign() <= 0 || requested.Cmp(*completed) > 0 || observed.Sign() <= 0 || observed.Cmp(*completed) > 0 {
		return false
	}
	conditions := pvc.Status.Conditions
	if len(conditions) != 1 || conditions[0].Type != corev1.PersistentVolumeClaimFileSystemResizePending ||
		conditions[0].Status != corev1.ConditionTrue {
		return false
	}
	statuses := pvc.Status.AllocatedResourceStatuses
	if len(statuses) != 0 && (len(statuses) != 1 || statuses[corev1.ResourceStorage] != corev1.PersistentVolumeClaimNodeResizePending) {
		return false
	}
	allocated := pvc.Status.AllocatedResources
	if len(allocated) == 0 {
		return true
	}
	size, ok := allocated[corev1.ResourceStorage]
	return len(allocated) == 1 && ok && size.Cmp(*requested) >= 0 && size.Cmp(*completed) <= 0
}

func (s *ControllerServer) observeMarkedReplacement(ctx context.Context, attempt replacementAttachAttempt, publish *csi.ControllerPublishVolumeRequest) (markedReplacementObservation, error) {
	var observation markedReplacementObservation
	deny := func(err error) (markedReplacementObservation, error) {
		if ctx.Err() != nil {
			return observation, status.FromContextError(ctx.Err()).Err()
		}
		return observation, attachmentEvidenceUnavailable("marked replacement Ceph retention is unverified", err)
	}
	history, err := s.readAttachmentHistory(ctx, attempt.VolumeID)
	if err != nil || history.PendingReplacementAttach == nil || *history.PendingReplacementAttach != attempt ||
		!attempt.matchesHistory(history) || !attempt.dispatchPossible() || attempt.Driver != s.driver.name {
		return deny(err)
	}
	disk, err := s.inspectReplacementDisk(ctx, attempt, true)
	if err != nil {
		return deny(err)
	}
	inspector, ok := s.volumeProvider.(opennebula.SameNodeRetentionInspector)
	if !ok {
		return deny(nil)
	}
	capacity, err := inspector.InspectSameNodeRetention(opennebula.WithExpectedImageID(ctx, attempt.ImageID), attempt.VolumeID, attempt.Node)
	if err != nil || capacity == nil || capacity.VolumeID != attempt.VolumeID || capacity.ImageID != attempt.ImageID ||
		capacity.NodeID != attempt.VMID || capacity.DiskID != disk.DiskID || capacity.Target != disk.Target ||
		!opennebula.DeviceSerialMatchesImage(capacity.DeviceSerial, attempt.ImageID) {
		return deny(err)
	}
	observation.disk, observation.capacity = *disk, capacity.SizeBytes
	proveVA := func(name string, va detachAttachmentAuthority, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) error {
		if publish == nil {
			if !va.deleting {
				return attachmentEvidenceUnavailable("marked replacement retention requires a deleting VolumeAttachment", nil)
			}
		} else if string(va.uid) == attempt.VAUID || va.deleting || va.attached {
			return attachmentEvidenceUnavailable("marked replacement completion requires an eligible successor VolumeAttachment", nil)
		}
		if err := s.checkReplacementVAOrigin(ctx, attempt, pv.Name, name, va); err != nil {
			return err
		}
		size, requested, ok := completedRetentionCapacity(pv, pvc)
		if size != capacity.SizeBytes || (!ok && (!markedFilesystemResizePending(pv, pvc) || (publish != nil && publish.GetVolumeCapability().GetMount() == nil))) {
			return attachmentEvidenceUnavailable("marked replacement Ceph capacity is pending or inconsistent", nil)
		}
		if err := validateMarkedReplacementMetadata(va, pv, disk.Target, attempt.ImageID); err != nil {
			return err
		}
		observation.vaName, observation.va, observation.requested = name, va, requested
		observation.pvVersion, observation.pvcVersion = pv.ResourceVersion, pvc.ResourceVersion
		return nil
	}
	if _, err := s.verifyReplacementBinding(ctx, history, attempt, disk.DiskID, proveVA); err != nil {
		return deny(err)
	}
	if err := s.checkReplacementAttemptReadback(ctx, attempt); err != nil {
		return deny(err)
	}
	return observation, ctx.Err()
}

func (s *ControllerServer) rescueMarkedReplacementUnpublish(ctx context.Context, volume, node string, imageID, vmID int) (bool, error) {
	nodeRelease := s.driver.operationLocks.Acquire(controllerNodeLockKey(node))
	defer nodeRelease()
	volumeRelease := s.driver.operationLocks.Acquire(controllerVolumeLockKey(volume))
	defer volumeRelease()

	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil {
		return true, attachmentEvidenceUnavailable("replacement attachment history is unverified", err)
	}
	attempt := history.PendingReplacementAttach
	if attempt == nil || !attempt.dispatchPossible() {
		return false, nil
	}
	desired, err := s.driver.kubeRuntime.DesiredNodeForVolume(ctx, volume)
	if err != nil {
		return true, attachmentEvidenceUnavailable("marked replacement demand is unverified", err)
	}
	if desired.DemandSource != volumeDemandSourceActivePod || !desired.desiresNode(node) || desired.MultipleNodes {
		return false, nil
	}
	if attempt.Node != node || attempt.ImageID != imageID || attempt.VMID != vmID {
		return true, attachmentEvidenceUnavailable("marked replacement Ceph identity changed", nil)
	}
	if enabled, known := s.driver.PluginConfig.GetBool(config.StuckAttachmentReconcilerEnabledVar); known && !enabled {
		return true, status.Error(codes.FailedPrecondition, "marked replacement retention requires the attachment reconciler")
	}
	first, err := s.observeMarkedReplacement(ctx, *attempt, nil)
	if err != nil {
		return true, err
	}
	last, err := s.observeMarkedReplacement(ctx, *attempt, nil)
	if err != nil || !reflect.DeepEqual(first, last) {
		return true, attachmentEvidenceUnavailable("marked replacement attachment changed during retention", err)
	}
	return true, ctx.Err()
}

func (s *ControllerServer) completeRetainedReplacement(ctx context.Context, req *csi.ControllerPublishVolumeRequest, attempt replacementAttachAttempt, node KubernetesNodeIdentity, runtimeCtx *VolumeRuntimeContext) (successfulAttachmentObservation, error) {
	first, err := s.observeMarkedReplacement(ctx, attempt, req)
	if err != nil {
		return successfulAttachmentObservation{}, err
	}
	disk := first.disk
	if err := s.rejectIfLocalDeviceReportBlocksAttachSuccess(ctx, req.VolumeId, req.NodeId, disk.Target, req.GetVolumeContext(), runtimeCtx); err != nil {
		return successfulAttachmentObservation{}, err
	}
	observation, err := s.verifyPublishedAttachment(ctx, req.VolumeId, req.NodeId, attempt.ImageID, attempt.VMID, disk.Target, "", node, true)
	if err != nil {
		return observation, err
	}
	last, err := s.observeMarkedReplacement(ctx, attempt, req)
	if err != nil || !reflect.DeepEqual(first, last) || observation.diskID != disk.DiskID || observation.serial != disk.Serial {
		return observation, attachmentEvidenceUnavailable("marked replacement attachment changed during completion", err)
	}
	observation.replacementAttempt = &attempt
	return observation, nil
}
