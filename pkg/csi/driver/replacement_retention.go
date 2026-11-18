package driver

import (
	"context"
	"reflect"
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

func (s *ControllerServer) replacementOriginalVAGone(ctx context.Context, attempt replacementAttachAttempt) (bool, error) {
	va, err := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(ctx, attempt.VAName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, ctx.Err()
	}
	if err != nil || va.UID == "" {
		return false, attachmentEvidenceUnavailable("pinned replacement VolumeAttachment identity is unverified", err)
	}
	return string(va.UID) != attempt.VAUID, ctx.Err()
}

func (s *ControllerServer) observeMarkedReplacement(ctx context.Context, attempt replacementAttachAttempt, deleting bool) (markedReplacementObservation, error) {
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
		capacity.NodeID != attempt.VMID || capacity.DiskID != disk.DiskID || capacity.Target != disk.Target || capacity.DeviceSerial != disk.Serial {
		return deny(err)
	}
	observation.disk, observation.capacity = *disk, capacity.SizeBytes
	proveVA := func(name string, va detachAttachmentAuthority, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) error {
		pinnedUID := string(va.uid) == attempt.VAUID
		if deleting {
			if name != attempt.VAName || !pinnedUID || !va.deleting {
				return attachmentEvidenceUnavailable("marked replacement retention requires the exact pinned deleting VolumeAttachment", nil)
			}
		} else {
			if pinnedUID || va.deleting || va.attached {
				return attachmentEvidenceUnavailable("marked replacement completion requires an eligible successor VolumeAttachment", nil)
			}
			gone, err := s.replacementOriginalVAGone(ctx, attempt)
			if err != nil || !gone {
				return attachmentEvidenceUnavailable("pinned replacement VolumeAttachment retirement is unverified", err)
			}
		}
		size, requested, ok := completedRetentionCapacity(pv, pvc)
		if !ok || size != capacity.SizeBytes {
			return attachmentEvidenceUnavailable("marked replacement Ceph capacity is pending or inconsistent", nil)
		}
		if backend, present := va.metadata[annotationBackend]; present && !isCephBackend(backend) {
			return attachmentEvidenceUnavailable("marked replacement VolumeAttachment backend changed", nil)
		}
		if target, present := va.metadata["volumeName"]; present && strings.TrimPrefix(strings.TrimSpace(target), "/dev/") != disk.Target {
			return attachmentEvidenceUnavailable("marked replacement VolumeAttachment target changed", nil)
		}
		for key, expected := range map[string]string{
			publishContextDeviceSerial: disk.Serial, paramPVName: pv.Name, paramPVCName: pvc.Name, paramPVCNamespace: pvc.Namespace,
		} {
			if value, present := va.metadata[key]; present && strings.TrimSpace(value) != expected {
				return attachmentEvidenceUnavailable("marked replacement VolumeAttachment metadata changed", nil)
			}
		}
		observation.vaName, observation.va, observation.requested = name, va, requested
		observation.pvVersion, observation.pvcVersion = pv.ResourceVersion, pvc.ResourceVersion
		return s.checkReplacementMaintenance(ctx)
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
	first, err := s.observeMarkedReplacement(ctx, *attempt, true)
	if err != nil {
		return true, err
	}
	last, err := s.observeMarkedReplacement(ctx, *attempt, true)
	if err != nil || !reflect.DeepEqual(first, last) {
		return true, attachmentEvidenceUnavailable("marked replacement attachment changed during retention", err)
	}
	return true, ctx.Err()
}

func (s *ControllerServer) completeRetainedReplacement(ctx context.Context, req *csi.ControllerPublishVolumeRequest, attempt replacementAttachAttempt, node KubernetesNodeIdentity, runtimeCtx *VolumeRuntimeContext) (successfulAttachmentObservation, error) {
	first, err := s.observeMarkedReplacement(ctx, attempt, false)
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
	last, err := s.observeMarkedReplacement(ctx, attempt, false)
	if err != nil || !reflect.DeepEqual(first, last) || observation.diskID != disk.DiskID || observation.serial != disk.Serial {
		return observation, attachmentEvidenceUnavailable("marked replacement attachment changed during completion", err)
	}
	observation.replacementAttempt = &attempt
	return observation, s.recordSuccessfulLocalVolumePublish(ctx, req.VolumeId, observation, req.GetVolumeContext(), runtimeCtx, nil)
}
