package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type physicalDetachIdentity struct {
	pvUID, nodeUID        types.UID
	imageID, vmID, diskID int
	background, ceph      bool
}

func (s *ControllerServer) checkPhysicalDetachAuthority(ctx context.Context, volume, nodeName string, identity physicalDetachIdentity) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		return nil
	}
	runtime := s.driver.kubeRuntime
	runtimeCtx, err := runtime.ResolveVolumeRuntimeContext(ctx, volume)
	if err != nil {
		return status.Errorf(codes.Unavailable, "cannot verify detach binding: %v", err)
	}
	pv, err := runtime.client.CoreV1().PersistentVolumes().Get(ctx, runtimeCtx.PVName, metav1.GetOptions{})
	if err != nil || pv == nil || pv.UID != identity.pvUID || pv.Spec.CSI == nil || pv.Spec.CSI.Driver != s.driver.name || pv.Spec.CSI.VolumeHandle != volume {
		return status.Errorf(codes.Unavailable, "cannot verify owned PV identity for detach: %v", err)
	}
	runtimeCtx.PVAnnotations = pv.Annotations
	runtimeCtx.Backend = strings.TrimSpace(pv.Annotations[annotationBackend])
	if identity.ceph && !isCephBackend(runtimeCtx.Backend) {
		return status.Error(codes.Unavailable, "Ceph backend changed before detach")
	}
	if pv.Spec.ClaimRef != nil {
		claim := pv.Spec.ClaimRef
		pvc, err := runtime.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		if (err != nil && !apierrors.IsNotFound(err)) || claim.UID == "" || (err == nil && (pvc == nil || pvc.UID != claim.UID || pvc.Spec.VolumeName != pv.Name)) {
			return status.Errorf(codes.Unavailable, "detach claim identity is unverified: %v", err)
		}
		if err == nil {
			runtimeCtx.PVCAnnotations = pvc.Annotations
		} else {
			runtimeCtx.PVCAnnotations = nil
		}
		pods, err := runtime.client.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return status.Errorf(codes.Unavailable, "cannot verify Pod release: %v", err)
		}
		for _, pod := range pods.Items {
			if pod.Spec.NodeName != "" && pod.Spec.NodeName != nodeName {
				continue
			}
			for _, v := range pod.Spec.Volumes {
				if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claim.Name {
					return status.Error(codes.Unavailable, "a current Pod still references the volume on the detach node")
				}
			}
		}
	}
	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil {
		return status.Errorf(codes.Unavailable, "detach history is unreadable: %v", err)
	}
	if history.Backend != "" && (isCephBackend(history.Backend) || isCephBackend(runtimeCtx.Backend)) && !strings.EqualFold(history.Backend, runtimeCtx.Backend) {
		return status.Error(codes.Unavailable, "detach backend contradicts observed history")
	}
	if history.LastSuccessfulImageID > 0 && history.LastSuccessfulImageID != identity.imageID {
		return status.Error(codes.Unavailable, "detach image differs from observed history")
	}
	if history.LastSuccessfulNodeName == nodeName && history.LastSuccessfulOpenNebulaVMID > 0 && history.LastSuccessfulOpenNebulaVMID != identity.vmID {
		return status.Error(codes.Unavailable, "detach VM differs from observed history")
	}
	control := resolvedRecoveryControlState(volume, runtimeCtx, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return status.Error(codes.FailedPrecondition, "physical detach is blocked by recovery controls")
	}
	if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx, identity.imageID); err != nil {
		return err
	}
	desired, err := runtime.DesiredNodeForVolume(ctx, volume)
	if err != nil || desired.desiresNode(nodeName) {
		return status.Errorf(codes.Unavailable, "current volume release is unverified: %v", err)
	}
	protection, err := s.localRWOProtectionProjection(ctx, volume, nodeName, runtimeCtx, false, true)
	if err != nil || protection.Invalid {
		return status.Errorf(codes.Unavailable, "detach protection is unverified: %v", err)
	}
	orphan, _ := s.orphanTeardownEligible(ctx, volume, nodeName, runtimeCtx, desired, nil)
	override, _ := crossNodeOverrideDeadline(runtimeCtx)
	if protection.Protected && protection.RequiredNode == nodeName && !bypassProtectionForOrphanTeardown(protection, orphan) && !override.After(time.Now().UTC()) {
		return status.Error(codes.Unavailable, "physical detach is blocked by local volume protection")
	}
	node, err := runtime.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || node == nil || node.UID != identity.nodeUID || node.DeletionTimestamp != nil {
		return status.Errorf(codes.Unavailable, "detach node identity is unverified: %v", err)
	}
	if history.LastSuccessfulNodeName == nodeName && history.LastSuccessfulNodeUID != "" && history.LastSuccessfulNodeUID != string(node.UID) {
		return status.Error(codes.Unavailable, "detach node identity differs from observed history")
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			ready = condition.Status == corev1.ConditionTrue
		}
	}
	if !ready && (identity.background || isCephBackend(runtimeCtx.Backend)) {
		return status.Error(codes.Unavailable, "detach node release observation is unavailable")
	}
	uniqueName := corev1.UniqueVolumeName(fmt.Sprintf("kubernetes.io/csi/%s^%s", s.driver.name, volume))
	for _, inUse := range node.Status.VolumesInUse {
		if inUse == uniqueName {
			return status.Error(codes.Unavailable, "detach node still reports the volume in use")
		}
	}
	vas, err := runtime.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return status.Errorf(codes.Unavailable, "cannot verify attachment deletion authority: %v", err)
	}
	for _, va := range vas.Items {
		if matchesVolumeAttachment(pv.Name, &va) && strings.TrimSpace(va.Spec.NodeName) == nodeName {
			if va.Spec.Attacher != s.driver.name || va.UID == "" || va.DeletionTimestamp.IsZero() {
				return status.Error(codes.Unavailable, "current VolumeAttachment does not authorize physical detach")
			}
			if err := validateAttachmentImageAssertions(va.Status.AttachmentMetadata, identity.imageID); err != nil {
				return status.Error(codes.Unavailable, err.Error())
			}
		}
	}
	return ctx.Err()
}

func (s *ControllerServer) detachVolumeWithAuthority(ctx context.Context, volume, node string, identity physicalDetachIdentity) (bool, error) {
	runtime := s.driver.kubeRuntime
	if runtime != nil && runtime.enabled {
		if identity.pvUID == "" {
			runtimeCtx, err := runtime.ResolveVolumeRuntimeContext(ctx, volume)
			if err != nil {
				return false, status.Errorf(codes.Unavailable, "cannot verify detach binding: %v", err)
			}
			pv, err := runtime.client.CoreV1().PersistentVolumes().Get(ctx, runtimeCtx.PVName, metav1.GetOptions{})
			if err != nil || pv.UID == "" {
				return false, status.Errorf(codes.Unavailable, "cannot verify detach PV UID: %v", err)
			}
			identity.pvUID = pv.UID
		}
		current, err := runtime.NodeIdentity(ctx, node)
		if err != nil || !current.Exists || current.UID == "" {
			return false, status.Errorf(codes.Unavailable, "cannot verify detach node UID: %v", err)
		}
		identity.nodeUID = types.UID(current.UID)
	}
	authorize := func(ctx context.Context, imageID, vmID, diskID int) error {
		if imageID != identity.imageID || vmID != identity.vmID || (identity.diskID >= 0 && diskID != identity.diskID) {
			return status.Error(codes.Unavailable, "provider attachment identity changed before detach")
		}
		return s.checkPhysicalDetachAuthority(ctx, volume, node, identity)
	}
	if err := s.checkPhysicalDetachAuthority(ctx, volume, node, identity); err != nil {
		return false, err
	}
	ctx = opennebula.WithExpectedImageID(ctx, identity.imageID)
	if provider, ok := s.volumeProvider.(opennebula.AuthorizedVolumeDetacher); ok {
		return provider.DetachVolumeWithAuthority(ctx, volume, node, authorize)
	}
	return false, s.volumeProvider.DetachVolume(ctx, volume, node)
}
