package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *ControllerServer) checkPhysicalDetachAuthority(ctx context.Context, volume, nodeName string) error {
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
	if err != nil || pv == nil || pv.UID == "" || pv.Spec.CSI == nil || pv.Spec.CSI.Driver != s.driver.name || pv.Spec.CSI.VolumeHandle != volume {
		return status.Errorf(codes.Unavailable, "cannot verify owned PV identity for detach: %v", err)
	}
	pvs, err := runtime.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return status.Errorf(codes.Unavailable, "cannot verify unique detach binding: %v", err)
	}
	owned := 0
	for _, candidate := range pvs.Items {
		if candidate.Spec.CSI != nil && candidate.Spec.CSI.Driver == s.driver.name && candidate.Spec.CSI.VolumeHandle == volume {
			owned++
			if candidate.UID != pv.UID {
				return status.Error(codes.Unavailable, "detach PV identity changed")
			}
		}
	}
	if owned != 1 {
		return status.Error(codes.Unavailable, "detach PV ownership is ambiguous")
	}
	if s.driver.volumeHistory == nil {
		return status.Error(codes.Unavailable, "detach history is unavailable")
	}
	if err := s.driver.volumeHistory.RefreshEntry(ctx, volume); err != nil {
		return status.Errorf(codes.Unavailable, "detach history is unreadable: %v", err)
	}
	if history := s.currentVolumeHistory(volume); strings.EqualFold(history.Backend, "ceph") && !strings.EqualFold(runtimeCtx.Backend, "ceph") {
		return status.Error(codes.Unavailable, "detach backend contradicts observed history")
	}
	control := resolvedRecoveryControlState(volume, runtimeCtx, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return status.Error(codes.FailedPrecondition, "physical detach is blocked by recovery controls")
	}
	if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx); err != nil {
		return err
	}
	desired, err := runtime.DesiredNodeForVolume(ctx, volume)
	if err != nil || desired.MultipleNodes || (desired.Desired && desired.NodeName == nodeName) {
		return status.Errorf(codes.Unavailable, "current volume release is unverified: %v", err)
	}
	protection, err := s.localRWOProtectionProjection(ctx, volume, nodeName, runtimeCtx, false)
	if err != nil || protection.Invalid {
		return status.Errorf(codes.Unavailable, "detach protection is unverified: %v", err)
	}
	orphan, _ := s.orphanTeardownEligible(ctx, volume, nodeName, runtimeCtx, desired, nil)
	override, hasOverride := crossNodeOverrideDeadline(runtimeCtx)
	if protection.Protected && protection.RequiredNode == nodeName && !bypassProtectionForOrphanTeardown(protection, orphan) && !(hasOverride && override.After(time.Now().UTC())) {
		return status.Error(codes.Unavailable, "physical detach is blocked by local volume protection")
	}
	if pv.Spec.ClaimRef != nil {
		claim := pv.Spec.ClaimRef
		pvc, err := runtime.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		if (err != nil && !apierrors.IsNotFound(err)) || claim.UID == "" || (err == nil && (pvc == nil || pvc.UID != claim.UID || pvc.Spec.VolumeName != pv.Name)) {
			return status.Errorf(codes.Unavailable, "detach claim identity is unverified: %v", err)
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
	node, err := runtime.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || node == nil || node.UID == "" || node.DeletionTimestamp != nil {
		return status.Errorf(codes.Unavailable, "detach node identity is unverified: %v", err)
	}
	history := s.currentVolumeHistory(volume)
	if history.LastSuccessfulNodeName == nodeName && history.LastSuccessfulNodeUID != "" && history.LastSuccessfulNodeUID != string(node.UID) {
		return status.Error(codes.Unavailable, "detach node identity differs from observed history")
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			ready = condition.Status == corev1.ConditionTrue
		}
	}
	if !ready {
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
			if va.Spec.Attacher != s.driver.name || va.UID == "" || va.DeletionTimestamp == nil {
				return status.Error(codes.Unavailable, "current VolumeAttachment does not authorize physical detach")
			}
		}
	}
	return ctx.Err()
}

func (s *ControllerServer) detachVolumeWithAuthority(ctx context.Context, volume, node string) error {
	pinned, err := s.pinKnownVolumeIdentity(ctx, volume)
	if err != nil {
		return status.Errorf(codes.Unavailable, "cannot pin detach image identity: %v", err)
	}
	if err := s.checkPhysicalDetachAuthority(pinned, volume, node); err != nil {
		return err
	}
	return s.volumeProvider.DetachVolume(pinned, volume, node)
}
