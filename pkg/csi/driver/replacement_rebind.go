package driver

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type reboundReplacementBinding struct {
	pv   *corev1.PersistentVolume
	pvc  *corev1.PersistentVolumeClaim
	node *corev1.Node
}

func (s *ControllerServer) retireReboundOrphanReceipt(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt) error {
	if !attempt.acceptedOrphan() || !attempt.matchesHistory(history) || attempt.Driver != s.driver.name ||
		history.PVName == "" || history.PVCNamespace == "" || history.PVCName == "" {
		return attachmentEvidenceUnavailable("static rebind receipt identity is unverified", nil)
	}
	first, err := s.observeReboundOrphanRelease(ctx, history, attempt)
	if err != nil {
		return err
	}
	second, err := s.observeReboundOrphanRelease(ctx, history, attempt)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(first, second) {
		return attachmentEvidenceUnavailable("static rebind changed during receipt verification", nil)
	}
	_, err = s.driver.volumeHistory.updateHistory(ctx, attempt.VolumeID, true, func(current *VolumeHistoryRecord) error {
		if current.PendingReplacementAttach == nil || *current.PendingReplacementAttach != attempt || !attempt.matchesHistory(*current) {
			return fmt.Errorf("static rebind receipt changed before retirement")
		}
		if err := s.recheckAttachmentBinding(ctx, second.pv, second.pvc, false); err != nil {
			return err
		}
		current.PendingReplacementAttach = nil
		return nil
	})
	return err
}

func (s *ControllerServer) observeReboundOrphanRelease(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt) (reboundReplacementBinding, error) {
	var binding reboundReplacementBinding
	deny := func(err error) (reboundReplacementBinding, error) {
		return binding, attachmentEvidenceUnavailable("static rebind release is unverified", err)
	}
	client := s.driver.kubeRuntime.client.CoreV1()
	volume := attempt.VolumeID
	pv, err := s.ownedDeletionPV(ctx, volume)
	if err != nil || pv == nil || string(pv.UID) == attempt.PVUID || pv.Status.Phase != corev1.VolumeBound ||
		pv.DeletionTimestamp != nil || pv.Annotations["pv.kubernetes.io/provisioned-by"] != "" ||
		!cephRetentionAccessMode(pv.Spec.AccessModes) || validateAttachmentImageAssertions(pv.Spec.CSI.VolumeAttributes, attempt.ImageID) != nil {
		return deny(err)
	}
	backend, err := persistentVolumeBackend(pv)
	if err != nil || !isCephBackend(backend) {
		return deny(err)
	}
	pvc, reused, err := s.driver.kubeRuntime.attachmentClaim(ctx, pv)
	if err != nil || reused || pvc == nil || string(pvc.UID) == attempt.PVCUID || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.DeletionTimestamp != nil || !slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes) {
		return deny(err)
	}
	runtimeCtx := &VolumeRuntimeContext{PVName: pv.Name, PVAnnotations: pv.Annotations, Backend: backend, AccessModes: pv.Spec.AccessModes,
		PVCNamespace: pvc.Namespace, PVCName: pvc.Name, PVCUID: pvc.UID, PVCAnnotations: pvc.Annotations, verifiedClaimBinding: true}
	if control := resolvedRecoveryControlState(volume, runtimeCtx, VolumeRecoveryControlState{}); control.Invalid || control.Active() {
		return deny(nil)
	}
	protection, err := s.localRWOProtectionProjection(ctx, volume, attempt.Node, runtimeCtx, false, true)
	if err != nil || protection.Invalid || protection.Protected {
		return deny(err)
	}
	node, err := client.Nodes().Get(ctx, attempt.Node, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		node = nil
		if err := s.verifyRetiredReplacementSource(ctx, attempt); err != nil {
			return deny(err)
		}
	} else {
		if err != nil || node == nil || node.Name != attempt.Node || string(node.UID) != attempt.ReplacementUID ||
			node.DeletionTimestamp != nil || !slices.ContainsFunc(node.Status.Conditions, func(condition corev1.NodeCondition) bool {
			return condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue
		}) || nodeReportsVolumeInUse(node, attempt.Driver, volume) {
			return deny(err)
		}
		if _, err := s.inspectReplacementDisk(ctx, attempt, false); err != nil {
			return deny(err)
		}
	}
	diskID, _ := strconv.Atoi(attempt.DetachAcceptance.DiskID)
	target := opennebula.HostArtifactAttachmentTarget{NodeName: attempt.Node, VMID: attempt.VMID, ImageID: attempt.ImageID, DiskID: diskID}
	if err := s.attachmentProtectionChecks(ctx, volume, nil, target); err != nil {
		return deny(err)
	}
	oldPV, err := client.PersistentVolumes().Get(ctx, history.PVName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) && (err != nil || history.PVName != pv.Name || oldPV == nil || oldPV.UID != pv.UID) {
		return deny(err)
	}
	oldPVC, err := client.PersistentVolumeClaims(history.PVCNamespace).Get(ctx, history.PVCName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) && (err != nil || history.PVCNamespace != pvc.Namespace || history.PVCName != pvc.Name || oldPVC == nil || oldPVC.UID != pvc.UID) {
		return deny(err)
	}
	if _, err := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(ctx, attempt.VAName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return deny(err)
	}
	vas, err := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil || slices.ContainsFunc(vas.Items, func(va storagev1.VolumeAttachment) bool {
		return string(va.UID) == attempt.VAUID || matchesVolumeAttachment(history.PVName, &va) || matchesVolumeAttachment(pv.Name, &va)
	}) {
		return deny(err)
	}
	pods, err := client.Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return deny(err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		for _, podVolume := range pod.Spec.Volumes {
			claim := podVolumeClaimName(pod, podVolume)
			if (pod.Namespace == history.PVCNamespace && claim == history.PVCName) ||
				(pod.Namespace == pvc.Namespace && claim == pvc.Name) {
				return deny(nil)
			}
		}
	}
	nodes, err := client.Nodes().List(ctx, metav1.ListOptions{})
	if err != nil || slices.ContainsFunc(nodes.Items, func(current corev1.Node) bool {
		return nodeReportsVolumeInUse(&current, attempt.Driver, volume)
	}) {
		return deny(err)
	}
	if node == nil {
		if _, err := client.Nodes().Get(ctx, attempt.Node, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return deny(err)
		}
	} else if err := s.recheckAttachmentReleaseNode(ctx, node, volume); err != nil {
		return deny(err)
	}
	if err := s.recheckAttachmentBinding(ctx, pv, pvc, false); err != nil {
		return deny(err)
	}
	return reboundReplacementBinding{pv: pv, pvc: pvc, node: node}, nil
}
