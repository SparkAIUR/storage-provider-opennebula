package driver

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type reboundReplacementBinding struct {
	pv   *corev1.PersistentVolume
	pvc  *corev1.PersistentVolumeClaim
	node *corev1.Node

	attachments map[string]reboundAttachment
	waiting     waitingConsumers
}

type reboundAttachment struct {
	node      *corev1.Node
	vmID      int
	authority detachAttachmentAuthority
}

func (s *ControllerServer) retireReboundOrphanReceipt(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt) error {
	second, err := s.verifyReboundOrphanRelease(ctx, history, attempt, "", 0)
	if err != nil {
		return err
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

func (s *ControllerServer) verifyReboundOrphanRelease(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt, canceledNode string, canceledVM int) (reboundReplacementBinding, error) {
	if !attempt.acceptedOrphan() || !attempt.matchesHistory(history) || attempt.Driver != s.driver.name ||
		history.PVName == "" || history.PVCNamespace == "" || history.PVCName == "" {
		return reboundReplacementBinding{}, attachmentEvidenceUnavailable("static rebind receipt identity is unverified", nil)
	}
	first, err := s.observeReboundOrphanRelease(ctx, history, attempt, canceledNode, canceledVM)
	if err != nil {
		return reboundReplacementBinding{}, err
	}
	second, err := s.observeReboundOrphanRelease(ctx, history, attempt, canceledNode, canceledVM)
	if err != nil {
		return reboundReplacementBinding{}, err
	}
	if !reflect.DeepEqual(first, second) {
		return reboundReplacementBinding{}, attachmentEvidenceUnavailable("static rebind changed during receipt verification", nil)
	}
	return second, nil
}

func reboundOrphanPV(pv *corev1.PersistentVolume, attempt replacementAttachAttempt) bool {
	return pv != nil && (string(pv.UID) != attempt.PVUID || (pv.Spec.ClaimRef != nil && string(pv.Spec.ClaimRef.UID) != attempt.PVCUID))
}

func (s *ControllerServer) verifyReboundOrphanBinding(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt, pv *corev1.PersistentVolume) (*corev1.PersistentVolumeClaim, error) {
	client := s.driver.kubeRuntime.client.CoreV1()
	if pv == nil || (string(pv.UID) == attempt.PVUID &&
		(pv.Name != history.PVName || pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain)) ||
		pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID == "" ||
		pv.Spec.ClaimRef.Namespace == "" || pv.Spec.ClaimRef.Name == "" || string(pv.Spec.ClaimRef.UID) == attempt.PVCUID ||
		pv.DeletionTimestamp != nil || pv.Annotations["pv.kubernetes.io/provisioned-by"] != "" ||
		!cephRetentionAccessMode(pv.Spec.AccessModes) {
		return nil, attachmentEvidenceUnavailable("static rebind PV identity is unverified", nil)
	}
	if err := validatePVImageAssertions(pv.Spec.CSI.VolumeAttributes, attempt.ImageID); err != nil {
		return nil, err
	}
	backend, err := persistentVolumeBackend(pv)
	if err != nil || !isCephBackend(backend) {
		return nil, attachmentEvidenceUnavailable("static rebind backend is unverified", err)
	}
	pvc, reused, err := s.driver.kubeRuntime.attachmentClaim(ctx, pv)
	bound := pv.Status.Phase == corev1.VolumeBound && pvc != nil && pvc.Status.Phase == corev1.ClaimBound &&
		pvc.DeletionTimestamp == nil && slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes)
	released := pv.Status.Phase == corev1.VolumeReleased && pvc == nil &&
		pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain
	if err != nil || reused || (!bound && !released) {
		return nil, attachmentEvidenceUnavailable("static rebind binding is unverified", err)
	}
	oldPV, err := client.PersistentVolumes().Get(ctx, history.PVName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) && (err != nil || history.PVName != pv.Name || oldPV == nil || oldPV.UID != pv.UID || (string(oldPV.UID) == attempt.PVUID && !reflect.DeepEqual(oldPV.Spec.ClaimRef, pv.Spec.ClaimRef))) {
		return nil, attachmentEvidenceUnavailable("original static binding retirement is unverified", err)
	}
	oldPVC, err := client.PersistentVolumeClaims(history.PVCNamespace).Get(ctx, history.PVCName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) && (err != nil || pvc == nil || history.PVCNamespace != pvc.Namespace || history.PVCName != pvc.Name ||
		oldPVC == nil || oldPVC.UID != pvc.UID) {
		return nil, attachmentEvidenceUnavailable("original static binding retirement is unverified", err)
	}
	if err := s.recheckAttachmentBinding(ctx, pv, pvc, false); err != nil {
		return nil, err
	}
	return pvc, nil
}

func (s *ControllerServer) observeReboundOrphanRelease(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt, canceledNode string, canceledVM int) (reboundReplacementBinding, error) {
	var binding reboundReplacementBinding
	deny := func(err error) (reboundReplacementBinding, error) {
		return binding, attachmentEvidenceUnavailable("static rebind release is unverified", err)
	}
	client := s.driver.kubeRuntime.client.CoreV1()
	volume := attempt.VolumeID
	pv, err := s.ownedDeletionPV(ctx, volume)
	if err != nil {
		return deny(err)
	}
	pvc, err := s.verifyReboundOrphanBinding(ctx, history, attempt, pv)
	if err != nil {
		return deny(err)
	}
	runtimeCtx := &VolumeRuntimeContext{PVName: pv.Name, PVAnnotations: pv.Annotations, Backend: "ceph", AccessModes: pv.Spec.AccessModes}
	if pvc != nil {
		runtimeCtx.PVCNamespace, runtimeCtx.PVCName, runtimeCtx.PVCUID = pvc.Namespace, pvc.Name, pvc.UID
		runtimeCtx.PVCAnnotations, runtimeCtx.verifiedClaimBinding = pvc.Annotations, true
	}
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
		}) {
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
	vas := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments()
	list, err := vas.List(ctx, metav1.ListOptions{})
	if err != nil {
		return deny(err)
	}
	attachments := make(map[string]reboundAttachment)
	var destination, canceled reboundAttachment
	for i := range list.Items {
		va := &list.Items[i]
		if string(va.UID) == attempt.VAUID {
			return deny(nil)
		}
		if !matchesVolumeAttachment(history.PVName, va) && !matchesVolumeAttachment(pv.Name, va) {
			continue
		}
		observed := detachAttachmentSnapshot(va)
		if !matchesVolumeAttachment(pv.Name, va) || va.Spec.NodeName == "" ||
			va.Name != csiVolumeAttachmentName(volume, attempt.Driver, va.Spec.NodeName) ||
			va.Spec.Attacher != attempt.Driver || va.UID == "" || observed.attached ||
			(observed.deleting && (va.Spec.NodeName != canceledNode || canceled.node != nil)) ||
			(!observed.deleting && (pv.Status.Phase != corev1.VolumeBound || destination.node != nil || va.Spec.NodeName == canceledNode)) ||
			(pvc != nil && (pvc.CreationTimestamp.IsZero() || observed.created.Before(pvc.CreationTimestamp.Time))) ||
			pv.CreationTimestamp.IsZero() || observed.created.Before(pv.CreationTimestamp.Time) ||
			validateAttachmentImageAssertions(observed.metadata, attempt.ImageID) != nil ||
			validateMarkedReplacementMetadata(observed, pv, "", attempt.DetachAcceptance.Serial) != nil {
			return deny(nil)
		}
		attachment, err := s.verifyReboundAttachmentNode(ctx, attempt, node, observed)
		if err != nil {
			return deny(err)
		}
		attachments[va.Name] = attachment
		if observed.deleting {
			canceled = attachment
		} else {
			destination = attachment
		}
	}
	if canceledNode != "" && (canceled.node == nil || canceled.vmID != canceledVM ||
		nodeReportsVolumeInUse(canceled.node, attempt.Driver, volume) || canceled.vmID == destination.vmID) {
		return deny(nil)
	}
	destinationInUse := destination.node != nil && nodeReportsVolumeInUse(destination.node, attempt.Driver, volume)
	if _, present := attachments[attempt.VAName]; !present {
		if _, err := vas.Get(ctx, attempt.VAName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return deny(err)
		}
	}
	pods, err := client.Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return deny(err)
	}
	waiting := make(waitingConsumers)
	for i := range pods.Items {
		pod := &pods.Items[i]
		currentClaim := pod.Namespace == pv.Spec.ClaimRef.Namespace
		if currentClaim {
			currentClaim, err = podReferencesClaim(pod, pv.Spec.ClaimRef.Name, pvc)
			if err != nil {
				return deny(err)
			}
		}
		oldClaim := pod.Namespace == history.PVCNamespace && slices.ContainsFunc(pod.Spec.Volumes, func(volume corev1.Volume) bool {
			return podVolumeClaimName(pod, volume) == history.PVCName
		})
		if !currentClaim && !oldClaim {
			continue
		}
		if !currentClaim || (oldClaim && history.PVCName != pv.Spec.ClaimRef.Name) || destination.node == nil || pvc == nil ||
			!pod.CreationTimestamp.After(pvc.CreationTimestamp.Time) || !replacementPodWaitingOnNode(pod, destination.node) ||
			pod.CreationTimestamp.After(time.Now()) || (destinationInUse && pod.Status.StartTime == nil) || waiting[pod.UID].name != "" {
			return deny(nil)
		}
		waiting[pod.UID] = observeWaitingConsumer(pod, pvc.Name)
	}
	if (destination.node != nil && len(waiting) == 0) || (len(waiting) > 1 && pv.Spec.AccessModes[0] == corev1.ReadWriteOncePod) {
		return deny(nil)
	}
	verifiedWaitingNode := func(current *corev1.Node) bool {
		return destinationInUse && len(waiting) != 0 && current.Name == destination.node.Name && current.UID == destination.node.UID
	}
	nodes, err := client.Nodes().List(ctx, metav1.ListOptions{})
	if err != nil || slices.ContainsFunc(nodes.Items, func(current corev1.Node) bool {
		return nodeReportsVolumeInUse(&current, attempt.Driver, volume) && !verifiedWaitingNode(&current)
	}) || (node != nil && nodeReportsVolumeInUse(node, attempt.Driver, volume) && !verifiedWaitingNode(node)) {
		return deny(err)
	}
	if node == nil {
		if _, err := client.Nodes().Get(ctx, attempt.Node, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return deny(err)
		}
	} else if err := s.recheckAttachmentReleaseNode(ctx, node, volume); err != nil {
		return deny(err)
	}
	for name, attachment := range attachments {
		if err := s.recheckAttachmentReleaseNode(ctx, attachment.node, volume); err != nil {
			return deny(err)
		}
		if err := s.recheckReplacementVolumeAttachment(ctx, pv.Name, name, attachment.authority); err != nil {
			return deny(err)
		}
	}
	if err := s.recheckAttachmentBinding(ctx, pv, pvc, false); err != nil {
		return deny(err)
	}
	return reboundReplacementBinding{pv: pv, pvc: pvc, node: node, attachments: attachments, waiting: waiting}, nil
}

func (s *ControllerServer) verifyReboundAttachmentNode(ctx context.Context, attempt replacementAttachAttempt, source *corev1.Node, observed detachAttachmentAuthority) (reboundAttachment, error) {
	node, err := s.driver.kubeRuntime.client.CoreV1().Nodes().Get(ctx, observed.node, metav1.GetOptions{})
	if err != nil {
		return reboundAttachment{}, err
	}
	if observed.node == attempt.Node {
		if source == nil || node == nil || source.UID != node.UID ||
			source.CreationTimestamp.IsZero() || !observed.created.After(source.CreationTimestamp.Time) {
			return reboundAttachment{}, fmt.Errorf("rebound source node is unverified")
		}
		return reboundAttachment{node: source, vmID: attempt.VMID, authority: observed}, nil
	}
	if !replacementDestinationReleased(node, observed.node, observed, attempt) {
		return reboundAttachment{}, fmt.Errorf("rebound destination node is unverified")
	}
	vmID, err := s.volumeProvider.NodeExists(ctx, observed.node)
	verifier, ok := s.volumeProvider.(opennebula.ReplacementDestinationAbsenceVerifier)
	if err != nil || !ok || vmID <= 0 || vmID == attempt.VMID || vmID == attempt.OriginalVMID {
		return reboundAttachment{}, attachmentEvidenceUnavailable("rebound destination VM is unverified", err)
	}
	if err := verifier.VerifyReplacementDestinationAbsent(ctx, attempt.VolumeID, node.Name, attempt.ImageID, vmID, 0); err != nil {
		return reboundAttachment{}, err
	}
	target := opennebula.HostArtifactAttachmentTarget{NodeName: node.Name, VMID: vmID, ImageID: attempt.ImageID, DiskID: -1}
	if err := s.attachmentProtectionChecks(ctx, attempt.VolumeID, nil, target); err != nil {
		return reboundAttachment{}, err
	}
	return reboundAttachment{node: node, vmID: vmID, authority: observed}, nil
}
