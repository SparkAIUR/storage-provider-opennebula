package driver

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (s *ControllerServer) verifyCanceledReplacementDestination(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt, nodeName string, imageID, vmID int) error {
	unavailable := func(err error) error {
		return attachmentEvidenceUnavailable("canceled replacement destination release is unverified", err)
	}
	volume := attempt.VolumeID
	if attempt.Driver != s.driver.name || imageID != attempt.ImageID || vmID <= 0 || vmID == attempt.VMID || vmID == attempt.OriginalVMID {
		return unavailable(nil)
	}
	pv, err := s.ownedDeletionPV(ctx, volume)
	if err != nil || pv == nil || pv.Name != history.PVName || string(pv.UID) != attempt.PVUID ||
		!cephRetentionAccessMode(pv.Spec.AccessModes) {
		return unavailable(err)
	}
	backend, err := persistentVolumeBackend(pv)
	if err != nil || !isCephBackend(backend) {
		return unavailable(err)
	}
	runtime := s.driver.kubeRuntime
	claim := pv.Spec.ClaimRef
	if claim == nil || string(claim.UID) != attempt.PVCUID || claim.Namespace != history.PVCNamespace || claim.Name != history.PVCName {
		return unavailable(nil)
	}
	pvc, replaced, err := runtime.attachmentClaim(ctx, pv)
	if err != nil || (pvc != nil && (pvc.Namespace != claim.Namespace || pvc.Name != claim.Name)) {
		return unavailable(err)
	}
	originalBound := !replaced && pvc != nil && pvc.Status.Phase == corev1.ClaimBound &&
		slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes)
	liveBinding := pv.Status.Phase == corev1.VolumeBound && originalBound &&
		pv.DeletionTimestamp == nil && pvc.DeletionTimestamp == nil
	terminatingClaim := originalBound && pvc.DeletionTimestamp != nil
	released := pv.Status.Phase == corev1.VolumeReleased && (pvc == nil || replaced || terminatingClaim)
	if !liveBinding && !released && !(pv.Status.Phase == corev1.VolumeBound && terminatingClaim) {
		return unavailable(nil)
	}
	if replaced {
		if err := runtime.verifyReleasedClaimNameReuse(ctx, pv, pvc); err != nil {
			return err
		}
	}
	attachments, err := s.detachVolumeAttachments(ctx, pv.Name, "")
	if err != nil {
		return err
	}
	destinationName := csiVolumeAttachmentName(volume, s.driver.name, nodeName)
	attachment := attachments[destinationName]
	if destinationName == attempt.VAName || attachment.node != nodeName || attachment.attacher != attempt.Driver || attachment.uid == "" ||
		string(attachment.uid) == attempt.VAUID || string(attachment.uid) == attempt.DetachCompletion.VAUID ||
		!attachment.deleting || attachment.attached {
		return unavailable(nil)
	}
	for name, observed := range attachments {
		if name == destinationName {
			continue
		}
		if name != attempt.VAName || observed.node != attempt.Node || observed.attacher != attempt.Driver ||
			observed.uid == "" || observed.uid == attachment.uid ||
			(attempt.prepared() && string(observed.uid) != attempt.VAUID) ||
			(attempt.detached() && string(observed.uid) != attempt.DetachCompletion.VAUID) {
			return unavailable(nil)
		}
		if err := validateAttachmentImageAssertions(observed.metadata, imageID); err != nil {
			return unavailable(err)
		}
	}
	if err := validateAttachmentImageAssertions(attachment.metadata, imageID); err != nil {
		return unavailable(err)
	}
	if err := validateMarkedReplacementMetadata(attachment, pv, "", fmt.Sprintf("onecsi-%d", imageID)); err != nil {
		return err
	}

	client := runtime.client.CoreV1()
	node, err := client.Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || !replacementDestinationReleased(node, nodeName, attachment, attempt) {
		return unavailable(err)
	}

	runtimeCtx := &VolumeRuntimeContext{PVName: pv.Name, PVAnnotations: pv.Annotations, Backend: backend, AccessModes: pv.Spec.AccessModes}
	if pvc != nil && !replaced {
		runtimeCtx.PVCNamespace, runtimeCtx.PVCName, runtimeCtx.PVCUID = pvc.Namespace, pvc.Name, pvc.UID
		runtimeCtx.PVCAnnotations, runtimeCtx.verifiedClaimBinding = pvc.Annotations, true
	}
	control := resolvedRecoveryControlState(volume, runtimeCtx, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return status.Error(codes.FailedPrecondition, "replacement destination release is blocked by recovery controls")
	}
	inUse := nodeReportsVolumeInUse(node, s.driver.name, volume)
	waiting, err := s.replacementDestinationConsumers(ctx, claim, pvc, replaced, liveBinding, node, attachment, inUse)
	if err != nil {
		return unavailable(err)
	}
	if len(waiting) > 1 && pv.Spec.AccessModes[0] == corev1.ReadWriteOncePod {
		return unavailable(nil)
	}
	verifier, ok := s.volumeProvider.(opennebula.ReplacementDestinationAbsenceVerifier)
	if !ok {
		return unavailable(nil)
	}
	sourceVMID := 0
	if attempt.dispatchPossible() {
		sourceVMID = attempt.VMID
	}
	if err := verifier.VerifyReplacementDestinationAbsent(ctx, volume, nodeName, imageID, vmID, sourceVMID); err != nil {
		return unavailable(err)
	}
	if err := s.checkReplacementMaintenance(ctx); err != nil {
		return err
	}
	target := opennebula.HostArtifactAttachmentTarget{NodeName: nodeName, VMID: vmID, ImageID: imageID, DiskID: -1}
	if err := s.recheckAttachmentAuthority(ctx, volume, runtimeCtx, target, pv, pvc, replaced); err != nil {
		return err
	}
	if replaced {
		if err := runtime.verifyReleasedClaimNameReuse(ctx, pv, pvc); err != nil {
			return err
		}
	}
	if err := s.checkReplacementAttemptReadback(ctx, attempt); err != nil {
		return err
	}
	current, err := client.Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || !replacementDestinationReleased(current, nodeName, attachment, attempt) || current.UID != node.UID ||
		!current.CreationTimestamp.Equal(&node.CreationTimestamp) || nodeReportsVolumeInUse(current, s.driver.name, volume) != inUse {
		return unavailable(err)
	}
	finalWaiting, err := s.replacementDestinationConsumers(ctx, claim, pvc, replaced, liveBinding, current, attachment, inUse)
	if err != nil || !maps.Equal(waiting, finalWaiting) {
		return unavailable(err)
	}
	return s.recheckDetachVolumeAttachments(ctx, pv.Name, "", attachments)
}

func (s *ControllerServer) replacementDestinationConsumers(ctx context.Context, claim *corev1.ObjectReference, pvc *corev1.PersistentVolumeClaim,
	replaced, liveBinding bool, node *corev1.Node, attachment detachAttachmentAuthority, inUse bool,
) (map[types.UID]string, error) {
	pods, err := s.driver.kubeRuntime.client.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	waiting := make(map[types.UID]string)
	for i := range pods.Items {
		pod := &pods.Items[i]
		usesClaim, err := podMayUseAttachmentClaim(pod, claim, pvc, replaced)
		if err != nil {
			return nil, err
		}
		if !usesClaim || terminalPodMayBeReleased(pod) || (pod.Spec.NodeName != "" && pod.Spec.NodeName != node.Name) {
			continue
		}
		if !liveBinding || !replacementPodWaitingOnNode(pod, node) || !pod.CreationTimestamp.After(attachment.created) ||
			pod.CreationTimestamp.After(time.Now()) || (inUse && pod.Status.StartTime == nil) {
			return nil, fmt.Errorf("destination consumer may already be using the volume")
		}
		waiting[pod.UID] = pod.ResourceVersion
	}
	if inUse && len(waiting) == 0 {
		return nil, fmt.Errorf("destination node still reports the volume in use")
	}
	return waiting, ctx.Err()
}

func replacementDestinationReleased(node *corev1.Node, name string, attachment detachAttachmentAuthority, attempt replacementAttachAttempt) bool {
	return node != nil && node.Name == name && node.UID != "" && string(node.UID) != attempt.ReplacementUID &&
		string(node.UID) != attempt.OriginalNodeUID && node.DeletionTimestamp == nil &&
		!node.CreationTimestamp.IsZero() && attachment.created.After(node.CreationTimestamp.Time) &&
		slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		})
}
