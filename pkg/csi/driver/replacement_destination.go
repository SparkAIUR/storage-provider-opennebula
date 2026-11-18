package driver

import (
	"context"
	"fmt"
	"slices"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *ControllerServer) verifyCanceledReplacementDestination(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt, nodeName string, imageID, vmID int) error {
	unavailable := func(err error) error {
		return attachmentEvidenceUnavailable("canceled replacement destination release is unverified", err)
	}
	volume := attempt.VolumeID
	if (!attempt.dispatchPossible() && !attempt.detached()) || attempt.Driver != s.driver.name ||
		imageID != attempt.ImageID || vmID <= 0 || vmID == attempt.VMID || vmID == attempt.OriginalVMID {
		return unavailable(nil)
	}
	pv, err := s.ownedDeletionPV(ctx, volume)
	if err != nil || pv == nil || pv.Name != history.PVName || string(pv.UID) != attempt.PVUID ||
		pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound || !cephRetentionAccessMode(pv.Spec.AccessModes) {
		return unavailable(err)
	}
	backend, err := persistentVolumeBackend(pv)
	if err != nil || !isCephBackend(backend) {
		return unavailable(err)
	}
	runtime := s.driver.kubeRuntime
	pvc, replaced, err := runtime.attachmentClaim(ctx, pv)
	if err != nil || replaced || pvc == nil || string(pvc.UID) != attempt.PVCUID ||
		pvc.Namespace != history.PVCNamespace || pvc.Name != history.PVCName ||
		pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || !slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes) {
		return unavailable(err)
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

	runtimeCtx := &VolumeRuntimeContext{PVName: pv.Name, PVCNamespace: pvc.Namespace, PVCName: pvc.Name, PVCUID: pvc.UID,
		verifiedClaimBinding: true, PVAnnotations: pv.Annotations, PVCAnnotations: pvc.Annotations, Backend: backend, AccessModes: pv.Spec.AccessModes}
	control := resolvedRecoveryControlState(volume, runtimeCtx, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return status.Error(codes.FailedPrecondition, "replacement destination release is blocked by recovery controls")
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
	if err := s.recheckAttachmentAuthority(ctx, volume, runtimeCtx, target, pv, pvc, false); err != nil {
		return err
	}
	if err := s.checkReplacementAttemptReadback(ctx, attempt); err != nil {
		return err
	}
	pods, err := client.Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return unavailable(err)
	}
	for _, pod := range pods.Items {
		usesClaim, err := podReferencesClaim(&pod, pvc.Name, pvc)
		if err != nil || usesClaim && !terminalPodMayBeReleased(&pod) && (pod.Spec.NodeName == nodeName || pod.Spec.NodeName == "") {
			return unavailable(err)
		}
	}
	current, err := client.Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || !replacementDestinationReleased(current, nodeName, attachment, attempt) || current.UID != node.UID ||
		!current.CreationTimestamp.Equal(&node.CreationTimestamp) || nodeReportsVolumeInUse(current, s.driver.name, volume) {
		return unavailable(err)
	}
	return s.recheckDetachVolumeAttachments(ctx, pv.Name, "", attachments)
}

func replacementDestinationReleased(node *corev1.Node, name string, attachment detachAttachmentAuthority, attempt replacementAttachAttempt) bool {
	return node != nil && node.Name == name && node.UID != "" && string(node.UID) != attempt.ReplacementUID &&
		string(node.UID) != attempt.OriginalNodeUID && node.DeletionTimestamp == nil &&
		!node.CreationTimestamp.IsZero() && attachment.created.After(node.CreationTimestamp.Time) &&
		slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		})
}
