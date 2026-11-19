package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

const annotationPublishMoveReceipt = "opennebula.csi/publish-move-receipt"
const publishMoveReservationPrefix = "reserved-"

type publishMoveNode struct {
	Name, UID string
	VM        int
}

type pendingPublishMove struct {
	Binding, Prior, Grant, Reservation string
	Expires                            time.Time
	Source, Destination                publishMoveNode
	Image, Slot                        int
	Target, Serial                     string
	Renewal                            *publishMoveRenewal `json:"renewal,omitempty"`
	SourceReadback                     string              `json:"sourceReadback,omitempty"`
}

type publishMoveGrant struct {
	Digest  string
	Expires time.Time
}

func (grant publishMoveGrant) valid() bool {
	digest, err := hex.DecodeString(grant.Digest)
	return err == nil && len(digest) == sha256.Size && !grant.Expires.IsZero()
}

type publishMoveRenewal struct {
	publishMoveGrant
	Previous *publishMoveGrant `json:"previous,omitempty"`
}

func (move pendingPublishMove) authorizedGrant() publishMoveGrant {
	if move.Renewal != nil {
		return move.Renewal.publishMoveGrant
	}
	return publishMoveGrant{Digest: move.Grant, Expires: move.Expires}
}

func (move pendingPublishMove) receipt() string {
	return move.receiptFor(move.authorizedGrant())
}

func (move pendingPublishMove) receiptFor(grant publishMoveGrant) string {
	move.Grant, move.Expires, move.Renewal = grant.Digest, grant.Expires, nil
	move.SourceReadback = ""
	return publishMoveDigest(move)
}

func (move pendingPublishMove) matchesHistory(history VolumeHistoryRecord) bool {
	generation := publishMoveGeneration(history)
	return generation == move.Prior || (history.LastSafeDetachNodeName == move.Source.Name &&
		!history.LastSafeDetachTime.IsZero() && generation == move.SourceReadback)
}

type publishMoveSourceKey struct{}

type publishMoveAdmission struct {
	source     publishMoveNode
	generation string
	err        error
}

func (s *ControllerServer) checkPublishMoveAdmission(ctx context.Context, volume string) error {
	if move, ok := ctx.Value(publishMoveSourceKey{}).(publishMoveAdmission); ok {
		if move.err != nil {
			return move.err
		}
		history, err := s.readAttachmentHistory(ctx, volume)
		if err != nil || history.PendingPublishMove != nil || history.PendingReplacementAttach != nil ||
			publishMoveGenerationBeforeRelease(history, volume) != move.generation {
			return attachmentEvidenceUnavailable("Publish move history changed before provider dispatch", nil)
		}
		return recheckPublishPVImage(ctx)
	}
	return nil
}

func publishMoveDigest(value any) string {
	payload, _ := json.Marshal(value)
	return fmt.Sprintf("%x", sha256.Sum256(payload))
}

func publishMoveGeneration(history VolumeHistoryRecord) string {
	history.PendingPublishMove = nil
	history.Version = stateObjectVersion
	return publishMoveDigest(history)
}

func publishMoveGenerationBeforeRelease(history VolumeHistoryRecord, volume string) string {
	history.VolumeID = volume
	history.LastSafeDetachNodeName = ""
	history.LastSafeDetachTime = time.Time{}
	return publishMoveGeneration(history)
}

func samePendingPublishMove(a, b *pendingPublishMove) bool {
	return reflect.DeepEqual(a, b)
}

func publishMoveBinding(binding *VolumeRuntimeContext, driver, volume string) string {
	if binding == nil || !binding.verifiedClaimBinding || binding.pvUID == "" || binding.PVCUID == "" || binding.pvDeleting || binding.pvcDeleting {
		return ""
	}
	pv, pvc := map[string]string{}, map[string]string{}
	for _, key := range publishAuthorityAnnotations {
		if key != annotationPublishMoveReceipt && key != annotationAllowCrossNodeUntil {
			pv[key], pvc[key] = binding.PVAnnotations[key], binding.PVCAnnotations[key]
		}
	}
	return publishMoveDigest([]any{driver, volume, binding.PVName, binding.pvUID, binding.pvClaimRef, binding.pvPhase,
		binding.PVCNamespace, binding.PVCName, binding.PVCUID, binding.pvcPhase, binding.AccessModes, binding.backendAttribute, pv, pvc})
}

func (s *ControllerServer) publishMoveNode(ctx context.Context, name string) (publishMoveNode, error) {
	node, err := s.publishNodeIdentity(ctx, name)
	if err != nil || !node.Exists || node.UID == "" || node.Deleting {
		return publishMoveNode{}, attachmentEvidenceUnavailable("Publish move Node identity is unverified", nil)
	}
	vm, err := s.volumeProvider.NodeExists(ctx, name)
	if err != nil || vm <= 0 {
		return publishMoveNode{}, attachmentEvidenceUnavailable("Publish move VM identity is unverified", nil)
	}
	return publishMoveNode{name, node.UID, vm}, nil
}

func (s *ControllerServer) reservePublishMove(ctx context.Context, req *csi.ControllerPublishVolumeRequest, admission publishPVAdmission) (*VolumeRuntimeContext, error) {
	deny := attachmentEvidenceUnavailable("Publish move reservation is unverified", nil)
	binding := admission.binding
	pvs := s.driver.kubeRuntime.client.CoreV1().PersistentVolumes()
	pv, err := pvs.Get(ctx, binding.PVName, metav1.GetOptions{})
	if err != nil || pv.ResourceVersion == "" || !publishPVMatches(pv, binding, s.driver.name, req.VolumeId) ||
		strings.HasPrefix(pv.Annotations[annotationPublishMoveReceipt], publishMoveReservationPrefix) {
		return nil, deny
	}
	if err := recheckPublishPVImage(ctx); err != nil {
		return nil, err
	}
	reserved := *binding
	reserved.PVAnnotations = cloneStringMap(binding.PVAnnotations)
	token := publishMoveReservationPrefix + publishMoveDigest([]string{string(binding.pvUID), string(binding.PVCUID), pv.ResourceVersion,
		binding.PVAnnotations[annotationAllowCrossNodeUntil], req.NodeId})
	pv.Annotations[annotationPublishMoveReceipt] = token
	reserved.PVAnnotations[annotationPublishMoveReceipt] = token
	if _, err := pvs.Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		return nil, deny
	}
	if err := admission.check(ctx, &reserved); err != nil {
		return &reserved, deny
	}
	return &reserved, nil
}

func (s *ControllerServer) releaseUnsentPublishMove(ctx context.Context, req *csi.ControllerPublishVolumeRequest, original publishPVAdmission, reserved *VolumeRuntimeContext, node KubernetesNodeIdentity, vmID, imageID int) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	move, ok := ctx.Value(publishMoveSourceKey{}).(publishMoveAdmission)
	if !ok || move.err != nil {
		return
	}
	pvs := s.driver.kubeRuntime.client.CoreV1().PersistentVolumes()
	pv, err := pvs.Get(ctx, reserved.PVName, metav1.GetOptions{})
	if err != nil || pv.ResourceVersion == "" || !publishPVMatches(pv, reserved, s.driver.name, req.VolumeId) {
		return
	}
	history, err := s.readAttachmentHistory(ctx, req.VolumeId)
	if err != nil || history.PendingPublishMove != nil || history.PendingReplacementAttach != nil ||
		publishMoveGenerationBeforeRelease(history, req.VolumeId) != move.generation {
		return
	}
	destination, err := s.publishMoveNode(ctx, req.NodeId)
	if err != nil || destination.UID != node.UID || destination.VM != vmID ||
		s.verifyPublishMoveSource(ctx, req, move.source, imageID, reserved, false) != nil {
		return
	}
	originalReceipt := original.binding.PVAnnotations[annotationPublishMoveReceipt]
	original.binding = reserved
	if err := recheckPublishPVImage(context.WithValue(ctx, publishPVImageCheckKey{}, original)); err != nil {
		return
	}
	if originalReceipt == "" {
		delete(pv.Annotations, annotationPublishMoveReceipt)
	} else {
		pv.Annotations[annotationPublishMoveReceipt] = originalReceipt
	}
	_, _ = pvs.Update(ctx, pv, metav1.UpdateOptions{})
}

func (s *ControllerServer) startPublishMove(ctx context.Context, req *csi.ControllerPublishVolumeRequest, disk successfulAttachmentObservation, binding *VolumeRuntimeContext, protection *LocalRWOProtectionDecision, verify func() error) (*pendingPublishMove, error) {
	deny := attachmentEvidenceUnavailable("Publish move has no verified dispatch and grant", nil)
	prepared, _ := ctx.Value(publishMoveSourceKey{}).(publishMoveAdmission)
	source := prepared.source
	scope := publishMoveBinding(binding, s.driver.name, req.VolumeId)
	if scope == "" || source.UID == "" || source.VM == disk.vmID || disk.nodeUID == "" || !disk.newAttachment || disk.adopted || disk.replacementAttempt != nil ||
		!strings.HasPrefix(binding.PVAnnotations[annotationPublishMoveReceipt], publishMoveReservationPrefix) {
		return nil, deny
	}
	expires, valid := crossNodeOverrideDeadline(binding)
	if !valid || !expires.Equal(protection.OverrideExpiresAt) || !expires.After(time.Now()) {
		return nil, deny
	}
	move := &pendingPublishMove{
		Binding: scope, Grant: publishMoveDigest(binding.PVAnnotations[annotationAllowCrossNodeUntil]), Expires: expires,
		Reservation: binding.PVAnnotations[annotationPublishMoveReceipt],
		Source:      source, Destination: publishMoveNode{disk.nodeName, disk.nodeUID, disk.vmID},
		Image: disk.imageID, Slot: disk.diskID, Target: disk.target, Serial: disk.serial,
	}
	_, err := s.driver.volumeHistory.upsert(ctx, req.VolumeId, func(history *VolumeHistoryRecord) error {
		if history.PendingPublishMove != nil || history.PendingReplacementAttach != nil ||
			publishMoveGenerationBeforeRelease(*history, req.VolumeId) != prepared.generation {
			return deny
		}
		if err := verify(); err != nil {
			return err
		}
		move.Prior = publishMoveGeneration(*history)
		history.PendingPublishMove = move
		return nil
	})
	if err != nil {
		return nil, attachmentEvidenceUnavailable("Publish move intent could not be persisted", nil)
	}
	return move, nil
}

func (s *ControllerServer) verifyPublishMove(ctx context.Context, req *csi.ControllerPublishVolumeRequest, move *pendingPublishMove, binding *VolumeRuntimeContext, renewed bool) error {
	deny := attachmentEvidenceUnavailable("Publish move authority changed or is unverified", nil)
	if move.Binding == "" || move.Binding != publishMoveBinding(binding, s.driver.name, req.VolumeId) ||
		move.Destination.Name != req.NodeId || move.Source.Name == req.NodeId || move.Source.VM == move.Destination.VM ||
		move.Image <= 0 || move.Slot < 0 || move.Target == "" || move.Serial != fmt.Sprintf("onecsi-%d", move.Image) ||
		move.Expires.IsZero() {
		return deny
	}
	history, err := s.readAttachmentHistory(ctx, req.VolumeId)
	if err != nil || history.PendingReplacementAttach != nil || !samePendingPublishMove(history.PendingPublishMove, move) ||
		!move.matchesHistory(history) {
		return deny
	}
	if renewed {
		target := opennebula.HostArtifactAttachmentTarget{NodeName: move.Source.Name, VMID: move.Source.VM, ImageID: move.Image, DiskID: -1}
		if err := s.attachmentProtectionChecks(ctx, req.VolumeId, binding, target); err != nil {
			return err
		}
	}
	return s.verifyPublishMoveSource(ctx, req, move.Source, move.Image, binding, renewed)
}

func (s *ControllerServer) verifyPublishMoveSource(ctx context.Context, req *csi.ControllerPublishVolumeRequest, original publishMoveNode, imageID int, binding *VolumeRuntimeContext, requireDemand bool) error {
	deny := attachmentEvidenceUnavailable("Publish move source or demand is unverified", nil)
	source, err := s.publishMoveNode(ctx, original.Name)
	if err != nil || source != original {
		return deny
	}
	if inspector, ok := s.volumeProvider.(opennebula.ReplacementAttachInspector); ok {
		if _, err := inspector.InspectReplacementRelease(ctx, imageID, source.VM, ""); err != nil {
			return deny
		}
	} else if _, err := s.volumeProvider.GetVolumeInNode(ctx, imageID, source.VM); !errors.Is(err, opennebula.ErrAttachmentNotFound) {
		return deny
	}
	desired, err := s.driver.kubeRuntime.DesiredNodeForVolume(ctx, req.VolumeId)
	if err != nil || desired.PVName != binding.PVName || desired.PVCName != binding.PVCName || desired.PVCNamespace != binding.PVCNamespace ||
		desired.PVDeleting || desired.PVCDeleting || desired.MultipleNodes || (desired.Desired && desired.NodeName != req.NodeId) ||
		(requireDemand && (!desired.Desired || desired.DemandSource != volumeDemandSourceActivePod)) {
		return deny
	}
	return ctx.Err()
}

func (s *ControllerServer) checkPublishMoveRenewal(ctx context.Context, req *csi.ControllerPublishVolumeRequest, move *pendingPublishMove, binding *VolumeRuntimeContext, deadline time.Time) error {
	placement, err := s.localRWOProtectionProjection(ctx, req.VolumeId, req.NodeId, binding, false, false)
	if err != nil || placement.Invalid || !placement.OverrideUsed || !placement.OverrideExpiresAt.Equal(deadline) ||
		placement.InferredRequiredNode != move.Source.Name ||
		(placement.Source == protectionSourceExplicitRequiredNode && placement.RequiredNode != req.NodeId) ||
		!deadline.After(time.Now()) {
		return attachmentEvidenceUnavailable("Publish move renewal authority is unverified", err)
	}
	return nil
}

func (s *ControllerServer) consumePublishMove(ctx context.Context, req *csi.ControllerPublishVolumeRequest, move *pendingPublishMove, admission publishPVAdmission, verify func(context.Context) error) (*VolumeRuntimeContext, error) {
	deny := attachmentEvidenceUnavailable("Publish move grant consumption is unverified", nil)
	binding := admission.binding
	pvs := s.driver.kubeRuntime.client.CoreV1().PersistentVolumes()
	grant, token := move.authorizedGrant(), move.receipt()
	predecessor := move.Reservation
	if move.Renewal != nil && move.Renewal.Previous != nil {
		predecessor = move.receiptFor(*move.Renewal.Previous)
	}
	consumed := *binding
	consumed.PVAnnotations = cloneStringMap(binding.PVAnnotations)
	delete(consumed.PVAnnotations, annotationAllowCrossNodeUntil)
	consumed.PVAnnotations[annotationPublishMoveReceipt] = token
	err := retryOnConflict(retry.DefaultRetry, func() error {
		pv, err := pvs.Get(ctx, binding.PVName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if _, present := pv.Annotations[annotationAllowCrossNodeUntil]; !present && publishPVMatches(pv, &consumed, s.driver.name, req.VolumeId) {
			return nil
		}
		if pv.ResourceVersion == "" || pv.Annotations[annotationPublishMoveReceipt] != predecessor || !publishPVMatches(pv, binding, s.driver.name, req.VolumeId) ||
			publishMoveDigest(pv.Annotations[annotationAllowCrossNodeUntil]) != grant.Digest {
			return deny
		}
		if err := verify(ctx); err != nil {
			return err
		}
		if !grant.Expires.After(time.Now()) {
			return deny
		}
		delete(pv.Annotations, annotationAllowCrossNodeUntil)
		pv.Annotations[annotationPublishMoveReceipt] = token
		_, err = pvs.Update(ctx, pv, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, deny
	}
	pv, err := pvs.Get(ctx, binding.PVName, metav1.GetOptions{})
	if err != nil || !publishPVMatches(pv, &consumed, s.driver.name, req.VolumeId) {
		return nil, deny
	}
	if _, present := pv.Annotations[annotationAllowCrossNodeUntil]; present {
		return nil, deny
	}
	return &consumed, nil
}

func (s *ControllerServer) resumePublishMove(ctx context.Context, req *csi.ControllerPublishVolumeRequest, imageID int, node KubernetesNodeIdentity, admission publishPVAdmission) (*diskPublishCompletion, bool, error) {
	nodeRelease := s.driver.operationLocks.Acquire(controllerNodeLockKey(req.NodeId))
	defer nodeRelease()
	volumeRelease := s.driver.operationLocks.Acquire(controllerVolumeLockKey(req.VolumeId))
	defer volumeRelease()
	history, err := s.readAttachmentHistory(ctx, req.VolumeId)
	if err != nil {
		return nil, true, attachmentEvidenceUnavailable("Publish move history is unreadable", nil)
	}
	move := history.PendingPublishMove
	if move == nil {
		if admission.binding != nil {
			receipt := admission.binding.PVAnnotations[annotationPublishMoveReceipt]
			if receipt != "" && receipt != history.LastPublishMoveReceipt {
				return nil, true, attachmentEvidenceUnavailable("Publish move dispatch is unverified", nil)
			}
		}
		return nil, false, nil
	}
	deny := attachmentEvidenceUnavailable("pending Publish move authority is unverified", nil)
	binding := admission.binding
	if move.Image != imageID || move.Destination.UID != node.UID || req.Readonly || !replacementPublishCanWrite(req) ||
		binding == nil || move.Binding != publishMoveBinding(binding, s.driver.name, req.VolumeId) {
		return nil, true, deny
	}
	raw, present := binding.PVAnnotations[annotationAllowCrossNodeUntil]
	receipt := binding.PVAnnotations[annotationPublishMoveReceipt]
	grant := move.authorizedGrant()
	renewal := move.Renewal
	var previous *publishMoveGrant
	if renewal != nil {
		previous = renewal.Previous
		if !grant.valid() || grant.Digest == move.Grant ||
			(previous != nil && (!previous.valid() || previous.Digest == grant.Digest ||
				(previous.Digest == move.Grant && !previous.Expires.Equal(move.Expires)))) {
			return nil, true, deny
		}
	}
	policyBinding := *binding
	if !present {
		if receipt != move.receipt() || !grant.Expires.After(time.Now()) {
			return nil, true, deny
		}
		policyBinding.PVAnnotations = cloneStringMap(binding.PVAnnotations)
		policyBinding.PVAnnotations[annotationAllowCrossNodeUntil] = grant.Expires.Format(time.RFC3339Nano)
	} else {
		deadline, valid := crossNodeOverrideDeadline(binding)
		now := time.Now()
		if !valid || !deadline.After(now) {
			return nil, true, deny
		}
		digest := publishMoveDigest(raw)
		switch {
		case receipt == move.Reservation && previous == nil:
		case receipt == move.receipt() && grant.valid() && !grant.Expires.After(now):
			consumed := grant
			previous = &consumed
		case previous != nil && receipt == move.receiptFor(*previous) && !previous.Expires.After(now):
		default:
			return nil, true, deny
		}
		if digest == grant.Digest {
			if !deadline.Equal(grant.Expires) {
				return nil, true, deny
			}
		} else {
			if digest == move.Grant || (previous != nil && digest == previous.Digest) {
				return nil, true, deny
			}
			grant = publishMoveGrant{Digest: digest, Expires: deadline}
			renewal = &publishMoveRenewal{publishMoveGrant: grant, Previous: previous}
		}
	}
	placement, err := s.localRWOProtectionProjection(ctx, req.VolumeId, req.NodeId, &policyBinding, false, false)
	if err != nil || placement.Invalid || (placement.Protected && placement.RequiredNode != req.NodeId && !placement.OverrideUsed) ||
		(placement.InferredRequiredNode != "" && placement.InferredRequiredNode != move.Source.Name) {
		return nil, true, deny
	}
	control := resolvedRecoveryControlState(req.VolumeId, binding, VolumeRecoveryControlState{})
	if control.Invalid || control.ManualActive() {
		return nil, true, deny
	}
	placement.OverrideUsed, placement.OverrideExpiresAt = true, grant.Expires
	protect, err := s.publishProtectionCheck(ctx, req.VolumeId, req.NodeId, binding, placement, control, move.Destination.VM, move.Image)
	if err != nil {
		return nil, true, err
	}
	ctx = context.WithValue(ctx, publishProtectionCheckKey{}, protect)
	ctx = context.WithValue(ctx, publishMoveSourceKey{}, publishMoveAdmission{source: move.Source})
	disk, err := s.verifyPublishedAttachment(ctx, req.VolumeId, req.NodeId, move.Image, move.Destination.VM, move.Target, "", node, true)
	if err != nil || disk.diskID != move.Slot || disk.serial != move.Serial {
		return nil, true, deny
	}
	publishContext := s.publishContextForVolume(ctx, req.VolumeId, disk, 0, req.GetVolumeContext())
	completed, err := s.finishPublishMove(ctx, req, disk, node, admission, move, renewal, publishContext)
	if err != nil {
		return nil, true, err
	}
	return completed, true, s.recheckPublishTarget(completed.withAdmission(ctx), req, disk, node)
}

func (s *ControllerServer) finishPublishMove(ctx context.Context, req *csi.ControllerPublishVolumeRequest, disk successfulAttachmentObservation, node KubernetesNodeIdentity, admission publishPVAdmission, move *pendingPublishMove, renewal *publishMoveRenewal, publishContext map[string]string) (*diskPublishCompletion, error) {
	grantBinding := *admission.binding
	if renewal != nil {
		if _, present := grantBinding.PVAnnotations[annotationAllowCrossNodeUntil]; !present {
			grantBinding.PVAnnotations = cloneStringMap(grantBinding.PVAnnotations)
			grantBinding.PVAnnotations[annotationAllowCrossNodeUntil] = renewal.Expires.Format(time.RFC3339Nano)
		}
	}
	verify := func(ctx context.Context) error {
		if err := s.verifyPublishMove(ctx, req, move, admission.binding, renewal != nil); err != nil {
			return err
		}
		if renewal != nil {
			if err := s.checkPublishMoveRenewal(ctx, req, move, &grantBinding, renewal.Expires); err != nil {
				return err
			}
		}
		return s.recheckPublishTarget(ctx, req, disk, node)
	}
	if !reflect.DeepEqual(move.Renewal, renewal) {
		updated := *move
		updated.Renewal = renewal
		deny := attachmentEvidenceUnavailable("Publish move renewal could not be persisted", nil)
		if _, err := s.driver.volumeHistory.upsert(ctx, req.VolumeId, func(current *VolumeHistoryRecord) error {
			if !samePendingPublishMove(current.PendingPublishMove, move) || !move.matchesHistory(*current) {
				return deny
			}
			if err := verify(ctx); err != nil {
				return err
			}
			current.PendingPublishMove = &updated
			return nil
		}); err != nil {
			return nil, deny
		}
		move = &updated
	}
	binding, err := s.consumePublishMove(ctx, req, move, admission, verify)
	if err != nil {
		return nil, err
	}
	admission.binding = binding
	completed := &diskPublishCompletion{
		observation: disk, response: &csi.ControllerPublishVolumeResponse{PublishContext: publishContext},
		admission: admission, annotatePlacement: true, move: move,
	}
	ctx = completed.withAdmission(ctx)
	var sourceHold StickyAttachmentState
	if err := s.recordSuccessfulLocalVolumePublish(ctx, req.VolumeId, disk, req.GetVolumeContext(), binding, time.Time{}, move, func() error {
		if err := verify(ctx); err != nil {
			return err
		}
		state, held := s.driver.stickyAttachments.Get(req.VolumeId)
		sourceHold = StickyAttachmentState{}
		if held && state.NodeID == move.Source.Name {
			if (state.PVCUID != "" && state.PVCUID != binding.PVCUID) ||
				(state.PVCName != "" && state.PVCName != binding.PVCName) ||
				(state.PVCNamespace != "" && state.PVCNamespace != binding.PVCNamespace) {
				return attachmentEvidenceUnavailable("Publish move sticky attachment claim changed", nil)
			}
			if renewal != nil {
				sourceHold = state
			} else {
				if err := s.driver.stickyAttachments.Clear(req.VolumeId); err != nil {
					return attachmentEvidenceUnavailable("could not release the prior sticky attachment hold", nil)
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if sourceHold.VolumeID != "" {
		if current, held := s.driver.stickyAttachments.Get(req.VolumeId); held {
			if current != sourceHold {
				return nil, attachmentEvidenceUnavailable("Publish move sticky attachment hold changed", nil)
			}
			if err := s.driver.stickyAttachments.Clear(req.VolumeId); err != nil {
				return nil, attachmentEvidenceUnavailable("could not release the prior sticky attachment hold", nil)
			}
		}
	}
	return completed, nil
}
