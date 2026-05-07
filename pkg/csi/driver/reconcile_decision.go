package driver

import (
	"context"
	"fmt"
	"strings"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

type VolumeReconcileOperation string

const (
	VolumeReconcileAttach VolumeReconcileOperation = "attach"
	VolumeReconcileDetach VolumeReconcileOperation = "detach"
)

type VolumeReconcileAction string

const (
	VolumeReconcileProceed  VolumeReconcileAction = "proceed"
	VolumeReconcileComplete VolumeReconcileAction = "complete"
	VolumeReconcilePause    VolumeReconcileAction = "pause"
	VolumeReconcileReject   VolumeReconcileAction = "reject"
	VolumeReconcileStale    VolumeReconcileAction = "stale"
)

type VolumeReconcileEvidence struct {
	VolumeID  string
	NodeID    string
	Operation VolumeReconcileOperation

	Lookup     *opennebula.VolumeLookupResult
	LookupErr  error
	History    VolumeHistoryRecord
	RuntimeCtx *VolumeRuntimeContext

	OpenNebulaImageID int
	OpenNebulaNodeID  int
	NodeLookupErr     error

	Cooldown *opennebula.HotplugCooldownState

	RecoveryControl VolumeRecoveryControlState
	RecoveryErr     error

	Attached       bool
	AttachedTarget string
	AttachedErr    error

	DesiredState VolumeDesiredState
	DesiredErr   error

	Protection     LocalRWOProtectionDecision
	ProtectionErr  error
	OrphanTeardown bool

	RepairState             *VolumeRepairState
	VolumeQuarantine        *VolumeQuarantineState
	HostArtifactQuarantined bool
}

type VolumeReconcileDecision struct {
	Action           VolumeReconcileAction
	Reason           string
	Message          string
	Code             codes.Code
	AttachmentTarget string
	Evidence         VolumeReconcileEvidence
}

func (d VolumeReconcileDecision) HotplugQueueValidation() HotplugQueueValidation {
	switch d.Action {
	case VolumeReconcileComplete:
		return HotplugQueueValidation{
			Decision:         HotplugQueueValidationCompleted,
			Reason:           d.Reason,
			Message:          d.Message,
			AttachmentTarget: d.AttachmentTarget,
		}
	case VolumeReconcilePause:
		return HotplugQueueValidation{
			Decision: HotplugQueueValidationPaused,
			Reason:   d.Reason,
			Message:  d.Message,
		}
	case VolumeReconcileReject, VolumeReconcileStale:
		return HotplugQueueValidation{
			Decision: HotplugQueueValidationStale,
			Reason:   d.Reason,
			Message:  d.Message,
		}
	default:
		return HotplugQueueValidation{Decision: HotplugQueueValidationValid}
	}
}

func (d VolumeReconcileDecision) Err() error {
	if d.Action != VolumeReconcileReject && d.Action != VolumeReconcilePause && d.Action != VolumeReconcileStale {
		return nil
	}
	code := d.Code
	if code == codes.OK {
		code = codes.Unavailable
	}
	return status.Error(code, d.Message)
}

func (s *ControllerServer) evaluateVolumeReconcile(ctx context.Context, node, operation, volume string) VolumeReconcileDecision {
	evidence := VolumeReconcileEvidence{
		VolumeID:  strings.TrimSpace(volume),
		NodeID:    strings.TrimSpace(node),
		Operation: VolumeReconcileOperation(strings.ToLower(strings.TrimSpace(operation))),
	}
	decision := VolumeReconcileDecision{
		Action:   VolumeReconcileProceed,
		Reason:   "valid",
		Evidence: evidence,
	}
	if s == nil || s.volumeProvider == nil || evidence.NodeID == "" || evidence.VolumeID == "" {
		return decision
	}

	lookup, history, runtimeCtx, lookupErr := s.lookupVolumeForPublish(ctx, evidence.VolumeID, evidence.NodeID)
	evidence.Lookup = lookup
	evidence.LookupErr = lookupErr
	evidence.History = history
	evidence.RuntimeCtx = runtimeCtx
	if lookupErr != nil {
		decision.Evidence = evidence
		switch status.Code(lookupErr) {
		case codes.NotFound:
			return decision.with(VolumeReconcileStale, "volume_not_found", codes.NotFound,
				fmt.Sprintf("volume %s no longer exists; dropping queued %s for node %s", evidence.VolumeID, evidence.Operation, evidence.NodeID))
		case codes.FailedPrecondition:
			return decision.with(VolumeReconcileReject, queueReasonRepairRequired, codes.FailedPrecondition, lookupErr.Error())
		default:
			return decision
		}
	}
	if lookup != nil {
		evidence.OpenNebulaImageID = lookup.ImageID
	}

	nodeID, nodeErr := s.volumeProvider.NodeExists(ctx, evidence.NodeID)
	evidence.OpenNebulaNodeID = nodeID
	evidence.NodeLookupErr = nodeErr
	if nodeErr != nil {
		decision.Evidence = evidence
		return decision
	}
	if nodeID == -1 {
		decision.Evidence = evidence
		return decision.with(VolumeReconcileStale, "node_not_found", codes.NotFound,
			fmt.Sprintf("node %s no longer exists; dropping queued %s for volume %s", evidence.NodeID, evidence.Operation, evidence.VolumeID))
	}

	if state, ok := s.hotplugCooldownState(ctx, evidence.NodeID); ok {
		evidence.Cooldown = &state
		decision.Evidence = evidence
		return decision.with(VolumeReconcilePause, state.Reason, codes.Unavailable, s.formatHotplugGuardMessage(state))
	}

	recoveryControl, recoveryErr := s.recoveryControlState(ctx, evidence.VolumeID, runtimeCtx)
	evidence.RecoveryControl = recoveryControl
	evidence.RecoveryErr = recoveryErr
	if recoveryErr != nil {
		klog.V(3).InfoS("Failed to resolve recovery control state during reconcile evaluation", "volume", evidence.VolumeID, "node", evidence.NodeID, "operation", evidence.Operation, "err", recoveryErr)
	} else if recoveryControl.Invalid {
		decision.Evidence = evidence
		return decision.with(VolumeReconcileReject, queueReasonRecoveryControlInvalid, codes.FailedPrecondition, recoveryControl.Message)
	}

	if evidence.OpenNebulaImageID > 0 && evidence.OpenNebulaNodeID > 0 {
		target, attachedErr := s.volumeProvider.GetVolumeInNode(ctx, evidence.OpenNebulaImageID, evidence.OpenNebulaNodeID)
		evidence.AttachedTarget = strings.TrimSpace(target)
		evidence.AttachedErr = attachedErr
		evidence.Attached = attachedErr == nil
	}

	switch evidence.Operation {
	case VolumeReconcileAttach:
		return s.evaluateVolumeAttachDecision(ctx, decision.withEvidence(evidence))
	case VolumeReconcileDetach:
		return s.evaluateVolumeDetachDecision(ctx, decision.withEvidence(evidence))
	default:
		return decision.withEvidence(evidence)
	}
}

func (s *ControllerServer) evaluateVolumeAttachDecision(ctx context.Context, decision VolumeReconcileDecision) VolumeReconcileDecision {
	evidence := decision.Evidence
	recoveryControl := evidence.RecoveryControl

	if recoveryControl.AdoptionRequested() {
		manualTarget, adoptErr := s.manualRecoveryAdoptionTarget(ctx, evidence.VolumeID, evidence.NodeID, evidence.History, evidence.RuntimeCtx, evidence.Lookup, recoveryControl)
		if adoptErr != nil {
			return decision.with(VolumeReconcileReject, queueReasonRecoveryControlInvalid, codes.FailedPrecondition, adoptErr.Error())
		}
		if manualTarget != "" {
			return decision.withAttachment(VolumeReconcileComplete, queueReasonManualRecoveryAdopted, codes.OK,
				fmt.Sprintf("manual recovery adoption is ready for volume %s on node %s using guest-visible device %s", evidence.VolumeID, evidence.NodeID, manualTarget),
				manualTarget)
		}
	}
	if recoveryControl.ManualActive() && !evidence.Attached {
		return decision.with(VolumeReconcilePause, queueReasonRecoveryModeManual, codes.Unavailable,
			recoveryControl.QueueBlockMessage(evidence.VolumeID, evidence.NodeID, string(evidence.Operation)))
	}
	if evidence.Attached {
		return decision.withAttachment(VolumeReconcileComplete, "already_attached", codes.OK,
			fmt.Sprintf("volume %s is already attached to node %s", evidence.VolumeID, evidence.NodeID),
			evidence.AttachedTarget)
	}
	if state, ok := s.repairStateForQueue(evidence.VolumeID); ok {
		evidence.RepairState = &state
		decision = decision.withEvidence(evidence)
		if evidence.RuntimeCtx != nil {
			s.recordActiveRepairStateEvent(ctx, evidence.RuntimeCtx, state)
		}
		return decision.with(VolumeReconcileReject, queueReasonRepairRequired, codes.FailedPrecondition,
			firstNonEmpty(state.Message, fmt.Sprintf("repair-required state %s is active for volume %s", state.Classification, evidence.VolumeID)))
	}
	if quarantine, ok := s.activeVolumeQuarantine(ctx, evidence.VolumeID); ok {
		evidence.VolumeQuarantine = &quarantine
		decision = decision.withEvidence(evidence)
		return decision.with(VolumeReconcileReject, queueReasonVolumeQuarantined, codes.FailedPrecondition, volumeQuarantineMessage(quarantine))
	}
	if s.activeHostArtifactQuarantine(ctx, evidence.VolumeID) {
		evidence.HostArtifactQuarantined = true
		decision = decision.withEvidence(evidence)
		return decision.with(VolumeReconcileReject, queueReasonRepairRequired, codes.FailedPrecondition,
			fmt.Sprintf("host artifact quarantine is active for volume %s", evidence.VolumeID))
	}
	protection, protectionErr := s.localRWOProtectionDecision(ctx, evidence.VolumeID, evidence.NodeID)
	evidence.Protection = protection
	evidence.ProtectionErr = protectionErr
	decision = decision.withEvidence(evidence)
	if protectionErr == nil && protection.Protected && strings.TrimSpace(protection.RequiredNode) != "" &&
		strings.TrimSpace(protection.RequiredNode) != strings.TrimSpace(evidence.NodeID) && !protection.OverrideUsed {
		return decision.with(VolumeReconcilePause, protection.Reason, codes.Unavailable, protection.Message)
	}
	if stale, reason := s.attachRequestStale(ctx, evidence.NodeID, evidence.VolumeID); stale {
		return decision.with(VolumeReconcileStale, reason, codes.Aborted,
			fmt.Sprintf("queued attach for volume %s on node %s is stale: %s", evidence.VolumeID, evidence.NodeID, reason))
	}
	return decision
}

func (s *ControllerServer) evaluateVolumeDetachDecision(ctx context.Context, decision VolumeReconcileDecision) VolumeReconcileDecision {
	evidence := decision.Evidence
	recoveryControl := evidence.RecoveryControl

	if recoveryControl.ManualActive() {
		return decision.with(VolumeReconcilePause, queueReasonRecoveryModeManual, codes.Unavailable,
			recoveryControl.QueueBlockMessage(evidence.VolumeID, evidence.NodeID, string(evidence.Operation)))
	}
	if !evidence.Attached {
		return decision.with(VolumeReconcileComplete, "already_detached", codes.OK,
			fmt.Sprintf("volume %s is already detached from node %s", evidence.VolumeID, evidence.NodeID))
	}
	if s.driver != nil && s.driver.kubeRuntime != nil {
		desiredState, desiredErr := s.driver.kubeRuntime.DesiredNodeForVolume(ctx, evidence.VolumeID)
		evidence.DesiredState = desiredState
		evidence.DesiredErr = desiredErr
		decision = decision.withEvidence(evidence)
		if desiredErr == nil && desiredState.Desired && !desiredState.MultipleNodes && strings.TrimSpace(desiredState.NodeName) == strings.TrimSpace(evidence.NodeID) {
			return decision.with(VolumeReconcilePause, queueReasonSameNodeReuseRequired, codes.Unavailable,
				fmt.Sprintf("queued detach for volume %s on node %s is paused because Kubernetes still desires the volume there (%s)", evidence.VolumeID, evidence.NodeID, desiredState.Reason))
		}
	}
	orphanTeardown, _ := s.orphanTeardownEligible(ctx, evidence.VolumeID, evidence.NodeID, evidence.RuntimeCtx, evidence.DesiredState, evidence.DesiredErr)
	evidence.OrphanTeardown = orphanTeardown
	if orphanTeardown {
		evidence.History = s.refreshVolumeHistory(ctx, evidence.VolumeID)
	}
	protection, protectionErr := s.localRWOProtectionDecision(ctx, evidence.VolumeID, evidence.NodeID)
	evidence.Protection = protection
	evidence.ProtectionErr = protectionErr
	decision = decision.withEvidence(evidence)
	if protectionErr == nil && protection.Protected && strings.TrimSpace(protection.RequiredNode) == strings.TrimSpace(evidence.NodeID) && !bypassProtectionForOrphanTeardown(protection, orphanTeardown) {
		return decision.with(VolumeReconcilePause, protection.Reason, codes.Unavailable,
			fmt.Sprintf("queued detach for volume %s on node %s is paused while local RWO protection is active (%s)", evidence.VolumeID, evidence.NodeID, protection.Reason))
	}
	return decision
}

func (d VolumeReconcileDecision) withEvidence(evidence VolumeReconcileEvidence) VolumeReconcileDecision {
	d.Evidence = evidence
	return d
}

func (d VolumeReconcileDecision) with(action VolumeReconcileAction, reason string, code codes.Code, message string) VolumeReconcileDecision {
	d.Action = action
	d.Reason = strings.TrimSpace(reason)
	d.Code = code
	d.Message = strings.TrimSpace(message)
	if d.Reason == "" {
		d.Reason = string(action)
	}
	return d
}

func (d VolumeReconcileDecision) withAttachment(action VolumeReconcileAction, reason string, code codes.Code, message, target string) VolumeReconcileDecision {
	d = d.with(action, reason, code, message)
	d.AttachmentTarget = strings.TrimSpace(target)
	return d
}

func (s *ControllerServer) backgroundDetachAllowedByDecision(ctx context.Context, volume, node string) (bool, string) {
	decision := s.evaluateVolumeReconcile(ctx, node, string(VolumeReconcileDetach), volume)
	switch decision.Action {
	case VolumeReconcileProceed:
		return true, ""
	case VolumeReconcileComplete, VolumeReconcileStale:
		return false, decision.Reason
	case VolumeReconcilePause, VolumeReconcileReject:
		return false, firstNonEmpty(decision.Message, decision.Reason)
	default:
		return false, decision.Reason
	}
}
