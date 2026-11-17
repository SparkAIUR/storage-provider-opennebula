package driver

import (
	"context"
	"errors"
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
			Err:      d.Err(),
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
		if evidence.Operation == VolumeReconcileDetach && !errors.Is(lookupErr, opennebula.ErrVolumeNotFound) {
			klog.V(2).InfoS("Detach image lookup failed", "err", lookupErr)
			if status.Code(lookupErr) == codes.FailedPrecondition {
				return decision.with(VolumeReconcileReject, queueReasonRepairRequired, codes.FailedPrecondition, "cannot verify detach image identity")
			}
			return decision.with(VolumeReconcilePause, "volume_lookup_unknown", codes.Unavailable, "cannot verify detach image identity")
		}
		switch status.Code(lookupErr) {
		case codes.NotFound:
			if !errors.Is(lookupErr, opennebula.ErrVolumeNotFound) {
				return decision.with(VolumeReconcilePause, "volume_lookup_unknown", codes.Unavailable, lookupErr.Error())
			}
			return decision.with(VolumeReconcileStale, "volume_not_found", codes.NotFound,
				fmt.Sprintf("volume %s no longer exists; dropping queued %s for node %s", evidence.VolumeID, evidence.Operation, evidence.NodeID))
		case codes.FailedPrecondition:
			return decision.with(VolumeReconcileReject, queueReasonRepairRequired, codes.FailedPrecondition, lookupErr.Error())
		default:
			return decision.with(VolumeReconcilePause, "volume_lookup_unknown", codes.Unavailable, lookupErr.Error())
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
		message := nodeErr.Error()
		if evidence.Operation == VolumeReconcileDetach {
			klog.V(2).InfoS("Detach node identity lookup failed", "err", nodeErr)
			message = "attachment node identity is unavailable"
		}
		return decision.with(VolumeReconcilePause, "node_lookup_unknown", codes.Unavailable, message)
	}
	if nodeID == -1 {
		decision.Evidence = evidence
		return decision.with(VolumeReconcilePause, "node_lookup_unknown", codes.Unavailable,
			fmt.Sprintf("node %s identity is unavailable for volume %s", evidence.NodeID, evidence.VolumeID))
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
		return decision.withEvidence(evidence).with(VolumeReconcilePause, "recovery_control_unknown", codes.Unavailable, recoveryErr.Error())
	} else if recoveryControl.Invalid {
		decision.Evidence = evidence
		return decision.with(VolumeReconcileReject, queueReasonRecoveryControlInvalid, codes.FailedPrecondition, recoveryControl.Message)
	}

	if evidence.OpenNebulaImageID > 0 && evidence.OpenNebulaNodeID > 0 {
		target, attachedErr := s.volumeProvider.GetVolumeInNode(ctx, evidence.OpenNebulaImageID, evidence.OpenNebulaNodeID)
		evidence.AttachedTarget = strings.TrimSpace(target)
		evidence.AttachedErr = attachedErr
		evidence.Attached = attachedErr == nil
		if attachedErr != nil && !errors.Is(attachedErr, opennebula.ErrAttachmentNotFound) {
			message := fmt.Sprintf("attachment state is unknown for volume %s on node %s", evidence.VolumeID, evidence.NodeID)
			if evidence.Operation == VolumeReconcileDetach {
				klog.V(2).InfoS(message, "err", attachedErr)
			} else {
				message = fmt.Sprintf("%s: %v", message, attachedErr)
			}
			return decision.withEvidence(evidence).with(VolumeReconcilePause, "attachment_unknown", codes.Unavailable, message)
		}
	} else {
		return decision.withEvidence(evidence).with(VolumeReconcilePause, "attachment_unknown", codes.Unavailable,
			"attachment identity could not be established")
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
	if state, ok, err := s.repairStateForQueue(ctx, evidence.VolumeID); err != nil {
		return decision.with(VolumeReconcilePause, queueReasonRepairRequired, codes.Unavailable, fmt.Sprintf("cannot verify current volume repair state: %v", err))
	} else if ok {
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
		if err := s.verifyDetachAbsenceTarget(ctx, evidence.VolumeID, evidence.NodeID, evidence.OpenNebulaImageID, evidence.OpenNebulaNodeID, ""); err != nil {
			return decision.with(VolumeReconcilePause, "attachment_absence_unverified", status.Code(err), status.Convert(err).Message())
		}
		return decision.with(VolumeReconcileComplete, "already_detached", codes.OK,
			fmt.Sprintf("volume %s is already detached from node %s", evidence.VolumeID, evidence.NodeID))
	}
	if recoveryControl.Invalid || recoveryControl.ObserveActive() {
		return decision.with(VolumeReconcilePause, queueReasonRecoveryModeObserve, codes.Unavailable, "physical detach is blocked by recovery controls")
	}
	if s.driver != nil && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if err := s.attachmentProtectionChecks(ctx, evidence.VolumeID, evidence.RuntimeCtx, opennebula.HostArtifactAttachmentTarget{
			NodeName: evidence.NodeID, VMID: evidence.OpenNebulaNodeID, ImageID: evidence.OpenNebulaImageID,
		}); err != nil {
			return decision.with(VolumeReconcilePause, "detach_protection_blocked", status.Code(err), err.Error())
		}
		desiredState, desiredErr := s.driver.kubeRuntime.DesiredNodeForVolume(ctx, evidence.VolumeID)
		evidence.DesiredState = desiredState
		evidence.DesiredErr = desiredErr
		decision = decision.withEvidence(evidence)
		if desiredErr != nil {
			return decision.with(VolumeReconcilePause, "desired_state_unknown", codes.Unavailable, fmt.Sprintf("cannot verify volume release: %v", desiredErr))
		}
		if desiredState.desiresNode(evidence.NodeID) {
			return decision.with(VolumeReconcilePause, queueReasonSameNodeReuseRequired, codes.Unavailable,
				fmt.Sprintf("queued detach for volume %s on node %s is paused because Kubernetes still desires the volume there (%s)", evidence.VolumeID, evidence.NodeID, desiredState.Reason))
		}
	}
	orphanTeardown, _ := s.orphanTeardownEligible(ctx, evidence.VolumeID, evidence.NodeID, evidence.RuntimeCtx, evidence.DesiredState, evidence.DesiredErr)
	evidence.OrphanTeardown = orphanTeardown
	if orphanTeardown {
		evidence.History = s.refreshVolumeHistory(ctx, evidence.VolumeID)
	}
	protection, protectionErr := s.localRWOProtectionProjection(ctx, evidence.VolumeID, evidence.NodeID, evidence.RuntimeCtx, true, true)
	evidence.Protection = protection
	evidence.ProtectionErr = protectionErr
	decision = decision.withEvidence(evidence)
	if protectionErr == nil && protection.Protected && strings.TrimSpace(protection.RequiredNode) == strings.TrimSpace(evidence.NodeID) && !bypassProtectionForOrphanTeardown(protection, orphanTeardown) {
		return decision.with(VolumeReconcilePause, protection.Reason, codes.Unavailable,
			fmt.Sprintf("queued detach for volume %s on node %s is paused while local RWO protection is active (%s)", evidence.VolumeID, evidence.NodeID, protection.Reason))
	}
	if protectionErr != nil || protection.Invalid {
		return decision.with(VolumeReconcilePause, "detach_protection_unknown", codes.Unavailable, fmt.Sprintf("cannot verify detach protection: %v", protectionErr))
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
