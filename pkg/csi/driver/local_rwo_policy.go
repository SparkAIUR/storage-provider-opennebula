package driver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	repairClassificationMissingImageRecord         = "missing_image_record"
	repairClassificationHistoricalNodeTombstone    = "historical_node_tombstone"
	repairClassificationProviderLookupInconsistent = "provider_lookup_inconsistent"
	repairClassificationWrongDeviceIdentity        = "wrong_device_identity"
	repairClassificationRuntimeAttachUnconfirmed   = "same_node_runtime_attach_unconfirmed"

	queueReasonDesiredStateChanged    = "desired_state_changed"
	queueReasonVolumeParked           = "volume_parked"
	queueReasonNodeTombstoned         = "node_tombstoned"
	queueReasonSameNodeReuseRequired  = "same_node_reuse_required"
	queueReasonVolumeQuarantined      = "volume_quarantined"
	queueReasonRepairRequired         = "repair_required"
	queueReasonRecoveryControlInvalid = "recovery_control_invalid"
	queueReasonManualRecoveryAdopted  = "manual_recovery_adopted"

	protectionReasonMaintenance          = "maintenance_cross_node_blocked"
	protectionReasonAutomaticMaintenance = "automatic_maintenance_cross_node_blocked"
	protectionReasonManualRequiredNode   = "manual_required_node"
	protectionReasonSticky               = "same_node_reuse_required"
	protectionReasonLocalDevice          = "local_device_recovery_active"
	protectionReasonHistory              = "historical_ownership_active"

	protectionSourceExplicitRequiredNode  = "explicit_required_node"
	protectionSourceExplicitPreferredNode = "explicit_preferred_node"
	protectionSourceExplicitMaintenance   = "explicit_maintenance"
	protectionSourceAutomaticMaintenance  = "automatic_maintenance"
	protectionSourceStickyReuse           = "sticky_reuse"
	protectionSourceLocalDeviceRecovery   = "local_device_recovery"
	protectionSourceHistoricalOwnership   = "historical_ownership"

	protectionTriggerDrainInProgress            = "drain_in_progress"
	protectionTriggerNodeUnschedulable          = "node_unschedulable"
	protectionTriggerNodeNotReady               = "node_not_ready"
	protectionTriggerControllerInferredSameNode = "controller_inferred_same_node_protection"
)

type LocalRWOProtectionDecision struct {
	Protected                   bool
	RequiredNode                string
	PreferredNode               string
	Reason                      string
	Source                      string
	PlacementSource             string
	PlacementDecision           string
	PlacementReason             string
	AutomaticTrigger            string
	Message                     string
	RuntimeContext              *VolumeRuntimeContext
	History                     VolumeHistoryRecord
	OverrideUsed                bool
	OverrideExpiresAt           time.Time
	Invalid                     bool
	ExplicitRequiredNode        string
	ExplicitRequiredNodeUntil   *time.Time
	ExplicitRequiredNodeExpired bool
	ExplicitPreferredNode       string
	LegacyPreferredNode         string
	InferredRequiredNode        string
	InferredReason              string
	InferredSource              string
	Warnings                    []string
}

func localRWOProtectionDecisionForDriver(ctx context.Context, driver *Driver, volumeID, requestedNode string) (LocalRWOProtectionDecision, error) {
	if driver == nil {
		return LocalRWOProtectionDecision{}, nil
	}
	return (&ControllerServer{driver: driver}).localRWOProtectionDecision(ctx, volumeID, requestedNode)
}

func (s *ControllerServer) localRWOProtectionDecision(ctx context.Context, volumeID, requestedNode string) (LocalRWOProtectionDecision, error) {
	return s.localRWOProtectionProjection(ctx, volumeID, requestedNode, nil, true, false)
}

func (s *ControllerServer) localRWOProtectionProjection(ctx context.Context, volumeID, requestedNode string, runtimeCtx *VolumeRuntimeContext, persist, ignoreExpiredSticky bool) (LocalRWOProtectionDecision, error) {
	decision := LocalRWOProtectionDecision{}
	if s == nil || s.driver == nil {
		return decision, nil
	}

	if runtimeCtx == nil && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if resolved, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID); err == nil {
			runtimeCtx = resolved
		}
	}
	decision.RuntimeContext = runtimeCtx

	history := s.currentVolumeHistory(volumeID)
	if localRWOHistoryRuntimeContextDegraded(runtimeCtx) {
		history = s.refreshVolumeHistory(ctx, volumeID)
	}
	if !volumeHistoryHasObservedSuccess(history) {
		if persist {
			history = s.bootstrapVolumeHistoryEvidence(ctx, volumeID, runtimeCtx, history)
		} else if seed, ok := buildVolumeHistoryBootstrapEvidence(ctx, s.driver.kubeRuntime, volumeID, runtimeCtx); ok {
			mergeVolumeHistoryBootstrap(&history, seed)
			normalizeVolumeHistoryRecord(&history)
		}
	}
	decision.History = history

	explicitPlacement := explicitNodePlacementForRuntimeContext(runtimeCtx)
	decision.ExplicitRequiredNode = explicitPlacement.RequiredNode
	decision.ExplicitRequiredNodeUntil = explicitPlacement.RequiredNodeUntil
	decision.ExplicitRequiredNodeExpired = explicitPlacement.RequiredNodeExpired
	decision.ExplicitPreferredNode = explicitPlacement.ExplicitPreferredNode
	decision.LegacyPreferredNode = explicitPlacement.LegacyPreferredNode
	decision.PlacementReason = explicitPlacement.PlacementReason
	decision.Warnings = append(decision.Warnings, explicitPlacement.Warnings...)

	eligible := runtimeContextEligibleForMaintenance(runtimeCtx, volumeID) || strings.EqualFold(strings.TrimSpace(history.Backend), "local")
	if !eligible {
		if hasManualNodeTargetingAnnotations(runtimeCtx) {
			decision.PlacementDecision = placementDecisionIgnored
			decision.Message = fmt.Sprintf("manual node targeting annotations are ignored for non-local or shared volume %s", volumeID)
		}
		return decision, nil
	}

	inferredRequiredNode, inferredReason, inferredSource, automaticTrigger, err := s.inferredLocalRWORequiredNode(ctx, volumeID, runtimeCtx, history, ignoreExpiredSticky)
	if err != nil {
		return decision, err
	}
	decision.InferredRequiredNode = inferredRequiredNode
	decision.InferredReason = inferredReason
	decision.InferredSource = inferredSource
	decision.AutomaticTrigger = automaticTrigger

	if explicitPlacement.RequiredNodeParseError != nil {
		decision.Invalid = true
		decision.PlacementDecision = placementDecisionInvalid
		decision.Message = explicitPlacement.RequiredNodeParseError.Error()
		return decision, nil
	}
	if explicitPlacement.RequiredNodeExpired {
		decision.Warnings = append(decision.Warnings, fmt.Sprintf("explicit required-node %q expired at %s", explicitPlacement.RequiredNode, explicitPlacement.RequiredNodeUntil.Format(time.RFC3339)))
	}

	if explicitPlacement.RequiredNode != "" && !explicitPlacement.RequiredNodeExpired {
		if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
			decision.Invalid = true
			decision.PlacementDecision = placementDecisionInvalid
			decision.Message = "kubernetes runtime is not enabled for manual node targeting validation"
			return decision, nil
		}
		if err := s.driver.kubeRuntime.ValidateManualNodeTarget(ctx, runtimeCtx, explicitPlacement.RequiredNode, "required-node"); err != nil {
			decision.Invalid = true
			decision.PlacementDecision = placementDecisionInvalid
			decision.Message = err.Error()
			return decision, nil
		}
		if inferredRequiredNode != "" && inferredRequiredNode != explicitPlacement.RequiredNode {
			if deadline, ok := crossNodeOverrideDeadline(runtimeCtx); ok && deadline.After(time.Now().UTC()) {
				decision.OverrideUsed = true
				decision.OverrideExpiresAt = deadline
			} else {
				decision.Invalid = true
				decision.PlacementDecision = placementDecisionConflicting
				decision.Message = fmt.Sprintf("explicit required-node %s conflicts with protected node %s for local RWO volume %s; use %s to authorize a bounded cross-node move", explicitPlacement.RequiredNode, inferredRequiredNode, volumeID, annotationAllowCrossNodeUntil)
				return decision, nil
			}
		}
		decision.Protected = true
		decision.RequiredNode = explicitPlacement.RequiredNode
		decision.Reason = protectionReasonManualRequiredNode
		decision.Source = protectionSourceExplicitRequiredNode
		decision.PlacementSource = explicitPlacement.RequiredNodeSource
		decision.PlacementDecision = placementDecisionRequired
		if strings.TrimSpace(requestedNode) == "" || strings.TrimSpace(requestedNode) == explicitPlacement.RequiredNode {
			return decision, nil
		}
		decision.Message = localRWOProtectionMessage(volumeID, requestedNode, explicitPlacement.RequiredNode, decision)
		return decision, nil
	}

	if inferredRequiredNode != "" {
		if inferredSource == protectionSourceHistoricalOwnership && strings.TrimSpace(requestedNode) != "" && strings.TrimSpace(requestedNode) != strings.TrimSpace(inferredRequiredNode) && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
			validation := s.driver.kubeRuntime.inspectNodeSchedulingReadiness(ctx, inferredRequiredNode, "historical owner")
			if validation.SchedulerUnavailable() {
				decision.PlacementDecision = placementDecisionIgnored
				decision.Message = historicalOwnershipSchedulerWarning(validation)
				decision.Warnings = append(decision.Warnings, decision.Message)
				return decision, nil
			}
		}
		decision.Protected = true
		decision.RequiredNode = inferredRequiredNode
		decision.Reason = inferredReason
		decision.Source = inferredSource
		decision.PlacementSource = inferredSource
		decision.PlacementDecision = placementDecisionRequired
		if strings.TrimSpace(requestedNode) == "" || strings.TrimSpace(requestedNode) == inferredRequiredNode {
			return decision, nil
		}
		if deadline, ok := crossNodeOverrideDeadline(runtimeCtx); ok && deadline.After(time.Now().UTC()) {
			decision.OverrideUsed = true
			decision.OverrideExpiresAt = deadline
			decision.Message = fmt.Sprintf("allowing protected cross-node attach for local RWO volume %s from %s to %s until %s because override annotation %s is active", volumeID, inferredRequiredNode, requestedNode, deadline.Format(time.RFC3339), annotationAllowCrossNodeUntil)
			return decision, nil
		}
		decision.Message = localRWOProtectionMessage(volumeID, requestedNode, inferredRequiredNode, decision)
		return decision, nil
	}

	if explicitPlacement.PreferredNode != "" {
		decision.PlacementReason = firstNonEmpty(explicitPlacement.PlacementReason, decision.PlacementReason)
		if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
			decision.Warnings = append(decision.Warnings, "manual preferred-node was ignored because kubernetes runtime is not enabled")
		} else if validation := s.driver.kubeRuntime.inspectNodeCandidate(ctx, runtimeCtx, explicitPlacement.PreferredNode, "preferred-node"); !validation.Valid() {
			decision.Warnings = append(decision.Warnings, softPlacementWarning(explicitPlacement.PreferredNodeSource, validation))
		} else {
			decision.PreferredNode = explicitPlacement.PreferredNode
			decision.Source = protectionSourceExplicitPreferredNode
			decision.PlacementSource = explicitPlacement.PreferredNodeSource
			decision.PlacementDecision = placementDecisionPreferred
			return decision, nil
		}
	}

	if runtimeCtx != nil && strings.TrimSpace(runtimeCtx.PVAnnotations[annotationLastAttachedNode]) != "" {
		lastAttachedNode := strings.TrimSpace(runtimeCtx.PVAnnotations[annotationLastAttachedNode])
		if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
			decision.PreferredNode = lastAttachedNode
			decision.PlacementSource = placementSourceLastAttachedNode
			decision.PlacementDecision = placementDecisionPreferred
		} else if validation := s.driver.kubeRuntime.inspectNodeCandidate(ctx, runtimeCtx, lastAttachedNode, "last-attached-node"); !validation.Valid() {
			decision.Warnings = append(decision.Warnings, softPlacementWarning(placementSourceLastAttachedNode, validation))
		} else {
			decision.PreferredNode = lastAttachedNode
			decision.PlacementSource = placementSourceLastAttachedNode
			decision.PlacementDecision = placementDecisionPreferred
		}
	}
	if decision.PreferredNode == "" && explicitPlacement.LegacyPreferredNode != "" {
		if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
			decision.PreferredNode = explicitPlacement.LegacyPreferredNode
			decision.PlacementSource = placementSourceLegacyPreferredLastNode
			decision.PlacementDecision = placementDecisionPreferred
		} else if validation := s.driver.kubeRuntime.inspectNodeCandidate(ctx, runtimeCtx, explicitPlacement.LegacyPreferredNode, "preferred-last-node"); !validation.Valid() {
			decision.Warnings = append(decision.Warnings, softPlacementWarning(placementSourceLegacyPreferredLastNode, validation))
		} else {
			decision.PreferredNode = explicitPlacement.LegacyPreferredNode
			decision.PlacementSource = placementSourceLegacyPreferredLastNode
			decision.PlacementDecision = placementDecisionPreferred
		}
	}
	return decision, nil
}

func localRWOHistoryRuntimeContextDegraded(runtimeCtx *VolumeRuntimeContext) bool {
	if runtimeCtx == nil {
		return true
	}
	if strings.TrimSpace(runtimeCtx.PVName) == "" {
		return true
	}
	return false
}

func (s *ControllerServer) inferredLocalRWORequiredNode(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext, history VolumeHistoryRecord, ignoreExpiredSticky bool) (string, string, string, string, error) {
	requiredNode := ""
	reason := ""
	source := ""
	automaticTrigger := ""
	if state, ok := s.activeStickyAttachment(volumeID); ok && strings.TrimSpace(state.NodeID) != "" && (!ignoreExpiredSticky || state.ExpiresAt.IsZero() || state.ExpiresAt.After(time.Now().UTC())) {
		requiredNode = strings.TrimSpace(state.NodeID)
		reason = protectionReasonSticky
		source = protectionSourceStickyReuse
		if state.Reason == maintenanceStickyReason {
			reason = protectionReasonMaintenance
			source = protectionSourceExplicitMaintenance
		}
	}
	if requiredNode == "" {
		if report, ok, err := s.latestLocalDeviceReport(ctx, volumeID); err != nil {
			return "", "", "", "", err
		} else if ok && strings.TrimSpace(report.Node) != "" {
			requiredNode = strings.TrimSpace(report.Node)
			reason = protectionReasonLocalDevice
			source = protectionSourceLocalDeviceRecovery
		}
	}
	if requiredNode == "" && s.driver.maintenanceMode != nil && s.driver.maintenanceMode.Active() {
		requiredNode = maintenanceLastNodeForVolume(ctx, s.driver, runtimeCtx, volumeID)
		if requiredNode != "" {
			reason = protectionReasonMaintenance
			source = protectionSourceExplicitMaintenance
		}
	}
	if requiredNode == "" && historySupportsSameNodeProtection(history, runtimeCtx) {
		requiredNode = strings.TrimSpace(history.LastSuccessfulNodeName)
		reason = protectionReasonHistory
		source = protectionSourceHistoricalOwnership
	}
	if requiredNode == "" {
		return "", "", "", "", nil
	}
	if s.driver.featureGates.LocalRWOAutoProtection && reason == protectionReasonHistory && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if trigger, ok, err := s.driver.kubeRuntime.AutomaticMaintenanceTrigger(ctx, requiredNode); err == nil && ok {
			reason = protectionReasonAutomaticMaintenance
			source = protectionSourceAutomaticMaintenance
			automaticTrigger = trigger
		} else if err != nil {
			automaticTrigger = protectionTriggerControllerInferredSameNode
		}
	}
	return requiredNode, reason, source, automaticTrigger, nil
}

func localRWOProtectionMessage(volumeID, requestedNode, requiredNode string, decision LocalRWOProtectionDecision) string {
	if strings.TrimSpace(decision.Source) == protectionSourceExplicitRequiredNode {
		return fmt.Sprintf(
			"manual local RWO required-node is active for volume %s: refusing publish to %s while required node is %s (%s source=%s)",
			volumeID,
			requestedNode,
			requiredNode,
			firstNonEmpty(decision.Reason, protectionReasonManualRequiredNode),
			firstNonEmpty(decision.PlacementSource, decision.Source),
		)
	}
	if strings.TrimSpace(decision.Source) == protectionSourceAutomaticMaintenance {
		return fmt.Sprintf(
			"automatic local RWO protection is active for volume %s: refusing cross-node attach to %s while protected node is %s (%s trigger=%s)",
			volumeID,
			requestedNode,
			requiredNode,
			firstNonEmpty(decision.Reason, protectionReasonAutomaticMaintenance),
			firstNonEmpty(decision.AutomaticTrigger, protectionTriggerControllerInferredSameNode),
		)
	}
	return fmt.Sprintf(
		"local RWO protection is active for volume %s: refusing cross-node attach to %s while protected node is %s (%s source=%s)",
		volumeID,
		requestedNode,
		requiredNode,
		firstNonEmpty(decision.Reason, protectionReasonHistory),
		firstNonEmpty(decision.Source, protectionSourceHistoricalOwnership),
	)
}

func crossNodeOverrideDeadline(runtimeCtx *VolumeRuntimeContext) (time.Time, bool) {
	if runtimeCtx == nil || runtimeCtx.PVAnnotations == nil {
		return time.Time{}, false
	}
	raw := strings.TrimSpace(runtimeCtx.PVAnnotations[annotationAllowCrossNodeUntil])
	if raw == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func (s *ControllerServer) activeStickyAttachment(volumeID string) (StickyAttachmentState, bool) {
	if s == nil || s.driver == nil || s.driver.stickyAttachments == nil {
		return StickyAttachmentState{}, false
	}
	return s.driver.stickyAttachments.Get(volumeID)
}

func (s *ControllerServer) latestLocalDeviceReport(ctx context.Context, volumeID string) (LocalDeviceMissingReport, bool, error) {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		return LocalDeviceMissingReport{}, false, nil
	}
	cm, err := s.driver.kubeRuntime.GetConfigMap(ctx, namespaceFromServiceAccount(), localDeviceStateConfigMapName)
	if err != nil || len(cm.Data) == 0 {
		return LocalDeviceMissingReport{}, false, nil
	}
	var latest LocalDeviceMissingReport
	for key, raw := range cm.Data {
		unrelated, err := localDeviceReportRoutesElsewhere(key, raw, volumeID)
		if err != nil {
			return LocalDeviceMissingReport{}, false, attachmentEvidenceUnavailable("node device report is malformed", err)
		}
		if unrelated {
			if err := rejectLocalDeviceKeyCollision(ctx, s.driver.kubeRuntime, key, volumeID); err != nil {
				return LocalDeviceMissingReport{}, false, attachmentEvidenceUnavailable("node device report key is unverified", err)
			}
			continue
		}
		var report LocalDeviceMissingReport
		if err := unmarshalAuthorityJSON(raw, &report); err != nil {
			if errors.Is(err, errDuplicateAuthorityKey) {
				return LocalDeviceMissingReport{}, false, attachmentEvidenceUnavailable("node device report is malformed", err)
			}
			continue
		}
		if strings.TrimSpace(report.VolumeID) != strings.TrimSpace(volumeID) {
			continue
		}
		if latest.LastObservedAt.Before(report.LastObservedAt) {
			latest = report
		}
	}
	if strings.TrimSpace(latest.VolumeID) == "" {
		return LocalDeviceMissingReport{}, false, nil
	}
	return latest, true, nil
}

func (s *ControllerServer) rejectIfActiveRepairState(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext) error {
	if s == nil || s.driver == nil || s.driver.volumeRepairState == nil {
		return nil
	}
	state, ok, err := s.driver.volumeRepairState.GetCurrent(ctx, volumeID)
	if err != nil {
		return status.Errorf(codes.Unavailable, "cannot verify current volume repair state: %v", err)
	}
	if !ok {
		return nil
	}
	message := firstNonEmpty(strings.TrimSpace(state.Message), fmt.Sprintf("repair-required state %s is active for volume %s", state.Classification, volumeID))
	s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, eventReasonVolumeRepairRequired, message)
	s.driver.metrics.RecordVolumeRepairState(state.Classification, "active")
	return status.Error(codes.FailedPrecondition, message)
}

func (ns *NodeServer) verifyStageRepairObservation(expected *VolumeRepairState, identity *LocalDiskIdentity) error {
	if expected == nil {
		return nil
	}
	if firstNonEmpty(expected.RequestedNode, expected.LastKnownNodeName) != ns.Driver.nodeID {
		return status.Error(codes.FailedPrecondition, "repair authority requires verification on its affected node")
	}
	if expected.Classification == repairClassificationWrongDeviceIdentity {
		matches, _ := localDiskIdentityMatches(expected.LastHealthyIdentity, identity)
		if expected.LastHealthyIdentity == nil || identity == nil || !matches {
			return status.Error(codes.FailedPrecondition, "device does not resolve the observed wrong-identity repair")
		}
	}
	return nil
}

// The record was read before device verification. Never adopt a newer fault here.
func (ns *NodeServer) clearObservedStageRepair(ctx context.Context, volumeID string, expected *VolumeRepairState, identity *LocalDiskIdentity) error {
	if ns.Driver.volumeRepairState == nil {
		return nil
	}
	if expected == nil {
		if _, exists, err := ns.Driver.volumeRepairState.GetCurrent(ctx, volumeID); err != nil || exists {
			return status.Errorf(codes.Unavailable, "new repair authority appeared during staging; retry: %v", err)
		}
		return nil
	}
	if err := ns.verifyStageRepairObservation(expected, identity); err != nil {
		return err
	}
	if err := ns.Driver.volumeRepairState.ClearObserved(ctx, *expected); err != nil {
		return status.Errorf(codes.Unavailable, "cannot retire observed repair authority: %v", err)
	}
	if _, exists, err := ns.Driver.volumeRepairState.GetCurrent(ctx, expected.VolumeID); err != nil || exists {
		return status.Errorf(codes.Unavailable, "repair authority changed during staging; retry: %v", err)
	}
	ns.Driver.metrics.RecordVolumeRepairState(expected.Classification, "cleared")
	return nil
}

type successfulAttachmentObservation struct {
	nodeName, nodeUID, target, serial string
	vmID, imageID, diskID             int
}

func (state *VolumeHistoryRecord) observeSuccessfulAttachment(observation successfulAttachmentObservation) {
	nodeChanged := (observation.nodeName != "" && observation.nodeName != state.LastSuccessfulNodeName) ||
		(observation.nodeUID != "" && state.LastSuccessfulNodeUID != "" && observation.nodeUID != state.LastSuccessfulNodeUID)
	vmChanged := observation.vmID > 0 && state.LastSuccessfulOpenNebulaVMID > 0 && observation.vmID != state.LastSuccessfulOpenNebulaVMID
	imageChanged := observation.imageID > 0 && state.LastSuccessfulImageID > 0 && observation.imageID != state.LastSuccessfulImageID
	diskChanged := (observation.diskID >= 0 && state.LastSuccessfulDiskID > 0 && observation.diskID != state.LastSuccessfulDiskID) ||
		(observation.target != "" && state.LastSuccessfulTarget != "" && observation.target != state.LastSuccessfulTarget) ||
		(observation.serial != "" && state.LastSuccessfulDeviceSerial != "" && observation.serial != state.LastSuccessfulDeviceSerial)
	if nodeChanged || vmChanged {
		state.LastSuccessfulNodeUID = ""
	}
	if nodeChanged || vmChanged || imageChanged {
		state.LastSuccessfulOpenNebulaVMID = 0
	}
	if nodeChanged || vmChanged || imageChanged || diskChanged {
		state.LastSuccessfulDiskID = 0
		state.LastSuccessfulTarget = ""
		state.LastSuccessfulDeviceSerial = ""
		state.LastHealthyIdentity = nil
	}
	state.LastSuccessfulNodeName = firstNonEmpty(observation.nodeName, state.LastSuccessfulNodeName)
	state.LastSuccessfulNodeUID = firstNonEmpty(observation.nodeUID, state.LastSuccessfulNodeUID)
	if observation.vmID > 0 {
		state.LastSuccessfulOpenNebulaVMID = observation.vmID
	}
	if observation.imageID > 0 {
		state.LastSuccessfulImageID = observation.imageID
	}
	if observation.diskID >= 0 {
		state.LastSuccessfulDiskID = observation.diskID
	}
	state.LastSuccessfulTarget = firstNonEmpty(observation.target, state.LastSuccessfulTarget)
	state.LastSuccessfulDeviceSerial = firstNonEmpty(observation.serial, state.LastSuccessfulDeviceSerial)
}

func (s *ControllerServer) verifyPublishedAttachment(ctx context.Context, volumeID, requestedNode string, imageID, vmID int, target, adoptedSerial string) (successfulAttachmentObservation, error) {
	var metadata *opennebula.VolumeAttachmentMetadata
	if inspector, ok := s.volumeProvider.(opennebula.VolumeAttachmentInspector); ok {
		if inspected, err := inspector.InspectVolumeAttachment(ctx, volumeID, requestedNode); err == nil {
			metadata = inspected
		}
	}
	observation := successfulAttachmentObservation{
		nodeName: strings.TrimSpace(requestedNode), target: strings.TrimSpace(target),
		imageID: imageID, vmID: vmID, diskID: -1, serial: firstNonEmpty(adoptedSerial, fmt.Sprintf("onecsi-%d", imageID)),
	}
	if observation.target == "" || imageID <= 0 || vmID <= 0 {
		return observation, attachmentEvidenceUnavailable("publish attachment identity is incomplete", nil)
	}
	if metadata != nil {
		if metadata.ImageID != imageID || metadata.RequestedNodeID != vmID ||
			strings.TrimSpace(metadata.RequestedNode) != observation.nodeName ||
			(metadata.ImageName != "" && metadata.ImageName != volumeID) ||
			(metadata.VolumeHandle != "" && metadata.VolumeHandle != volumeID) {
			return observation, attachmentEvidenceUnavailable("publish attachment identity changed", nil)
		}
		if adoptedSerial != "" && len(metadata.ConflictingOwnerVMIDs()) != 0 {
			return observation, attachmentEvidenceUnavailable("publish adoption has conflicting attachment owners", nil)
		}
		for _, disk := range metadata.DiskRecords {
			if disk.NodeID != vmID {
				continue
			}
			matchingTarget := strings.TrimSpace(disk.Target) == observation.target
			if serial := strings.TrimSpace(disk.Serial); (matchingTarget || adoptedSerial != "") && serial != "" && !strings.EqualFold(serial, observation.serial) {
				return observation, attachmentEvidenceUnavailable("publish disk serial conflicts with the verified target", nil)
			}
			if !matchingTarget {
				continue
			}
			if observation.diskID >= 0 {
				return observation, attachmentEvidenceUnavailable("publish verified target has conflicting disk slots", nil)
			}
			observation.diskID = max(0, disk.DiskID)
		}
		if adoptedSerial == "" && (!metadata.AttachedToRequestedNode || observation.diskID < 0) {
			return observation, attachmentEvidenceUnavailable("publish attachment target changed", nil)
		}
	} else if adoptedSerial == "" {
		currentTarget, err := s.volumeProvider.GetVolumeInNode(ctx, imageID, vmID)
		if err != nil || strings.TrimSpace(currentTarget) != observation.target {
			return observation, attachmentEvidenceUnavailable("publish attachment target changed or is unverified", err)
		}
	}
	currentImage, _, err := s.volumeProvider.VolumeExists(ctx, volumeID)
	if err != nil || currentImage != imageID {
		return observation, attachmentEvidenceUnavailable("publish image identity changed or is unverified", err)
	}
	currentVM, err := s.volumeProvider.NodeExists(ctx, requestedNode)
	if err != nil || currentVM != vmID {
		return observation, attachmentEvidenceUnavailable("publish VM identity changed or is unverified", err)
	}
	if s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if identity, err := s.driver.kubeRuntime.NodeIdentity(ctx, requestedNode); err == nil {
			observation.nodeUID = identity.UID
		}
	}
	observation.diskID = max(0, observation.diskID)
	return observation, ctx.Err()
}

func (s *ControllerServer) recordSuccessfulLocalVolumePublish(ctx context.Context, volumeID string, observation successfulAttachmentObservation, sourceContext map[string]string, runtimeCtx *VolumeRuntimeContext, protection *LocalRWOProtectionDecision) {
	if s == nil || s.driver == nil || s.driver.volumeHistory == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	now := time.Now().UTC()
	_, err := s.driver.volumeHistory.Upsert(ctx, volumeID, func(state *VolumeHistoryRecord) {
		state.PVName = firstNonEmpty(state.PVName, runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVName }), strings.TrimSpace(sourceContext[paramPVName]))
		state.PVCNamespace = firstNonEmpty(state.PVCNamespace, runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVCNamespace }), strings.TrimSpace(sourceContext[paramPVCNamespace]))
		state.PVCName = firstNonEmpty(state.PVCName, runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVCName }), strings.TrimSpace(sourceContext[paramPVCName]))
		if backend, err := runtimeCtx.verifiedAttachmentBackend(); err == nil {
			state.Backend = backend
		}
		if runtimeCtx != nil {
			if runtimeCtx.DatastoreID > 0 {
				state.DatastoreID = runtimeCtx.DatastoreID
			} else if parsed := parseIntOrZero(volumeRuntimeAnnotation(runtimeCtx, annotationDatastoreID)); parsed > 0 && state.DatastoreID == 0 {
				state.DatastoreID = parsed
			}
			state.DatastoreName = firstNonEmpty(volumeRuntimeAnnotation(runtimeCtx, annotationDatastoreName), state.DatastoreName)
			state.RestartOptimization = firstNonEmpty(strings.TrimSpace(runtimeCtx.RestartMode), volumeRuntimeAnnotation(runtimeCtx, annotationRestartOpt), state.RestartOptimization)
		}
		state.EvidenceSource = volumeHistoryEvidenceSourceObserved
		state.Bootstrapped = false
		state.BootstrappedAt = time.Time{}
		state.BootstrappedFields = nil
		state.observeSuccessfulAttachment(observation)
		state.LastSuccessfulPublishTime = now
	})
	if err == nil {
		s.driver.metrics.RecordVolumeHistory("publish", "persisted")
		if protection != nil && protection.OverrideUsed && protection.RuntimeContext != nil {
			runtimeCtx := protection.RuntimeContext
			_ = s.driver.kubeRuntime.DeletePVAnnotation(ctx, runtimeCtx.PVName, annotationAllowCrossNodeUntil)
			s.recordPVCEventFromRuntimeContext(ctx, runtimeCtx, eventReasonCrossNodeOverrideUsed, protection.Message)
		}
	}
}

func (s *ControllerServer) recordSafeDetach(ctx context.Context, volumeID, nodeID string) {
	if s == nil || s.driver == nil || s.driver.volumeHistory == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	_, err := s.driver.volumeHistory.Upsert(ctx, volumeID, func(state *VolumeHistoryRecord) {
		state.LastSafeDetachNodeName = strings.TrimSpace(nodeID)
		state.LastSafeDetachTime = time.Now().UTC()
	})
	if err == nil {
		s.driver.metrics.RecordVolumeHistory("safe_detach", "persisted")
	}
}

func (ns *NodeServer) recordSuccessfulLocalVolumeStage(ctx context.Context, volumeID string, publishContext map[string]string, identity *LocalDiskIdentity) {
	if ns == nil || ns.Driver == nil || ns.Driver.volumeHistory == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	normalizeLocalDiskIdentity(identity)
	observation := successfulAttachmentObservation{nodeName: strings.TrimSpace(ns.Driver.nodeID), diskID: -1}
	if observation.nodeName != "" && ns.Driver.kubeRuntime != nil && ns.Driver.kubeRuntime.enabled {
		if node, err := ns.Driver.kubeRuntime.NodeIdentity(ctx, observation.nodeName); err == nil && node.Exists {
			observation.nodeUID = strings.TrimSpace(node.UID)
		}
	}
	if identity != nil {
		observation.serial = firstNonEmpty(localDiskObservedDeviceSerial(identity), localDiskAssertedDeviceSerial(identity))
		observation.target = localDiskAssertedDiskTarget(identity)
	}
	_, err := ns.Driver.volumeHistory.Upsert(ctx, volumeID, func(state *VolumeHistoryRecord) {
		state.PVName = firstNonEmpty(state.PVName, strings.TrimSpace(publishContext[paramPVName]))
		state.PVCNamespace = firstNonEmpty(state.PVCNamespace, strings.TrimSpace(publishContext[paramPVCNamespace]))
		state.PVCName = firstNonEmpty(state.PVCName, strings.TrimSpace(publishContext[paramPVCName]))
		if backend, err := verifiedAttachmentBackend("", publishContext[annotationBackend]); state.Backend == "" && err == nil {
			state.Backend = backend
		}
		state.EvidenceSource = volumeHistoryEvidenceSourceObserved
		state.Bootstrapped = false
		state.BootstrappedAt = time.Time{}
		state.BootstrappedFields = nil
		state.observeSuccessfulAttachment(observation)
		state.LastSuccessfulStageTime = time.Now().UTC()
		if identity != nil {
			state.LastHealthyIdentity = identity
			if imageID := parseIntOrZero(localDiskAssertedImageID(identity)); state.LastSuccessfulImageID == 0 && imageID > 0 {
				state.LastSuccessfulImageID = imageID
			}
		}
	})
	if err == nil {
		ns.Driver.metrics.RecordVolumeHistory("stage", "persisted")
	}
}

func (ns *NodeServer) recordWrongDeviceIdentityRepairState(ctx context.Context, session localDiskSession, observed *LocalDiskIdentity) {
	if ns == nil || ns.Driver == nil || ns.Driver.volumeRepairState == nil || strings.TrimSpace(session.VolumeID) == "" {
		return
	}
	expected := session.Identity
	normalizeLocalDiskIdentity(expected)
	normalizeLocalDiskIdentity(observed)
	state := VolumeRepairState{
		Version:               stateObjectVersion,
		VolumeID:              session.VolumeID,
		Classification:        repairClassificationWrongDeviceIdentity,
		Message:               wrongDeviceIdentityMessage(session.VolumeID, expected, observed),
		RequestedNode:         strings.TrimSpace(ns.Driver.nodeID),
		LastKnownNodeName:     strings.TrimSpace(ns.Driver.nodeID),
		LastKnownImageID:      parseIntOrZero(localDiskAssertedImageID(expected)),
		LastKnownTarget:       localDiskAssertedDiskTarget(expected),
		LastKnownDeviceSerial: firstNonEmpty(localDiskObservedDeviceSerial(expected), localDiskAssertedDeviceSerial(expected)),
		EvidenceSource:        "node_stage_identity_check",
		LastHealthyIdentity:   expected,
		LastObservedIdentity:  observed,
	}
	if persisted, err := ns.Driver.volumeRepairState.Mark(ctx, state); err == nil {
		ns.Driver.metrics.RecordVolumeRepairState(persisted.Classification, "observed")
	}
}

func wrongDeviceIdentityMessage(volumeID string, expected, observed *LocalDiskIdentity) string {
	return fmt.Sprintf(
		"wrong device identity for local RWO volume %s: expected asserted(serial=%s imageID=%s target=%s) observed(serial=%s byID=%s fsUUID=%s partUUID=%s); observed asserted(serial=%s imageID=%s target=%s) observed(serial=%s byID=%s fsUUID=%s partUUID=%s)",
		volumeID,
		identityField(expected, localDiskAssertedDeviceSerial),
		identityField(expected, localDiskAssertedImageID),
		identityField(expected, localDiskAssertedDiskTarget),
		identityField(expected, localDiskObservedDeviceSerial),
		identityField(expected, localDiskObservedByIDPath),
		identityField(expected, localDiskObservedFilesystemUUID),
		identityField(expected, localDiskObservedPartitionUUID),
		identityField(observed, localDiskAssertedDeviceSerial),
		identityField(observed, localDiskAssertedImageID),
		identityField(observed, localDiskAssertedDiskTarget),
		identityField(observed, localDiskObservedDeviceSerial),
		identityField(observed, localDiskObservedByIDPath),
		identityField(observed, localDiskObservedFilesystemUUID),
		identityField(observed, localDiskObservedPartitionUUID),
	)
}

func runtimeCtxField(runtimeCtx *VolumeRuntimeContext, getter func(*VolumeRuntimeContext) string) string {
	if runtimeCtx == nil || getter == nil {
		return ""
	}
	return strings.TrimSpace(getter(runtimeCtx))
}

func requestedNodeDiskEvidence(metadata *opennebula.VolumeAttachmentMetadata, requestedNode string) (int, string, string) {
	if metadata == nil {
		return 0, "", ""
	}
	requestedNode = strings.TrimSpace(requestedNode)
	for _, record := range metadata.DiskRecords {
		if strings.TrimSpace(record.NodeName) != requestedNode && metadata.RequestedNodeID > 0 && record.NodeID != metadata.RequestedNodeID {
			continue
		}
		return record.DiskID, strings.TrimSpace(record.Target), strings.TrimSpace(record.Serial)
	}
	if len(metadata.DiskRecords) == 1 {
		record := metadata.DiskRecords[0]
		return record.DiskID, strings.TrimSpace(record.Target), strings.TrimSpace(record.Serial)
	}
	return 0, "", ""
}

func (s *ControllerServer) classifyRepairRequiredLookup(ctx context.Context, volumeID, requestedNode string, runtimeCtx *VolumeRuntimeContext, history VolumeHistoryRecord, result *opennebula.VolumeLookupResult) error {
	if s == nil || s.driver == nil || s.driver.volumeRepairState == nil || result == nil {
		return nil
	}
	classification := ""
	message := ""
	evidenceSource := "provider_lookup"
	lastKnownNode := strings.TrimSpace(history.LastSuccessfulNodeName)
	lastKnownNodeUID := strings.TrimSpace(history.LastSuccessfulNodeUID)
	lastKnownVMID := history.LastSuccessfulOpenNebulaVMID
	lastKnownImageID := history.LastSuccessfulImageID
	lastKnownDiskID := history.LastSuccessfulDiskID
	lastKnownTarget := strings.TrimSpace(history.LastSuccessfulTarget)
	lastKnownSerial := strings.TrimSpace(history.LastSuccessfulDeviceSerial)
	if history.Backend == "" && runtimeCtx != nil {
		history.Backend = runtimeCtx.Backend
	}
	if !volumeHistoryHasLocalHistoricalEvidence(history, runtimeCtx) {
		return nil
	}
	historySource := volumeHistoryEvidenceSource(history)
	if historySource != "" {
		evidenceSource = "provider_lookup+" + historySource
	}

	if lastKnownNode != "" && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if identity, err := s.driver.kubeRuntime.NodeIdentity(ctx, lastKnownNode); err == nil && (!identity.Exists || (lastKnownNodeUID != "" && identity.UID != "" && identity.UID != lastKnownNodeUID)) {
			classification = repairClassificationHistoricalNodeTombstone
			evidenceSource = firstNonEmpty(historySource, "volume_history") + "_node_identity"
			message = fmt.Sprintf("historical node tombstone for local RWO volume %s: last known node=%s uid=%s currentUID=%s lastKnownVMID=%d imageID=%d diskID=%d target=%s evidenceSource=%s bootstrapped=%t", volumeID, lastKnownNode, lastKnownNodeUID, identity.UID, lastKnownVMID, lastKnownImageID, lastKnownDiskID, lastKnownTarget, firstNonEmpty(historySource, "volume_history"), history.Bootstrapped)
		}
	}
	if classification == "" {
		switch result.Status {
		case opennebula.VolumeLookupNotFound, opennebula.VolumeLookupImageRecordMissing:
			classification = repairClassificationMissingImageRecord
			message = fmt.Sprintf("missing image record for local RWO volume %s: lastKnownImageID=%d lastKnownNode=%s lastKnownNodeUID=%s lastKnownVMID=%d lastKnownDiskID=%d lastKnownTarget=%s requestedNode=%s evidenceSource=%s bootstrapped=%t", volumeID, lastKnownImageID, lastKnownNode, lastKnownNodeUID, lastKnownVMID, lastKnownDiskID, lastKnownTarget, requestedNode, firstNonEmpty(historySource, "volume_history"), history.Bootstrapped)
		case opennebula.VolumeLookupProviderInconsistent:
			classification = repairClassificationProviderLookupInconsistent
			message = fmt.Sprintf("provider lookup inconsistent for local RWO volume %s: %s evidenceSource=%s bootstrapped=%t", volumeID, firstNonEmpty(result.Message, "OpenNebula lookup resolved an inconsistent image state"), firstNonEmpty(historySource, "volume_history"), history.Bootstrapped)
		default:
			return nil
		}
	}

	state := VolumeRepairState{
		Version:                 stateObjectVersion,
		VolumeID:                volumeID,
		Classification:          classification,
		Reason:                  queueReasonRepairRequired,
		Message:                 message,
		RequestedNode:           strings.TrimSpace(requestedNode),
		LastKnownNodeName:       lastKnownNode,
		LastKnownNodeUID:        lastKnownNodeUID,
		LastKnownOpenNebulaVMID: lastKnownVMID,
		LastKnownImageID:        lastKnownImageID,
		LastKnownDiskID:         lastKnownDiskID,
		LastKnownTarget:         lastKnownTarget,
		LastKnownDeviceSerial:   lastKnownSerial,
		EvidenceSource:          evidenceSource,
		LastHealthyIdentity:     history.LastHealthyIdentity,
	}
	persisted, err := s.driver.volumeRepairState.Mark(ctx, state)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%s", message)
	}
	s.driver.metrics.RecordVolumeRepairState(persisted.Classification, "observed")
	s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, eventReasonVolumeRepairRequired, message)
	return status.Error(codes.FailedPrecondition, message)
}

func (s *ControllerServer) lookupVolumeForPublish(ctx context.Context, volumeID, requestedNode string) (*opennebula.VolumeLookupResult, VolumeHistoryRecord, *VolumeRuntimeContext, error) {
	result, history, runtimeCtx, err := s.lookupVolumeForAttachment(ctx, volumeID, requestedNode)
	if err == nil && result.ImageID == 0 {
		return result, history, runtimeCtx, status.Error(codes.Unavailable, "volume identity is unknown")
	}
	return result, history, runtimeCtx, err
}

func (s *ControllerServer) lookupVolumeForAttachment(ctx context.Context, volumeID, requestedNode string) (*opennebula.VolumeLookupResult, VolumeHistoryRecord, *VolumeRuntimeContext, error) {
	history := s.currentVolumeHistory(volumeID)
	var runtimeCtx *VolumeRuntimeContext
	if s != nil && s.driver != nil && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if resolved, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID); err == nil {
			runtimeCtx = resolved
		}
	}
	if inspector, ok := s.volumeProvider.(opennebula.VolumeLookupInspector); ok {
		result, err := inspector.InspectVolumeLookup(ctx, volumeID, requestedNode)
		if err != nil {
			if errors.Is(err, opennebula.ErrVolumeNotFound) {
				result = &opennebula.VolumeLookupResult{Status: opennebula.VolumeLookupNotFound, VolumeHandle: volumeID, RequestedNode: requestedNode}
			} else if status.Code(err) == codes.FailedPrecondition {
				return nil, history, runtimeCtx, err
			} else {
				return nil, history, runtimeCtx, status.Errorf(codes.Unavailable, "volume lookup is unknown: %v", err)
			}
		}
		if result == nil {
			return nil, history, runtimeCtx, status.Error(codes.Unavailable, "volume lookup returned no observation")
		}
		if result.Status != opennebula.VolumeLookupPresent {
			history = s.refreshVolumeHistory(ctx, volumeID)
			history = s.bootstrapVolumeHistoryEvidence(ctx, volumeID, runtimeCtx, history)
		}
		switch result.Status {
		case opennebula.VolumeLookupPresent:
			if result.ImageID < 0 {
				return result, history, runtimeCtx, status.Error(codes.Unavailable, "volume identity is unknown")
			}
			return result, history, runtimeCtx, nil
		case opennebula.VolumeLookupNotFound:
			if !volumeHistoryHasLocalHistoricalEvidence(history, runtimeCtx) {
				return result, history, runtimeCtx, fmt.Errorf("%w: %w", opennebula.ErrVolumeNotFound, status.Error(codes.NotFound, "volume not found"))
			}
			repairResult := *result
			repairResult.Status = opennebula.VolumeLookupImageRecordMissing
			repairResult.Message = firstNonEmpty(repairResult.Message, "provider lookup returned not found for a historically local RWO volume")
			return &repairResult, history, runtimeCtx, s.classifyRepairRequiredLookup(ctx, volumeID, requestedNode, runtimeCtx, history, &repairResult)
		case opennebula.VolumeLookupImageRecordMissing, opennebula.VolumeLookupProviderInconsistent:
			if volumeHistoryHasLocalHistoricalEvidence(history, runtimeCtx) {
				return result, history, runtimeCtx, s.classifyRepairRequiredLookup(ctx, volumeID, requestedNode, runtimeCtx, history, result)
			}
			return result, history, runtimeCtx, status.Error(codes.FailedPrecondition, firstNonEmpty(result.Message, "provider volume observations are inconsistent"))
		}
		return result, history, runtimeCtx, status.Error(codes.Unavailable, "volume lookup state is unknown")
	}

	imageID, sizeBytes, err := s.volumeProvider.VolumeExists(ctx, volumeID)
	if err != nil && !errors.Is(err, opennebula.ErrVolumeNotFound) && !opennebula.IsAuthoritativeVolumeAbsence(err) {
		return nil, history, runtimeCtx, status.Errorf(codes.Unavailable, "volume lookup is unknown: %v", err)
	}
	if err != nil || imageID == -1 {
		history = s.refreshVolumeHistory(ctx, volumeID)
		history = s.bootstrapVolumeHistoryEvidence(ctx, volumeID, runtimeCtx, history)
		if !volumeHistoryHasLocalHistoricalEvidence(history, runtimeCtx) {
			return nil, history, runtimeCtx, fmt.Errorf("%w: %w", opennebula.ErrVolumeNotFound, status.Error(codes.NotFound, "volume not found"))
		}
		result := &opennebula.VolumeLookupResult{
			Status:        opennebula.VolumeLookupImageRecordMissing,
			VolumeHandle:  volumeID,
			RequestedNode: requestedNode,
		}
		return result, history, runtimeCtx, s.classifyRepairRequiredLookup(ctx, volumeID, requestedNode, runtimeCtx, history, result)
	}
	if imageID < 0 {
		return nil, history, runtimeCtx, status.Error(codes.Unavailable, "volume identity is unknown")
	}
	return &opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  volumeID,
		RequestedNode: requestedNode,
		ImageID:       imageID,
		SizeBytes:     int64(sizeBytes),
	}, history, runtimeCtx, nil
}

func (s *ControllerServer) bootstrapVolumeHistoryEvidence(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext, history VolumeHistoryRecord) VolumeHistoryRecord {
	if s == nil || s.driver == nil || s.driver.volumeHistory == nil || volumeHistoryHasObservedSuccess(history) {
		return history
	}
	record, seeded, err := s.driver.volumeHistory.SeedFromRuntimeContext(ctx, volumeID, runtimeCtx)
	if err != nil {
		klog.V(3).InfoS("Failed to bootstrap volume history evidence", "volumeID", volumeID, "err", err)
		return history
	}
	if seeded {
		s.driver.metrics.RecordVolumeHistory("bootstrap", "persisted")
		return record
	}
	if strings.TrimSpace(record.VolumeID) != "" {
		return record
	}
	return history
}

func (s *ControllerServer) currentVolumeHistory(volumeID string) VolumeHistoryRecord {
	if s == nil || s.driver == nil || s.driver.volumeHistory == nil {
		return VolumeHistoryRecord{}
	}
	if record, ok := s.driver.volumeHistory.Get(volumeID); ok {
		return record
	}
	return VolumeHistoryRecord{}
}

func (s *ControllerServer) refreshVolumeHistory(ctx context.Context, volumeID string) VolumeHistoryRecord {
	if s == nil || s.driver == nil || s.driver.volumeHistory == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeHistoryRecord{}
	}
	if err := s.driver.volumeHistory.RefreshEntry(ctx, volumeID); err != nil {
		klog.V(3).InfoS("Failed to refresh volume history state", "volumeID", volumeID, "err", err)
	}
	if record, ok := s.driver.volumeHistory.Get(volumeID); ok {
		return record
	}
	return VolumeHistoryRecord{}
}

func parseIntOrZero(value string) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return parsed
}

func identityField(identity *LocalDiskIdentity, getter func(*LocalDiskIdentity) string) string {
	if identity == nil || getter == nil {
		return "unknown"
	}
	value := strings.TrimSpace(getter(identity))
	if value == "" {
		return "unknown"
	}
	return value
}

func (s *ControllerServer) repairStateForQueue(ctx context.Context, volumeID string) (VolumeRepairState, bool, error) {
	if s == nil || s.driver == nil || s.driver.volumeRepairState == nil {
		return VolumeRepairState{}, false, nil
	}
	return s.driver.volumeRepairState.GetCurrent(ctx, volumeID)
}

func (s *ControllerServer) recordActiveRepairStateEvent(ctx context.Context, runtimeCtx *VolumeRuntimeContext, state VolumeRepairState) {
	if runtimeCtx == nil {
		return
	}
	s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, eventReasonVolumeRepairRequired, firstNonEmpty(state.Message, fmt.Sprintf("repair-required state %s is active", state.Classification)))
}

func (s *ControllerServer) activeVolumeQuarantine(ctx context.Context, volumeID string) (VolumeQuarantineState, bool, error) {
	if s == nil || s.driver == nil || s.driver.volumeQuarantine == nil {
		return VolumeQuarantineState{}, false, nil
	}
	if err := s.driver.volumeQuarantine.RefreshEntry(ctx, volumeID); err != nil {
		if errors.Is(err, errDuplicateAuthorityKey) {
			return VolumeQuarantineState{}, false, attachmentEvidenceUnavailable("volume quarantine is malformed", err)
		}
		klog.V(3).InfoS("Failed to refresh volume quarantine state", "volumeID", volumeID, "err", err)
	}
	state, active := s.driver.volumeQuarantine.GetActive(volumeID, time.Now().UTC())
	return state, active, nil
}

func (s *ControllerServer) activeHostArtifactQuarantine(ctx context.Context, volumeID string) (bool, error) {
	if s == nil || s.driver == nil || s.driver.hostArtifactQuarantine == nil {
		return false, nil
	}
	if err := s.driver.hostArtifactQuarantine.RefreshVolume(ctx, volumeID); err != nil {
		return false, attachmentEvidenceUnavailable("host artifact quarantine is unverified", err)
	}
	for _, state := range s.driver.hostArtifactQuarantine.Snapshot() {
		if strings.TrimSpace(state.VolumeID) == strings.TrimSpace(volumeID) && (state.ExpiresAt.IsZero() || state.ExpiresAt.After(time.Now().UTC())) {
			return true, nil
		}
	}
	return false, nil
}

func (s *ControllerServer) recordCrossNodeOverrideUsed(ctx context.Context, runtimeCtx *VolumeRuntimeContext, message string) {
	if runtimeCtx == nil {
		return
	}
	s.recordPVCEventFromRuntimeContext(ctx, runtimeCtx, eventReasonCrossNodeOverrideUsed, message)
}
