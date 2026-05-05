package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	repairClassificationMissingImageRecord         = "missing_image_record"
	repairClassificationHistoricalNodeTombstone    = "historical_node_tombstone"
	repairClassificationProviderLookupInconsistent = "provider_lookup_inconsistent"
	repairClassificationWrongDeviceIdentity        = "wrong_device_identity"

	queueReasonDesiredStateChanged   = "desired_state_changed"
	queueReasonVolumeParked          = "volume_parked"
	queueReasonNodeTombstoned        = "node_tombstoned"
	queueReasonSameNodeReuseRequired = "same_node_reuse_required"
	queueReasonVolumeQuarantined     = "volume_quarantined"
	queueReasonRepairRequired        = "repair_required"

	protectionReasonMaintenance = "maintenance_cross_node_blocked"
	protectionReasonSticky      = "same_node_reuse_required"
	protectionReasonLocalDevice = "local_device_recovery_active"
	protectionReasonHistory     = "historical_ownership_active"
)

type LocalRWOProtectionDecision struct {
	Protected         bool
	RequiredNode      string
	Reason            string
	Message           string
	RuntimeContext    *VolumeRuntimeContext
	History           VolumeHistoryRecord
	OverrideUsed      bool
	OverrideExpiresAt time.Time
}

func localRWOProtectionDecisionForDriver(ctx context.Context, driver *Driver, volumeID, requestedNode string) (LocalRWOProtectionDecision, error) {
	if driver == nil {
		return LocalRWOProtectionDecision{}, nil
	}
	return (&ControllerServer{driver: driver}).localRWOProtectionDecision(ctx, volumeID, requestedNode)
}

func (s *ControllerServer) localRWOProtectionDecision(ctx context.Context, volumeID, requestedNode string) (LocalRWOProtectionDecision, error) {
	decision := LocalRWOProtectionDecision{}
	if s == nil || s.driver == nil {
		return decision, nil
	}

	var runtimeCtx *VolumeRuntimeContext
	if s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if resolved, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID); err == nil {
			runtimeCtx = resolved
		}
	}
	decision.RuntimeContext = runtimeCtx

	var history VolumeHistoryRecord
	if s.driver.volumeHistory != nil {
		if current, ok := s.driver.volumeHistory.Get(volumeID); ok {
			history = current
		}
	}
	decision.History = history

	eligible := runtimeContextEligibleForMaintenance(runtimeCtx, volumeID) || strings.EqualFold(strings.TrimSpace(history.Backend), "local")
	if !eligible {
		return decision, nil
	}

	requiredNode := ""
	reason := ""
	if state, ok := s.activeStickyAttachment(volumeID); ok && strings.TrimSpace(state.NodeID) != "" {
		requiredNode = strings.TrimSpace(state.NodeID)
		reason = protectionReasonSticky
		if state.Reason == maintenanceStickyReason {
			reason = protectionReasonMaintenance
		}
	}
	if requiredNode == "" {
		if report, ok := s.latestLocalDeviceReport(ctx, volumeID); ok && strings.TrimSpace(report.Node) != "" {
			requiredNode = strings.TrimSpace(report.Node)
			reason = protectionReasonLocalDevice
		}
	}
	if requiredNode == "" && s.driver.maintenanceMode != nil && s.driver.maintenanceMode.Active() {
		requiredNode = maintenanceLastNodeForVolume(s.driver, runtimeCtx, volumeID)
		if requiredNode != "" {
			reason = protectionReasonMaintenance
		}
	}
	if requiredNode == "" && historyRequiresSafeRelease(history) {
		requiredNode = strings.TrimSpace(history.LastSuccessfulNodeName)
		reason = protectionReasonHistory
	}
	if requiredNode == "" {
		return decision, nil
	}
	if s.driver.featureGates.LocalRWOAutoProtection && reason == protectionReasonHistory && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		identity, err := s.driver.kubeRuntime.NodeIdentity(ctx, requiredNode)
		if err == nil && identity.Exists && (identity.Unschedulable || !identity.Ready) {
			reason = protectionReasonMaintenance
		}
	}

	decision.Protected = true
	decision.RequiredNode = requiredNode
	decision.Reason = reason
	if strings.TrimSpace(requestedNode) == "" || strings.TrimSpace(requestedNode) == requiredNode {
		return decision, nil
	}
	if deadline, ok := crossNodeOverrideDeadline(runtimeCtx); ok && deadline.After(time.Now().UTC()) {
		decision.OverrideUsed = true
		decision.OverrideExpiresAt = deadline
		decision.Message = fmt.Sprintf("allowing protected cross-node attach for local RWO volume %s from %s to %s until %s because override annotation %s is active", volumeID, requiredNode, requestedNode, deadline.Format(time.RFC3339), annotationAllowCrossNodeUntil)
		return decision, nil
	}
	decision.Message = fmt.Sprintf("local RWO protection is active for volume %s: refusing cross-node attach to %s while protected node is %s (%s)", volumeID, requestedNode, requiredNode, reason)
	return decision, nil
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

func (s *ControllerServer) latestLocalDeviceReport(ctx context.Context, volumeID string) (LocalDeviceMissingReport, bool) {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		return LocalDeviceMissingReport{}, false
	}
	cm, err := s.driver.kubeRuntime.GetConfigMap(ctx, namespaceFromServiceAccount(), localDeviceStateConfigMapName)
	if err != nil || len(cm.Data) == 0 {
		return LocalDeviceMissingReport{}, false
	}
	var latest LocalDeviceMissingReport
	for _, raw := range cm.Data {
		var report LocalDeviceMissingReport
		if err := json.Unmarshal([]byte(raw), &report); err != nil {
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
		return LocalDeviceMissingReport{}, false
	}
	return latest, true
}

func (s *ControllerServer) rejectIfActiveRepairState(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext) error {
	if s == nil || s.driver == nil || s.driver.volumeRepairState == nil {
		return nil
	}
	state, ok := s.driver.volumeRepairState.Get(volumeID)
	if !ok {
		return nil
	}
	message := firstNonEmpty(strings.TrimSpace(state.Message), fmt.Sprintf("repair-required state %s is active for volume %s", state.Classification, volumeID))
	s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, eventReasonVolumeRepairRequired, message)
	s.driver.metrics.RecordVolumeRepairState(state.Classification, "active")
	return status.Error(codes.FailedPrecondition, message)
}

func (s *ControllerServer) clearRepairStateOnSuccess(ctx context.Context, volumeID string) {
	if s == nil || s.driver == nil || s.driver.volumeRepairState == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	if state, ok := s.driver.volumeRepairState.Get(volumeID); ok {
		if err := s.driver.volumeRepairState.Clear(ctx, volumeID); err == nil {
			s.driver.metrics.RecordVolumeRepairState(state.Classification, "cleared")
		}
	}
}

func (ns *NodeServer) clearRepairStateOnSuccess(ctx context.Context, volumeID string) {
	if ns == nil || ns.Driver == nil || ns.Driver.volumeRepairState == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	if state, ok := ns.Driver.volumeRepairState.Get(volumeID); ok {
		if err := ns.Driver.volumeRepairState.Clear(ctx, volumeID); err == nil {
			ns.Driver.metrics.RecordVolumeRepairState(state.Classification, "cleared")
		}
	}
}

func (s *ControllerServer) recordSuccessfulLocalVolumePublish(ctx context.Context, volumeID, requestedNode, target string, sourceContext map[string]string, protection *LocalRWOProtectionDecision, metadata *opennebula.VolumeAttachmentMetadata) {
	if s == nil || s.driver == nil || s.driver.volumeHistory == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	if metadata == nil {
		if inspector, ok := s.volumeProvider.(opennebula.VolumeAttachmentInspector); ok {
			if inspected, err := inspector.InspectVolumeAttachment(ctx, volumeID, requestedNode); err == nil {
				metadata = inspected
			}
		}
	}
	runtimeCtx := volumeRuntimeContextFromSource(sourceContext)
	if runtimeCtx == nil && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if resolved, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID); err == nil {
			runtimeCtx = resolved
		}
	}
	nodeUID := ""
	if s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if identity, err := s.driver.kubeRuntime.NodeIdentity(ctx, requestedNode); err == nil {
			nodeUID = identity.UID
		}
	}
	imageID, deviceSerial := imageAndSerialFromPublishContext(sourceContext)
	diskID, diskTarget, serial := requestedNodeDiskEvidence(metadata, requestedNode)
	if diskTarget == "" {
		diskTarget = strings.TrimSpace(target)
	}
	if serial != "" {
		deviceSerial = serial
	}
	if imageID == 0 && metadata != nil {
		imageID = metadata.ImageID
	}
	now := time.Now().UTC()
	record, err := s.driver.volumeHistory.Upsert(ctx, volumeID, func(state *VolumeHistoryRecord) {
		state.PVName = firstNonEmpty(state.PVName, runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVName }), strings.TrimSpace(sourceContext[paramPVName]))
		state.PVCNamespace = firstNonEmpty(state.PVCNamespace, runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVCNamespace }), strings.TrimSpace(sourceContext[paramPVCNamespace]))
		state.PVCName = firstNonEmpty(state.PVCName, runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVCName }), strings.TrimSpace(sourceContext[paramPVCName]))
		state.Backend = firstNonEmpty(runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.Backend }), state.Backend, "local")
		state.LastSuccessfulNodeName = strings.TrimSpace(requestedNode)
		state.LastSuccessfulNodeUID = nodeUID
		state.LastSuccessfulPublishTime = now
		if metadata != nil && metadata.RequestedNodeID > 0 {
			state.LastSuccessfulOpenNebulaVMID = metadata.RequestedNodeID
		}
		if imageID > 0 {
			state.LastSuccessfulImageID = imageID
		}
		if diskID > 0 {
			state.LastSuccessfulDiskID = diskID
		}
		if diskTarget != "" {
			state.LastSuccessfulTarget = diskTarget
		}
		if deviceSerial != "" {
			state.LastSuccessfulDeviceSerial = deviceSerial
		}
	})
	if err == nil {
		s.driver.metrics.RecordVolumeHistory("publish", "persisted")
		if protection != nil && protection.OverrideUsed && runtimeCtx != nil {
			_ = s.driver.kubeRuntime.DeletePVAnnotation(ctx, runtimeCtx.PVName, annotationAllowCrossNodeUntil)
			s.recordPVCEventFromRuntimeContext(ctx, runtimeCtx, eventReasonCrossNodeOverrideUsed, protection.Message)
		}
		_ = record
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
	_, err := ns.Driver.volumeHistory.Upsert(ctx, volumeID, func(state *VolumeHistoryRecord) {
		state.PVName = firstNonEmpty(state.PVName, strings.TrimSpace(publishContext[paramPVName]))
		state.PVCNamespace = firstNonEmpty(state.PVCNamespace, strings.TrimSpace(publishContext[paramPVCNamespace]))
		state.PVCName = firstNonEmpty(state.PVCName, strings.TrimSpace(publishContext[paramPVCName]))
		state.Backend = firstNonEmpty(state.Backend, strings.TrimSpace(publishContext[annotationBackend]), "local")
		state.LastSuccessfulStageTime = time.Now().UTC()
		if identity != nil {
			state.LastHealthyIdentity = identity
			if identity.OpenNebulaImageID != "" && state.LastSuccessfulImageID == 0 {
				if parsed, err := strconv.Atoi(identity.OpenNebulaImageID); err == nil {
					state.LastSuccessfulImageID = parsed
				}
			}
			if identity.DeviceSerial != "" {
				state.LastSuccessfulDeviceSerial = identity.DeviceSerial
			}
			if identity.DiskTarget != "" {
				state.LastSuccessfulTarget = identity.DiskTarget
			}
		}
	})
	if err == nil {
		ns.Driver.metrics.RecordVolumeHistory("stage", "persisted")
		ns.clearRepairStateOnSuccess(ctx, volumeID)
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
		LastKnownImageID:      parseIntOrZero(expected.OpenNebulaImageID),
		LastKnownTarget:       expected.DiskTarget,
		LastKnownDeviceSerial: expected.DeviceSerial,
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
		"wrong device identity for local RWO volume %s: expected serial=%s fsUUID=%s partUUID=%s imageID=%s target=%s, observed serial=%s fsUUID=%s partUUID=%s imageID=%s target=%s",
		volumeID,
		identityField(expected, func(v *LocalDiskIdentity) string { return v.DeviceSerial }),
		identityField(expected, func(v *LocalDiskIdentity) string { return v.FilesystemUUID }),
		identityField(expected, func(v *LocalDiskIdentity) string { return v.PartitionUUID }),
		identityField(expected, func(v *LocalDiskIdentity) string { return v.OpenNebulaImageID }),
		identityField(expected, func(v *LocalDiskIdentity) string { return v.DiskTarget }),
		identityField(observed, func(v *LocalDiskIdentity) string { return v.DeviceSerial }),
		identityField(observed, func(v *LocalDiskIdentity) string { return v.FilesystemUUID }),
		identityField(observed, func(v *LocalDiskIdentity) string { return v.PartitionUUID }),
		identityField(observed, func(v *LocalDiskIdentity) string { return v.OpenNebulaImageID }),
		identityField(observed, func(v *LocalDiskIdentity) string { return v.DiskTarget }),
	)
}

func volumeRuntimeContextFromSource(params map[string]string) *VolumeRuntimeContext {
	if len(params) == 0 {
		return nil
	}
	return &VolumeRuntimeContext{
		PVName:       strings.TrimSpace(params[paramPVName]),
		PVCNamespace: strings.TrimSpace(params[paramPVCNamespace]),
		PVCName:      strings.TrimSpace(params[paramPVCName]),
		Backend:      strings.TrimSpace(params[annotationBackend]),
	}
}

func runtimeCtxField(runtimeCtx *VolumeRuntimeContext, getter func(*VolumeRuntimeContext) string) string {
	if runtimeCtx == nil || getter == nil {
		return ""
	}
	return strings.TrimSpace(getter(runtimeCtx))
}

func imageAndSerialFromPublishContext(params map[string]string) (int, string) {
	if len(params) == 0 {
		return 0, ""
	}
	imageID, _ := strconv.Atoi(strings.TrimSpace(params[publishContextOpenNebulaImageID]))
	return imageID, strings.TrimSpace(params[publishContextDeviceSerial])
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

	if lastKnownNode != "" && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if identity, err := s.driver.kubeRuntime.NodeIdentity(ctx, lastKnownNode); err == nil && (!identity.Exists || (lastKnownNodeUID != "" && identity.UID != "" && identity.UID != lastKnownNodeUID)) {
			classification = repairClassificationHistoricalNodeTombstone
			evidenceSource = "volume_history_node_identity"
			message = fmt.Sprintf("historical node tombstone for local RWO volume %s: last known node=%s uid=%s currentUID=%s lastKnownVMID=%d imageID=%d diskID=%d target=%s", volumeID, lastKnownNode, lastKnownNodeUID, identity.UID, lastKnownVMID, lastKnownImageID, lastKnownDiskID, lastKnownTarget)
		}
	}
	if classification == "" {
		switch result.Status {
		case opennebula.VolumeLookupImageRecordMissing:
			classification = repairClassificationMissingImageRecord
			message = fmt.Sprintf("missing image record for local RWO volume %s: lastKnownImageID=%d lastKnownNode=%s lastKnownNodeUID=%s lastKnownVMID=%d lastKnownDiskID=%d lastKnownTarget=%s requestedNode=%s", volumeID, lastKnownImageID, lastKnownNode, lastKnownNodeUID, lastKnownVMID, lastKnownDiskID, lastKnownTarget, requestedNode)
		case opennebula.VolumeLookupProviderInconsistent:
			classification = repairClassificationProviderLookupInconsistent
			message = fmt.Sprintf("provider lookup inconsistent for local RWO volume %s: %s", volumeID, firstNonEmpty(result.Message, "OpenNebula lookup resolved an inconsistent image state"))
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
	var history VolumeHistoryRecord
	if s != nil && s.driver != nil && s.driver.volumeHistory != nil {
		if record, ok := s.driver.volumeHistory.Get(volumeID); ok {
			history = record
		}
	}
	var runtimeCtx *VolumeRuntimeContext
	if s != nil && s.driver != nil && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if resolved, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID); err == nil {
			runtimeCtx = resolved
		}
	}
	if inspector, ok := s.volumeProvider.(opennebula.VolumeLookupInspector); ok {
		result, err := inspector.InspectVolumeLookup(ctx, volumeID, requestedNode)
		if err != nil {
			return nil, history, runtimeCtx, err
		}
		switch result.Status {
		case opennebula.VolumeLookupPresent:
			return result, history, runtimeCtx, nil
		case opennebula.VolumeLookupNotFound:
			if history.LastSuccessfulPublishTime.IsZero() {
				return result, history, runtimeCtx, status.Error(codes.NotFound, "volume not found")
			}
		case opennebula.VolumeLookupImageRecordMissing, opennebula.VolumeLookupProviderInconsistent:
			if !history.LastSuccessfulPublishTime.IsZero() {
				return result, history, runtimeCtx, s.classifyRepairRequiredLookup(ctx, volumeID, requestedNode, runtimeCtx, history, result)
			}
			return result, history, runtimeCtx, status.Error(codes.NotFound, "volume not found")
		}
		if history.LastSuccessfulPublishTime.IsZero() {
			return result, history, runtimeCtx, status.Error(codes.NotFound, "volume not found")
		}
		return result, history, runtimeCtx, s.classifyRepairRequiredLookup(ctx, volumeID, requestedNode, runtimeCtx, history, result)
	}

	imageID, sizeBytes, err := s.volumeProvider.VolumeExists(ctx, volumeID)
	if err != nil || imageID == -1 {
		if history.LastSuccessfulPublishTime.IsZero() {
			return nil, history, runtimeCtx, status.Error(codes.NotFound, "volume not found")
		}
		result := &opennebula.VolumeLookupResult{
			Status:        opennebula.VolumeLookupImageRecordMissing,
			VolumeHandle:  volumeID,
			RequestedNode: requestedNode,
		}
		return result, history, runtimeCtx, s.classifyRepairRequiredLookup(ctx, volumeID, requestedNode, runtimeCtx, history, result)
	}
	return &opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  volumeID,
		RequestedNode: requestedNode,
		ImageID:       imageID,
		SizeBytes:     int64(sizeBytes),
	}, history, runtimeCtx, nil
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

func localRWORepairStateVolumeIDs(snapshot map[string]VolumeRepairState) []string {
	if len(snapshot) == 0 {
		return nil
	}
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (s *ControllerServer) repairStateForQueue(volumeID string) (VolumeRepairState, bool) {
	if s == nil || s.driver == nil || s.driver.volumeRepairState == nil {
		return VolumeRepairState{}, false
	}
	return s.driver.volumeRepairState.Get(volumeID)
}

func (s *ControllerServer) recordActiveRepairStateEvent(ctx context.Context, runtimeCtx *VolumeRuntimeContext, state VolumeRepairState) {
	if runtimeCtx == nil {
		return
	}
	s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, eventReasonVolumeRepairRequired, firstNonEmpty(state.Message, fmt.Sprintf("repair-required state %s is active", state.Classification)))
}

func (s *ControllerServer) activeVolumeQuarantine(volumeID string) (VolumeQuarantineState, bool) {
	if s == nil || s.driver == nil || s.driver.volumeQuarantine == nil {
		return VolumeQuarantineState{}, false
	}
	return s.driver.volumeQuarantine.GetActive(volumeID, time.Now().UTC())
}

func (s *ControllerServer) activeHostArtifactQuarantine(volumeID string) bool {
	if s == nil || s.driver == nil || s.driver.hostArtifactQuarantine == nil {
		return false
	}
	for _, state := range s.driver.hostArtifactQuarantine.Snapshot() {
		if strings.TrimSpace(state.VolumeID) == strings.TrimSpace(volumeID) && (state.ExpiresAt.IsZero() || state.ExpiresAt.After(time.Now().UTC())) {
			return true
		}
	}
	return false
}

func (s *ControllerServer) recordCrossNodeOverrideUsed(ctx context.Context, runtimeCtx *VolumeRuntimeContext, message string) {
	if runtimeCtx == nil {
		return
	}
	s.recordPVCEventFromRuntimeContext(ctx, runtimeCtx, eventReasonCrossNodeOverrideUsed, message)
}

func localRWOVolumeProtectedError(req *csi.ControllerPublishVolumeRequest, decision LocalRWOProtectionDecision) error {
	if req == nil {
		return nil
	}
	return status.Error(codes.Unavailable, decision.Message)
}
