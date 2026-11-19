package driver

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
)

func (s *ControllerServer) rejectIfHostArtifactQuarantineActive(ctx context.Context, req *csi.ControllerPublishVolumeRequest) error {
	if s == nil || s.driver == nil || req == nil || !s.hostArtifactQuarantineEnabled() {
		return nil
	}
	manager := s.driver.hostArtifactQuarantine
	if manager.runtime != nil && manager.runtime.enabled {
		cm, err := manager.runtime.GetConfigMap(ctx, manager.namespace, hostArtifactStateConfigMapName)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return attachmentEvidenceUnavailable("host artifact quarantine is unverified", err)
		}
		if len(cm.Data) == 0 {
			return nil
		}
	} else if len(manager.Snapshot()) == 0 {
		return nil
	}
	inspector, ok := s.volumeProvider.(opennebula.HostArtifactAttachmentTargetInspector)
	if !ok {
		return attachmentEvidenceUnavailable("host artifact quarantine target is unverified", nil)
	}
	target, err := inspector.InspectHostArtifactAttachmentTarget(ctx, req.VolumeId, req.NodeId, req.GetVolumeContext())
	if err != nil {
		return attachmentEvidenceUnavailable("host artifact quarantine target is unverified", err)
	}
	if target == nil {
		return nil
	}
	return s.checkHostArtifactAttachTarget(ctx, req, target)
}

func (s *ControllerServer) checkHostArtifactAttachTarget(ctx context.Context, req *csi.ControllerPublishVolumeRequest, target *opennebula.HostArtifactAttachmentTarget) error {
	if !s.hostArtifactQuarantineEnabled() {
		return nil
	}
	var runtimeCtx *VolumeRuntimeContext
	if s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if resolved, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, req.VolumeId); err == nil {
			runtimeCtx = resolved
		}
	}
	recoveryControl, controlErr := s.recoveryControlState(ctx, req.VolumeId, runtimeCtx)
	if controlErr == nil {
		s.recordRecoveryModeExpiryWarning(ctx, runtimeCtx, recoveryControl)
	}
	manager := s.driver.hostArtifactQuarantine
	var persisted map[string]string
	now := time.Now().UTC()
	if manager.runtime != nil && manager.runtime.enabled {
		cm, err := manager.runtime.GetConfigMap(ctx, manager.namespace, hostArtifactStateConfigMapName)
		switch {
		case apierrors.IsNotFound(err):
			persisted = map[string]string{}
		case err != nil:
			return attachmentEvidenceUnavailable("host artifact quarantine is unverified", err)
		default:
			persisted = cm.Data
			if persisted == nil {
				persisted = map[string]string{}
			}
		}
	}
	state, active, err := manager.activeForAttach(req.VolumeId, target, persisted, now)
	if err != nil {
		return attachmentEvidenceUnavailable("host artifact quarantine is malformed", err)
	}
	if !active {
		return nil
	}
	message := hostArtifactAttachHoldMessage(target, state.Key)
	s.recordHostArtifactWarning(ctx, req.VolumeId, req.GetVolumeContext(), eventReasonHostArtifactQuarantined, message)
	if recoveryControl.Active() && recoveryControl.SuppressRepairActions {
		return nil
	}
	s.driver.metrics.RecordHostArtifactQuarantine(state.Classification, "active")
	return status.Error(codes.FailedPrecondition, message)
}

func hostArtifactAttachHoldMessage(target *opennebula.HostArtifactAttachmentTarget, key string) string {
	if target != nil && slices.Contains(hostArtifactTargetQuarantineKeys(*target), key) {
		return hostArtifactSlotBlocked
	}
	return "host artifact quarantine is active for the requested volume"
}

const hostArtifactSlotBlocked = "host artifact quarantine is active for the requested attachment slot"

func (s *ControllerServer) authorizeHostArtifactAttach(ctx context.Context, req *csi.ControllerPublishVolumeRequest) context.Context {
	return opennebula.WithAttachAuthorization(ctx, func(ctx context.Context, target *opennebula.HostArtifactAttachmentTarget) error {
		return s.checkHostArtifactAttachTarget(ctx, req, target)
	})
}

func (s *ControllerServer) rejectIfHostArtifactQuarantineActiveForVolumeNode(ctx context.Context, volumeID, node string, params map[string]string) error {
	return s.rejectIfHostArtifactQuarantineActive(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId:      strings.TrimSpace(volumeID),
		NodeId:        strings.TrimSpace(node),
		VolumeContext: params,
	})
}

func hostArtifactTargetQuarantineKeys(target opennebula.HostArtifactAttachmentTarget) []string {
	candidates := []string{
		target.Key(),
		(opennebula.HostArtifactAttachmentTarget{VMID: target.VMID}).Key(),
		(opennebula.HostArtifactAttachmentTarget{NodeName: target.NodeName}).Key(),
		(opennebula.HostArtifactAttachmentTarget{NodeName: target.NodeName, DiskID: target.DiskID}).Key(),
	}
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		keys = append(keys, candidate)
	}
	return keys
}

func (s *ControllerServer) handleHostArtifactConflict(ctx context.Context, req *csi.ControllerPublishVolumeRequest, conflict *opennebula.HostArtifactConflictError) error {
	if s == nil || s.driver == nil || req == nil || conflict == nil {
		return nil
	}
	var runtimeCtx *VolumeRuntimeContext
	if s.driver != nil && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if resolved, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, req.VolumeId); err == nil {
			runtimeCtx = resolved
		}
	}
	recoveryControl, recoveryErr := s.recoveryControlState(ctx, req.VolumeId, runtimeCtx)
	if recoveryErr == nil {
		s.recordRecoveryModeExpiryWarning(ctx, runtimeCtx, recoveryControl)
	}
	if !s.hostArtifactQuarantineEnabled() {
		message := conflict.Error()
		s.recordHostArtifactWarning(ctx, req.VolumeId, req.GetVolumeContext(), eventReasonHostArtifactQuarantined, message)
		return status.Error(codes.FailedPrecondition, message)
	}
	if recoveryControl.Active() && recoveryControl.SuppressRepairActions {
		message := conflict.Error()
		s.recordHostArtifactWarning(ctx, req.VolumeId, req.GetVolumeContext(), eventReasonHostArtifactQuarantined, message)
		return status.Error(codes.FailedPrecondition, message)
	}
	state := hostArtifactStateFromConflict(req.VolumeId, conflict)
	state, active, err := s.driver.hostArtifactQuarantine.MarkFailure(
		ctx,
		state,
		s.hostArtifactQuarantineFailureThreshold(),
		s.hostArtifactQuarantineTTL(),
	)
	if err != nil {
		klog.V(2).InfoS("Failed to persist host artifact quarantine state", "volumeID", req.VolumeId, "key", state.Key, "err", err)
	}
	outcome := "observed"
	message := state.Message
	if active {
		outcome = "active"
		message = hostArtifactQuarantineMessage(state)
	}
	s.driver.metrics.RecordHostArtifactQuarantine(state.Classification, outcome)
	s.recordHostArtifactWarning(ctx, req.VolumeId, req.GetVolumeContext(), eventReasonHostArtifactQuarantined, message)
	return status.Error(codes.FailedPrecondition, message)
}

func (s *ControllerServer) handleHostArtifactConflictForVolumeNode(ctx context.Context, volumeID, node string, params map[string]string, conflict *opennebula.HostArtifactConflictError) error {
	return s.handleHostArtifactConflict(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId:      strings.TrimSpace(volumeID),
		NodeId:        strings.TrimSpace(node),
		VolumeContext: params,
	}, conflict)
}

func hostArtifactStateFromConflict(volumeID string, conflict *opennebula.HostArtifactConflictError) HostArtifactQuarantineState {
	target := conflict.Target
	classification := strings.TrimSpace(conflict.Classification)
	if classification == "" {
		classification = opennebula.HostArtifactConflictClassification
	}
	key := target.Key()
	if key == "" {
		key = strings.TrimSpace(volumeID)
	}
	state := HostArtifactQuarantineState{
		Key:                 key,
		Classification:      classification,
		Reason:              hostArtifactReason,
		VolumeID:            strings.TrimSpace(volumeID),
		ImageID:             target.ImageID,
		NodeName:            target.NodeName,
		VMID:                target.VMID,
		HostID:              target.HostID,
		HostName:            target.HostName,
		SystemDatastoreID:   target.SystemDatastoreID,
		SystemDatastoreName: target.SystemDatastoreName,
		SystemDatastoreTM:   target.SystemDatastoreTM,
		DiskID:              target.DiskID,
		Target:              target.Target,
		LVName:              target.LVName,
		Message:             conflict.Error(),
	}
	if state.LVName != "" {
		state.RepairHint = fmt.Sprintf("read-only quarantine: inspect the OpenNebula VM log and host-local LV %s; after an operator confirms it is stale and not attached to the VM, repair the host artifact externally, then delete ConfigMap %s key %s or wait for the TTL before retrying", state.LVName, hostArtifactStateConfigMapName, state.Key)
	} else {
		state.RepairHint = fmt.Sprintf("read-only quarantine: inspect the OpenNebula VM log and host-local LVM artifacts; after an operator confirms the artifact is stale, repair it externally, then delete ConfigMap %s key %s or wait for the TTL before retrying", hostArtifactStateConfigMapName, state.Key)
	}
	return state
}

func (s *ControllerServer) recordHostArtifactWarning(ctx context.Context, volumeID string, params map[string]string, reason, message string) {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		s.recordPVCWarningFromParams(ctx, params, reason, message)
		return
	}
	runtimeCtx, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID)
	if err != nil || runtimeCtx == nil {
		s.recordPVCWarningFromParams(ctx, params, reason, message)
		return
	}
	s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, reason, message)
}

func (s *ControllerServer) hostArtifactQuarantineEnabled() bool {
	if s == nil || s.driver == nil || s.driver.hostArtifactQuarantine == nil {
		return false
	}
	enabled, ok := s.driver.PluginConfig.GetBool(config.HostArtifactQuarantineEnabledVar)
	return !ok || enabled
}

func (s *ControllerServer) hostArtifactQuarantineFailureThreshold() int {
	if s == nil || s.driver == nil {
		return 1
	}
	threshold, ok := s.driver.PluginConfig.GetInt(config.HostArtifactQuarantineFailureThresholdVar)
	if !ok || threshold <= 0 {
		return 1
	}
	return threshold
}

func (s *ControllerServer) hostArtifactQuarantineTTL() time.Duration {
	if s == nil || s.driver == nil {
		return time.Hour
	}
	seconds, ok := s.driver.PluginConfig.GetInt(config.HostArtifactQuarantineTTLSecondsVar)
	if !ok || seconds <= 0 {
		seconds = 3600
	}
	return time.Duration(seconds) * time.Second
}
