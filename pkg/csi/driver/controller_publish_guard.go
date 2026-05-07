package driver

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *ControllerServer) rejectIfLocalDeviceReportBlocksAttachSuccess(ctx context.Context, volumeID, node, target string, params map[string]string, runtimeCtx *VolumeRuntimeContext) error {
	report, ok := s.latestLocalDeviceReport(ctx, volumeID)
	if !ok || !localDeviceReportMatchesAttachNode(report, node) || !localDeviceReportMatchesAttachTarget(report, target) {
		return nil
	}
	blocked, code, message := localDeviceReportAttachSuccessBlock(report, node, target)
	if !blocked {
		return nil
	}
	if runtimeCtx != nil {
		s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, eventReasonLocalDeviceMissingReported, message)
	} else {
		s.recordPVCWarningFromParams(ctx, params, eventReasonLocalDeviceMissingReported, message)
	}
	return status.Error(code, message)
}

func localDeviceReportAttachSuccessBlock(report LocalDeviceMissingReport, node, target string) (bool, codes.Code, string) {
	volumeID := strings.TrimSpace(report.VolumeID)
	if volumeID == "" {
		return false, codes.OK, ""
	}
	base := fmt.Sprintf("refusing metadata-only attach success for volume %s on node %s", volumeID, strings.TrimSpace(node))
	if target = strings.TrimSpace(target); target != "" {
		base += fmt.Sprintf(" target %s", target)
	}
	switch strings.TrimSpace(report.ConfirmationState) {
	case localDeviceConfirmationStateRepairRequired:
		return true, codes.FailedPrecondition, base + ": node-side runtime attachment was not confirmed and now requires manual repair"
	case localDeviceConfirmationStatePending:
		return true, codes.Unavailable, base + ": node-side runtime attachment confirmation is still pending"
	}
	if localDeviceFailureClass(report) == localDeviceFailureClassWrongIdentity {
		return true, codes.FailedPrecondition, base + ": node reported a wrong local device identity"
	}
	if strings.TrimSpace(report.AttachmentState) == localDeviceAttachmentStateRuntimeUnconfirmed {
		return true, codes.Unavailable, base + ": node reported runtime attachment missing/unconfirmed"
	}
	if localDeviceFailureClass(report) == localDeviceFailureClassRuntimeAttachmentMissing && report.Attempts > 0 {
		return true, codes.Unavailable, base + ": node reported the advertised local device is missing"
	}
	return false, codes.OK, ""
}

func localDeviceReportMatchesAttachNode(report LocalDeviceMissingReport, node string) bool {
	return strings.TrimSpace(report.Node) != "" && strings.TrimSpace(report.Node) == strings.TrimSpace(node)
}

func localDeviceReportMatchesAttachTarget(report LocalDeviceMissingReport, target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return true
	}
	candidates := []string{
		report.VolumeName,
		report.ExpectedTarget,
		report.MetadataTarget,
	}
	observedTarget := false
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		observedTarget = true
		if candidate == target {
			return true
		}
	}
	return !observedTarget
}
