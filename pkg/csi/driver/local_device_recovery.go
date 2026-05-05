package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

const localDeviceStateConfigMapName = "opennebula-csi-node-device-state"

const (
	localDeviceFailureClassMissingDevice            = "device_missing"
	localDeviceFailureClassRuntimeAttachmentMissing = "runtime_attachment_missing"
	localDeviceFailureClassMountFailed              = "mount_failed"
	localDeviceFailureClassWrongIdentity            = "wrong_device_identity"

	localDeviceConfirmationStatePending        = "pending_runtime_confirmation"
	localDeviceConfirmationStateConfirmed      = "confirmed_by_node"
	localDeviceConfirmationStateTimedOut       = "timed_out_waiting_for_node_confirmation"
	localDeviceConfirmationStateRepairRequired = "repair_required_runtime_attach_unconfirmed"

	localDeviceRecoveryMethodRuntimeRepublish             = "runtime_republish"
	localDeviceRecoveryMethodSameNodeDetachAttachFallback = "same_node_detach_attach_fallback"
	localDeviceAttachmentStateMetadataAttached            = "metadata_attached"
	localDeviceAttachmentStateMetadataDetached            = "metadata_detached"
	localDeviceAttachmentStateRuntimeConfirmedByNode      = "runtime_confirmed_by_node"
	localDeviceAttachmentStateRuntimeUnconfirmed          = "runtime_unconfirmed"
)

type LocalDeviceMissingReport struct {
	Node                   string             `json:"node"`
	VolumeID               string             `json:"volumeID"`
	VolumeName             string             `json:"volumeName,omitempty"`
	FailureClass           string             `json:"failureClass,omitempty"`
	ExpectedTarget         string             `json:"expectedTarget,omitempty"`
	DevicePath             string             `json:"devicePath,omitempty"`
	DeviceSerial           string             `json:"deviceSerial,omitempty"`
	OpenNebulaImageID      string             `json:"openNebulaImageID,omitempty"`
	FsType                 string             `json:"fsType,omitempty"`
	ResolvedBy             string             `json:"resolvedBy,omitempty"`
	ExpectedIdentity       *LocalDiskIdentity `json:"expectedIdentity,omitempty"`
	ObservedIdentity       *LocalDiskIdentity `json:"observedIdentity,omitempty"`
	PVCNamespace           string             `json:"pvcNamespace,omitempty"`
	PVCName                string             `json:"pvcName,omitempty"`
	PVName                 string             `json:"pvName,omitempty"`
	StagingTargetPath      string             `json:"stagingTargetPath,omitempty"`
	FirstObservedAt        time.Time          `json:"firstObservedAt"`
	LastObservedAt         time.Time          `json:"lastObservedAt"`
	Attempts               int                `json:"attempts"`
	LastError              string             `json:"lastError,omitempty"`
	RecoveryAttempts       int                `json:"recoveryAttempts,omitempty"`
	LastRecoveryAt         *time.Time         `json:"lastRecoveryAt,omitempty"`
	RecoveryMethod         string             `json:"recoveryMethod,omitempty"`
	RecoveryToken          string             `json:"recoveryToken,omitempty"`
	LastRecoveryError      string             `json:"lastRecoveryError,omitempty"`
	LastRecoveryOutcome    string             `json:"lastRecoveryOutcome,omitempty"`
	LastRecoverySignature  string             `json:"lastRecoverySignature,omitempty"`
	ConfirmationState      string             `json:"confirmationState,omitempty"`
	ConfirmationDeadline   *time.Time         `json:"confirmationDeadline,omitempty"`
	ConfirmationObservedAt *time.Time         `json:"confirmationObservedAt,omitempty"`
	AttachmentState        string             `json:"attachmentState,omitempty"`
	MetadataAttachedToNode bool               `json:"metadataAttachedToNode,omitempty"`
	MetadataNode           string             `json:"metadataNode,omitempty"`
	MetadataTarget         string             `json:"metadataTarget,omitempty"`
}

type localDeviceRecoveryAttempt struct {
	Method           string
	ExpectedTarget   string
	AttachmentState  string
	MetadataAttached bool
	MetadataNode     string
	MetadataTarget   string
}

type localDeviceReportIdentity struct {
	PVCNamespace string
	PVCName      string
	PVName       string
}

func (ns *NodeServer) recordLocalDeviceMissing(ctx context.Context, volumeID, volumeName, stagingTargetPath string, publishContext map[string]string, cause error) {
	if ns == nil || ns.Driver == nil || ns.Driver.kubeRuntime == nil || !ns.Driver.kubeRuntime.enabled {
		return
	}
	if enabled, ok := ns.Driver.PluginConfig.GetBool(config.LocalDeviceRecoveryEnabledVar); ok && !enabled {
		return
	}
	volumeID = strings.TrimSpace(volumeID)
	node := strings.TrimSpace(ns.Driver.nodeID)
	if volumeID == "" || node == "" || opennebula.IsSharedFilesystemVolumeID(volumeID) {
		return
	}
	if backend := strings.TrimSpace(publishContext[annotationBackend]); backend != "" && !strings.EqualFold(backend, "local") {
		return
	}

	now := time.Now().UTC()
	key := localDeviceReportKey(node, volumeID)
	namespace := namespaceFromServiceAccount()
	identity := ns.localDeviceReportIdentity(ctx, volumeID, publishContext)
	var attempts int
	err := updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespace, key, func(report *LocalDeviceMissingReport) {
		if report.FirstObservedAt.IsZero() {
			report.FirstObservedAt = now
		}
		report.Node = node
		report.VolumeID = volumeID
		report.VolumeName = strings.TrimSpace(volumeName)
		report.FailureClass = localDeviceFailureClassRuntimeAttachmentMissing
		report.ExpectedTarget = strings.TrimSpace(volumeName)
		report.DevicePath = ""
		report.DeviceSerial = strings.TrimSpace(publishContext[publishContextDeviceSerial])
		report.OpenNebulaImageID = strings.TrimSpace(publishContext[publishContextOpenNebulaImageID])
		report.FsType = ""
		report.ResolvedBy = ""
		report.PVCNamespace = identity.PVCNamespace
		report.PVCName = identity.PVCName
		report.PVName = identity.PVName
		report.StagingTargetPath = strings.TrimSpace(stagingTargetPath)
		report.LastObservedAt = now
		report.Attempts++
		attempts = report.Attempts
		if report.ConfirmationState == "" {
			report.AttachmentState = localDeviceAttachmentStateRuntimeUnconfirmed
		}
		if cause != nil {
			report.LastError = cause.Error()
		}
	})
	if err != nil {
		klog.V(2).InfoS("Failed to persist local device missing report",
			"node", node, "volumeID", volumeID, "configMap", localDeviceStateConfigMapName, "err", err)
		return
	}
	if ns.Driver.metrics != nil {
		ns.Driver.metrics.RecordLocalVolumeHealth("device_missing", "reported")
	}
	minAttempts := 3
	if configured, ok := ns.Driver.PluginConfig.GetInt(config.LocalDeviceRecoveryMinAttemptsVar); ok && configured > 0 {
		minAttempts = configured
	}
	if attempts == 1 || attempts%minAttempts == 0 {
		message := fmt.Sprintf("node %s reported missing local device for volume %s after %d attempt(s): %v", node, volumeID, attempts, cause)
		ns.recordPVCWarningFromPublishContext(ctx, publishContext, eventReasonLocalDeviceMissingReported, message)
	}
}

func (ns *NodeServer) recordLocalDeviceMountFailure(ctx context.Context, volumeID, volumeName, devicePath, stagingTargetPath, fsType string, resolution deviceResolutionResult, publishContext map[string]string, cause error) {
	if ns == nil || ns.Driver == nil || ns.Driver.kubeRuntime == nil || !ns.Driver.kubeRuntime.enabled {
		return
	}
	if enabled, ok := ns.Driver.PluginConfig.GetBool(config.LocalDeviceRecoveryEnabledVar); ok && !enabled {
		return
	}
	volumeID = strings.TrimSpace(volumeID)
	node := strings.TrimSpace(ns.Driver.nodeID)
	if volumeID == "" || node == "" || opennebula.IsSharedFilesystemVolumeID(volumeID) {
		return
	}
	if backend := strings.TrimSpace(publishContext[annotationBackend]); backend != "" && !strings.EqualFold(backend, "local") {
		return
	}

	now := time.Now().UTC()
	key := localDeviceReportKey(node, volumeID)
	namespace := namespaceFromServiceAccount()
	identity := ns.localDeviceReportIdentity(ctx, volumeID, publishContext)
	var attempts int
	err := updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespace, key, func(report *LocalDeviceMissingReport) {
		if report.FirstObservedAt.IsZero() {
			report.FirstObservedAt = now
		}
		report.Node = node
		report.VolumeID = volumeID
		report.VolumeName = strings.TrimSpace(volumeName)
		report.FailureClass = localDeviceFailureClassMountFailed
		report.ExpectedTarget = strings.TrimSpace(volumeName)
		report.DevicePath = strings.TrimSpace(devicePath)
		report.DeviceSerial = strings.TrimSpace(publishContext[publishContextDeviceSerial])
		report.OpenNebulaImageID = strings.TrimSpace(publishContext[publishContextOpenNebulaImageID])
		report.FsType = strings.TrimSpace(fsType)
		report.ResolvedBy = strings.TrimSpace(resolution.ResolvedBy)
		report.PVCNamespace = identity.PVCNamespace
		report.PVCName = identity.PVCName
		report.PVName = identity.PVName
		report.StagingTargetPath = strings.TrimSpace(stagingTargetPath)
		report.LastObservedAt = now
		report.Attempts++
		attempts = report.Attempts
		report.AttachmentState = localDeviceAttachmentStateRuntimeConfirmedByNode
		report.ConfirmationState = ""
		report.ConfirmationDeadline = nil
		report.RecoveryToken = ""
		if cause != nil {
			report.LastError = cause.Error()
		}
	})
	if err != nil {
		klog.V(2).InfoS("Failed to persist local device mount failure report",
			"node", node, "volumeID", volumeID, "devicePath", devicePath, "configMap", localDeviceStateConfigMapName, "err", err)
		return
	}
	if ns.Driver.metrics != nil {
		ns.Driver.metrics.RecordLocalVolumeHealth("device_mount_failed", "reported")
	}
	minAttempts := 3
	if configured, ok := ns.Driver.PluginConfig.GetInt(config.LocalDeviceRecoveryMinAttemptsVar); ok && configured > 0 {
		minAttempts = configured
	}
	if attempts == 1 || attempts%minAttempts == 0 {
		message := fmt.Sprintf("node %s failed to mount local device %s for volume %s after %d attempt(s) (fsType=%s resolvedBy=%s): %v", node, devicePath, volumeID, attempts, fsType, resolution.ResolvedBy, cause)
		ns.recordPVCWarningFromPublishContext(ctx, publishContext, eventReasonLocalDeviceMountFailed, message)
	}
}

func (ns *NodeServer) recordWrongDeviceIdentityReport(ctx context.Context, session localDiskSession, observed *LocalDiskIdentity, cause error) {
	if ns == nil || ns.Driver == nil || ns.Driver.kubeRuntime == nil || !ns.Driver.kubeRuntime.enabled {
		return
	}
	volumeID := strings.TrimSpace(session.VolumeID)
	node := strings.TrimSpace(ns.Driver.nodeID)
	if volumeID == "" || node == "" {
		return
	}
	now := time.Now().UTC()
	key := localDeviceReportKey(node, volumeID)
	namespace := namespaceFromServiceAccount()
	err := updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespace, key, func(report *LocalDeviceMissingReport) {
		if report.FirstObservedAt.IsZero() {
			report.FirstObservedAt = now
		}
		report.Node = node
		report.VolumeID = volumeID
		report.VolumeName = strings.TrimSpace(session.VolumeName)
		report.FailureClass = localDeviceFailureClassWrongIdentity
		report.ExpectedTarget = strings.TrimSpace(localDiskAssertedDiskTarget(session.Identity))
		report.DevicePath = strings.TrimSpace(session.DevicePath)
		report.DeviceSerial = strings.TrimSpace(session.DeviceSerial)
		report.OpenNebulaImageID = strings.TrimSpace(session.OpenNebulaImageID)
		report.FsType = strings.TrimSpace(session.FSType)
		report.PVCNamespace = strings.TrimSpace(session.PVCNamespace)
		report.PVCName = strings.TrimSpace(session.PVCName)
		report.PVName = strings.TrimSpace(session.PVName)
		report.StagingTargetPath = strings.TrimSpace(session.StagingTargetPath)
		report.ExpectedIdentity = session.Identity
		report.ObservedIdentity = observed
		report.LastObservedAt = now
		report.Attempts++
		report.AttachmentState = localDeviceAttachmentStateRuntimeConfirmedByNode
		report.ConfirmationState = localDeviceConfirmationStateConfirmed
		report.ConfirmationDeadline = nil
		report.RecoveryToken = ""
		if cause != nil {
			report.LastError = cause.Error()
		}
	})
	if err != nil {
		klog.V(2).InfoS("Failed to persist wrong-device identity report", "node", node, "volumeID", volumeID, "err", err)
		return
	}
	ns.recordPVCWarningFromPublishContext(ctx, map[string]string{
		paramPVCNamespace: session.PVCNamespace,
		paramPVCName:      session.PVCName,
	}, eventReasonWrongDeviceIdentity, wrongDeviceIdentityMessage(volumeID, session.Identity, observed))
}

func (ns *NodeServer) clearLocalDeviceMissing(ctx context.Context, volumeID string) {
	if ns == nil || ns.Driver == nil || ns.Driver.kubeRuntime == nil || !ns.Driver.kubeRuntime.enabled {
		return
	}
	node := strings.TrimSpace(ns.Driver.nodeID)
	volumeID = strings.TrimSpace(volumeID)
	if node == "" || volumeID == "" {
		return
	}
	if err := ns.Driver.kubeRuntime.DeleteConfigMapKey(ctx, namespaceFromServiceAccount(), localDeviceStateConfigMapName, localDeviceReportKey(node, volumeID)); err != nil {
		klog.V(3).InfoS("Failed to clear local device missing report", "node", node, "volumeID", volumeID, "err", err)
	}
}

func (ns *NodeServer) currentLocalDeviceReport(ctx context.Context, volumeID string) (LocalDeviceMissingReport, bool) {
	if ns == nil || ns.Driver == nil || ns.Driver.kubeRuntime == nil || !ns.Driver.kubeRuntime.enabled {
		return LocalDeviceMissingReport{}, false
	}
	node := strings.TrimSpace(ns.Driver.nodeID)
	volumeID = strings.TrimSpace(volumeID)
	if node == "" || volumeID == "" {
		return LocalDeviceMissingReport{}, false
	}
	cm, err := ns.Driver.kubeRuntime.GetConfigMap(ctx, namespaceFromServiceAccount(), localDeviceStateConfigMapName)
	if err != nil {
		return LocalDeviceMissingReport{}, false
	}
	raw := strings.TrimSpace(cm.Data[localDeviceReportKey(node, volumeID)])
	if raw == "" {
		return LocalDeviceMissingReport{}, false
	}
	var report LocalDeviceMissingReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return LocalDeviceMissingReport{}, false
	}
	return report, true
}

func (ns *NodeServer) confirmLocalDeviceRecovery(ctx context.Context, report *LocalDeviceMissingReport, volumeID, devicePath string, observed *LocalDiskIdentity, publishContext map[string]string) {
	if ns == nil || ns.Driver == nil || ns.Driver.kubeRuntime == nil || !ns.Driver.kubeRuntime.enabled {
		return
	}
	volumeID = strings.TrimSpace(volumeID)
	node := strings.TrimSpace(ns.Driver.nodeID)
	if node == "" || volumeID == "" {
		return
	}
	if report == nil {
		ns.clearLocalDeviceMissing(ctx, volumeID)
		return
	}
	key := localDeviceReportKey(node, volumeID)
	now := time.Now().UTC()
	_ = updateLocalDeviceReport(ctx, ns.Driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) {
		if strings.TrimSpace(current.Node) == "" {
			*current = *report
		}
		current.DevicePath = firstNonEmpty(strings.TrimSpace(devicePath), current.DevicePath)
		current.ExpectedTarget = firstNonEmpty(current.ExpectedTarget, strings.TrimSpace(publishContext["volumeName"]), strings.TrimSpace(current.VolumeName))
		current.AttachmentState = localDeviceAttachmentStateRuntimeConfirmedByNode
		current.ConfirmationState = localDeviceConfirmationStateConfirmed
		current.ConfirmationDeadline = nil
		current.RecoveryToken = ""
		current.MetadataAttachedToNode = true
		current.MetadataNode = node
		current.LastObservedAt = now
		current.LastRecoveryOutcome = localDeviceConfirmationStateConfirmed
		current.LastRecoveryError = ""
		current.ConfirmationObservedAt = &now
		if observed != nil {
			current.ObservedIdentity = observed
		}
	})
	if strings.TrimSpace(report.RecoveryMethod) != "" || strings.TrimSpace(report.ConfirmationState) != "" {
		ns.recordPVCEventFromPublishContext(ctx, publishContext, eventReasonLocalDeviceRecoverySucceeded, fmt.Sprintf("node confirmed local device visibility for volume %s on node %s after controller recovery using %s", volumeID, node, firstNonEmpty(strings.TrimSpace(report.RecoveryMethod), localDeviceRecoveryMethodRuntimeRepublish)))
	}
	ns.clearLocalDeviceMissing(ctx, volumeID)
}

func (ns *NodeServer) localDeviceReportIdentity(ctx context.Context, volumeID string, publishContext map[string]string) localDeviceReportIdentity {
	identity := localDeviceReportIdentity{
		PVCNamespace: strings.TrimSpace(publishContext[paramPVCNamespace]),
		PVCName:      strings.TrimSpace(publishContext[paramPVCName]),
		PVName:       strings.TrimSpace(publishContext[paramPVName]),
	}
	if identity.PVCNamespace != "" && identity.PVCName != "" && identity.PVName != "" {
		return identity
	}
	if ns == nil || ns.Driver == nil || ns.Driver.kubeRuntime == nil || !ns.Driver.kubeRuntime.enabled {
		return identity
	}
	runtimeCtx, err := ns.Driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID)
	if err != nil || runtimeCtx == nil {
		return identity
	}
	if identity.PVCNamespace == "" {
		identity.PVCNamespace = strings.TrimSpace(runtimeCtx.PVCNamespace)
	}
	if identity.PVCName == "" {
		identity.PVCName = strings.TrimSpace(runtimeCtx.PVCName)
	}
	if identity.PVName == "" {
		identity.PVName = strings.TrimSpace(runtimeCtx.PVName)
	}
	return identity
}

func (s *ControllerServer) runLocalDeviceRecovery(ctx context.Context) {
	interval := s.localDeviceRecoveryInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.reconcileLocalDeviceRecovery(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcileLocalDeviceRecovery(ctx)
		}
	}
}

func (s *ControllerServer) reconcileLocalDeviceRecovery(ctx context.Context) {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled || s.volumeProvider == nil {
		return
	}
	if s.driver.controllerLeadership != nil && !s.driver.controllerLeadership.IsLeader() {
		return
	}
	cm, err := s.driver.kubeRuntime.GetConfigMap(ctx, namespaceFromServiceAccount(), localDeviceStateConfigMapName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.V(2).InfoS("Failed to read local device recovery reports", "configMap", localDeviceStateConfigMapName, "err", err)
		}
		return
	}
	if len(cm.Data) == 0 {
		return
	}

	keys := make([]string, 0, len(cm.Data))
	for key := range cm.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	now := time.Now().UTC()
	for _, key := range keys {
		raw := strings.TrimSpace(cm.Data[key])
		if raw == "" {
			continue
		}
		var report LocalDeviceMissingReport
		if err := json.Unmarshal([]byte(raw), &report); err != nil {
			klog.V(2).InfoS("Skipping malformed local device recovery report", "key", key, "err", err)
			continue
		}
		if refreshed, changed, refreshErr := s.refreshLocalDeviceReportConfirmationState(ctx, key, report, now); refreshErr != nil {
			klog.V(3).InfoS("Failed to refresh local device recovery confirmation state", "key", key, "err", refreshErr)
		} else {
			report = refreshed
			if changed {
				continue
			}
		}
		if ready, reason := s.localDeviceReportReady(report, now); !ready {
			if reason == "repair_required_wrong_identity" || reason == "repair_required_runtime_attach_unconfirmed" {
				if err := s.markLocalDeviceRecoverySkipped(ctx, key, report, reason, nil); err != nil {
					klog.V(3).InfoS("Failed to persist local device recovery skip", "key", key, "reason", reason, "err", err)
				}
			}
			klog.V(5).InfoS("Local device recovery report is not ready", "key", key, "reason", reason)
			continue
		}
		if err := s.recoverLocalDeviceReport(ctx, key, report); err != nil {
			klog.V(2).InfoS("Local device recovery failed", "key", key, "node", report.Node, "volumeID", report.VolumeID, "err", err)
		}
	}
}

func (s *ControllerServer) recoverLocalDeviceReport(ctx context.Context, key string, report LocalDeviceMissingReport) error {
	node := strings.TrimSpace(report.Node)
	volumeID := strings.TrimSpace(report.VolumeID)
	if node == "" || volumeID == "" {
		_ = s.clearLocalDeviceReport(ctx, key)
		return nil
	}

	runtimeCtx, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID)
	if err != nil {
		if strings.Contains(err.Error(), "persistent volume for handle") {
			return s.clearLocalDeviceReportWithEvent(ctx, key, report, nil, "persistent volume no longer exists")
		}
		return s.markLocalDeviceRecoverySkipped(ctx, key, report, "runtime_context_unavailable", err)
	}
	if !runtimeContextEligibleForMaintenance(runtimeCtx, volumeID) {
		return s.clearLocalDeviceReportWithEvent(ctx, key, report, runtimeCtx, "volume is not an eligible local single-writer disk")
	}
	desired, desiredReason, err := s.driver.kubeRuntime.VolumeDesiredOnNode(ctx, volumeID, node)
	if err != nil {
		return s.markLocalDeviceRecoverySkipped(ctx, key, report, "desired_state_unavailable", err)
	}
	if !desired {
		return s.clearLocalDeviceReportWithEvent(ctx, key, report, runtimeCtx, fmt.Sprintf("volume is no longer desired on node %s", node))
	}
	kubeReady, err := s.driver.kubeRuntime.IsNodeReady(ctx, node)
	if err != nil {
		return s.markLocalDeviceRecoverySkipped(ctx, key, report, "kubernetes_node_unavailable", err)
	}
	if !kubeReady {
		return s.markLocalDeviceRecoverySkipped(ctx, key, report, "kubernetes_node_not_ready", nil)
	}
	vmReady, err := s.volumeProvider.NodeReady(ctx, node)
	if err != nil {
		return s.markLocalDeviceRecoverySkipped(ctx, key, report, "opennebula_node_unavailable", err)
	}
	if !vmReady {
		return s.markLocalDeviceRecoverySkipped(ctx, key, report, "opennebula_node_not_ready", nil)
	}
	volumeNumericID, _, err := s.volumeProvider.VolumeExists(ctx, volumeID)
	if err != nil {
		return s.markLocalDeviceRecoverySkipped(ctx, key, report, "volume_lookup_failed", err)
	}
	if volumeNumericID == -1 {
		return s.clearLocalDeviceReportWithEvent(ctx, key, report, runtimeCtx, "OpenNebula volume no longer exists")
	}
	nodeNumericID, err := s.volumeProvider.NodeExists(ctx, node)
	if err != nil {
		return s.markLocalDeviceRecoverySkipped(ctx, key, report, "node_lookup_failed", err)
	}
	if nodeNumericID == -1 {
		return s.clearLocalDeviceReportWithEvent(ctx, key, report, runtimeCtx, "OpenNebula node no longer exists")
	}

	message := fmt.Sprintf("attempting same-node device recovery for local RWO volume %s on node %s after %d missing-device report(s) (%s)", volumeID, node, report.Attempts, desiredReason)
	if localDeviceFailureClass(report) == localDeviceFailureClassMountFailed {
		message = fmt.Sprintf("attempting same-node device recovery for local RWO volume %s on node %s after %d mount failure report(s) using %s (%s)", volumeID, node, report.Attempts, strings.TrimSpace(report.DevicePath), desiredReason)
	}
	s.recordLocalDeviceRecoveryEvent(ctx, report, runtimeCtx, eventReasonLocalDeviceRecoveryAttempted, message)
	if s.driver.metrics != nil {
		s.driver.metrics.RecordLocalVolumeHealth("device_recovery", "attempted")
	}

	attempt, err := s.executeLocalDeviceRecovery(ctx, report, runtimeCtx, volumeNumericID, nodeNumericID)
	if err != nil {
		if s.driver.metrics != nil {
			s.driver.metrics.RecordLocalVolumeHealth("device_recovery", "failed")
		}
		s.recordLocalDeviceRecoveryEvent(ctx, report, runtimeCtx, eventReasonLocalDeviceRecoveryFailed, fmt.Sprintf("same-node device recovery for volume %s on node %s failed: %v", volumeID, node, err))
		return s.updateLocalDeviceRecoveryFailure(ctx, key, report, err)
	}

	if s.driver.metrics != nil {
		s.driver.metrics.RecordLocalVolumeHealth("device_recovery", "pending_runtime_confirmation")
	}
	window := s.localDeviceRecoveryConfirmationWindow(report)
	deadline := time.Now().UTC().Add(window)
	if err := s.markLocalDeviceRecoveryPendingConfirmation(ctx, key, report, attempt, deadline); err != nil {
		return err
	}
	s.recordLocalDeviceRecoveryEvent(ctx, report, runtimeCtx, eventReasonLocalDeviceRecoveryPending, fmt.Sprintf("same-node device recovery for volume %s on node %s completed at the controller; waiting until %s for node runtime confirmation", volumeID, node, deadline.Format(time.RFC3339)))
	return nil
}

func (s *ControllerServer) executeLocalDeviceRecovery(ctx context.Context, report LocalDeviceMissingReport, runtimeCtx *VolumeRuntimeContext, volumeNumericID, nodeNumericID int) (*localDeviceRecoveryAttempt, error) {
	node := strings.TrimSpace(report.Node)
	volumeID := strings.TrimSpace(report.VolumeID)
	params := localDeviceRecoveryParams(report, runtimeCtx)
	attempt := &localDeviceRecoveryAttempt{
		ExpectedTarget:  firstNonEmpty(strings.TrimSpace(report.ExpectedTarget), strings.TrimSpace(report.VolumeName)),
		AttachmentState: localDeviceAttachmentStateMetadataDetached,
		MetadataNode:    node,
	}
	err := s.withQueuedHotplug(ctx, node, "reattach", volumeID, hotplugQueuePriorityCritical, func(queueCtx context.Context) error {
		nodeRelease := s.driver.operationLocks.Acquire(controllerNodeLockKey(node))
		defer nodeRelease()

		volumeRelease := s.driver.operationLocks.Acquire(controllerVolumeLockKey(volumeID))
		defer volumeRelease()

		if desired, reason, err := s.driver.kubeRuntime.VolumeDesiredOnNode(queueCtx, volumeID, node); err != nil {
			return fmt.Errorf("failed to confirm same-node desired state: %w", err)
		} else if !desired {
			return fmt.Errorf("volume is no longer desired on node %s", node)
		} else {
			klog.V(3).InfoS("Confirmed local device recovery desired state", "volumeID", volumeID, "node", node, "reason", reason)
		}

		target, attachedErr := s.volumeProvider.GetVolumeInNode(queueCtx, volumeNumericID, nodeNumericID)
		sizeBytes, sizeErr := s.volumeProvider.ResolveVolumeSizeBytes(queueCtx, volumeID)
		if attachedErr == nil {
			// OpenNebula does not expose a safe runtime-only re-hotplug for an already-declared VM disk.
			// When metadata still shows the disk attached, the narrowest available repair is a same-node
			// detach/attach sequence followed by node-side runtime confirmation.
			attempt.Method = localDeviceRecoveryMethodSameNodeDetachAttachFallback
			attempt.AttachmentState = localDeviceAttachmentStateMetadataAttached
			attempt.MetadataAttached = true
			attempt.MetadataTarget = strings.TrimSpace(target)
			detachStarted := time.Now()
			if err := s.volumeProvider.DetachVolume(queueCtx, volumeID, node); err != nil {
				s.handleHotplugTimeout(queueCtx, node, volumeID, params, "detach", "disk", err)
				return fmt.Errorf("failed to detach existing same-node attachment target %s: %w", target, err)
			}
			if sizeErr == nil {
				s.driver.observeAdaptiveTimeout(queueCtx, "detach", "disk", sizeBytes, time.Since(detachStarted))
			}
		} else {
			attempt.Method = localDeviceRecoveryMethodRuntimeRepublish
			klog.V(2).InfoS("Local device recovery found no existing OpenNebula attachment; attaching directly",
				"volumeID", volumeID, "node", node, "err", attachedErr)
			if s.driver.metrics != nil {
				s.driver.metrics.RecordLocalVolumeHealth("device_recovery", "attachment_absent")
			}
		}

		attachStarted := time.Now()
		if err := s.volumeProvider.AttachVolume(queueCtx, volumeID, node, false, params); err != nil {
			s.handleHotplugTimeout(queueCtx, node, volumeID, params, "attach", "disk", err)
			return fmt.Errorf("failed to attach volume back to same node: %w", err)
		}
		target, err := s.volumeProvider.GetVolumeInNode(queueCtx, volumeNumericID, nodeNumericID)
		if err != nil {
			return fmt.Errorf("failed to confirm recovered same-node attachment: %w", err)
		}
		attempt.AttachmentState = localDeviceAttachmentStateRuntimeUnconfirmed
		attempt.MetadataAttached = true
		attempt.MetadataTarget = strings.TrimSpace(target)
		if sizeErr == nil {
			s.driver.observeAdaptiveTimeout(queueCtx, "attach", "disk", sizeBytes, time.Since(attachStarted))
		}
		s.clearHotplugGuardState(queueCtx, node)
		s.clearStickyReuseState(queueCtx, volumeID, node, "disk")
		s.annotateRestartOptimization(queueCtx, runtimeCtx, node, s.effectiveDetachGraceSeconds(runtimeCtx))
		klog.V(1).InfoS("Local device recovery reattached volume",
			"volumeID", volumeID, "node", node, "target", target)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attempt, nil
}

func (s *ControllerServer) localDeviceReportReady(report LocalDeviceMissingReport, now time.Time) (bool, string) {
	if strings.TrimSpace(report.Node) == "" || strings.TrimSpace(report.VolumeID) == "" {
		return false, "missing_identity"
	}
	if localDeviceFailureClass(report) == localDeviceFailureClassWrongIdentity {
		return false, "repair_required_wrong_identity"
	}
	if strings.TrimSpace(report.ConfirmationState) == localDeviceConfirmationStateRepairRequired {
		return false, "repair_required_runtime_attach_unconfirmed"
	}
	if strings.TrimSpace(report.ConfirmationState) == localDeviceConfirmationStatePending {
		if report.ConfirmationDeadline == nil || report.ConfirmationDeadline.After(now) {
			return false, "pending_runtime_confirmation"
		}
	}
	if report.Attempts < s.localDeviceRecoveryMinAttempts() {
		return false, "min_attempts"
	}
	firstObserved := report.FirstObservedAt
	if firstObserved.IsZero() {
		firstObserved = report.LastObservedAt
	}
	if !firstObserved.IsZero() && now.Sub(firstObserved) < s.localDeviceRecoveryMinAge() {
		return false, "min_age"
	}
	if report.LastRecoveryAt != nil && s.localDeviceRecoveryCooldown() > 0 && now.Sub(*report.LastRecoveryAt) < s.localDeviceRecoveryCooldown() {
		return false, "cooldown"
	}
	if maxAttempts := s.localDeviceRecoveryMaxAttempts(); maxAttempts > 0 && report.RecoveryAttempts >= maxAttempts {
		if report.LastRecoveryAt == nil || !report.LastObservedAt.After(*report.LastRecoveryAt) {
			return false, "max_recovery_attempts"
		}
		currentSignature := localDeviceRecoverySignature(report)
		if strings.TrimSpace(report.LastRecoverySignature) == "" || currentSignature == report.LastRecoverySignature {
			return false, "max_recovery_attempts_same_failure"
		}
	}
	return true, ""
}

func (s *ControllerServer) refreshLocalDeviceReportConfirmationState(ctx context.Context, key string, report LocalDeviceMissingReport, now time.Time) (LocalDeviceMissingReport, bool, error) {
	if strings.TrimSpace(report.ConfirmationState) != localDeviceConfirmationStatePending || report.ConfirmationDeadline == nil || report.ConfirmationDeadline.IsZero() || report.ConfirmationDeadline.After(now) {
		return report, false, nil
	}
	message := fmt.Sprintf("same-node runtime attach for volume %s on node %s was not confirmed by NodeStageVolume before %s", report.VolumeID, report.Node, report.ConfirmationDeadline.Format(time.RFC3339))
	if maxAttempts := s.localDeviceRecoveryMaxAttempts(); maxAttempts > 0 && report.RecoveryAttempts >= maxAttempts {
		if err := s.recordRuntimeAttachRepairState(ctx, report, message); err != nil {
			return report, false, err
		}
		if err := updateLocalDeviceReport(ctx, s.driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) {
			if strings.TrimSpace(current.Node) == "" {
				*current = report
			}
			current.AttachmentState = localDeviceAttachmentStateRuntimeUnconfirmed
			current.ConfirmationState = localDeviceConfirmationStateRepairRequired
			current.ConfirmationDeadline = nil
			current.RecoveryToken = ""
			current.MetadataNode = report.Node
			current.LastRecoveryError = message
			current.LastRecoveryOutcome = localDeviceConfirmationStateRepairRequired
			current.ConfirmationObservedAt = &now
		}); err != nil {
			return report, false, err
		}
		report.AttachmentState = localDeviceAttachmentStateRuntimeUnconfirmed
		report.ConfirmationState = localDeviceConfirmationStateRepairRequired
		report.ConfirmationDeadline = nil
		report.RecoveryToken = ""
		report.MetadataNode = report.Node
		report.LastRecoveryError = message
		report.LastRecoveryOutcome = localDeviceConfirmationStateRepairRequired
		report.ConfirmationObservedAt = &now
		s.recordLocalDeviceRecoveryEvent(ctx, report, nil, eventReasonLocalDeviceRecoveryFailed, message+"; automatic same-node recovery is now blocked pending manual repair")
		if s.driver != nil && s.driver.metrics != nil {
			s.driver.metrics.RecordLocalVolumeHealth("device_recovery", localDeviceConfirmationStateRepairRequired)
		}
		return report, true, nil
	}
	if err := updateLocalDeviceReport(ctx, s.driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) {
		if strings.TrimSpace(current.Node) == "" {
			*current = report
		}
		current.AttachmentState = localDeviceAttachmentStateRuntimeUnconfirmed
		current.ConfirmationState = localDeviceConfirmationStateTimedOut
		current.ConfirmationDeadline = nil
		current.RecoveryToken = ""
		current.MetadataNode = report.Node
		current.LastRecoveryError = message
		current.LastRecoveryOutcome = localDeviceConfirmationStateTimedOut
		current.ConfirmationObservedAt = &now
	}); err != nil {
		return report, false, err
	}
	report.AttachmentState = localDeviceAttachmentStateRuntimeUnconfirmed
	report.ConfirmationState = localDeviceConfirmationStateTimedOut
	report.ConfirmationDeadline = nil
	report.RecoveryToken = ""
	report.MetadataNode = report.Node
	report.LastRecoveryError = message
	report.LastRecoveryOutcome = localDeviceConfirmationStateTimedOut
	report.ConfirmationObservedAt = &now
	s.recordLocalDeviceRecoveryEvent(ctx, report, nil, eventReasonLocalDeviceRecoveryFailed, message+"; controller will re-evaluate the same-node recovery episode")
	if s.driver != nil && s.driver.metrics != nil {
		s.driver.metrics.RecordLocalVolumeHealth("device_recovery", localDeviceConfirmationStateTimedOut)
	}
	return report, true, nil
}

func (s *ControllerServer) updateLocalDeviceRecoveryFailure(ctx context.Context, key string, report LocalDeviceMissingReport, cause error) error {
	now := time.Now().UTC()
	message := "unknown error"
	if cause != nil {
		message = cause.Error()
	}
	return updateLocalDeviceReport(ctx, s.driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) {
		if strings.TrimSpace(current.Node) == "" {
			*current = report
		}
		if current.FirstObservedAt.IsZero() {
			current.FirstObservedAt = report.FirstObservedAt
		}
		if current.LastObservedAt.Before(report.LastObservedAt) {
			current.LastObservedAt = report.LastObservedAt
		}
		if current.Attempts < report.Attempts {
			current.Attempts = report.Attempts
		}
		current.Node = report.Node
		current.VolumeID = report.VolumeID
		current.VolumeName = report.VolumeName
		current.FailureClass = localDeviceFailureClass(report)
		current.ExpectedTarget = report.ExpectedTarget
		current.DevicePath = report.DevicePath
		current.DeviceSerial = report.DeviceSerial
		current.OpenNebulaImageID = report.OpenNebulaImageID
		current.FsType = report.FsType
		current.ResolvedBy = report.ResolvedBy
		current.PVCNamespace = report.PVCNamespace
		current.PVCName = report.PVCName
		current.PVName = report.PVName
		current.StagingTargetPath = report.StagingTargetPath
		current.RecoveryAttempts++
		current.LastRecoveryAt = &now
		current.RecoveryMethod = ""
		current.RecoveryToken = ""
		current.ConfirmationState = ""
		current.ConfirmationDeadline = nil
		current.ConfirmationObservedAt = nil
		current.AttachmentState = localDeviceAttachmentStateRuntimeUnconfirmed
		current.MetadataAttachedToNode = false
		current.MetadataNode = report.Node
		current.MetadataTarget = ""
		current.LastRecoveryError = message
		current.LastRecoveryOutcome = "failed"
		current.LastRecoverySignature = localDeviceRecoverySignature(*current)
	})
}

func (s *ControllerServer) markLocalDeviceRecoverySkipped(ctx context.Context, key string, report LocalDeviceMissingReport, reason string, cause error) error {
	message := reason
	if cause != nil {
		message = fmt.Sprintf("%s: %v", reason, cause)
	}
	klog.V(3).InfoS("Skipping local device recovery", "node", report.Node, "volumeID", report.VolumeID, "reason", message)
	if s.driver != nil && s.driver.metrics != nil {
		s.driver.metrics.RecordLocalVolumeHealth("device_recovery", "skipped_"+reason)
	}
	if strings.TrimSpace(report.LastRecoveryOutcome) != "skipped" || strings.TrimSpace(report.LastRecoveryError) != message {
		s.recordLocalDeviceRecoveryEvent(ctx, report, nil, eventReasonLocalDeviceRecoverySkipped, fmt.Sprintf("skipped local device recovery for volume %s on node %s: %s", report.VolumeID, report.Node, message))
	}
	return updateLocalDeviceReport(ctx, s.driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) {
		if strings.TrimSpace(current.Node) == "" {
			*current = report
		}
		current.LastRecoveryError = message
		current.LastRecoveryOutcome = "skipped"
	})
}

func (s *ControllerServer) recordRuntimeAttachRepairState(ctx context.Context, report LocalDeviceMissingReport, message string) error {
	if s == nil || s.driver == nil || s.driver.volumeRepairState == nil {
		return nil
	}
	state := VolumeRepairState{
		Version:               stateObjectVersion,
		VolumeID:              strings.TrimSpace(report.VolumeID),
		Classification:        repairClassificationRuntimeAttachUnconfirmed,
		Reason:                localDeviceFailureClass(report),
		Message:               strings.TrimSpace(message),
		RequestedNode:         strings.TrimSpace(report.Node),
		LastKnownNodeName:     strings.TrimSpace(report.Node),
		LastKnownTarget:       firstNonEmpty(strings.TrimSpace(report.MetadataTarget), strings.TrimSpace(report.ExpectedTarget), strings.TrimSpace(report.VolumeName)),
		LastKnownDeviceSerial: strings.TrimSpace(report.DeviceSerial),
		EvidenceSource:        "local_device_report",
		LastObservedIdentity:  report.ObservedIdentity,
	}
	if s.driver.volumeHistory != nil {
		if history, ok := s.driver.volumeHistory.Get(report.VolumeID); ok {
			state.LastKnownNodeUID = history.LastSuccessfulNodeUID
			state.LastKnownOpenNebulaVMID = history.LastSuccessfulOpenNebulaVMID
			state.LastKnownImageID = history.LastSuccessfulImageID
			state.LastKnownDiskID = history.LastSuccessfulDiskID
			state.LastHealthyIdentity = history.LastHealthyIdentity
		}
	}
	if persisted, err := s.driver.volumeRepairState.Mark(ctx, state); err == nil {
		s.driver.metrics.RecordVolumeRepairState(persisted.Classification, "persisted")
	} else {
		return err
	}
	return nil
}

func (s *ControllerServer) markLocalDeviceRecoveryPendingConfirmation(ctx context.Context, key string, report LocalDeviceMissingReport, attempt *localDeviceRecoveryAttempt, deadline time.Time) error {
	if attempt == nil {
		return fmt.Errorf("local device recovery attempt is required")
	}
	now := time.Now().UTC()
	token := fmt.Sprintf("%s-%d", sanitizeLocalDeviceReportKey(report.VolumeID), now.UnixNano())
	return updateLocalDeviceReport(ctx, s.driver.kubeRuntime, namespaceFromServiceAccount(), key, func(current *LocalDeviceMissingReport) {
		if strings.TrimSpace(current.Node) == "" {
			*current = report
		}
		current.ExpectedTarget = firstNonEmpty(strings.TrimSpace(attempt.ExpectedTarget), current.ExpectedTarget, strings.TrimSpace(current.VolumeName))
		current.RecoveryAttempts++
		current.LastRecoveryAt = &now
		current.RecoveryMethod = strings.TrimSpace(attempt.Method)
		current.RecoveryToken = token
		current.ConfirmationState = localDeviceConfirmationStatePending
		current.ConfirmationDeadline = &deadline
		current.ConfirmationObservedAt = nil
		current.AttachmentState = localDeviceAttachmentStateRuntimeUnconfirmed
		current.MetadataAttachedToNode = attempt.MetadataAttached
		current.MetadataNode = firstNonEmpty(strings.TrimSpace(attempt.MetadataNode), current.Node)
		current.MetadataTarget = strings.TrimSpace(attempt.MetadataTarget)
		current.LastRecoveryError = ""
		current.LastRecoveryOutcome = localDeviceConfirmationStatePending
		current.LastRecoverySignature = localDeviceRecoverySignature(*current)
	})
}

func (s *ControllerServer) clearLocalDeviceReportWithEvent(ctx context.Context, key string, report LocalDeviceMissingReport, runtimeCtx *VolumeRuntimeContext, reason string) error {
	if s.driver != nil && s.driver.metrics != nil {
		s.driver.metrics.RecordLocalVolumeHealth("device_recovery", "cleared")
	}
	s.recordLocalDeviceRecoveryEvent(ctx, report, runtimeCtx, eventReasonLocalDeviceRecoverySkipped, fmt.Sprintf("cleared missing-device report for volume %s: %s", report.VolumeID, reason))
	return s.clearLocalDeviceReport(ctx, key)
}

func (s *ControllerServer) clearLocalDeviceReport(ctx context.Context, key string) error {
	return s.driver.kubeRuntime.DeleteConfigMapKey(ctx, namespaceFromServiceAccount(), localDeviceStateConfigMapName, key)
}

func (s *ControllerServer) recordLocalDeviceRecoveryEvent(ctx context.Context, report LocalDeviceMissingReport, runtimeCtx *VolumeRuntimeContext, reason, message string) {
	namespace := strings.TrimSpace(report.PVCNamespace)
	name := strings.TrimSpace(report.PVCName)
	if runtimeCtx != nil {
		if namespace == "" {
			namespace = runtimeCtx.PVCNamespace
		}
		if name == "" {
			name = runtimeCtx.PVCName
		}
	}
	if namespace == "" || name == "" || s == nil || s.driver == nil || s.driver.kubeRuntime == nil {
		return
	}
	if strings.HasSuffix(reason, "Failed") || strings.HasSuffix(reason, "Skipped") {
		s.driver.kubeRuntime.EmitWarningEventOnPVC(ctx, namespace, name, reason, message)
		return
	}
	s.driver.kubeRuntime.EmitPVCEvent(ctx, namespace, name, reason, message)
}

func updateLocalDeviceReport(ctx context.Context, runtime *KubeRuntime, namespace, key string, mutate func(*LocalDeviceMissingReport)) error {
	if runtime == nil || !runtime.enabled {
		return fmt.Errorf("kubernetes runtime is not enabled")
	}
	if namespace == "" {
		namespace = namespaceFromServiceAccount()
	}
	cmClient := runtime.client.CoreV1().ConfigMaps(namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := cmClient.Get(ctx, localDeviceStateConfigMapName, metav1.GetOptions{})
		report := LocalDeviceMissingReport{}
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			mutate(&report)
			payload, marshalErr := json.Marshal(report)
			if marshalErr != nil {
				return marshalErr
			}
			_, err = cmClient.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: localDeviceStateConfigMapName, Namespace: namespace},
				Data:       map[string]string{key: string(payload)},
			}, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				return apierrors.NewConflict(corev1.Resource("configmaps"), localDeviceStateConfigMapName, err)
			}
			return err
		}
		if current.Data == nil {
			current.Data = map[string]string{}
		}
		if raw := strings.TrimSpace(current.Data[key]); raw != "" {
			if err := json.Unmarshal([]byte(raw), &report); err != nil {
				klog.V(3).InfoS("Replacing malformed local device report", "key", key, "err", err)
				report = LocalDeviceMissingReport{}
			}
		}
		mutate(&report)
		payload, err := json.Marshal(report)
		if err != nil {
			return err
		}
		current.Data[key] = string(payload)
		_, err = cmClient.Update(ctx, current, metav1.UpdateOptions{})
		return err
	})
}

func localDeviceRecoveryParams(report LocalDeviceMissingReport, runtimeCtx *VolumeRuntimeContext) map[string]string {
	params := map[string]string{
		annotationBackend: "local",
	}
	if value := strings.TrimSpace(report.PVCNamespace); value != "" {
		params[paramPVCNamespace] = value
	} else if runtimeCtx != nil && runtimeCtx.PVCNamespace != "" {
		params[paramPVCNamespace] = runtimeCtx.PVCNamespace
	}
	if value := strings.TrimSpace(report.PVCName); value != "" {
		params[paramPVCName] = value
	} else if runtimeCtx != nil && runtimeCtx.PVCName != "" {
		params[paramPVCName] = runtimeCtx.PVCName
	}
	if value := strings.TrimSpace(report.PVName); value != "" {
		params[paramPVName] = value
	} else if runtimeCtx != nil && runtimeCtx.PVName != "" {
		params[paramPVName] = runtimeCtx.PVName
	}
	if value := strings.TrimSpace(report.DeviceSerial); value != "" {
		params[publishContextDeviceSerial] = value
	}
	if value := strings.TrimSpace(report.OpenNebulaImageID); value != "" {
		params[publishContextOpenNebulaImageID] = value
	}
	return params
}

func localDeviceFailureClass(report LocalDeviceMissingReport) string {
	switch strings.TrimSpace(report.FailureClass) {
	case localDeviceFailureClassRuntimeAttachmentMissing:
		return localDeviceFailureClassRuntimeAttachmentMissing
	case localDeviceFailureClassMountFailed:
		return localDeviceFailureClassMountFailed
	case localDeviceFailureClassWrongIdentity:
		return localDeviceFailureClassWrongIdentity
	default:
		return localDeviceFailureClassMissingDevice
	}
}

func localDeviceRecoverySignature(report LocalDeviceMissingReport) string {
	parts := []string{
		localDeviceFailureClass(report),
		strings.TrimSpace(report.VolumeName),
		strings.TrimSpace(report.ExpectedTarget),
		strings.TrimSpace(report.DevicePath),
		strings.TrimSpace(report.DeviceSerial),
		strings.TrimSpace(report.OpenNebulaImageID),
		strings.TrimSpace(report.FsType),
		strings.TrimSpace(report.ResolvedBy),
		strings.TrimSpace(report.StagingTargetPath),
	}
	return strings.Join(parts, "|")
}

func localDeviceReportKey(node, volumeID string) string {
	return sanitizeLocalDeviceReportKey(node) + "." + sanitizeLocalDeviceReportKey(volumeID)
}

func sanitizeLocalDeviceReportKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(value)
}

func (s *ControllerServer) localDeviceRecoveryEnabled() bool {
	if s == nil || s.driver == nil {
		return false
	}
	enabled, ok := s.driver.PluginConfig.GetBool(config.LocalDeviceRecoveryEnabledVar)
	return !ok || enabled
}

func (s *ControllerServer) localDeviceRecoveryMinAttempts() int {
	attempts, ok := s.driver.PluginConfig.GetInt(config.LocalDeviceRecoveryMinAttemptsVar)
	if !ok || attempts <= 0 {
		return 3
	}
	return attempts
}

func (s *ControllerServer) localDeviceRecoveryMinAge() time.Duration {
	seconds, ok := s.driver.PluginConfig.GetInt(config.LocalDeviceRecoveryMinAgeSecondsVar)
	if !ok || seconds < 0 {
		seconds = 60
	}
	return time.Duration(seconds) * time.Second
}

func (s *ControllerServer) localDeviceRecoveryInterval() time.Duration {
	seconds, ok := s.driver.PluginConfig.GetInt(config.LocalDeviceRecoveryIntervalSecondsVar)
	if !ok || seconds <= 0 {
		seconds = 15
	}
	return time.Duration(seconds) * time.Second
}

func (s *ControllerServer) localDeviceRecoveryCooldown() time.Duration {
	seconds, ok := s.driver.PluginConfig.GetInt(config.LocalDeviceRecoveryCooldownSecondsVar)
	if !ok || seconds < 0 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

func (s *ControllerServer) localDeviceRecoveryMaxAttempts() int {
	attempts, ok := s.driver.PluginConfig.GetInt(config.LocalDeviceRecoveryMaxAttemptsVar)
	if !ok || attempts <= 0 {
		return 2
	}
	return attempts
}

func (s *ControllerServer) localDeviceRecoveryConfirmationWindow(report LocalDeviceMissingReport) time.Duration {
	window := s.localDeviceRecoveryCooldown()
	if window <= 0 {
		window = 45 * time.Second
	}
	if window < 30*time.Second {
		window = 30 * time.Second
	}
	if strings.TrimSpace(report.LastRecoveryOutcome) == localDeviceConfirmationStateTimedOut && window < time.Minute {
		window = time.Minute
	}
	return window
}
