package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
)

const (
	volumeRecoveryControlStateConfigMapName = "opennebula-csi-volume-recovery-control-state"

	recoveryModeObserve  = "observe"
	recoveryModeManual   = "manual"
	recoveryModeDisabled = "disabled"

	recoverySourcePV  = "pv"
	recoverySourcePVC = "pvc"

	queueReasonRecoveryModeObserve = "recovery_mode_observe"
	queueReasonRecoveryModeManual  = "recovery_mode_manual"
)

type VolumeRecoveryControlState struct {
	Version                          int        `json:"version"`
	VolumeID                         string     `json:"volumeID"`
	PVName                           string     `json:"pvName,omitempty"`
	PVCNamespace                     string     `json:"pvcNamespace,omitempty"`
	PVCName                          string     `json:"pvcName,omitempty"`
	Mode                             string     `json:"mode,omitempty"`
	ModeSource                       string     `json:"modeSource,omitempty"`
	Ticket                           string     `json:"ticket,omitempty"`
	TicketSource                     string     `json:"ticketSource,omitempty"`
	AdoptAttachedDevice              bool       `json:"adoptAttachedDevice,omitempty"`
	ConfirmedDeviceSerial            string     `json:"confirmedDeviceSerial,omitempty"`
	ConfirmedVolumeName              string     `json:"confirmedVolumeName,omitempty"`
	ExpiresAt                        *time.Time `json:"expiresAt,omitempty"`
	Expired                          bool       `json:"expired,omitempty"`
	Invalid                          bool       `json:"invalid,omitempty"`
	Message                          string     `json:"message,omitempty"`
	Warning                          string     `json:"warning,omitempty"`
	SuppressQueue                    bool       `json:"suppressQueue,omitempty"`
	SuppressRepairActions            bool       `json:"suppressRepairActions,omitempty"`
	Adopted                          bool       `json:"adopted,omitempty"`
	AdoptedNode                      string     `json:"adoptedNode,omitempty"`
	AdoptedAt                        *time.Time `json:"adoptedAt,omitempty"`
	AdoptedDeviceSerial              string     `json:"adoptedDeviceSerial,omitempty"`
	AdoptedVolumeName                string     `json:"adoptedVolumeName,omitempty"`
	QueueBlockerReason               string     `json:"queueBlockerReason,omitempty"`
	QueueBlockerMessage              string     `json:"queueBlockerMessage,omitempty"`
	VolumeQuarantineReason           string     `json:"volumeQuarantineReason,omitempty"`
	VolumeQuarantineMessage          string     `json:"volumeQuarantineMessage,omitempty"`
	HostArtifactReason               string     `json:"hostArtifactReason,omitempty"`
	HostArtifactMessage              string     `json:"hostArtifactMessage,omitempty"`
	ExpectedDeviceSerial             string     `json:"expectedDeviceSerial,omitempty"`
	ExpectedVolumeName               string     `json:"expectedVolumeName,omitempty"`
	ActualGuestVisibleDevice         string     `json:"actualGuestVisibleDevice,omitempty"`
	CurrentStageMountSource          string     `json:"currentStageMountSource,omitempty"`
	CurrentStageMountMatchesExpected bool       `json:"currentStageMountMatchesExpected,omitempty"`
	ManualState                      string     `json:"manualState,omitempty"`
	LastObservedAt                   time.Time  `json:"lastObservedAt,omitempty"`
	ExpiryWarningEmittedAt           *time.Time `json:"expiryWarningEmittedAt,omitempty"`
}

func (s VolumeRecoveryControlState) Active() bool {
	return !s.Invalid && !s.Expired && s.Mode != "" && s.Mode != recoveryModeDisabled
}

func (s VolumeRecoveryControlState) ManualActive() bool {
	return s.Active() && s.Mode == recoveryModeManual
}

func (s VolumeRecoveryControlState) ObserveActive() bool {
	return s.Active() && s.Mode == recoveryModeObserve
}

func (s VolumeRecoveryControlState) AdoptionRequested() bool {
	return s.ManualActive() && s.AdoptAttachedDevice
}

func (s VolumeRecoveryControlState) QueueBlockReason() string {
	if s.ManualActive() {
		return queueReasonRecoveryModeManual
	}
	if s.ObserveActive() {
		return queueReasonRecoveryModeObserve
	}
	return ""
}

func (s VolumeRecoveryControlState) QueueBlockMessage(volumeID, node, operation string) string {
	mode := firstNonEmpty(strings.TrimSpace(s.Mode), recoveryModeManual)
	if s.AdoptionRequested() && strings.EqualFold(strings.TrimSpace(operation), "attach") {
		return fmt.Sprintf("manual recovery mode is active for volume %s on node %s; normal attach is paused until adopt-attached-device completes or recovery-mode is cleared", volumeID, node)
	}
	return fmt.Sprintf("recovery mode %s is active for volume %s on node %s; suppressing controller %s churn until recovery-mode is cleared or expires", mode, volumeID, node, operation)
}

type VolumeRecoveryControlManager struct {
	mu        sync.RWMutex
	runtime   *KubeRuntime
	namespace string
	entries   map[string]VolumeRecoveryControlState
}

func NewVolumeRecoveryControlManager(runtime *KubeRuntime, namespace string) *VolumeRecoveryControlManager {
	if strings.TrimSpace(namespace) == "" {
		namespace = namespaceFromServiceAccount()
	}
	return &VolumeRecoveryControlManager{
		runtime:   runtime,
		namespace: namespace,
		entries:   map[string]VolumeRecoveryControlState{},
	}
}

func (m *VolumeRecoveryControlManager) LoadFromConfigMap(ctx context.Context) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	cm, err := m.runtime.GetConfigMap(ctx, m.namespace, volumeRecoveryControlStateConfigMapName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	loaded := map[string]VolumeRecoveryControlState{}
	for key, raw := range cm.Data {
		var state VolumeRecoveryControlState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			klog.Warningf("failed to decode volume recovery control state %s: %v", key, err)
			continue
		}
		if strings.TrimSpace(state.VolumeID) == "" {
			state.VolumeID = key
		}
		state.Version = stateObjectVersion
		loaded[state.VolumeID] = state
	}
	m.mu.Lock()
	m.entries = loaded
	m.mu.Unlock()
	return nil
}

func (m *VolumeRecoveryControlManager) Get(volumeID string) (VolumeRecoveryControlState, bool) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeRecoveryControlState{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.entries[strings.TrimSpace(volumeID)]
	return state, ok
}

func (m *VolumeRecoveryControlManager) Snapshot() map[string]VolumeRecoveryControlState {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot := make(map[string]VolumeRecoveryControlState, len(m.entries))
	for key, state := range m.entries {
		snapshot[key] = state
	}
	return snapshot
}

func (m *VolumeRecoveryControlManager) Upsert(ctx context.Context, volumeID string, mutate func(*VolumeRecoveryControlState)) (VolumeRecoveryControlState, error) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeRecoveryControlState{}, nil
	}
	volumeID = strings.TrimSpace(volumeID)
	m.mu.Lock()
	state := m.entries[volumeID]
	if strings.TrimSpace(state.VolumeID) == "" {
		state.VolumeID = volumeID
	}
	state.Version = stateObjectVersion
	mutate(&state)
	state.VolumeID = volumeID
	state.Version = stateObjectVersion
	state.LastObservedAt = time.Now().UTC()
	m.entries[volumeID] = state
	m.mu.Unlock()
	if err := m.persistEntry(ctx, state); err != nil {
		return state, err
	}
	return state, nil
}

func (m *VolumeRecoveryControlManager) Clear(ctx context.Context, volumeID string) error {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return nil
	}
	volumeID = strings.TrimSpace(volumeID)
	m.mu.Lock()
	delete(m.entries, volumeID)
	m.mu.Unlock()
	if m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	return m.runtime.DeleteConfigMapKey(ctx, m.namespace, volumeRecoveryControlStateConfigMapName, volumeID)
}

func (m *VolumeRecoveryControlManager) SyncFromRuntimeContext(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext) (VolumeRecoveryControlState, error) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeRecoveryControlState{}, nil
	}
	volumeID = strings.TrimSpace(volumeID)
	existing, _ := m.Get(volumeID)
	resolved := resolvedRecoveryControlState(volumeID, runtimeCtx, existing)
	if !resolved.Active() && !resolved.Expired && !resolved.Invalid && !resolved.Adopted {
		if err := m.Clear(ctx, volumeID); err != nil {
			return VolumeRecoveryControlState{}, err
		}
		return VolumeRecoveryControlState{}, nil
	}
	return m.Upsert(ctx, volumeID, func(current *VolumeRecoveryControlState) {
		adopted := current.Adopted
		adoptedNode := current.AdoptedNode
		adoptedAt := current.AdoptedAt
		adoptedSerial := current.AdoptedDeviceSerial
		adoptedVolumeName := current.AdoptedVolumeName
		expiryWarningEmittedAt := current.ExpiryWarningEmittedAt
		*current = resolved
		current.Adopted = adopted
		current.AdoptedNode = adoptedNode
		current.AdoptedAt = adoptedAt
		current.AdoptedDeviceSerial = adoptedSerial
		current.AdoptedVolumeName = adoptedVolumeName
		current.ExpiryWarningEmittedAt = expiryWarningEmittedAt
	})
}

func (m *VolumeRecoveryControlManager) MarkAdopted(ctx context.Context, volumeID, node, serial, volumeName string) (VolumeRecoveryControlState, error) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeRecoveryControlState{}, nil
	}
	now := time.Now().UTC()
	return m.Upsert(ctx, volumeID, func(state *VolumeRecoveryControlState) {
		state.Adopted = true
		state.AdoptedNode = strings.TrimSpace(node)
		state.AdoptedAt = &now
		state.AdoptedDeviceSerial = strings.TrimSpace(serial)
		state.AdoptedVolumeName = strings.TrimSpace(volumeName)
	})
}

func (m *VolumeRecoveryControlManager) MarkExpiryWarningEmitted(ctx context.Context, volumeID string) error {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return nil
	}
	_, err := m.Upsert(ctx, volumeID, func(state *VolumeRecoveryControlState) {
		now := time.Now().UTC()
		state.ExpiryWarningEmittedAt = &now
	})
	return err
}

func (m *VolumeRecoveryControlManager) persistEntry(ctx context.Context, state VolumeRecoveryControlState) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal volume recovery control state: %w", err)
	}
	return m.runtime.UpsertConfigMapData(ctx, m.namespace, volumeRecoveryControlStateConfigMapName, map[string]string{
		state.VolumeID: string(payload),
	})
}

func resolvedRecoveryControlState(volumeID string, runtimeCtx *VolumeRuntimeContext, existing VolumeRecoveryControlState) VolumeRecoveryControlState {
	state := VolumeRecoveryControlState{
		Version:        stateObjectVersion,
		VolumeID:       strings.TrimSpace(volumeID),
		PVName:         runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVName }),
		PVCNamespace:   runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVCNamespace }),
		PVCName:        runtimeCtxField(runtimeCtx, func(v *VolumeRuntimeContext) string { return v.PVCName }),
		LastObservedAt: time.Now().UTC(),
	}
	if runtimeCtx == nil {
		return state
	}
	if value, source := runtimeAnnotationValue(runtimeCtx, annotationRecoveryMode); value != "" {
		state.Mode = value
		state.ModeSource = source
	}
	if value, source := runtimeAnnotationValue(runtimeCtx, annotationRecoveryTicket); value != "" {
		state.Ticket = value
		state.TicketSource = source
	}
	if value, _, ok, err := runtimeBoolAnnotationValue(runtimeCtx, annotationAdoptAttachedDevice); err != nil {
		state.Invalid = true
		state.Message = err.Error()
		return state
	} else if ok {
		state.AdoptAttachedDevice = value
	}
	if value, _ := runtimeAnnotationValue(runtimeCtx, annotationConfirmedDeviceSerial); value != "" {
		state.ConfirmedDeviceSerial = value
	}
	if value, _ := runtimeAnnotationValue(runtimeCtx, annotationConfirmedVolumeName); value != "" {
		state.ConfirmedVolumeName = value
	}
	if rawUntil, _ := runtimeAnnotationValue(runtimeCtx, annotationRecoveryModeUntil); rawUntil != "" {
		expiresAt, err := time.Parse(time.RFC3339, rawUntil)
		if err != nil {
			state.Invalid = true
			state.Message = fmt.Sprintf("invalid %s annotation %q: %v", annotationRecoveryModeUntil, rawUntil, err)
			return state
		}
		expiry := expiresAt.UTC()
		state.ExpiresAt = &expiry
		if !expiry.After(time.Now().UTC()) {
			state.Expired = true
			state.Warning = fmt.Sprintf("recovery mode %s for volume %s expired at %s and is no longer active", firstNonEmpty(state.Mode, recoveryModeManual), state.VolumeID, expiry.Format(time.RFC3339))
		}
	}

	switch state.Mode {
	case "", recoveryModeDisabled:
		if strings.EqualFold(state.Mode, recoveryModeDisabled) {
			state.Warning = fmt.Sprintf("recovery mode for volume %s is explicitly disabled", state.VolumeID)
		}
	case recoveryModeObserve:
		state.SuppressRepairActions = !state.Expired
	case recoveryModeManual:
		state.SuppressQueue = !state.Expired
		state.SuppressRepairActions = !state.Expired
	default:
		state.Invalid = true
		state.Message = fmt.Sprintf("invalid %s annotation %q: expected one of %s, %s, or %s", annotationRecoveryMode, state.Mode, recoveryModeObserve, recoveryModeManual, recoveryModeDisabled)
		return state
	}

	if state.AdoptionRequested() && strings.TrimSpace(state.ConfirmedDeviceSerial) == "" {
		state.Invalid = true
		state.Message = fmt.Sprintf("%s=true requires %s", annotationAdoptAttachedDevice, annotationConfirmedDeviceSerial)
		return state
	}
	if state.AdoptionRequested() && strings.TrimSpace(state.ConfirmedVolumeName) == "" {
		state.Invalid = true
		state.Message = fmt.Sprintf("%s=true requires %s", annotationAdoptAttachedDevice, annotationConfirmedVolumeName)
		return state
	}
	if state.Mode == "" || state.Mode == recoveryModeDisabled {
		return state
	}

	state.Adopted = existing.Adopted
	state.AdoptedNode = existing.AdoptedNode
	state.AdoptedAt = existing.AdoptedAt
	state.AdoptedDeviceSerial = existing.AdoptedDeviceSerial
	state.AdoptedVolumeName = existing.AdoptedVolumeName
	state.ExpiryWarningEmittedAt = existing.ExpiryWarningEmittedAt
	return state
}

func runtimeAnnotationValue(runtimeCtx *VolumeRuntimeContext, key string) (string, string) {
	if value := annotationValue(runtimeCtx, key, recoverySourcePV); value != "" {
		return value, recoverySourcePV
	}
	if value := annotationValue(runtimeCtx, key, recoverySourcePVC); value != "" {
		return value, recoverySourcePVC
	}
	return "", ""
}

func runtimeBoolAnnotationValue(runtimeCtx *VolumeRuntimeContext, key string) (bool, string, bool, error) {
	value, source := runtimeAnnotationValue(runtimeCtx, key)
	if value == "" {
		return false, "", false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, source, true, fmt.Errorf("invalid %s annotation %q: %v", key, value, err)
	}
	return parsed, source, true, nil
}

func (s *ControllerServer) recoveryControlState(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext) (VolumeRecoveryControlState, error) {
	if s == nil || s.driver == nil || s.driver.volumeRecoveryControl == nil {
		return VolumeRecoveryControlState{}, nil
	}
	if runtimeCtx == nil && s.driver.kubeRuntime != nil && s.driver.kubeRuntime.enabled {
		if resolved, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeID); err == nil {
			runtimeCtx = resolved
		}
	}
	return s.driver.volumeRecoveryControl.SyncFromRuntimeContext(ctx, volumeID, runtimeCtx)
}

func (s *ControllerServer) recordRecoveryModeExpiryWarning(ctx context.Context, runtimeCtx *VolumeRuntimeContext, state VolumeRecoveryControlState) {
	if s == nil || s.driver == nil || s.driver.volumeRecoveryControl == nil || runtimeCtx == nil || !state.Expired || strings.TrimSpace(state.Warning) == "" {
		return
	}
	if state.ExpiryWarningEmittedAt != nil && !state.ExpiryWarningEmittedAt.IsZero() {
		return
	}
	s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, eventReasonRecoveryModeExpired, state.Warning)
	_ = s.driver.volumeRecoveryControl.MarkExpiryWarningEmitted(ctx, state.VolumeID)
}

func (s *ControllerServer) publishContextForRecoveryAdoption(ctx context.Context, volumeID string, sourceContext map[string]string, control VolumeRecoveryControlState, deviceDiscoveryTimeout time.Duration) map[string]string {
	publishContext := s.publishContextForVolume(ctx, volumeID, strings.TrimSpace(control.ConfirmedVolumeName), deviceDiscoveryTimeout, sourceContext)
	if strings.TrimSpace(control.ConfirmedVolumeName) != "" {
		publishContext["volumeName"] = strings.TrimSpace(control.ConfirmedVolumeName)
	}
	if strings.TrimSpace(control.ConfirmedDeviceSerial) != "" {
		publishContext[publishContextDeviceSerial] = strings.TrimSpace(control.ConfirmedDeviceSerial)
	}
	if strings.TrimSpace(control.Mode) != "" {
		publishContext[publishContextRecoveryMode] = strings.TrimSpace(control.Mode)
	}
	if strings.TrimSpace(control.Ticket) != "" {
		publishContext[publishContextRecoveryTicket] = strings.TrimSpace(control.Ticket)
	}
	publishContext[publishContextRecoveryAdoptedDevice] = strconv.FormatBool(control.AdoptionRequested() || control.Adopted)
	return publishContext
}

func (s *ControllerServer) manualRecoveryAdoptionTarget(ctx context.Context, volumeID, requestedNode string, history VolumeHistoryRecord, runtimeCtx *VolumeRuntimeContext, lookup *opennebula.VolumeLookupResult, control VolumeRecoveryControlState) (string, error) {
	if !control.AdoptionRequested() {
		return "", nil
	}
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		return "", fmt.Errorf("manual recovery adoption requires kubernetes runtime support")
	}
	desired, reason, err := s.driver.kubeRuntime.VolumeDesiredOnNode(ctx, volumeID, requestedNode)
	if err != nil {
		return "", fmt.Errorf("failed to confirm desired node for manual recovery adoption: %w", err)
	}
	if !desired {
		return "", fmt.Errorf("manual recovery adoption for volume %s requires current Kubernetes demand on node %s, got %s", volumeID, requestedNode, reason)
	}
	expectedSerial := expectedRecoveryDeviceSerial(history, lookup)
	confirmedSerial := strings.TrimSpace(control.ConfirmedDeviceSerial)
	if expectedSerial == "" {
		return "", fmt.Errorf("manual recovery adoption for volume %s requires a known expected device serial", volumeID)
	}
	if !strings.EqualFold(confirmedSerial, expectedSerial) {
		return "", fmt.Errorf("manual recovery adoption for volume %s requires confirmed serial %s to match expected serial %s", volumeID, confirmedSerial, expectedSerial)
	}
	confirmedTarget := normalizedRecoveryDeviceName(control.ConfirmedVolumeName)
	if confirmedTarget == "" {
		return "", fmt.Errorf("manual recovery adoption for volume %s requires a non-empty %s", volumeID, annotationConfirmedVolumeName)
	}
	if lookup != nil && lookup.AttachmentMetadata != nil {
		metadata := lookup.AttachmentMetadata
		if conflictingOwners := metadata.ConflictingOwnerVMIDs(); len(conflictingOwners) > 0 {
			return "", fmt.Errorf("manual recovery adoption for volume %s is blocked by conflicting OpenNebula owners %v", volumeID, conflictingOwners)
		}
		if _, _, serial := requestedNodeDiskEvidence(metadata, requestedNode); serial != "" && !strings.EqualFold(strings.TrimSpace(serial), confirmedSerial) {
			return "", fmt.Errorf("manual recovery adoption for volume %s observed metadata serial %s but operator confirmed %s", volumeID, serial, confirmedSerial)
		}
	}
	if report, ok := s.latestLocalDeviceReport(ctx, volumeID); ok && strings.EqualFold(strings.TrimSpace(report.Node), requestedNode) {
		if reportSerial := strings.TrimSpace(report.DeviceSerial); reportSerial != "" && !strings.EqualFold(reportSerial, confirmedSerial) {
			return "", fmt.Errorf("manual recovery adoption for volume %s observed node report serial %s but operator confirmed %s", volumeID, reportSerial, confirmedSerial)
		}
		reportTarget := normalizedRecoveryDeviceName(firstNonEmpty(strings.TrimSpace(report.DevicePath), strings.TrimSpace(report.ExpectedTarget), strings.TrimSpace(report.VolumeName)))
		if reportTarget != "" && reportTarget != confirmedTarget {
			return "", fmt.Errorf("manual recovery adoption for volume %s observed node report target %s but operator confirmed %s", volumeID, reportTarget, confirmedTarget)
		}
	}
	return confirmedTarget, nil
}

func expectedRecoveryDeviceSerial(history VolumeHistoryRecord, lookup *opennebula.VolumeLookupResult) string {
	if serial := strings.TrimSpace(history.LastSuccessfulDeviceSerial); serial != "" {
		return serial
	}
	if lookup != nil && lookup.ImageID > 0 {
		return fmt.Sprintf("onecsi-%d", lookup.ImageID)
	}
	return ""
}

func normalizedRecoveryDeviceName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "/dev/") {
		return filepath.Base(value)
	}
	return filepath.Base(value)
}
