package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func (m *VolumeRecoveryControlManager) updateEntry(ctx context.Context, volumeID string, change func(VolumeRecoveryControlState) (*VolumeRecoveryControlState, error)) (VolumeRecoveryControlState, error) {
	volumeID = strings.TrimSpace(volumeID)
	if m == nil || volumeID == "" {
		return VolumeRecoveryControlState{}, nil
	}
	apply := func(current VolumeRecoveryControlState) (*VolumeRecoveryControlState, error) {
		next, err := change(current)
		if err != nil || next == nil || reflect.DeepEqual(*next, current) {
			return next, err
		}
		next.VolumeID, next.Version = volumeID, stateObjectVersion
		next.LastObservedAt = time.Now().UTC()
		return next, nil
	}
	if m.runtime == nil || !m.runtime.enabled {
		m.mu.Lock()
		defer m.mu.Unlock()
		next, err := apply(m.entries[volumeID])
		if err != nil {
			return VolumeRecoveryControlState{}, err
		}
		if next == nil {
			delete(m.entries, volumeID)
			return VolumeRecoveryControlState{}, nil
		}
		m.entries[volumeID] = *next
		return *next, nil
	}

	maps := m.runtime.client.CoreV1().ConfigMaps(m.namespace)
	cm, err := maps.Get(ctx, volumeRecoveryControlStateConfigMapName, metav1.GetOptions{})
	missing := errors.IsNotFound(err)
	if err != nil && !missing {
		return VolumeRecoveryControlState{}, err
	}
	current := VolumeRecoveryControlState{}
	if missing {
		cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: volumeRecoveryControlStateConfigMapName, Namespace: m.namespace}}
	} else if raw, exists := cm.Data[volumeID]; exists {
		if err := decodeCephRetentionProtection(raw, &current); err != nil || current.VolumeID != volumeID {
			return VolumeRecoveryControlState{}, attachmentEvidenceUnavailable("recovery control record is unverified", nil)
		}
	}
	next, err := apply(current)
	if err != nil {
		return VolumeRecoveryControlState{}, err
	}
	if err := ctx.Err(); err != nil {
		return VolumeRecoveryControlState{}, err
	}
	if next == nil {
		if _, exists := cm.Data[volumeID]; exists {
			delete(cm.Data, volumeID)
			_, err = maps.Update(ctx, cm, metav1.UpdateOptions{})
		}
	} else if !reflect.DeepEqual(*next, current) {
		payload, marshalErr := json.Marshal(next)
		if marshalErr != nil {
			return VolumeRecoveryControlState{}, marshalErr
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[volumeID] = string(payload)
		if missing {
			_, err = maps.Create(ctx, cm, metav1.CreateOptions{})
		} else {
			_, err = maps.Update(ctx, cm, metav1.UpdateOptions{})
		}
	}
	if err != nil {
		return VolumeRecoveryControlState{}, attachmentEvidenceUnavailable("recovery control write is unverified", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cached := m.entries[volumeID]
	if next == nil {
		if !cached.LastObservedAt.After(current.LastObservedAt) {
			delete(m.entries, volumeID)
		}
		return VolumeRecoveryControlState{}, nil
	}
	if !cached.LastObservedAt.After(next.LastObservedAt) {
		m.entries[volumeID] = *next
	}
	return *next, nil
}

func (m *VolumeRecoveryControlManager) Upsert(ctx context.Context, volumeID string, mutate func(*VolumeRecoveryControlState)) (VolumeRecoveryControlState, error) {
	return m.updateEntry(ctx, volumeID, func(current VolumeRecoveryControlState) (*VolumeRecoveryControlState, error) {
		mutate(&current)
		return &current, nil
	})
}

func (m *VolumeRecoveryControlManager) clearIfCurrent(ctx context.Context, volumeID string, expected VolumeRecoveryControlState) error {
	_, err := m.updateEntry(ctx, volumeID, func(current VolumeRecoveryControlState) (*VolumeRecoveryControlState, error) {
		if reflect.DeepEqual(current, expected) {
			return nil, nil
		}
		return &current, nil
	})
	return err
}

func (m *VolumeRecoveryControlManager) SyncFromRuntimeContext(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext) (VolumeRecoveryControlState, error) {
	var resolved VolumeRecoveryControlState
	_, err := m.updateEntry(ctx, volumeID, func(current VolumeRecoveryControlState) (*VolumeRecoveryControlState, error) {
		resolved = resolvedRecoveryControlState(volumeID, runtimeCtx, current)
		if current.Adopted {
			return &current, nil
		}
		resolved.LastObservedAt = current.LastObservedAt
		if reflect.DeepEqual(resolved, current) || (runtimeCtx == nil && current.VolumeID != "") {
			return &current, nil
		}
		if runtimeCtx != nil && m.runtime != nil && m.runtime.enabled &&
			(current.VolumeID != "" || resolved.Active() || resolved.Expired || resolved.Invalid) {
			runtime := m.runtime
			pv, err := runtime.client.CoreV1().PersistentVolumes().Get(ctx, runtimeCtx.PVName, metav1.GetOptions{})
			if err != nil || !publishPVMatches(pv, runtimeCtx, firstNonEmpty(runtime.driverName, DefaultDriverName), volumeID) {
				return nil, attachmentEvidenceUnavailable("recovery control volume changed before refresh", err)
			}
			binding := runtime.volumeRuntimeContext(ctx, pv)
			if binding.PVCUID != runtimeCtx.PVCUID || binding.PVCName != runtimeCtx.PVCName ||
				binding.PVCNamespace != runtimeCtx.PVCNamespace || binding.pvcDeleting != runtimeCtx.pvcDeleting ||
				!samePublishAnnotations(runtimeCtx.PVCAnnotations, binding.PVCAnnotations) {
				return nil, attachmentEvidenceUnavailable("recovery control claim changed before refresh", nil)
			}
		}
		if !resolved.Active() && !resolved.Expired && !resolved.Invalid {
			return nil, nil
		}
		return &resolved, nil
	})
	return resolved, err
}

func (m *VolumeRecoveryControlManager) readAdoption(ctx context.Context, volume string) (VolumeRecoveryControlState, error) {
	var adoption VolumeRecoveryControlState
	cm, err := m.runtime.GetConfigMap(ctx, m.namespace, volumeRecoveryControlStateConfigMapName)
	if err != nil {
		return adoption, err
	}
	err = decodeCephRetentionProtection(cm.Data[volume], &adoption)
	return adoption, err
}

func (s *ControllerServer) persistPublishAdoption(ctx context.Context, volume string, binding *VolumeRuntimeContext, disk successfulAttachmentObservation, metadata map[string]string, verifyPublish func() error) (func(context.Context) error, time.Time, error) {
	manager := s.driver.volumeRecoveryControl
	unverified := attachmentEvidenceUnavailable("Publish guest adoption evidence is unverified", nil)
	if manager == nil || manager.runtime == nil || !manager.runtime.enabled ||
		binding == nil || !binding.verifiedClaimBinding || binding.pvUID == "" || binding.PVCUID == "" ||
		disk.vmID <= 0 || disk.nodeUID == "" || disk.diskID < 0 || disk.target == "" ||
		disk.serial != fmt.Sprintf("onecsi-%d", disk.imageID) {
		return nil, time.Time{}, unverified
	}
	control := resolvedRecoveryControlState(volume, binding, VolumeRecoveryControlState{})
	guest := normalizedRecoveryDeviceName(control.ConfirmedVolumeName)
	if !control.AdoptionRequested() || control.Invalid || guest == "" || guest != metadata["volumeName"] ||
		control.Ticket != metadata[publishContextRecoveryTicket] || !strings.EqualFold(control.ConfirmedDeviceSerial, disk.serial) {
		return nil, time.Time{}, unverified
	}
	now := time.Now().UTC()
	control.Adopted = true
	control.AdoptedNode = disk.nodeName
	control.AdoptedAt = &now
	control.AdoptedVolumeName = guest
	control.AdoptedDeviceSerial = disk.serial
	verifyDemand := func(ctx context.Context) error {
		desired, _, err := manager.runtime.VolumeDesiredOnNode(ctx, volume, disk.nodeName)
		if err != nil || !desired {
			return unverified
		}
		return nil
	}
	if _, err := manager.updateEntry(ctx, volume, func(current VolumeRecoveryControlState) (*VolumeRecoveryControlState, error) {
		if current.AdoptedAt != nil && !current.AdoptedAt.Before(now) {
			return nil, unverified
		}
		if err := verifyDemand(ctx); err != nil {
			return nil, err
		}
		if err := verifyPublish(); err != nil {
			return nil, err
		}
		if current.Mode == control.Mode && reflect.DeepEqual(current.ExpiresAt, control.ExpiresAt) &&
			current.Ticket == control.Ticket && current.PVName == control.PVName &&
			current.PVCName == control.PVCName && current.PVCNamespace == control.PVCNamespace {
			control.ExpiryWarningEmittedAt = current.ExpiryWarningEmittedAt
		}
		return &control, nil
	}); err != nil {
		return nil, time.Time{}, unverified
	}
	verify := func(ctx context.Context) error {
		current, err := manager.readAdoption(ctx, volume)
		current.LastObservedAt = control.LastObservedAt
		current.ExpiryWarningEmittedAt = control.ExpiryWarningEmittedAt
		if err != nil || !reflect.DeepEqual(current, control) {
			return unverified
		}
		return verifyDemand(ctx)
	}
	return verify, now, verify(ctx)
}

func (m *VolumeRecoveryControlManager) verifyAdoptedGuestTarget(ctx context.Context, volume, node, pvName string, pvc *corev1.PersistentVolumeClaim, history VolumeHistoryRecord, metadata map[string]string) error {
	unverified := attachmentEvidenceUnavailable("adopted guest device does not match the current attachment", nil)
	guest := strings.TrimPrefix(strings.TrimSpace(metadata["volumeName"]), "/dev/")
	if m == nil || m.runtime == nil || !m.runtime.enabled || metadata[publishContextRecoveryAdoptedDevice] != "true" ||
		guest == "" || pvc == nil || pvc.UID == "" || pvc.DeletionTimestamp != nil ||
		pvc.Status.Phase != corev1.ClaimBound || pvName == "" || pvc.Spec.VolumeName != pvName ||
		metadata[paramPVName] != pvName || metadata[paramPVCName] != pvc.Name || metadata[paramPVCNamespace] != pvc.Namespace ||
		history.VolumeID != volume || history.PVName != pvName || history.PVCName != pvc.Name || history.PVCNamespace != pvc.Namespace ||
		history.LastSuccessfulNodeName != node || history.LastSuccessfulNodeUID == "" ||
		history.LastSuccessfulPublishTime.IsZero() || history.LastSuccessfulOpenNebulaVMID <= 0 ||
		history.LastSuccessfulImageID <= 0 || history.LastSuccessfulDiskID == nil || history.LastSuccessfulTarget == "" ||
		history.LastSuccessfulDeviceSerial != fmt.Sprintf("onecsi-%d", history.LastSuccessfulImageID) ||
		metadata[publishContextDeviceSerial] != history.LastSuccessfulDeviceSerial ||
		metadata[publishContextOpenNebulaImageID] == "" || validateAttachmentImageAssertions(metadata, history.LastSuccessfulImageID) != nil {
		return unverified
	}
	adoption, err := m.readAdoption(ctx, volume)
	if err != nil {
		return attachmentEvidenceUnavailable("adopted guest device evidence is unreadable", err)
	}
	if adoption.VolumeID != volume || adoption.Version != stateObjectVersion || !adoption.Adopted || adoption.Invalid ||
		adoption.AdoptedAt == nil || adoption.AdoptedAt.Before(history.LastSuccessfulPublishTime) ||
		adoption.AdoptedAt.Before(pvc.CreationTimestamp.Time) ||
		adoption.PVName != pvName || adoption.PVCName != pvc.Name || adoption.PVCNamespace != pvc.Namespace ||
		adoption.AdoptedNode != node || adoption.AdoptedVolumeName != guest || adoption.AdoptedDeviceSerial != history.LastSuccessfulDeviceSerial ||
		adoption.Ticket != metadata[publishContextRecoveryTicket] {
		return unverified
	}
	return nil
}

func (m *VolumeRecoveryControlManager) markExpiryWarningEmitted(ctx context.Context, expected VolumeRecoveryControlState) error {
	_, err := m.updateEntry(ctx, expected.VolumeID, func(current VolumeRecoveryControlState) (*VolumeRecoveryControlState, error) {
		if current.VolumeID == expected.VolumeID && current.PVName == expected.PVName &&
			current.PVCName == expected.PVCName && current.PVCNamespace == expected.PVCNamespace &&
			current.Mode == expected.Mode && current.Ticket == expected.Ticket && reflect.DeepEqual(current.ExpiresAt, expected.ExpiresAt) &&
			current.ExpiryWarningEmittedAt == nil {
			now := time.Now().UTC()
			current.ExpiryWarningEmittedAt = &now
		}
		return &current, nil
	})
	return err
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
	_ = s.driver.volumeRecoveryControl.markExpiryWarningEmitted(ctx, state)
}

func (s *ControllerServer) publishContextForRecoveryAdoption(ctx context.Context, volumeID string, observation successfulAttachmentObservation, sourceContext map[string]string, control VolumeRecoveryControlState, deviceDiscoveryTimeout time.Duration) map[string]string {
	publishContext := s.publishContextForVolume(ctx, volumeID, observation, deviceDiscoveryTimeout, sourceContext)
	publishContext["volumeName"] = normalizedRecoveryDeviceName(control.ConfirmedVolumeName)
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
	if report, ok, err := s.latestLocalDeviceReport(ctx, volumeID); err != nil {
		return "", err
	} else if ok && strings.EqualFold(strings.TrimSpace(report.Node), requestedNode) {
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
