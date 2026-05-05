package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
)

const (
	volumeHistoryStateConfigMapName = "opennebula-csi-volume-history-state"
	volumeRepairStateConfigMapName  = "opennebula-csi-volume-repair-state"
	stateObjectVersion              = 1
)

type LocalDiskIdentity struct {
	Version              int                           `json:"version"`
	ObservedAt           time.Time                     `json:"observedAt,omitempty"`
	AssertedByController *LocalDiskControllerAssertion `json:"assertedByController,omitempty"`
	ObservedFromDevice   *LocalDiskObservedIdentity    `json:"observedFromDevice,omitempty"`

	LegacyDevicePath          string `json:"devicePath,omitempty"`
	LegacyDeviceSerial        string `json:"deviceSerial,omitempty"`
	LegacyFilesystemUUID      string `json:"filesystemUUID,omitempty"`
	LegacyFilesystemType      string `json:"filesystemType,omitempty"`
	LegacyPartitionUUID       string `json:"partitionUUID,omitempty"`
	LegacyOpenNebulaImageID   string `json:"openNebulaImageID,omitempty"`
	LegacyDiskTarget          string `json:"diskTarget,omitempty"`
	LegacyMountSourceIdentity string `json:"mountSourceIdentity,omitempty"`
}

type LocalDiskControllerAssertion struct {
	DeviceSerial      string `json:"deviceSerial,omitempty"`
	OpenNebulaImageID string `json:"openNebulaImageID,omitempty"`
	DiskTarget        string `json:"diskTarget,omitempty"`
}

type LocalDiskObservedBlockIdentity struct {
	DevicePath          string `json:"devicePath,omitempty"`
	ByIDPath            string `json:"byIDPath,omitempty"`
	DeviceSerial        string `json:"deviceSerial,omitempty"`
	MountSourceIdentity string `json:"mountSourceIdentity,omitempty"`
}

type LocalDiskObservedFilesystemIdentity struct {
	FilesystemUUID string `json:"filesystemUUID,omitempty"`
	FilesystemType string `json:"filesystemType,omitempty"`
	PartitionUUID  string `json:"partitionUUID,omitempty"`
}

type LocalDiskObservedIdentity struct {
	Block      *LocalDiskObservedBlockIdentity      `json:"block,omitempty"`
	Filesystem *LocalDiskObservedFilesystemIdentity `json:"filesystem,omitempty"`
}

type VolumeHistoryRecord struct {
	Version                      int                `json:"version"`
	VolumeID                     string             `json:"volumeID"`
	PVName                       string             `json:"pvName,omitempty"`
	PVCNamespace                 string             `json:"pvcNamespace,omitempty"`
	PVCName                      string             `json:"pvcName,omitempty"`
	Backend                      string             `json:"backend,omitempty"`
	LastSuccessfulNodeName       string             `json:"lastSuccessfulNodeName,omitempty"`
	LastSuccessfulNodeUID        string             `json:"lastSuccessfulNodeUID,omitempty"`
	LastSuccessfulOpenNebulaVMID int                `json:"lastSuccessfulOpenNebulaVMID,omitempty"`
	LastSuccessfulImageID        int                `json:"lastSuccessfulImageID,omitempty"`
	LastSuccessfulDiskID         int                `json:"lastSuccessfulDiskID,omitempty"`
	LastSuccessfulTarget         string             `json:"lastSuccessfulTarget,omitempty"`
	LastSuccessfulDeviceSerial   string             `json:"lastSuccessfulDeviceSerial,omitempty"`
	LastSuccessfulPublishTime    time.Time          `json:"lastSuccessfulPublishTime,omitempty"`
	LastSuccessfulStageTime      time.Time          `json:"lastSuccessfulStageTime,omitempty"`
	LastSafeDetachNodeName       string             `json:"lastSafeDetachNodeName,omitempty"`
	LastSafeDetachTime           time.Time          `json:"lastSafeDetachTime,omitempty"`
	LastHealthyIdentity          *LocalDiskIdentity `json:"lastHealthyIdentity,omitempty"`
}

type VolumeRepairState struct {
	Version                 int                `json:"version"`
	VolumeID                string             `json:"volumeID"`
	Classification          string             `json:"classification"`
	Reason                  string             `json:"reason,omitempty"`
	Message                 string             `json:"message,omitempty"`
	RequestedNode           string             `json:"requestedNode,omitempty"`
	LastKnownNodeName       string             `json:"lastKnownNodeName,omitempty"`
	LastKnownNodeUID        string             `json:"lastKnownNodeUID,omitempty"`
	LastKnownOpenNebulaVMID int                `json:"lastKnownOpenNebulaVMID,omitempty"`
	LastKnownImageID        int                `json:"lastKnownImageID,omitempty"`
	LastKnownDiskID         int                `json:"lastKnownDiskID,omitempty"`
	LastKnownTarget         string             `json:"lastKnownTarget,omitempty"`
	LastKnownDeviceSerial   string             `json:"lastKnownDeviceSerial,omitempty"`
	EvidenceSource          string             `json:"evidenceSource,omitempty"`
	FailureCount            int                `json:"failureCount"`
	FirstObservedAt         time.Time          `json:"firstObservedAt"`
	LastObservedAt          time.Time          `json:"lastObservedAt"`
	LastObservedIdentity    *LocalDiskIdentity `json:"lastObservedIdentity,omitempty"`
	LastHealthyIdentity     *LocalDiskIdentity `json:"lastHealthyIdentity,omitempty"`
}

type VolumeHistoryManager struct {
	mu        sync.RWMutex
	runtime   *KubeRuntime
	namespace string
	entries   map[string]VolumeHistoryRecord
}

func NewVolumeHistoryManager(runtime *KubeRuntime, namespace string) *VolumeHistoryManager {
	if strings.TrimSpace(namespace) == "" {
		namespace = namespaceFromServiceAccount()
	}
	return &VolumeHistoryManager{
		runtime:   runtime,
		namespace: namespace,
		entries:   map[string]VolumeHistoryRecord{},
	}
}

func (m *VolumeHistoryManager) LoadFromConfigMap(ctx context.Context) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	cm, err := m.runtime.GetConfigMap(ctx, m.namespace, volumeHistoryStateConfigMapName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	loaded := map[string]VolumeHistoryRecord{}
	for key, raw := range cm.Data {
		var state VolumeHistoryRecord
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			klog.Warningf("failed to decode volume history state %s: %v", key, err)
			continue
		}
		if strings.TrimSpace(state.VolumeID) == "" {
			state.VolumeID = key
		}
		state.Version = stateObjectVersion
		normalizeLocalDiskIdentity(state.LastHealthyIdentity)
		loaded[state.VolumeID] = state
	}
	m.mu.Lock()
	m.entries = loaded
	m.mu.Unlock()
	return nil
}

func (m *VolumeHistoryManager) Get(volumeID string) (VolumeHistoryRecord, bool) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeHistoryRecord{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.entries[strings.TrimSpace(volumeID)]
	return state, ok
}

func (m *VolumeHistoryManager) Upsert(ctx context.Context, volumeID string, mutate func(*VolumeHistoryRecord)) (VolumeHistoryRecord, error) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeHistoryRecord{}, nil
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
	normalizeLocalDiskIdentity(state.LastHealthyIdentity)
	m.entries[volumeID] = state
	m.mu.Unlock()
	if err := m.persistEntry(ctx, state); err != nil {
		return state, err
	}
	return state, nil
}

func (m *VolumeHistoryManager) Clear(ctx context.Context, volumeID string) error {
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
	return m.runtime.DeleteConfigMapKey(ctx, m.namespace, volumeHistoryStateConfigMapName, volumeID)
}

func (m *VolumeHistoryManager) Snapshot() map[string]VolumeHistoryRecord {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot := make(map[string]VolumeHistoryRecord, len(m.entries))
	for key, state := range m.entries {
		snapshot[key] = state
	}
	return snapshot
}

func (m *VolumeHistoryManager) persistEntry(ctx context.Context, state VolumeHistoryRecord) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal volume history state: %w", err)
	}
	return m.runtime.UpsertConfigMapData(ctx, m.namespace, volumeHistoryStateConfigMapName, map[string]string{
		state.VolumeID: string(payload),
	})
}

type VolumeRepairStateManager struct {
	mu        sync.RWMutex
	runtime   *KubeRuntime
	namespace string
	entries   map[string]VolumeRepairState
}

func NewVolumeRepairStateManager(runtime *KubeRuntime, namespace string) *VolumeRepairStateManager {
	if strings.TrimSpace(namespace) == "" {
		namespace = namespaceFromServiceAccount()
	}
	return &VolumeRepairStateManager{
		runtime:   runtime,
		namespace: namespace,
		entries:   map[string]VolumeRepairState{},
	}
}

func (m *VolumeRepairStateManager) LoadFromConfigMap(ctx context.Context) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	cm, err := m.runtime.GetConfigMap(ctx, m.namespace, volumeRepairStateConfigMapName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	loaded := map[string]VolumeRepairState{}
	for key, raw := range cm.Data {
		var state VolumeRepairState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			klog.Warningf("failed to decode volume repair state %s: %v", key, err)
			continue
		}
		if strings.TrimSpace(state.VolumeID) == "" {
			state.VolumeID = key
		}
		state.Version = stateObjectVersion
		normalizeLocalDiskIdentity(state.LastObservedIdentity)
		normalizeLocalDiskIdentity(state.LastHealthyIdentity)
		loaded[state.VolumeID] = state
	}
	m.mu.Lock()
	m.entries = loaded
	m.mu.Unlock()
	return nil
}

func (m *VolumeRepairStateManager) Get(volumeID string) (VolumeRepairState, bool) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeRepairState{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.entries[strings.TrimSpace(volumeID)]
	return state, ok
}

func (m *VolumeRepairStateManager) Mark(ctx context.Context, state VolumeRepairState) (VolumeRepairState, error) {
	if m == nil || strings.TrimSpace(state.VolumeID) == "" {
		return state, nil
	}
	state.VolumeID = strings.TrimSpace(state.VolumeID)
	state.Version = stateObjectVersion
	state.Classification = strings.TrimSpace(state.Classification)
	state.RequestedNode = strings.TrimSpace(state.RequestedNode)
	state.LastKnownNodeName = strings.TrimSpace(state.LastKnownNodeName)
	state.LastKnownNodeUID = strings.TrimSpace(state.LastKnownNodeUID)
	state.LastKnownTarget = strings.TrimSpace(state.LastKnownTarget)
	state.LastKnownDeviceSerial = strings.TrimSpace(state.LastKnownDeviceSerial)
	state.EvidenceSource = strings.TrimSpace(state.EvidenceSource)
	normalizeLocalDiskIdentity(state.LastObservedIdentity)
	normalizeLocalDiskIdentity(state.LastHealthyIdentity)
	now := time.Now().UTC()
	state.LastObservedAt = now

	m.mu.Lock()
	previous, hadPrevious := m.entries[state.VolumeID]
	if hadPrevious && sameRepairStateSeries(previous, state) {
		state.FirstObservedAt = previous.FirstObservedAt
		state.FailureCount = previous.FailureCount + 1
	} else {
		state.FirstObservedAt = now
		state.FailureCount = 1
	}
	m.entries[state.VolumeID] = state
	m.mu.Unlock()

	if err := m.persistEntry(ctx, state); err != nil {
		return state, err
	}
	return state, nil
}

func (m *VolumeRepairStateManager) Clear(ctx context.Context, volumeID string) error {
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
	return m.runtime.DeleteConfigMapKey(ctx, m.namespace, volumeRepairStateConfigMapName, volumeID)
}

func (m *VolumeRepairStateManager) Snapshot() map[string]VolumeRepairState {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot := make(map[string]VolumeRepairState, len(m.entries))
	for key, state := range m.entries {
		snapshot[key] = state
	}
	return snapshot
}

func (m *VolumeRepairStateManager) persistEntry(ctx context.Context, state VolumeRepairState) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal volume repair state: %w", err)
	}
	return m.runtime.UpsertConfigMapData(ctx, m.namespace, volumeRepairStateConfigMapName, map[string]string{
		state.VolumeID: string(payload),
	})
}

func sameRepairStateSeries(previous, next VolumeRepairState) bool {
	return strings.TrimSpace(previous.Classification) == strings.TrimSpace(next.Classification) &&
		strings.TrimSpace(previous.RequestedNode) == strings.TrimSpace(next.RequestedNode) &&
		strings.TrimSpace(previous.LastKnownNodeName) == strings.TrimSpace(next.LastKnownNodeName) &&
		previous.LastKnownImageID == next.LastKnownImageID &&
		previous.LastKnownDiskID == next.LastKnownDiskID
}

func normalizeLocalDiskIdentity(identity *LocalDiskIdentity) {
	if identity == nil {
		return
	}
	identity.Version = stateObjectVersion
	if identity.AssertedByController == nil && (identity.LegacyDeviceSerial != "" || identity.LegacyOpenNebulaImageID != "" || identity.LegacyDiskTarget != "") {
		identity.AssertedByController = &LocalDiskControllerAssertion{
			DeviceSerial:      identity.LegacyDeviceSerial,
			OpenNebulaImageID: identity.LegacyOpenNebulaImageID,
			DiskTarget:        identity.LegacyDiskTarget,
		}
	}
	if identity.ObservedFromDevice == nil && (identity.LegacyDevicePath != "" || identity.LegacyDeviceSerial != "" || identity.LegacyFilesystemUUID != "" || identity.LegacyFilesystemType != "" || identity.LegacyPartitionUUID != "" || identity.LegacyMountSourceIdentity != "") {
		identity.ObservedFromDevice = &LocalDiskObservedIdentity{
			Block: &LocalDiskObservedBlockIdentity{
				DevicePath:          identity.LegacyDevicePath,
				DeviceSerial:        identity.LegacyDeviceSerial,
				MountSourceIdentity: identity.LegacyMountSourceIdentity,
			},
			Filesystem: &LocalDiskObservedFilesystemIdentity{
				FilesystemUUID: identity.LegacyFilesystemUUID,
				FilesystemType: identity.LegacyFilesystemType,
				PartitionUUID:  identity.LegacyPartitionUUID,
			},
		}
	}
	if identity.AssertedByController != nil {
		identity.AssertedByController.DeviceSerial = strings.TrimSpace(identity.AssertedByController.DeviceSerial)
		identity.AssertedByController.OpenNebulaImageID = strings.TrimSpace(identity.AssertedByController.OpenNebulaImageID)
		identity.AssertedByController.DiskTarget = strings.TrimSpace(identity.AssertedByController.DiskTarget)
		if identity.AssertedByController.DeviceSerial == "" && identity.AssertedByController.OpenNebulaImageID == "" && identity.AssertedByController.DiskTarget == "" {
			identity.AssertedByController = nil
		}
	}
	if identity.ObservedFromDevice != nil {
		if identity.ObservedFromDevice.Block != nil {
			identity.ObservedFromDevice.Block.DevicePath = strings.TrimSpace(identity.ObservedFromDevice.Block.DevicePath)
			identity.ObservedFromDevice.Block.ByIDPath = strings.TrimSpace(identity.ObservedFromDevice.Block.ByIDPath)
			identity.ObservedFromDevice.Block.DeviceSerial = strings.TrimSpace(identity.ObservedFromDevice.Block.DeviceSerial)
			identity.ObservedFromDevice.Block.MountSourceIdentity = strings.TrimSpace(identity.ObservedFromDevice.Block.MountSourceIdentity)
			if identity.ObservedFromDevice.Block.DevicePath == "" && identity.ObservedFromDevice.Block.ByIDPath == "" && identity.ObservedFromDevice.Block.DeviceSerial == "" && identity.ObservedFromDevice.Block.MountSourceIdentity == "" {
				identity.ObservedFromDevice.Block = nil
			}
		}
		if identity.ObservedFromDevice.Filesystem != nil {
			identity.ObservedFromDevice.Filesystem.FilesystemUUID = strings.TrimSpace(identity.ObservedFromDevice.Filesystem.FilesystemUUID)
			identity.ObservedFromDevice.Filesystem.FilesystemType = strings.TrimSpace(identity.ObservedFromDevice.Filesystem.FilesystemType)
			identity.ObservedFromDevice.Filesystem.PartitionUUID = strings.TrimSpace(identity.ObservedFromDevice.Filesystem.PartitionUUID)
			if identity.ObservedFromDevice.Filesystem.FilesystemUUID == "" && identity.ObservedFromDevice.Filesystem.FilesystemType == "" && identity.ObservedFromDevice.Filesystem.PartitionUUID == "" {
				identity.ObservedFromDevice.Filesystem = nil
			}
		}
		if identity.ObservedFromDevice.Block == nil && identity.ObservedFromDevice.Filesystem == nil {
			identity.ObservedFromDevice = nil
		}
	}
	identity.LegacyDevicePath = ""
	identity.LegacyDeviceSerial = ""
	identity.LegacyFilesystemUUID = ""
	identity.LegacyFilesystemType = ""
	identity.LegacyPartitionUUID = ""
	identity.LegacyOpenNebulaImageID = ""
	identity.LegacyDiskTarget = ""
	identity.LegacyMountSourceIdentity = ""
}

func localDiskAssertedDeviceSerial(identity *LocalDiskIdentity) string {
	if identity == nil || identity.AssertedByController == nil {
		return ""
	}
	return strings.TrimSpace(identity.AssertedByController.DeviceSerial)
}

func localDiskAssertedImageID(identity *LocalDiskIdentity) string {
	if identity == nil || identity.AssertedByController == nil {
		return ""
	}
	return strings.TrimSpace(identity.AssertedByController.OpenNebulaImageID)
}

func localDiskAssertedDiskTarget(identity *LocalDiskIdentity) string {
	if identity == nil || identity.AssertedByController == nil {
		return ""
	}
	return strings.TrimSpace(identity.AssertedByController.DiskTarget)
}

func localDiskObservedDevicePath(identity *LocalDiskIdentity) string {
	if identity == nil || identity.ObservedFromDevice == nil || identity.ObservedFromDevice.Block == nil {
		return ""
	}
	return strings.TrimSpace(identity.ObservedFromDevice.Block.DevicePath)
}

func localDiskObservedByIDPath(identity *LocalDiskIdentity) string {
	if identity == nil || identity.ObservedFromDevice == nil || identity.ObservedFromDevice.Block == nil {
		return ""
	}
	return strings.TrimSpace(identity.ObservedFromDevice.Block.ByIDPath)
}

func localDiskObservedDeviceSerial(identity *LocalDiskIdentity) string {
	if identity == nil || identity.ObservedFromDevice == nil || identity.ObservedFromDevice.Block == nil {
		return ""
	}
	return strings.TrimSpace(identity.ObservedFromDevice.Block.DeviceSerial)
}

func localDiskObservedMountSourceIdentity(identity *LocalDiskIdentity) string {
	if identity == nil || identity.ObservedFromDevice == nil || identity.ObservedFromDevice.Block == nil {
		return ""
	}
	return strings.TrimSpace(identity.ObservedFromDevice.Block.MountSourceIdentity)
}

func localDiskObservedFilesystemUUID(identity *LocalDiskIdentity) string {
	if identity == nil || identity.ObservedFromDevice == nil || identity.ObservedFromDevice.Filesystem == nil {
		return ""
	}
	return strings.TrimSpace(identity.ObservedFromDevice.Filesystem.FilesystemUUID)
}

func localDiskObservedFilesystemType(identity *LocalDiskIdentity) string {
	if identity == nil || identity.ObservedFromDevice == nil || identity.ObservedFromDevice.Filesystem == nil {
		return ""
	}
	return strings.TrimSpace(identity.ObservedFromDevice.Filesystem.FilesystemType)
}

func localDiskObservedPartitionUUID(identity *LocalDiskIdentity) string {
	if identity == nil || identity.ObservedFromDevice == nil || identity.ObservedFromDevice.Filesystem == nil {
		return ""
	}
	return strings.TrimSpace(identity.ObservedFromDevice.Filesystem.PartitionUUID)
}

func historyRequiresSafeRelease(state VolumeHistoryRecord) bool {
	if strings.TrimSpace(state.LastSuccessfulNodeName) == "" || state.LastSuccessfulPublishTime.IsZero() {
		return false
	}
	if state.LastSafeDetachTime.IsZero() {
		return true
	}
	return state.LastSafeDetachTime.Before(state.LastSuccessfulPublishTime)
}

func sortedRepairStates(snapshot map[string]VolumeRepairState) []VolumeRepairState {
	if len(snapshot) == 0 {
		return nil
	}
	states := make([]VolumeRepairState, 0, len(snapshot))
	for _, state := range snapshot {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].VolumeID < states[j].VolumeID
	})
	return states
}
