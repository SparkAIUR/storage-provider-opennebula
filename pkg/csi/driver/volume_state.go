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

	volumeHistoryEvidenceSourceObserved  = "observed"
	volumeHistoryEvidenceSourceBootstrap = "bootstrap_runtime_annotations"
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
	DatastoreID                  int                `json:"datastoreID,omitempty"`
	DatastoreName                string             `json:"datastoreName,omitempty"`
	RestartOptimization          string             `json:"restartOptimization,omitempty"`
	EvidenceSource               string             `json:"evidenceSource,omitempty"`
	Bootstrapped                 bool               `json:"bootstrapped,omitempty"`
	BootstrappedAt               time.Time          `json:"bootstrappedAt,omitempty"`
	BootstrappedFields           []string           `json:"bootstrappedFields,omitempty"`
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
		normalizeVolumeHistoryRecord(&state)
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
	normalizeVolumeHistoryRecord(&state)
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

func (m *VolumeHistoryManager) RefreshEntry(ctx context.Context, volumeID string) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled || strings.TrimSpace(volumeID) == "" {
		return nil
	}
	volumeID = strings.TrimSpace(volumeID)
	cm, err := m.runtime.GetConfigMap(ctx, m.namespace, volumeHistoryStateConfigMapName)
	if err != nil {
		if errors.IsNotFound(err) {
			m.mu.Lock()
			delete(m.entries, volumeID)
			m.mu.Unlock()
			return nil
		}
		return err
	}
	raw := strings.TrimSpace(cm.Data[volumeID])
	if raw == "" {
		m.mu.Lock()
		delete(m.entries, volumeID)
		m.mu.Unlock()
		return nil
	}
	var state VolumeHistoryRecord
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return err
	}
	if strings.TrimSpace(state.VolumeID) == "" {
		state.VolumeID = volumeID
	}
	normalizeVolumeHistoryRecord(&state)
	m.mu.Lock()
	m.entries[volumeID] = state
	m.mu.Unlock()
	return nil
}

func (m *VolumeHistoryManager) SeedFromRuntimeContext(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext) (VolumeHistoryRecord, bool, error) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return VolumeHistoryRecord{}, false, nil
	}
	volumeID = strings.TrimSpace(volumeID)
	existing, _ := m.Get(volumeID)
	if volumeHistoryHasObservedSuccess(existing) {
		return existing, false, nil
	}
	seed, ok := buildVolumeHistoryBootstrapEvidence(ctx, m.runtime, volumeID, runtimeCtx)
	if !ok {
		return existing, false, nil
	}
	merged, err := m.Upsert(ctx, volumeID, func(state *VolumeHistoryRecord) {
		if volumeHistoryHasObservedSuccess(*state) {
			return
		}
		mergeVolumeHistoryBootstrap(state, seed)
	})
	if err != nil {
		return merged, false, err
	}
	return merged, true, nil
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

func normalizeVolumeHistoryRecord(state *VolumeHistoryRecord) {
	if state == nil {
		return
	}
	state.Version = stateObjectVersion
	state.VolumeID = strings.TrimSpace(state.VolumeID)
	state.PVName = strings.TrimSpace(state.PVName)
	state.PVCNamespace = strings.TrimSpace(state.PVCNamespace)
	state.PVCName = strings.TrimSpace(state.PVCName)
	state.Backend = strings.TrimSpace(state.Backend)
	state.DatastoreName = strings.TrimSpace(state.DatastoreName)
	state.RestartOptimization = strings.TrimSpace(state.RestartOptimization)
	state.EvidenceSource = strings.TrimSpace(state.EvidenceSource)
	state.LastSuccessfulNodeName = strings.TrimSpace(state.LastSuccessfulNodeName)
	state.LastSuccessfulNodeUID = strings.TrimSpace(state.LastSuccessfulNodeUID)
	state.LastSuccessfulTarget = strings.TrimSpace(state.LastSuccessfulTarget)
	state.LastSuccessfulDeviceSerial = strings.TrimSpace(state.LastSuccessfulDeviceSerial)
	state.LastSafeDetachNodeName = strings.TrimSpace(state.LastSafeDetachNodeName)
	state.BootstrappedFields = compactSortedStrings(state.BootstrappedFields)
	if volumeHistoryHasObservedSuccess(*state) {
		state.EvidenceSource = volumeHistoryEvidenceSourceObserved
		state.Bootstrapped = false
		state.BootstrappedAt = time.Time{}
		state.BootstrappedFields = nil
	} else if state.Bootstrapped && state.EvidenceSource == "" {
		state.EvidenceSource = volumeHistoryEvidenceSourceBootstrap
	}
	normalizeLocalDiskIdentity(state.LastHealthyIdentity)
}

func compactSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	compacted := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		compacted = append(compacted, value)
	}
	sort.Strings(compacted)
	return compacted
}

func volumeHistoryHasObservedSuccess(state VolumeHistoryRecord) bool {
	return !state.LastSuccessfulPublishTime.IsZero() || !state.LastSuccessfulStageTime.IsZero()
}

func volumeHistoryEvidenceSource(state VolumeHistoryRecord) string {
	if volumeHistoryHasObservedSuccess(state) {
		return volumeHistoryEvidenceSourceObserved
	}
	return strings.TrimSpace(state.EvidenceSource)
}

func volumeHistoryHasLocalHistoricalEvidence(state VolumeHistoryRecord, runtimeCtx *VolumeRuntimeContext) bool {
	backend := firstNonEmpty(strings.TrimSpace(state.Backend), volumeRuntimeAnnotation(runtimeCtx, annotationBackend))
	if !strings.EqualFold(backend, "local") {
		return false
	}
	if volumeHistoryHasObservedSuccess(state) {
		return true
	}
	return state.Bootstrapped && strings.TrimSpace(state.LastSuccessfulNodeName) != ""
}

func historySupportsSameNodeProtection(state VolumeHistoryRecord, runtimeCtx *VolumeRuntimeContext) bool {
	if strings.TrimSpace(state.LastSuccessfulNodeName) == "" {
		return false
	}
	return volumeHistoryHasLocalHistoricalEvidence(state, runtimeCtx)
}

func buildVolumeHistoryBootstrapEvidence(ctx context.Context, runtime *KubeRuntime, volumeID string, runtimeCtx *VolumeRuntimeContext) (VolumeHistoryRecord, bool) {
	volumeID = strings.TrimSpace(volumeID)
	if volumeID == "" || runtimeCtx == nil {
		return VolumeHistoryRecord{}, false
	}
	backend := firstNonEmpty(strings.TrimSpace(runtimeCtx.Backend), volumeRuntimeAnnotation(runtimeCtx, annotationBackend))
	if !strings.EqualFold(backend, "local") {
		return VolumeHistoryRecord{}, false
	}
	if len(runtimeCtx.AccessModes) > 0 && !hasSingleWriterAccessMode(runtimeCtx.AccessModes) {
		return VolumeHistoryRecord{}, false
	}

	lastNode, lastNodeField := bootstrappedLastAttachedNode(runtimeCtx)
	if lastNode == "" {
		return VolumeHistoryRecord{}, false
	}

	fields := []string{}
	addField := func(field string) {
		fields = append(fields, field)
	}
	record := VolumeHistoryRecord{
		Version:                      stateObjectVersion,
		VolumeID:                     volumeID,
		PVName:                       strings.TrimSpace(runtimeCtx.PVName),
		PVCNamespace:                 strings.TrimSpace(runtimeCtx.PVCNamespace),
		PVCName:                      strings.TrimSpace(runtimeCtx.PVCName),
		Backend:                      backend,
		RestartOptimization:          firstNonEmpty(strings.TrimSpace(runtimeCtx.RestartMode), volumeRuntimeAnnotation(runtimeCtx, annotationRestartOpt)),
		EvidenceSource:               volumeHistoryEvidenceSourceBootstrap,
		Bootstrapped:                 true,
		BootstrappedAt:               time.Now().UTC(),
		LastSuccessfulNodeName:       lastNode,
		LastSuccessfulNodeUID:        "",
		LastSuccessfulPublishTime:    time.Time{},
		LastSuccessfulStageTime:      time.Time{},
		LastSuccessfulOpenNebulaVMID: 0,
	}
	if record.PVName != "" {
		addField("pvName")
	}
	if record.PVCNamespace != "" {
		addField("pvcNamespace")
	}
	if record.PVCName != "" {
		addField("pvcName")
	}
	addField("backend")
	if record.RestartOptimization != "" {
		addField("restartOptimization")
	}
	if lastNodeField != "" {
		addField(lastNodeField)
	}
	if runtimeCtx.DatastoreID > 0 {
		record.DatastoreID = runtimeCtx.DatastoreID
		addField("datastoreID")
	} else if parsed := parseIntOrZero(volumeRuntimeAnnotation(runtimeCtx, annotationDatastoreID)); parsed > 0 {
		record.DatastoreID = parsed
		addField("datastoreID")
	}
	if datastoreName := volumeRuntimeAnnotation(runtimeCtx, annotationDatastoreName); datastoreName != "" {
		record.DatastoreName = datastoreName
		addField("datastoreName")
	}
	if runtime != nil && runtime.enabled {
		if identity, err := runtime.NodeIdentity(ctx, lastNode); err == nil && identity.Exists {
			record.LastSuccessfulNodeUID = strings.TrimSpace(identity.UID)
			if record.LastSuccessfulNodeUID != "" {
				addField("lastAttachedNodeUID")
			}
		}
	}
	record.BootstrappedFields = compactSortedStrings(fields)
	normalizeVolumeHistoryRecord(&record)
	return record, true
}

func mergeVolumeHistoryBootstrap(state *VolumeHistoryRecord, seed VolumeHistoryRecord) {
	if state == nil || strings.TrimSpace(seed.VolumeID) == "" || volumeHistoryHasObservedSuccess(*state) {
		return
	}
	state.VolumeID = firstNonEmpty(state.VolumeID, seed.VolumeID)
	state.PVName = firstNonEmpty(state.PVName, seed.PVName)
	state.PVCNamespace = firstNonEmpty(state.PVCNamespace, seed.PVCNamespace)
	state.PVCName = firstNonEmpty(state.PVCName, seed.PVCName)
	state.Backend = firstNonEmpty(state.Backend, seed.Backend)
	if state.DatastoreID == 0 {
		state.DatastoreID = seed.DatastoreID
	}
	state.DatastoreName = firstNonEmpty(state.DatastoreName, seed.DatastoreName)
	state.RestartOptimization = firstNonEmpty(state.RestartOptimization, seed.RestartOptimization)
	state.LastSuccessfulNodeName = firstNonEmpty(state.LastSuccessfulNodeName, seed.LastSuccessfulNodeName)
	state.LastSuccessfulNodeUID = firstNonEmpty(state.LastSuccessfulNodeUID, seed.LastSuccessfulNodeUID)
	state.EvidenceSource = volumeHistoryEvidenceSourceBootstrap
	state.Bootstrapped = true
	if state.BootstrappedAt.IsZero() {
		state.BootstrappedAt = seed.BootstrappedAt
	}
	state.BootstrappedFields = compactSortedStrings(append(state.BootstrappedFields, seed.BootstrappedFields...))
}

func volumeRuntimeAnnotation(runtimeCtx *VolumeRuntimeContext, key string) string {
	if runtimeCtx == nil || strings.TrimSpace(key) == "" {
		return ""
	}
	if value := strings.TrimSpace(runtimeCtx.PVAnnotations[key]); value != "" {
		return value
	}
	return strings.TrimSpace(runtimeCtx.PVCAnnotations[key])
}

func bootstrappedLastAttachedNode(runtimeCtx *VolumeRuntimeContext) (string, string) {
	if runtimeCtx == nil {
		return "", ""
	}
	if value := strings.TrimSpace(runtimeCtx.PVAnnotations[annotationLastAttachedNode]); value != "" {
		return value, "pv.lastAttachedNode"
	}
	if value := strings.TrimSpace(runtimeCtx.PVCAnnotations[annotationLastAttachedNode]); value != "" {
		return value, "pvc.lastAttachedNode"
	}
	if value := placementSummaryLastAttachedNode(runtimeCtx.PVAnnotations); value != "" {
		return value, "pv.placementSummary.lastAttachedNode"
	}
	if value := placementSummaryLastAttachedNode(runtimeCtx.PVCAnnotations); value != "" {
		return value, "pvc.placementSummary.lastAttachedNode"
	}
	return "", ""
}

func placementSummaryLastAttachedNode(annotations map[string]string) string {
	raw := strings.TrimSpace(annotations[annotationPlacementSummary])
	if raw == "" {
		return ""
	}
	var report PlacementReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return ""
	}
	return strings.TrimSpace(report.LastAttachedNode)
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
