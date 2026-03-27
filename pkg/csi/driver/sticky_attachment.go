package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
)

const stickyAttachmentStateConfigMapName = "opennebula-csi-sticky-attachment-state"

type StickyAttachmentState struct {
	VolumeID           string    `json:"volumeID"`
	NodeID             string    `json:"nodeID"`
	Backend            string    `json:"backend"`
	DatastoreID        int       `json:"datastoreID"`
	PVCNamespace       string    `json:"pvcNamespace"`
	PVCName            string    `json:"pvcName"`
	StartedAt          time.Time `json:"startedAt"`
	ExpiresAt          time.Time `json:"expiresAt"`
	GraceSeconds       int       `json:"graceSeconds"`
	Reason             string    `json:"reason"`
	LastKnownNodeReady bool      `json:"lastKnownNodeReady"`
}

type StickyAttachmentManager struct {
	mu        sync.RWMutex
	runtime   *KubeRuntime
	namespace string
	entries   map[string]StickyAttachmentState
}

func NewStickyAttachmentManager(runtime *KubeRuntime, namespace string) *StickyAttachmentManager {
	return &StickyAttachmentManager{
		runtime:   runtime,
		namespace: namespace,
		entries:   map[string]StickyAttachmentState{},
	}
}

func (m *StickyAttachmentManager) StartGrace(state StickyAttachmentState) error {
	if m == nil {
		return nil
	}
	if strings.TrimSpace(state.VolumeID) == "" {
		return fmt.Errorf("volumeID is required")
	}
	m.mu.Lock()
	m.entries[state.VolumeID] = state
	m.mu.Unlock()
	if err := m.persistEntry(context.Background(), state); err != nil {
		m.mu.Lock()
		delete(m.entries, state.VolumeID)
		m.mu.Unlock()
		return err
	}
	return nil
}

func (m *StickyAttachmentManager) Get(volumeID string) (StickyAttachmentState, bool) {
	if m == nil {
		return StickyAttachmentState{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.entries[volumeID]
	return state, ok
}

func (m *StickyAttachmentManager) Clear(volumeID string) error {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return nil
	}
	m.mu.Lock()
	delete(m.entries, volumeID)
	m.mu.Unlock()
	return m.deleteEntry(context.Background(), volumeID)
}

func (m *StickyAttachmentManager) ListExpired(now time.Time) []StickyAttachmentState {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	expired := make([]StickyAttachmentState, 0)
	for _, entry := range m.entries {
		if !entry.ExpiresAt.After(now) {
			expired = append(expired, entry)
		}
	}
	return expired
}

func (m *StickyAttachmentManager) LoadFromConfigMap(ctx context.Context) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	cm, err := m.runtime.GetConfigMap(ctx, m.namespace, stickyAttachmentStateConfigMapName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	loaded := map[string]StickyAttachmentState{}
	for key, raw := range cm.Data {
		var state StickyAttachmentState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			klog.Warningf("failed to decode sticky attachment state %s: %v", key, err)
			continue
		}
		if strings.TrimSpace(state.VolumeID) == "" {
			state.VolumeID = key
		}
		loaded[state.VolumeID] = state
	}

	m.mu.Lock()
	m.entries = loaded
	m.mu.Unlock()
	return nil
}

func (m *StickyAttachmentManager) persistEntry(ctx context.Context, state StickyAttachmentState) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return m.runtime.UpsertConfigMapData(ctx, m.namespace, stickyAttachmentStateConfigMapName, map[string]string{
		state.VolumeID: string(payload),
	})
}

func (m *StickyAttachmentManager) deleteEntry(ctx context.Context, volumeID string) error {
	if m == nil || m.runtime == nil || !m.runtime.enabled {
		return nil
	}
	return m.runtime.DeleteConfigMapKey(ctx, m.namespace, stickyAttachmentStateConfigMapName, volumeID)
}
