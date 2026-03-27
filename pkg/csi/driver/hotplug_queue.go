package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
)

const hotplugQueueStateConfigMapName = "opennebula-csi-hotplug-queue-state"

type HotplugQueueTimeoutError struct {
	Node      string
	Operation string
	Volume    string
	Timeout   time.Duration
}

func (e *HotplugQueueTimeoutError) Error() string {
	if e == nil {
		return "hotplug queue timeout"
	}
	return fmt.Sprintf("timed out after %s waiting for hotplug queue on node %s for %s of volume %s", e.Timeout, e.Node, e.Operation, e.Volume)
}

type HotplugQueuePriority string

const (
	hotplugQueuePriorityCritical   HotplugQueuePriority = "critical"
	hotplugQueuePriorityNormal     HotplugQueuePriority = "normal"
	hotplugQueuePriorityBackground HotplugQueuePriority = "background"
)

type HotplugQueueItemSnapshot struct {
	ID         uint64               `json:"id"`
	Node       string               `json:"node"`
	Operation  string               `json:"operation"`
	Volume     string               `json:"volume"`
	Priority   HotplugQueuePriority `json:"priority"`
	EnqueuedAt time.Time            `json:"enqueuedAt"`
}

type HotplugQueueNodeSnapshot struct {
	Node             string                     `json:"node"`
	Active           *HotplugQueueItemSnapshot  `json:"active,omitempty"`
	Queued           []HotplugQueueItemSnapshot `json:"queued,omitempty"`
	QueuedCount      int                        `json:"queuedCount"`
	OldestAgeSeconds int64                      `json:"oldestAgeSeconds"`
}

type hotplugQueueRequest struct {
	id         uint64
	node       string
	operation  string
	volume     string
	priority   HotplugQueuePriority
	enqueuedAt time.Time
	ctx        context.Context
	fn         func(context.Context) error
	started    chan struct{}
	done       chan error
}

type hotplugNodeQueue struct {
	active  *hotplugQueueRequest
	pending []*hotplugQueueRequest
	running bool
}

type HotplugQueueManager struct {
	mu        sync.Mutex
	runtime   *KubeRuntime
	namespace string
	metrics   *DriverMetrics
	maxWait   time.Duration
	ageBoost  time.Duration
	seq       atomic.Uint64
	nodes     map[string]*hotplugNodeQueue
}

func NewHotplugQueueManager(runtime *KubeRuntime, namespace string, metrics *DriverMetrics, maxWait, ageBoost time.Duration) *HotplugQueueManager {
	return &HotplugQueueManager{
		runtime:   runtime,
		namespace: namespace,
		metrics:   metrics,
		maxWait:   maxWait,
		ageBoost:  ageBoost,
		nodes:     map[string]*hotplugNodeQueue{},
	}
}

func (m *HotplugQueueManager) Run(ctx context.Context, node, operation, volume string, priority HotplugQueuePriority, fn func(context.Context) error) error {
	if m == nil || node == "" {
		return fn(ctx)
	}
	req := &hotplugQueueRequest{
		id:         m.seq.Add(1),
		node:       node,
		operation:  operation,
		volume:     volume,
		priority:   priority,
		enqueuedAt: time.Now().UTC(),
		ctx:        ctx,
		fn:         fn,
		started:    make(chan struct{}),
		done:       make(chan error, 1),
	}

	waitCtx := ctx
	var cancel context.CancelFunc
	if m.maxWait > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, m.maxWait)
		defer cancel()
	}

	m.enqueue(req)
	select {
	case <-req.started:
	case <-waitCtx.Done():
		if m.cancel(req.id, node) {
			m.metrics.RecordHotplugQueueWait(operation, "timeout", time.Since(req.enqueuedAt))
			m.metrics.RecordHotplugQueueDispatch(operation, string(priority), "timeout")
			return &HotplugQueueTimeoutError{
				Node:      node,
				Operation: operation,
				Volume:    volume,
				Timeout:   m.maxWait,
			}
		}
	}

	err := <-req.done
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	m.metrics.RecordHotplugQueueWait(operation, outcome, time.Since(req.enqueuedAt))
	m.metrics.RecordHotplugQueueDispatch(operation, string(priority), outcome)
	return err
}

func (m *HotplugQueueManager) Snapshot() map[string]HotplugQueueNodeSnapshot {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]HotplugQueueNodeSnapshot, len(m.nodes))
	for node := range m.nodes {
		out[node] = m.snapshotLocked(node)
	}
	return out
}

func (m *HotplugQueueManager) HasVolume(volume string) bool {
	if m == nil || volume == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, nodeQueue := range m.nodes {
		if nodeQueue == nil {
			continue
		}
		if nodeQueue.active != nil && nodeQueue.active.volume == volume {
			return true
		}
		for _, req := range nodeQueue.pending {
			if req.volume == volume {
				return true
			}
		}
	}
	return false
}

func (m *HotplugQueueManager) enqueue(req *hotplugQueueRequest) {
	m.mu.Lock()
	nodeQueue := m.nodes[req.node]
	if nodeQueue == nil {
		nodeQueue = &hotplugNodeQueue{}
		m.nodes[req.node] = nodeQueue
	}
	nodeQueue.pending = append(nodeQueue.pending, req)
	shouldStart := !nodeQueue.running
	if shouldStart {
		nodeQueue.running = true
	}
	snapshot := m.snapshotLocked(req.node)
	m.mu.Unlock()

	m.persistSnapshot(snapshot)
	if shouldStart {
		go m.runNodeQueue(req.node)
	}
}

func (m *HotplugQueueManager) cancel(id uint64, node string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodeQueue := m.nodes[node]
	if nodeQueue == nil {
		return false
	}
	for idx, req := range nodeQueue.pending {
		if req.id != id {
			continue
		}
		nodeQueue.pending = slices.Delete(nodeQueue.pending, idx, idx+1)
		snapshot := m.snapshotLocked(node)
		go m.persistSnapshot(snapshot)
		return true
	}
	return false
}

func (m *HotplugQueueManager) runNodeQueue(node string) {
	for {
		req := m.next(node)
		if req == nil {
			return
		}
		close(req.started)
		err := req.fn(req.ctx)
		req.done <- err
		m.finish(node)
	}
}

func (m *HotplugQueueManager) next(node string) *hotplugQueueRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodeQueue := m.nodes[node]
	if nodeQueue == nil || len(nodeQueue.pending) == 0 {
		if nodeQueue != nil {
			nodeQueue.running = false
			delete(m.nodes, node)
			go m.persistSnapshot(HotplugQueueNodeSnapshot{Node: node})
		}
		return nil
	}

	now := time.Now()
	selectedIdx := 0
	selectedScore := m.effectivePriority(nodeQueue.pending[0], now)
	for idx := 1; idx < len(nodeQueue.pending); idx++ {
		score := m.effectivePriority(nodeQueue.pending[idx], now)
		if score < selectedScore {
			selectedIdx = idx
			selectedScore = score
		}
	}
	req := nodeQueue.pending[selectedIdx]
	nodeQueue.pending = slices.Delete(nodeQueue.pending, selectedIdx, selectedIdx+1)
	nodeQueue.active = req
	snapshot := m.snapshotLocked(node)
	go m.persistSnapshot(snapshot)
	return req
}

func (m *HotplugQueueManager) finish(node string) {
	m.mu.Lock()
	nodeQueue := m.nodes[node]
	if nodeQueue != nil {
		nodeQueue.active = nil
	}
	snapshot := m.snapshotLocked(node)
	m.mu.Unlock()
	m.persistSnapshot(snapshot)
}

func (m *HotplugQueueManager) effectivePriority(req *hotplugQueueRequest, now time.Time) int {
	base := 2
	switch req.priority {
	case hotplugQueuePriorityCritical:
		base = 0
	case hotplugQueuePriorityNormal:
		base = 1
	}
	if m.ageBoost <= 0 {
		return base
	}
	boosts := int(now.Sub(req.enqueuedAt) / m.ageBoost)
	if boosts > base {
		boosts = base
	}
	return base - boosts
}

func (m *HotplugQueueManager) snapshotLocked(node string) HotplugQueueNodeSnapshot {
	snapshot := HotplugQueueNodeSnapshot{Node: node}
	nodeQueue := m.nodes[node]
	if nodeQueue == nil {
		return snapshot
	}
	if nodeQueue.active != nil {
		active := requestSnapshot(nodeQueue.active)
		snapshot.Active = &active
	}
	if len(nodeQueue.pending) > 0 {
		snapshot.Queued = make([]HotplugQueueItemSnapshot, 0, len(nodeQueue.pending))
		oldest := int64(0)
		now := time.Now()
		for _, req := range nodeQueue.pending {
			item := requestSnapshot(req)
			snapshot.Queued = append(snapshot.Queued, item)
			age := int64(now.Sub(req.enqueuedAt).Seconds())
			if age > oldest {
				oldest = age
			}
		}
		snapshot.QueuedCount = len(snapshot.Queued)
		snapshot.OldestAgeSeconds = oldest
	}
	return snapshot
}

func (m *HotplugQueueManager) persistSnapshot(snapshot HotplugQueueNodeSnapshot) {
	if m != nil && m.metrics != nil {
		critical, normal, background := 0, 0, 0
		if snapshot.Node != "" {
			for _, item := range snapshot.Queued {
				switch item.Priority {
				case hotplugQueuePriorityCritical:
					critical++
				case hotplugQueuePriorityNormal:
					normal++
				default:
					background++
				}
			}
			if snapshot.Active != nil {
				switch snapshot.Active.Priority {
				case hotplugQueuePriorityCritical:
					critical++
				case hotplugQueuePriorityNormal:
					normal++
				default:
					background++
				}
			}
		}
		m.metrics.SetHotplugQueueDepth(string(hotplugQueuePriorityCritical), critical)
		m.metrics.SetHotplugQueueDepth(string(hotplugQueuePriorityNormal), normal)
		m.metrics.SetHotplugQueueDepth(string(hotplugQueuePriorityBackground), background)
	}
	if m == nil || m.runtime == nil || !m.runtime.enabled || snapshot.Node == "" {
		return
	}
	if snapshot.Active == nil && snapshot.QueuedCount == 0 {
		if err := m.runtime.DeleteConfigMapKey(context.Background(), m.namespace, hotplugQueueStateConfigMapName, snapshot.Node); err != nil {
			klog.V(4).InfoS("Failed to clear hotplug queue snapshot", "node", snapshot.Node, "err", err)
		}
		return
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		klog.V(4).InfoS("Failed to marshal hotplug queue snapshot", "node", snapshot.Node, "err", err)
		return
	}
	if err := m.runtime.UpsertConfigMapData(context.Background(), m.namespace, hotplugQueueStateConfigMapName, map[string]string{
		snapshot.Node: string(payload),
	}); err != nil {
		klog.V(4).InfoS("Failed to persist hotplug queue snapshot", "node", snapshot.Node, "err", err)
	}
}

func requestSnapshot(req *hotplugQueueRequest) HotplugQueueItemSnapshot {
	return HotplugQueueItemSnapshot{
		ID:         req.id,
		Node:       req.node,
		Operation:  req.operation,
		Volume:     req.volume,
		Priority:   req.priority,
		EnqueuedAt: req.enqueuedAt,
	}
}
