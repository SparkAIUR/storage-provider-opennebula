package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

const (
	sharedPublishContextCephFSMounter = "cephfsMounter"

	sharedCephFSSecretFile = "ceph.secret"
	sharedCephFSConfFile   = "ceph.conf"

	defaultSharedFilesystemSessionDir           = "/var/lib/kubelet/plugins/csi.opennebula.io/cephfs-sessions"
	defaultSharedFilesystemRecoveryStartupDelay = 10 * time.Second
	defaultSharedFilesystemRecoveryInterval     = 2 * time.Minute
)

var (
	sharedFilesystemSessionRootPath      = defaultSharedFilesystemSessionDir
	sharedFilesystemRecoveryStartupDelay = defaultSharedFilesystemRecoveryStartupDelay
	sharedFilesystemRecoveryInterval     = defaultSharedFilesystemRecoveryInterval
	sharedFilesystemProcFilesystemsPath  = "/proc/filesystems"
)

type sharedFilesystemMounter string

const (
	sharedFilesystemMounterFuse   sharedFilesystemMounter = "fuse"
	sharedFilesystemMounterKernel sharedFilesystemMounter = "kernel"
)

type sharedFilesystemPublishedTarget struct {
	TargetPath   string   `json:"targetPath"`
	MountOptions []string `json:"mountOptions,omitempty"`
}

type sharedFilesystemSession struct {
	Unstaging         bool                              `json:"unstaging,omitempty"`
	VolumeID          string                            `json:"volumeID"`
	Mounter           sharedFilesystemMounter           `json:"mounter"`
	UserID            string                            `json:"userID"`
	StagingTargetPath string                            `json:"stagingTargetPath"`
	Monitors          []string                          `json:"monitors"`
	FSName            string                            `json:"fsName"`
	Subpath           string                            `json:"subpath"`
	StageMountOptions []string                          `json:"stageMountOptions,omitempty"`
	KeyringPath       string                            `json:"keyringPath,omitempty"`
	SecretFilePath    string                            `json:"secretFilePath,omitempty"`
	PublishedTargets  []sharedFilesystemPublishedTarget `json:"publishedTargets,omitempty"`
}

type sharedFilesystemSessionHealth struct {
	RecoverStage    bool
	Reason          string
	TargetsToRebind []sharedFilesystemPublishedTarget
}

type sharedFilesystemSessionStore struct {
	root string
	mu   sync.Mutex
}

type sharedFilesystemRecoveryManager struct {
	ns           *NodeServer
	store        *sharedFilesystemSessionStore
	queue        chan string
	mu           sync.Mutex
	queued       map[string]struct{}
	lastFailures map[string]time.Time
	again        map[string]bool
	failures     map[string]int
}

func newSharedFilesystemRecoveryManager(ns *NodeServer) *sharedFilesystemRecoveryManager {
	if ns == nil || ns.Driver == nil {
		return nil
	}
	return &sharedFilesystemRecoveryManager{
		ns:           ns,
		store:        newSharedFilesystemSessionStore(sharedFilesystemSessionRootPath),
		queue:        make(chan string, 128),
		queued:       make(map[string]struct{}),
		lastFailures: make(map[string]time.Time),
		again:        make(map[string]bool),
		failures:     make(map[string]int),
	}
}

func newSharedFilesystemSessionStore(root string) *sharedFilesystemSessionStore {
	root = strings.TrimSpace(root)
	if root == "" {
		root = defaultSharedFilesystemSessionDir
	}
	return &sharedFilesystemSessionStore{root: root}
}

func (s *sharedFilesystemSessionStore) Save(session sharedFilesystemSession) error {
	if strings.TrimSpace(session.VolumeID) == "" {
		return fmt.Errorf("shared filesystem session is missing volume ID")
	}
	if session.KeyringPath == "" && session.StagingTargetPath != "" {
		session.KeyringPath = sharedCephFSKeyringPath(session.StagingTargetPath)
	}
	if session.SecretFilePath == "" && session.StagingTargetPath != "" {
		session.SecretFilePath = sharedCephFSSecretPath(session.StagingTargetPath)
	}
	session.StageMountOptions = uniqueStrings(session.StageMountOptions)
	session.PublishedTargets = normalizeSharedFilesystemPublishedTargets(session.PublishedTargets)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return err
	}

	targetPath := s.pathForVolume(session.VolumeID)
	tmpPath := targetPath + ".tmp"
	return durableSharedFilesystemWrite(tmpPath, targetPath, payload)
}

func (s *sharedFilesystemSessionStore) Load(volumeID string) (sharedFilesystemSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(volumeID)
}

func (s *sharedFilesystemSessionStore) loadLocked(volumeID string) (sharedFilesystemSession, bool, error) {
	payload, err := os.ReadFile(s.pathForVolume(volumeID))
	if err != nil {
		if os.IsNotExist(err) {
			return sharedFilesystemSession{}, false, nil
		}
		return sharedFilesystemSession{}, false, err
	}
	var session sharedFilesystemSession
	if err := json.Unmarshal(payload, &session); err != nil {
		return sharedFilesystemSession{}, false, err
	}
	if session.VolumeID != strings.TrimSpace(volumeID) {
		return sharedFilesystemSession{}, false, fmt.Errorf("persisted session volume does not match its record key")
	}
	if session.KeyringPath == "" && session.StagingTargetPath != "" {
		session.KeyringPath = sharedCephFSKeyringPath(session.StagingTargetPath)
	}
	if session.SecretFilePath == "" && session.StagingTargetPath != "" {
		session.SecretFilePath = sharedCephFSSecretPath(session.StagingTargetPath)
	}
	session.StageMountOptions = uniqueStrings(session.StageMountOptions)
	session.PublishedTargets = normalizeSharedFilesystemPublishedTargets(session.PublishedTargets)
	if session.Mounter == "" {
		session.Mounter = sharedFilesystemMounterFuse
	}
	return session, true, nil
}

func (s *sharedFilesystemSessionStore) Delete(volumeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.pathForVolume(volumeID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	// After unstage has proved that all references are gone, cleanup intents
	// for this volume can no longer protect a recoverable mount.
	prefix := strings.TrimSuffix(filepath.Base(s.pathForVolume(volumeID)), ".json") + "-"
	entries, err := os.ReadDir(s.root)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), ".unpublish") {
			if err := os.Remove(filepath.Join(s.root, entry.Name())); err != nil {
				return err
			}
		}
	}
	return syncSharedFilesystemDirectory(s.root)
}

func (s *sharedFilesystemSessionStore) List() ([]sharedFilesystemSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	sessions := make([]sharedFilesystemSession, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		payload, err := os.ReadFile(filepath.Join(s.root, entry.Name()))
		if err != nil {
			return nil, err
		}
		var session sharedFilesystemSession
		if err := json.Unmarshal(payload, &session); err != nil {
			return nil, err
		}
		if entry.Name() != filepath.Base(s.pathForVolume(session.VolumeID)) {
			return nil, fmt.Errorf("persisted session filename does not match its volume")
		}
		if session.KeyringPath == "" && session.StagingTargetPath != "" {
			session.KeyringPath = sharedCephFSKeyringPath(session.StagingTargetPath)
		}
		if session.SecretFilePath == "" && session.StagingTargetPath != "" {
			session.SecretFilePath = sharedCephFSSecretPath(session.StagingTargetPath)
		}
		session.StageMountOptions = uniqueStrings(session.StageMountOptions)
		session.PublishedTargets = normalizeSharedFilesystemPublishedTargets(session.PublishedTargets)
		if session.Mounter == "" {
			session.Mounter = sharedFilesystemMounterFuse
		}
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].VolumeID < sessions[j].VolumeID
	})
	return sessions, nil
}

func (s *sharedFilesystemSessionStore) pathForVolume(volumeID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(volumeID)))
	return filepath.Join(s.root, hex.EncodeToString(sum[:])+".json")
}

func (m *sharedFilesystemRecoveryManager) Start(ctx context.Context) {
	if m == nil || m.ns == nil || m.ns.Driver == nil || !m.ns.Driver.featureGates.CephFSPersistentRecovery {
		return
	}
	go m.run(ctx)
}

func (m *sharedFilesystemRecoveryManager) run(ctx context.Context) {
	var workers sync.WaitGroup
	defer workers.Wait()
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case volumeID := <-m.queue:
					m.recoverQueuedVolume(ctx, volumeID)
				}
			}
		}()
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(sharedFilesystemRecoveryStartupDelay):
	}
	ticker := time.NewTicker(sharedFilesystemRecoveryInterval)
	defer ticker.Stop()
	for {
		attempt, cancel := context.WithTimeout(ctx, sharedFilesystemAttemptTimeout)
		m.seedSessionsFromMounts(attempt)
		cancel()
		m.enqueueUnhealthySessions(ctx, "sweep")
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *sharedFilesystemRecoveryManager) enqueue(volumeID, reason string) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return
	}

	m.mu.Lock()
	if _, ok := m.queued[volumeID]; ok {
		if reason != "sweep" {
			m.again[volumeID] = true
		}
		m.mu.Unlock()
		return
	}
	m.queued[volumeID] = struct{}{}
	m.mu.Unlock()

	select {
	case m.queue <- volumeID:
		klog.V(4).InfoS("Queued shared filesystem recovery", "volumeID", volumeID, "reason", reason)
	default:
		m.mu.Lock()
		delete(m.queued, volumeID)
		m.mu.Unlock()
		klog.V(2).InfoS("Shared filesystem recovery queue is full", "volumeID", volumeID, "reason", reason)
	}
}

func (m *sharedFilesystemRecoveryManager) recoverQueuedVolume(ctx context.Context, volumeID string) {
	started := time.Now()
	err := m.recoverVolume(ctx, volumeID)
	m.mu.Lock()
	defer m.mu.Unlock()
	retry := m.again[volumeID] || err != nil
	delete(m.again, volumeID)
	delete(m.queued, volumeID)
	if err == nil && !retry {
		delete(m.failures, volumeID)
		delete(m.lastFailures, volumeID)
	} else if err != nil {
		lastFailure := m.lastFailures[volumeID]
		if lastFailure.IsZero() || time.Since(lastFailure) > 10*time.Minute {
			klog.ErrorS(err, "Shared filesystem recovery failed", "volumeID", volumeID, "duration", time.Since(started))
			m.lastFailures[volumeID] = time.Now()
		}
		outcome := "failed"
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			outcome = "timeout"
		}
		m.ns.Driver.metrics.RecordCephFSSubvolume("recovery_attempt", outcome)
	}
	m.ns.Driver.metrics.cephFSRecoveryPending.Set(float64(len(m.lastFailures)))
	if retry && ctx.Err() == nil {
		m.failures[volumeID]++
		backoff := 5 * time.Second
		for i := 1; i < m.failures[volumeID] && backoff < sharedFilesystemRecoveryInterval; i++ {
			backoff *= 2
		}
		if backoff > sharedFilesystemRecoveryInterval {
			backoff = sharedFilesystemRecoveryInterval
		}
		time.AfterFunc(backoff, func() {
			if ctx.Err() == nil {
				m.enqueue(volumeID, "retry")
			}
		})
	}
}

// Enqueue without probing paths: a blocked volume must not starve the sweep.
func (m *sharedFilesystemRecoveryManager) enqueueUnhealthySessions(ctx context.Context, reason string) {
	sessions, err := m.store.List()
	if err != nil {
		klog.ErrorS(err, "Failed to list shared filesystem sessions")
		return
	}
	present := make(map[string]bool, len(sessions))
	for _, session := range sessions {
		present[session.VolumeID] = true
	}
	m.mu.Lock()
	for id := range m.lastFailures {
		if !present[id] {
			delete(m.lastFailures, id)
		}
	}
	m.ns.Driver.metrics.cephFSRecoveryPending.Set(float64(len(m.lastFailures)))
	m.mu.Unlock()
	for _, session := range sessions {
		if ctx.Err() != nil {
			return
		}
		m.enqueue(session.VolumeID, reason)
	}
}

func (m *sharedFilesystemRecoveryManager) garbageCollectOrphanedSession(ctx context.Context, session sharedFilesystemSession) (bool, error) {
	if m == nil || m.ns == nil || m.ns.Driver == nil || m.ns.Driver.kubeRuntime == nil || !m.ns.Driver.kubeRuntime.enabled || strings.TrimSpace(session.VolumeID) == "" {
		return false, nil
	}
	allPodsGone := true
	for _, target := range session.PublishedTargets {
		podUID := podUIDFromKubeletPath(target.TargetPath)
		if podUID == "" {
			allPodsGone = false
			break
		}
		exists, known := m.ns.podUIDExistsKnown(ctx, podUID)
		if !known {
			klog.V(2).InfoS("Skipping CephFS session GC because pod liveness could not be confirmed", "volumeID", session.VolumeID, "podUID", podUID)
			allPodsGone = false
			break
		}
		if exists {
			allPodsGone = false
			break
		}
	}
	if !allPodsGone {
		return false, nil
	}
	if m.ns.Driver != nil && m.ns.Driver.kubeRuntime != nil {
		if _, err := m.ns.Driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, session.VolumeID); err == nil {
			return false, nil
		} else if !strings.Contains(strings.ToLower(err.Error()), "persistent volume for handle") {
			klog.V(2).InfoS("Skipping CephFS session GC because volume runtime context could not be confirmed", "volumeID", session.VolumeID, "err", err)
			return false, nil
		}
	}
	if err := m.ns.verifySharedFilesystemSessionMode(ctx, session, true); err != nil {
		return false, nil
	}
	// Preflight every target before committing the cleanup direction.
	for _, target := range session.PublishedTargets {
		if _, mounted, err := m.ns.mountPointForPath(target.TargetPath); err != nil {
			return false, err
		} else if mounted {
			if err := m.ns.sharedFS.probe(ctx, target.TargetPath); !isDisconnectedSharedFilesystemError(err) {
				return false, nil
			}
		}
	}
	// Save each target's direction first. If interrupted, normal recovery can
	// prune those targets and repeat GC for the remaining orphaned targets.
	for _, target := range session.PublishedTargets {
		if err := m.store.setUnpublishIntent(session.VolumeID, target.TargetPath, true); err != nil {
			return false, err
		}
	}
	session.Unstaging = true
	if err := m.store.Save(session); err != nil {
		return false, err
	}
	if err := m.ns.finishSharedFilesystemUnstage(ctx, session); err != nil {
		return false, err
	}
	m.ns.Driver.metrics.RecordCephFSSubvolume("session_gc", "succeeded")
	klog.InfoS("Garbage collected orphaned CephFS session", "volumeID", session.VolumeID)
	return true, nil
}

func (m *sharedFilesystemRecoveryManager) recoverVolume(ctx context.Context, volumeID string) error {
	if m == nil || m.ns == nil {
		return nil
	}
	release, acquired := m.ns.Driver.operationLocks.TryAcquire("sharedfs:" + strings.TrimSpace(volumeID))
	if !acquired {
		return status.Error(codes.Aborted, "shared filesystem operation already in progress")
	}
	ctx, cancel := context.WithTimeout(ctx, sharedFilesystemAttemptTimeout)
	defer cancel()
	ctx, release = fenceSharedFilesystemOperation(ctx, release, func() { m.enqueue(volumeID, "process_reaped") })
	defer release()
	return m.recoverVolumeLocked(ctx, volumeID)
}

// The caller owns sharedfs:<volumeID>. Never acquire that lock again here.
func (m *sharedFilesystemRecoveryManager) recoverVolumeLocked(ctx context.Context, volumeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session, exists, err := m.store.Load(volumeID)
	if err != nil {
		return err
	}
	if !exists {
		return nil // A queued exit can race a completed unstage.
	}
	if session.Unstaging {
		return m.ns.finishSharedFilesystemUnstage(ctx, session)
	}
	if err := m.ns.pruneSharedFilesystemUnpublishedTargets(ctx, &session); err != nil {
		return err
	}
	if collected, err := m.garbageCollectOrphanedSession(ctx, session); collected || err != nil {
		return err
	}

	if err := m.ns.verifySharedFilesystemSession(ctx, session); err != nil {
		m.ns.Driver.metrics.RecordCephFSSubvolume("recovery_ownership", "rejected")
		return err
	}
	if runtime := m.ns.Driver.kubeRuntime; runtime != nil {
		runtimeContext, err := runtime.ResolveVolumeRuntimeContext(ctx, session.VolumeID)
		if err != nil {
			return fmt.Errorf("cannot confirm recovery PV ownership: %w", err)
		}
		for _, target := range session.PublishedTargets {
			if filepath.Base(filepath.Dir(target.TargetPath)) != runtimeContext.PVName {
				return fmt.Errorf("target does not belong to recovery PV")
			}
		}
	}
	health, err := m.ns.evaluateSharedFilesystemSession(ctx, session)
	if err != nil {
		return err
	}
	if !health.RecoverStage && len(health.TargetsToRebind) == 0 {
		return nil
	}

	m.ns.Driver.metrics.RecordCephFSSubvolume("recovery_attempt_"+string(session.Mounter), "attempted")

	if health.RecoverStage {
		for _, target := range session.PublishedTargets {
			if err := m.ns.cleanupSharedFilesystemPath(ctx, target.TargetPath); err != nil {
				return fmt.Errorf("failed to cleanup stale target %s before stage recovery: %w", target.TargetPath, err)
			}
		}
		if err := m.ns.cleanupSharedFilesystemPath(ctx, session.StagingTargetPath); err != nil {
			return status.Errorf(codes.Internal, "failed to cleanup stale shared filesystem stage %s: %v", session.StagingTargetPath, err)
		}
		if err := m.ns.mountSharedFilesystemSession(ctx, session); err != nil {
			m.ns.Driver.metrics.RecordCephFSSubvolume("recovery_"+string(session.Mounter), "failed")
			return err
		}
		for _, target := range session.PublishedTargets {
			if err := m.ns.publishSharedFilesystemTarget(ctx, session.StagingTargetPath, target); err != nil {
				m.ns.Driver.metrics.RecordCephFSSubvolume("recovery_"+string(session.Mounter), "failed")
				return err
			}
		}
		if err := m.ns.confirmSharedFilesystemRecovery(ctx, session); err != nil {
			return err
		}
		m.ns.Driver.metrics.RecordCephFSSubvolume("recovery_"+string(session.Mounter), "succeeded")
		return nil
	}

	for _, target := range health.TargetsToRebind {
		if err := m.ns.cleanupSharedFilesystemPath(ctx, target.TargetPath); err != nil {
			return fmt.Errorf("failed to cleanup shared filesystem target %s before rebinding: %w", target.TargetPath, err)
		}
		if err := m.ns.publishSharedFilesystemTarget(ctx, session.StagingTargetPath, target); err != nil {
			m.ns.Driver.metrics.RecordCephFSSubvolume("recovery_"+string(session.Mounter), "failed")
			return err
		}
	}

	if err := m.ns.confirmSharedFilesystemRecovery(ctx, session); err != nil {
		return err
	}
	m.ns.Driver.metrics.RecordCephFSSubvolume("recovery_"+string(session.Mounter), "succeeded")
	return nil
}

func (m *sharedFilesystemRecoveryManager) seedSessionsFromMounts(ctx context.Context) {
	if m == nil || m.ns == nil {
		return
	}

	mountPoints, err := m.ns.mounter.List()
	if err != nil {
		klog.ErrorS(err, "Failed to list mount points while seeding shared filesystem sessions")
		return
	}

	datastoreCache := make(map[int]opennebula.Datastore)
	for _, mountPoint := range mountPoints {
		volumeID, metaErr := sharedFilesystemStageVolumeID(mountPoint.Path)
		if metaErr != nil {
			continue
		}

		release, acquired := m.ns.Driver.operationLocks.TryAcquire("sharedfs:" + volumeID)
		if !acquired {
			continue
		}
		func() {
			defer release()
			session, exists, err := m.store.Load(volumeID)
			if err != nil {
				klog.ErrorS(err, "Failed to load shared filesystem session during seed", "volumeID", volumeID)
				return
			}
			if exists && session.Unstaging {
				return
			}
			if !exists {
				session, err = m.ns.reconstructSharedFilesystemSessionFromMount(ctx, mountPoint, mountPoints, datastoreCache)
				if err != nil {
					klog.V(2).ErrorS(err, "Failed to reconstruct shared filesystem session from mount state", "stagePath", mountPoint.Path)
					return
				}
			}

			if exists && session.StagingTargetPath != mountPoint.Path {
				klog.ErrorS(fmt.Errorf("persisted staging path mismatch"), "Refusing CephFS session seed", "volumeID", volumeID)
				return
			}
			session.StagingTargetPath = mountPoint.Path
			session.Mounter = sharedFilesystemMounterFromMountType(mountPoint.Type)
			if session.KeyringPath == "" {
				session.KeyringPath = sharedCephFSKeyringPath(mountPoint.Path)
			}
			if session.SecretFilePath == "" {
				session.SecretFilePath = sharedCephFSSecretPath(mountPoint.Path)
			}
			if session.UserID == "" {
				session.UserID = sharedFilesystemUserIDFromKeyring(session.KeyringPath)
			}
			targets, err := m.ns.discoverSharedFilesystemTargets(volumeID, mountPoint.Path)
			if err != nil {
				klog.ErrorS(err, "Cannot prove CephFS mount ownership", "stagePath", mountPoint.Path)
				return
			}
			session.PublishedTargets = normalizeSharedFilesystemPublishedTargets(append(session.PublishedTargets, targets...))
			if err := m.store.Save(session); err != nil {
				klog.ErrorS(err, "Failed to persist seeded shared filesystem session", "volumeID", volumeID)
			}
		}()
	}
}

func (ns *NodeServer) evaluateSharedFilesystemSession(ctx context.Context, session sharedFilesystemSession) (sharedFilesystemSessionHealth, error) {
	health := sharedFilesystemSessionHealth{}
	if strings.TrimSpace(session.StagingTargetPath) == "" {
		return health, nil
	}

	stageCheck, err := ns.checkMountPoint("", session.StagingTargetPath, nil)
	if err != nil {
		return health, err
	}
	if !stageCheck.targetIsMountPoint {
		health.RecoverStage = true
		health.Reason = "stage_missing"
		return health, nil
	}
	if err := ns.sharedFS.probe(ctx, session.StagingTargetPath); err != nil {
		if !isDisconnectedSharedFilesystemError(err) {
			return health, err
		}
		health.RecoverStage = true
		health.Reason = "stage_stale"
		return health, nil
	}

	for _, target := range session.PublishedTargets {
		targetCheck, err := ns.checkMountPoint(session.StagingTargetPath, target.TargetPath, nil)
		if err != nil {
			return health, err
		}
		if !targetCheck.targetIsMountPoint {
			health.TargetsToRebind = append(health.TargetsToRebind, target)
			continue
		}
		if err := ns.sharedFS.probe(ctx, target.TargetPath); err != nil {
			if !isDisconnectedSharedFilesystemError(err) {
				return health, err
			}
			health.TargetsToRebind = append(health.TargetsToRebind, target)
		}
	}
	health.TargetsToRebind = normalizeSharedFilesystemPublishedTargets(health.TargetsToRebind)
	return health, nil
}

func (ns *NodeServer) reconstructSharedFilesystemSessionFromMount(ctx context.Context, stageMount mount.MountPoint, mountPoints []mount.MountPoint, datastoreCache map[int]opennebula.Datastore) (sharedFilesystemSession, error) {
	volumeID, metaErr := sharedFilesystemStageVolumeID(stageMount.Path)
	if metaErr != nil {
		return sharedFilesystemSession{}, fmt.Errorf("stage path %s does not map to a shared filesystem volume", stageMount.Path)
	}

	metadata, err := opennebula.DecodeSharedVolumeID(volumeID)
	if err != nil {
		return sharedFilesystemSession{}, err
	}

	datastore, ok := datastoreCache[metadata.DatastoreID]
	if !ok {
		datastore, err = ns.resolveSharedFilesystemDatastore(ctx, metadata.DatastoreID)
		if err != nil {
			return sharedFilesystemSession{}, err
		}
		datastoreCache[metadata.DatastoreID] = datastore
	}

	session := sharedFilesystemSession{
		VolumeID:          volumeID,
		Mounter:           sharedFilesystemMounterFromMountType(stageMount.Type),
		UserID:            sharedFilesystemUserIDFromKeyring(sharedCephFSKeyringPath(stageMount.Path)),
		StagingTargetPath: stageMount.Path,
		Monitors:          append([]string(nil), datastore.CephFS.Monitors...),
		FSName:            metadata.FSName,
		Subpath:           metadata.Subpath,
		StageMountOptions: uniqueStrings(datastore.CephFS.MountOptions),
		KeyringPath:       sharedCephFSKeyringPath(stageMount.Path),
		SecretFilePath:    sharedCephFSSecretPath(stageMount.Path),
	}
	return session, nil
}

func (ns *NodeServer) resolveSharedFilesystemDatastore(ctx context.Context, datastoreID int) (opennebula.Datastore, error) {
	endpoint, ok := ns.Driver.PluginConfig.GetString(config.OpenNebulaRPCEndpointVar)
	if !ok || strings.TrimSpace(endpoint) == "" {
		return opennebula.Datastore{}, fmt.Errorf("ONE_XMLRPC is not configured")
	}
	credentials, ok := ns.Driver.PluginConfig.GetString(config.OpenNebulaCredentialsVar)
	if !ok || strings.TrimSpace(credentials) == "" {
		return opennebula.Datastore{}, fmt.Errorf("ONE_AUTH is not configured")
	}

	client := opennebula.NewClient(opennebula.OpenNebulaConfig{
		Endpoint:    endpoint,
		Credentials: credentials,
	})
	pool, err := goca.NewController(client.Client).Datastores().InfoContext(ctx)
	if err != nil {
		return opennebula.Datastore{}, err
	}

	selection := opennebula.DatastoreSelectionConfig{
		Identifiers:  []string{fmt.Sprintf("%d", datastoreID)},
		AllowedTypes: []string{"cephfs"},
	}
	resolved, err := opennebula.ResolveDatastores(pool.Datastores, selection)
	if err != nil {
		return opennebula.Datastore{}, err
	}
	if len(resolved) == 0 {
		return opennebula.Datastore{}, fmt.Errorf("datastore %d was not resolved", datastoreID)
	}
	return resolved[0], nil
}

func (ns *NodeServer) acquireSharedFilesystemOperationLock(volumeID string) func() {
	if ns == nil || ns.Driver == nil || ns.Driver.operationLocks == nil {
		return func() {}
	}
	return ns.Driver.operationLocks.Acquire("sharedfs:" + strings.TrimSpace(volumeID))
}

func (ns *NodeServer) handleDisconnectedSharedFilesystemPath(volumeID, volumePath string, err error) error {
	if ns != nil && ns.Driver != nil {
		ns.Driver.metrics.RecordCephFSSubvolume("stale_mount_detected", "failure")
		if ns.sharedFilesystemRecovery != nil {
			ns.sharedFilesystemRecovery.enqueue(volumeID, "volume_stats")
		}
	}
	return status.Errorf(codes.FailedPrecondition, "stale CephFS mount detected at %s: %v; restage the volume to recover", volumePath, err)
}

func (ns *NodeServer) recordSharedFilesystemSession(session sharedFilesystemSession) error {
	if ns == nil || ns.sharedFilesystemRecovery == nil {
		return fmt.Errorf("shared filesystem session store is unavailable")
	}
	return ns.sharedFilesystemRecovery.store.Save(session)
}

func (ns *NodeServer) deleteSharedFilesystemSession(volumeID string) error {
	if ns == nil || ns.sharedFilesystemRecovery == nil {
		return fmt.Errorf("shared filesystem session store is unavailable")
	}
	return ns.sharedFilesystemRecovery.store.Delete(volumeID)
}

func (ns *NodeServer) ensureSharedFilesystemSessionForPublish(req *csi.NodePublishVolumeRequest) (sharedFilesystemSession, error) {
	if ns == nil || ns.sharedFilesystemRecovery == nil || req == nil {
		return sharedFilesystemSession{}, nil
	}
	volumeID := strings.TrimSpace(req.GetVolumeId())
	if volumeID == "" {
		return sharedFilesystemSession{}, nil
	}

	session, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	if err != nil {
		return sharedFilesystemSession{}, err
	}
	if exists {
		if session.Unstaging {
			return session, fmt.Errorf("volume is being unstaged")
		}
		if session.StagingTargetPath != req.GetStagingTargetPath() {
			return session, fmt.Errorf("staging path differs from persisted session")
		}
		return session, nil
	}

	session, ok, err := ns.sharedFilesystemSessionFromPublishRequest(req)
	if err != nil {
		return session, err
	}
	if !ok {
		return session, fmt.Errorf("cannot reconstruct session from publish request")
	}
	if err := ns.sharedFilesystemRecovery.store.Save(session); err != nil {
		return sharedFilesystemSession{}, err
	}
	return session, nil
}

func (ns *NodeServer) sharedFilesystemSessionFromPublishRequest(req *csi.NodePublishVolumeRequest) (sharedFilesystemSession, bool, error) {
	if req == nil {
		return sharedFilesystemSession{}, false, nil
	}
	stagingTargetPath := strings.TrimSpace(req.GetStagingTargetPath())
	publishContext := req.GetPublishContext()
	monitors := splitCSV(publishContext[sharedPublishContextCephFSMonitors])
	fsName := strings.TrimSpace(publishContext[sharedPublishContextCephFSFSName])
	subpath := strings.TrimSpace(publishContext[sharedPublishContextCephFSSubpath])
	if stagingTargetPath == "" || len(monitors) == 0 || fsName == "" || subpath == "" {
		return sharedFilesystemSession{}, false, nil
	}

	mounterName, err := normalizeSharedFilesystemMounter(firstNonEmpty(
		publishContext[sharedPublishContextCephFSMounter],
		req.GetVolumeContext()[storageClassParamCephFSMounter],
	))
	if err != nil {
		return sharedFilesystemSession{}, false, err
	}
	if mountPoint, mounted, err := ns.mountPointForPath(stagingTargetPath); err == nil && mounted {
		mounterName = sharedFilesystemMounterFromMountType(mountPoint.Type)
	}

	keyringPath := sharedCephFSKeyringPath(stagingTargetPath)
	userID := sharedFilesystemUserIDFromKeyring(keyringPath)
	if userID == "" {
		return sharedFilesystemSession{}, false, nil
	}

	return sharedFilesystemSession{
		VolumeID:          req.GetVolumeId(),
		Mounter:           mounterName,
		UserID:            userID,
		StagingTargetPath: stagingTargetPath,
		Monitors:          monitors,
		FSName:            fsName,
		Subpath:           subpath,
		StageMountOptions: sharedFilesystemStageMountOptionsFromPublishRequest(req),
		KeyringPath:       keyringPath,
		SecretFilePath:    sharedCephFSSecretPath(stagingTargetPath),
	}, true, nil
}

func sharedFilesystemStageMountOptionsFromPublishRequest(req *csi.NodePublishVolumeRequest) []string {
	if req == nil {
		return nil
	}
	options := splitCSV(req.GetPublishContext()[sharedPublishContextCephFSMounts])
	if capability := req.GetVolumeCapability(); capability != nil && capability.GetMount() != nil {
		options = append(options, capability.GetMount().GetMountFlags()...)
	}
	if strings.EqualFold(strings.TrimSpace(req.GetPublishContext()[sharedPublishContextCephFSReadonly]), "true") {
		options = append(options, "ro")
	}
	return uniqueStrings(options)
}

func (ns *NodeServer) updateSharedFilesystemPublishedTarget(volumeID string, target sharedFilesystemPublishedTarget) error {
	session, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("shared filesystem session is missing")
	}
	targets := make([]sharedFilesystemPublishedTarget, 0, len(session.PublishedTargets)+1)
	for _, existing := range session.PublishedTargets {
		if existing.TargetPath != target.TargetPath {
			targets = append(targets, existing)
		}
	}
	session.PublishedTargets = append(targets, target)
	return ns.recordSharedFilesystemSession(session)
}

func (ns *NodeServer) removeSharedFilesystemPublishedTarget(volumeID, targetPath string) error {
	session, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	if err != nil || !exists {
		return err
	}
	filtered := make([]sharedFilesystemPublishedTarget, 0, len(session.PublishedTargets))
	for _, target := range session.PublishedTargets {
		if target.TargetPath != targetPath {
			filtered = append(filtered, target)
		}
	}
	session.PublishedTargets = filtered
	return ns.recordSharedFilesystemSession(session)
}

func normalizeSharedFilesystemPublishedTargets(targets []sharedFilesystemPublishedTarget) []sharedFilesystemPublishedTarget {
	if len(targets) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(targets))
	normalized := make([]sharedFilesystemPublishedTarget, 0, len(targets))
	for _, target := range targets {
		target.TargetPath = strings.TrimSpace(target.TargetPath)
		if target.TargetPath == "" {
			continue
		}
		if _, ok := seen[target.TargetPath]; ok {
			continue
		}
		target.MountOptions = uniqueStrings(target.MountOptions)
		seen[target.TargetPath] = struct{}{}
		normalized = append(normalized, target)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].TargetPath < normalized[j].TargetPath
	})
	return normalized
}

func sharedFilesystemMountOptionsFromMountPoint(mountPoint mount.MountPoint) []string {
	options := make([]string, 0, len(mountPoint.Opts))
	for _, option := range mountPoint.Opts {
		trimmed := strings.TrimSpace(option)
		switch trimmed {
		case "", "bind", "rw", "defaults":
			continue
		default:
			options = append(options, trimmed)
		}
	}
	return uniqueStrings(options)
}

func normalizeSharedFilesystemMounter(raw string) (sharedFilesystemMounter, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(sharedFilesystemMounterFuse):
		return sharedFilesystemMounterFuse, nil
	case string(sharedFilesystemMounterKernel):
		return sharedFilesystemMounterKernel, nil
	default:
		return "", fmt.Errorf("unsupported CephFS mounter %q", raw)
	}
}

func sharedFilesystemMounterFromMountType(fsType string) sharedFilesystemMounter {
	switch strings.ToLower(strings.TrimSpace(fsType)) {
	case "ceph":
		return sharedFilesystemMounterKernel
	default:
		return sharedFilesystemMounterFuse
	}
}

func sharedFilesystemStageMountOptions(req *csi.NodeStageVolumeRequest) []string {
	publishContext := req.GetPublishContext()
	options := splitCSV(publishContext[sharedPublishContextCephFSMounts])
	if capability := req.GetVolumeCapability(); capability != nil && capability.GetMount() != nil {
		options = append(options, capability.GetMount().GetMountFlags()...)
	}
	if strings.EqualFold(strings.TrimSpace(publishContext[sharedPublishContextCephFSReadonly]), "true") {
		options = append(options, "ro")
	}
	return uniqueStrings(options)
}

func sharedFilesystemBindMountOptions(req *csi.NodePublishVolumeRequest) []string {
	options := make([]string, 0, 4)
	if req.GetReadonly() {
		options = append(options, "ro")
	}
	if capability := req.GetVolumeCapability(); capability != nil && capability.GetMount() != nil {
		options = append(options, capability.GetMount().GetMountFlags()...)
	}
	return uniqueStrings(options)
}

func sharedFilesystemSessionFromStageRequest(req *csi.NodeStageVolumeRequest) (sharedFilesystemSession, error) {
	mounterName, err := normalizeSharedFilesystemMounter(firstNonEmpty(
		req.GetPublishContext()[sharedPublishContextCephFSMounter],
		req.GetVolumeContext()[storageClassParamCephFSMounter],
	))
	if err != nil {
		return sharedFilesystemSession{}, status.Error(codes.InvalidArgument, err.Error())
	}

	publishContext := req.GetPublishContext()
	monitors := splitCSV(publishContext[sharedPublishContextCephFSMonitors])
	fsName := strings.TrimSpace(publishContext[sharedPublishContextCephFSFSName])
	subpath := strings.TrimSpace(publishContext[sharedPublishContextCephFSSubpath])
	if len(monitors) == 0 || fsName == "" || subpath == "" {
		return sharedFilesystemSession{}, status.Error(codes.InvalidArgument, "CephFS publish context is incomplete")
	}

	secrets := req.GetSecrets()
	userID := strings.TrimSpace(secrets[sharedNodeStageSecretUserIDKey])
	if userID == "" {
		return sharedFilesystemSession{}, status.Errorf(codes.InvalidArgument, "CephFS node staging requires secret key %q", sharedNodeStageSecretUserIDKey)
	}

	stagingTargetPath := req.GetStagingTargetPath()
	return sharedFilesystemSession{
		VolumeID:          req.GetVolumeId(),
		Mounter:           mounterName,
		UserID:            userID,
		StagingTargetPath: stagingTargetPath,
		Monitors:          monitors,
		FSName:            fsName,
		Subpath:           subpath,
		StageMountOptions: sharedFilesystemStageMountOptions(req),
		KeyringPath:       sharedCephFSKeyringPath(stagingTargetPath),
		SecretFilePath:    sharedCephFSSecretPath(stagingTargetPath),
	}, nil
}

func sharedFilesystemTargetFromPublishRequest(req *csi.NodePublishVolumeRequest) sharedFilesystemPublishedTarget {
	return sharedFilesystemPublishedTarget{
		TargetPath:   req.GetTargetPath(),
		MountOptions: sharedFilesystemBindMountOptions(req),
	}
}

func sharedCephFSKeyringPath(stagingTargetPath string) string {
	return filepath.Join(sharedCephFSKeyringDir(stagingTargetPath), sharedCephFSKeyringFile)
}

func sharedCephFSSecretPath(stagingTargetPath string) string {
	return filepath.Join(sharedCephFSKeyringDir(stagingTargetPath), sharedCephFSSecretFile)
}

func sharedCephFSConfPath(stagingTargetPath string) string {
	return filepath.Join(sharedCephFSKeyringDir(stagingTargetPath), sharedCephFSConfFile)
}

func ensureSharedFilesystemCephConf(stagingTargetPath string) (string, error) {
	keyringDir := sharedCephFSKeyringDir(stagingTargetPath)
	if err := os.MkdirAll(keyringDir, 0o700); err != nil {
		return "", status.Errorf(codes.Internal, "failed to create CephFS config directory: %v", err)
	}
	confPath := sharedCephFSConfPath(stagingTargetPath)
	if _, err := os.Stat(confPath); err == nil {
		return confPath, nil
	}
	contents := []byte("[global]\n")
	if err := os.WriteFile(confPath, contents, 0o600); err != nil {
		return "", status.Errorf(codes.Internal, "failed to write CephFS config file: %v", err)
	}
	return confPath, nil
}

func ensureSharedFilesystemCredentials(stagingTargetPath, userID, userKey string) (string, string, error) {
	keyringDir := sharedCephFSKeyringDir(stagingTargetPath)
	if err := os.MkdirAll(keyringDir, 0o700); err != nil {
		return "", "", status.Errorf(codes.Internal, "failed to create CephFS credential directory: %v", err)
	}

	keyringPath := sharedCephFSKeyringPath(stagingTargetPath)
	keyringContents := []byte("[client." + userID + "]\n\tkey = " + userKey + "\n")
	if err := os.WriteFile(keyringPath, keyringContents, 0o600); err != nil {
		return "", "", status.Errorf(codes.Internal, "failed to write CephFS keyring file: %v", err)
	}

	secretPath := sharedCephFSSecretPath(stagingTargetPath)
	if err := os.WriteFile(secretPath, []byte(userKey+"\n"), 0o600); err != nil {
		return "", "", status.Errorf(codes.Internal, "failed to write CephFS secret file: %v", err)
	}

	return keyringPath, secretPath, nil
}

func sharedFilesystemUserIDFromKeyring(keyringPath string) string {
	payload, err := os.ReadFile(keyringPath)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(payload), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[client.") && strings.HasSuffix(line, "]") {
			return strings.TrimSuffix(strings.TrimPrefix(line, "[client."), "]")
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func sharedFilesystemKernelSupported() error {
	supported, err := hostSupportsCephKernelClient(sharedFilesystemProcFilesystemsPath)
	if err != nil {
		return err
	}
	if !supported {
		return fmt.Errorf("host kernel does not expose CephFS client support in %s; on Omni/Talos this must come from the node image or system extensions", sharedFilesystemProcFilesystemsPath)
	}
	return nil
}

func hostSupportsCephKernelClient(procFilesystemsPath string) (bool, error) {
	payload, err := os.ReadFile(procFilesystemsPath)
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) == 0 {
			continue
		}
		if fields[len(fields)-1] == "ceph" {
			return true, nil
		}
	}
	return false, nil
}

func sharedFilesystemKernelMonitorOption(monitors []string) string {
	if len(monitors) == 0 {
		return ""
	}
	return strings.Join(uniqueStrings(monitors), "/")
}

func (ns *NodeServer) lockSharedFilesystemOperation(ctx context.Context, volumeID string) (context.Context, func(), error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return ctx, nil, status.FromContextError(err).Err()
		}
		if release, ok := ns.Driver.operationLocks.TryAcquire("sharedfs:" + strings.TrimSpace(volumeID)); ok {
			ctx, release = fenceSharedFilesystemOperation(ctx, release, func() { ns.sharedFilesystemRecovery.enqueue(volumeID, "process_reaped") })
			return ctx, release, nil
		}
		select {
		case <-ctx.Done():
			return ctx, nil, status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
		}
	}
}
