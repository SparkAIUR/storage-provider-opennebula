package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"k8s.io/client-go/kubernetes/fake"
	mount "k8s.io/mount-utils"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSharedFilesystemStalePublishDoesNotReenterVolumeLock(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	require.NotNil(t, ns.Driver.operationLocks)
	ns.Driver.featureGates.CephFSSelfHealing = true
	id, stage, target := stageSharedFilesystemFixture(t, ns, "lock-regression")
	original := detectSharedFilesystemMount
	detectSharedFilesystemMount = func(string) error { return errors.New("transport endpoint is not connected") }
	t.Cleanup(func() { detectSharedFilesystemMount = original })
	done := make(chan error, 1)
	go func() {
		_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
		done <- err
	}()
	select {
	case err := <-done:
		require.Equal(t, codes.Unavailable, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("stale publish blocked while already owning the per-volume lock")
	}
}

func sharedFilesystemFixturePaths(t *testing.T, ns *NodeServer, label string) (string, string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	id, err := opennebula.EncodeSharedVolumeID(opennebula.SharedVolumeMetadata{
		DatastoreID: 125, Mode: opennebula.SharedVolumeModeDynamic, FSName: "cephfs-prod", Subpath: "/kubernetes/dynamic/" + label, SubvolumeName: label,
	})
	require.NoError(t, err)
	sum := sha256.Sum256([]byte(id))
	stage := filepath.Join(root, "plugins/kubernetes.io/csi/csi.opennebula.io", hex.EncodeToString(sum[:]), "globalmount")
	target := filepath.Join(root, "pods", "pod-"+label, "volumes/kubernetes.io~csi", "pvc-"+label, "mount")
	for _, path := range []string{stage, target} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0750))
		metadata, err := json.Marshal(sharedFilesystemVolumeData{DriverName: DefaultDriverName, VolumeHandle: id, SpecVolID: "pvc-" + label, NodeName: ns.Driver.nodeID})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(path), "vol_data.json"), metadata, 0600))
	}
	return id, stage, target
}

func stageSharedFilesystemFixture(t *testing.T, ns *NodeServer, label string) (string, string, string) {
	t.Helper()
	id, stage, target := sharedFilesystemFixturePaths(t, ns, label)
	_, err := ns.NodeStageVolume(context.Background(), newSharedFilesystemStageRequest(id, stage, "fuse"))
	require.NoError(t, err)
	return id, stage, target
}

func TestSharedFilesystemBusyVolumeDoesNotBlockAnotherRecovery(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	first, _, _ := stageSharedFilesystemFixture(t, ns, "busy")
	second, _, _ := stageSharedFilesystemFixture(t, ns, "ready")
	release := ns.acquireSharedFilesystemOperationLock(first)
	defer release()
	require.Equal(t, codes.Aborted, status.Code(ns.sharedFilesystemRecovery.recoverVolume(context.Background(), first)))
	require.NoError(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), second))
}

func TestSharedFilesystemUnmountFailurePreservesBytesAndSession(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "unmount-failure")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
	sentinel := filepath.Join(target, "keep.bin")
	require.NoError(t, os.WriteFile(sentinel, []byte("preserve"), 0600))
	ns.sharedFS.unmount = func(context.Context, string) error { return syscall.EBUSY }
	_, err = ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: id, TargetPath: target})
	require.Error(t, err)
	bytes, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(bytes))
	session, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, session.PublishedTargets, 1)
}

func TestSharedFilesystemRecoveryRejectsForeignTarget(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	first, _, _ := stageSharedFilesystemFixture(t, ns, "first")
	second, stage, target := stageSharedFilesystemFixture(t, ns, "second")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(second, stage, target))
	require.NoError(t, err)
	session, _, err := ns.sharedFilesystemRecovery.store.Load(first)
	require.NoError(t, err)
	session.PublishedTargets = []sharedFilesystemPublishedTarget{{TargetPath: target}}
	require.NoError(t, ns.sharedFilesystemRecovery.store.Save(session))
	before, err := ns.mounter.List()
	require.NoError(t, err)
	require.ErrorContains(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), first), "ownership mismatch")
	after, err := ns.mounter.List()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestSharedFilesystemDiscoverySeparatesGenericFuseSources(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	first, stage1, target1 := stageSharedFilesystemFixture(t, ns, "first")
	_, stage2, target2 := stageSharedFilesystemFixture(t, ns, "second")
	ns.sharedFS.mountInfo = func() ([]mount.MountInfo, error) {
		return []mount.MountInfo{
			{MountPoint: stage1, Minor: 223, Root: "/", Source: "ceph-fuse", FsType: "fuse.ceph-fuse"},
			{MountPoint: target1, Minor: 223, Root: "/", Source: "ceph-fuse", FsType: "fuse.ceph-fuse"},
			{MountPoint: stage2, Minor: 422, Root: "/", Source: "ceph-fuse", FsType: "fuse.ceph-fuse"},
			{MountPoint: target2, Minor: 422, Root: "/", Source: "ceph-fuse", FsType: "fuse.ceph-fuse"},
		}, nil
	}
	targets, err := ns.discoverSharedFilesystemTargets(first, stage1)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.Equal(t, target1, targets[0].TargetPath)
}

func TestSharedFilesystemCommandCancellationPreventsLateWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "late-write")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := runSharedFilesystemCommand(ctx, "sh", "-c", "sleep 0.2; printf late > \"$1\"", "test", path)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	time.Sleep(250 * time.Millisecond)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), fmt.Sprint(err))
}

func TestSharedFilesystemRecoveryWorkerContinuesPastBlockedProbe(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	first, firstStage, _ := stageSharedFilesystemFixture(t, ns, "blocked-probe")
	second, secondStage, _ := stageSharedFilesystemFixture(t, ns, "independent-probe")
	started, completed := make(chan struct{}, 1), make(chan struct{}, 1)
	ns.sharedFS.probe = func(ctx context.Context, path string) error {
		if path == firstStage {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
		if path == secondStage {
			select {
			case completed <- struct{}{}:
			default:
			}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ns.sharedFilesystemRecovery.run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	ns.sharedFilesystemRecovery.enqueue(first, "test")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first recovery did not start")
	}
	ns.sharedFilesystemRecovery.enqueue(second, "test")
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("blocked volume starved the other recovery worker")
	}
}

func TestSharedFilesystemPublishRejectsDifferentPersistedStage(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	first, _, target := stageSharedFilesystemFixture(t, ns, "first")
	_, otherStage, _ := stageSharedFilesystemFixture(t, ns, "second")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(first, otherStage, target))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, mounted, err := ns.mountPointForPath(target)
	require.NoError(t, err)
	require.False(t, mounted)
}

func TestSharedFilesystemSeedKeepsMissingPublishedTarget(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "seed-target")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
	require.NoError(t, ns.mounter.Interface.Unmount(target))
	ns.sharedFilesystemRecovery.seedSessionsFromMounts(context.Background())
	session, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, session.PublishedTargets, 1)
	require.Equal(t, target, session.PublishedTargets[0].TargetPath)
}

func TestSharedFilesystemUnreapedChildFencesOnlyItsVolume(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	ctx, cancel := context.WithCancel(context.Background())
	ctx, release, err := ns.lockSharedFilesystemOperation(ctx, "cephfs:blocked-child")
	require.NoError(t, err)
	done := make(chan struct{})
	cancel()
	started := time.Now()
	require.ErrorIs(t, awaitSharedFilesystemExit(ctx, done, func() {}), context.Canceled)
	require.Less(t, time.Since(started), 2*time.Second)
	release()
	_, acquired := ns.Driver.operationLocks.TryAcquire("sharedfs:cephfs:blocked-child")
	require.False(t, acquired)
	other, acquired := ns.Driver.operationLocks.TryAcquire("sharedfs:cephfs:other-child")
	require.True(t, acquired)
	other()
	close(done)
	require.Eventually(t, func() bool {
		unlock, ok := ns.Driver.operationLocks.TryAcquire("sharedfs:cephfs:blocked-child")
		if ok {
			unlock()
		}
		return ok
	}, time.Second, time.Millisecond)
}

func TestSharedFilesystemUnpublishIntentSurvivesFailedSessionSave(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "unpublish-restart")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
	unmount := ns.sharedFS.unmount
	ns.sharedFS.unmount = func(ctx context.Context, path string) error {
		if err := unmount(ctx, path); err != nil {
			return err
		}
		return os.Mkdir(ns.sharedFilesystemRecovery.store.pathForVolume(id)+".tmp", 0700)
	}
	_, err = ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: id, TargetPath: target})
	require.Error(t, err)
	require.NoError(t, os.Remove(ns.sharedFilesystemRecovery.store.pathForVolume(id)+".tmp"))
	ns.sharedFS.unmount = unmount
	// A newly constructed manager sees stale session JSON but durable intent.
	ns.sharedFilesystemRecovery = newSharedFilesystemRecoveryManager(ns)
	require.NoError(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id))
	_, mounted, err := ns.mountPointForPath(target)
	require.NoError(t, err)
	require.False(t, mounted)
	session, _, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	require.Empty(t, session.PublishedTargets)
	// A later explicit kubelet publish is the only action which reverses intent.
	_, err = ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
}

func TestSharedFilesystemUnpublishIntentFailureLeavesMountUntouched(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "intent-failure")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
	path := ns.sharedFilesystemRecovery.store.unpublishIntentPath(id, target) + ".tmp"
	require.NoError(t, os.Mkdir(path, 0700))
	before, err := ns.mounter.List()
	require.NoError(t, err)
	_, err = ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: id, TargetPath: target})
	require.Error(t, err)
	after, err := ns.mounter.List()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestSharedFilesystemUnstageRetainsMissingStageWithPublishedBind(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "unstage-bind")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
	require.NoError(t, ns.sharedFS.unmount(context.Background(), stage))
	_, err = ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{VolumeId: id, StagingTargetPath: stage})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	session, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	require.True(t, exists)
	require.False(t, session.Unstaging)
	require.Len(t, session.PublishedTargets, 1)
	require.FileExists(t, session.KeyringPath)
}

func TestSharedFilesystemUnstageRejectsForeignStage(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	first, _, _ := stageSharedFilesystemFixture(t, ns, "unstage-first")
	_, otherStage, _ := stageSharedFilesystemFixture(t, ns, "unstage-second")
	before, err := ns.mounter.List()
	require.NoError(t, err)
	_, err = ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{VolumeId: first, StagingTargetPath: otherStage})
	require.Error(t, err)
	after, err := ns.mounter.List()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestSharedFilesystemHealthyStageAliasRejectedWithoutTargets(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "stage-alias")
	_, otherStage, _ := stageSharedFilesystemFixture(t, ns, "stage-owner")
	ns.sharedFS.mountInfo = func() ([]mount.MountInfo, error) {
		return []mount.MountInfo{
			{MountPoint: stage, Minor: 223, Root: "/", FsType: "fuse.ceph-fuse"},
			{MountPoint: otherStage, Minor: 223, Root: "/", FsType: "fuse.ceph-fuse"},
		}, nil
	}
	_, err := ns.NodeStageVolume(context.Background(), newSharedFilesystemStageRequest(id, stage, "fuse"))
	require.ErrorContains(t, err, "belongs to another volume")
	_, err = ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.ErrorContains(t, err, "belongs to another volume")
	require.Error(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id))
}

func TestSharedFilesystemRecoveryDoesNotResurrectMissingKubeletTarget(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "missing-kubelet-target")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
	require.NoError(t, ns.sharedFS.unmount(context.Background(), target))
	require.NoError(t, os.RemoveAll(filepath.Dir(target)))
	require.Error(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id))
	_, mounted, err := ns.mountPointForPath(target)
	require.NoError(t, err)
	require.False(t, mounted)
}

func TestSharedFilesystemUnknownTargetProbeErrorCannotReportHealthy(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "unknown-probe")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
	original := ns.sharedFS.probe
	ns.sharedFS.probe = func(ctx context.Context, path string) error {
		if path == target {
			return syscall.EIO
		}
		return original(ctx, path)
	}
	require.ErrorIs(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id), syscall.EIO)
}

func TestSharedFilesystemLegacySessionRecoversMissingStage(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "legacy-session")
	_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
	require.NoError(t, err)
	session, _, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	// Same JSON shape as v0.5.27, also exercise older omitted derived paths.
	session.KeyringPath, session.SecretFilePath = "", ""
	payload, err := json.Marshal(session)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(ns.sharedFilesystemRecovery.store.pathForVolume(id), payload, 0600))
	require.NoError(t, ns.sharedFS.unmount(context.Background(), stage))
	require.NoError(t, os.Remove(stage))
	ns.sharedFilesystemRecovery = newSharedFilesystemRecoveryManager(ns)
	require.NoError(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id))
	require.NoError(t, ns.verifySharedFilesystemBind(stage, target))
}

func TestSharedFilesystemUnstageReplaysAfterRestart(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, _ := stageSharedFilesystemFixture(t, ns, "unstage-replay")
	session, _, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	session.Unstaging = true
	require.NoError(t, ns.sharedFilesystemRecovery.store.Save(session))
	ns.sharedFilesystemRecovery = newSharedFilesystemRecoveryManager(ns)
	require.NoError(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id))
	_, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	require.False(t, exists)
	require.NoDirExists(t, stage)
	require.NoDirExists(t, sharedCephFSKeyringDir(stage))
}

func TestSharedFilesystemRecoveryRetainsEventDuringAttempt(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, _ := stageSharedFilesystemFixture(t, ns, "event-during-attempt")
	m := ns.sharedFilesystemRecovery
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := ns.sharedFS.probe
	once := false
	ns.sharedFS.probe = func(ctx context.Context, path string) error {
		if path == stage && !once {
			once = true
			m.enqueue(id, "fuse_exit")
		}
		return probe(ctx, path)
	}
	m.enqueue(id, "sweep")
	require.Equal(t, id, <-m.queue)
	m.recoverQueuedVolume(ctx, id)
	select {
	case next := <-m.queue:
		require.Equal(t, id, next)
	case <-time.After(7 * time.Second):
		t.Fatal("event raised during recovery was lost")
	}
}

func TestSharedFilesystemRejectsSessionStoredUnderAnotherVolume(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	first, _, _ := stageSharedFilesystemFixture(t, ns, "session-owner")
	second, secondStage, _ := stageSharedFilesystemFixture(t, ns, "session-other")
	payload, err := os.ReadFile(ns.sharedFilesystemRecovery.store.pathForVolume(second))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(ns.sharedFilesystemRecovery.store.pathForVolume(first), payload, 0600))
	require.NoError(t, ns.sharedFS.unmount(context.Background(), secondStage))
	before, err := ns.mounter.List()
	require.NoError(t, err)
	require.Error(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), first))
	after, err := ns.mounter.List()
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = ns.sharedFilesystemRecovery.store.List()
	require.Error(t, err)
}

func TestSharedFilesystemOrphanCleanupRetriesPartialIntentWrite(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, target := stageSharedFilesystemFixture(t, ns, "gc-intent")
	other := strings.Replace(target, "pod-gc-intent", "pod-gc-intent-other", 1)
	require.NoError(t, os.MkdirAll(filepath.Dir(other), 0750))
	metadata, err := os.ReadFile(filepath.Join(filepath.Dir(target), "vol_data.json"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(other), "vol_data.json"), metadata, 0600))
	for _, path := range []string{target, other} {
		_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, path))
		require.NoError(t, err)
		require.NoError(t, ns.sharedFS.unmount(context.Background(), path))
	}
	ns.Driver.kubeRuntime = &KubeRuntime{client: fake.NewSimpleClientset(), enabled: true}
	session, _, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	failedPath := ns.sharedFilesystemRecovery.store.unpublishIntentPath(id, session.PublishedTargets[1].TargetPath) + ".tmp"
	require.NoError(t, os.Mkdir(failedPath, 0700))
	collected, err := ns.sharedFilesystemRecovery.garbageCollectOrphanedSession(context.Background(), session)
	require.Error(t, err)
	require.False(t, collected)
	persisted, _, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	require.False(t, persisted.Unstaging)
	require.NoError(t, os.Remove(failedPath))
	ns.sharedFilesystemRecovery = newSharedFilesystemRecoveryManager(ns)
	require.NoError(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id))
	_, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestSharedFilesystemOrphanUnmountFailurePreservesVolumeBytes(t *testing.T) {
	withSharedFilesystemTestPaths(t)
	ns := getTestNodeServer(nil)
	id, stage, _ := stageSharedFilesystemFixture(t, ns, "gc-busy")
	sentinel := filepath.Join(stage, "keep.bin")
	require.NoError(t, os.WriteFile(sentinel, []byte("preserve orphan bytes"), 0600))
	ns.Driver.kubeRuntime = &KubeRuntime{client: fake.NewSimpleClientset(), enabled: true}
	ns.sharedFS.unmount = func(context.Context, string) error { return syscall.EBUSY }
	session, _, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	collected, err := ns.sharedFilesystemRecovery.garbageCollectOrphanedSession(context.Background(), session)
	require.Error(t, err)
	require.False(t, collected)
	payload, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, "preserve orphan bytes", string(payload))
	_, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
	require.NoError(t, err)
	require.True(t, exists)
}
