package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	mount "k8s.io/mount-utils"
)

func TestLoadFeatureGatesIncludesCephFSRecoveryDefaults(t *testing.T) {
	gates := defaultFeatureGates()
	assert.True(t, gates.CephFSPersistentRecovery)
	assert.False(t, gates.CephFSKernelMounts)

	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.FeatureGatesVar, "cephfsPersistentRecovery=false,cephfsKernelMounts=true")
	loaded := loadFeatureGates(cfg)
	assert.False(t, loaded.CephFSPersistentRecovery)
	assert.True(t, loaded.CephFSKernelMounts)
}

func TestStageSharedFilesystemPersistsSessionAndCredentials(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	ns := getTestNodeServer(nil)
	volumeID, stagePath, _ := sharedFilesystemFixturePaths(t, ns, "test")

	resp, err := ns.NodeStageVolume(context.Background(), newSharedFilesystemStageRequest(volumeID, stagePath, "fuse"))
	require.NoError(t, err)
	assert.Equal(t, &csi.NodeStageVolumeResponse{}, resp)

	session, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, sharedFilesystemMounterFuse, session.Mounter)
	assert.Equal(t, "csi-node", session.UserID)
	assert.Equal(t, []string{"mon1", "mon2"}, session.Monitors)
	assert.Equal(t, "/kubernetes/dynamic/test", session.Subpath)

	_, err = os.Stat(sharedCephFSKeyringPath(stagePath))
	assert.NoError(t, err)
	_, err = os.Stat(sharedCephFSSecretPath(stagePath))
	assert.NoError(t, err)
}

func TestNodeGetVolumeStatsQueuesRecoveryForStaleCephFSMount(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	tempDir := t.TempDir()
	volumeID := "cephfs:test-stale-stats"
	ns := getTestNodeServer([]string{tempDir})

	originalStat := nodeVolumePathStat
	originalStatfs := nodeVolumePathFS
	t.Cleanup(func() {
		nodeVolumePathStat = originalStat
		nodeVolumePathFS = originalStatfs
	})

	nodeVolumePathStat = func(name string) (os.FileInfo, error) {
		return originalStat(name)
	}
	nodeVolumePathFS = func(path string, buf *unix.Statfs_t) error {
		return syscall.ENOTCONN
	}

	resp, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   volumeID,
		VolumePath: tempDir,
	})

	assert.Nil(t, resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stale CephFS mount detected")

	ns.sharedFilesystemRecovery.mu.Lock()
	_, queued := ns.sharedFilesystemRecovery.queued[volumeID]
	ns.sharedFilesystemRecovery.mu.Unlock()
	assert.True(t, queued)
}

func TestSharedFilesystemRecoveryRebindsMissingTarget(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	ns := getTestNodeServer(nil)
	volumeID, stagePath, targetPath := stageSharedFilesystemFixture(t, ns, "rebind")
	var err error

	_, err = ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(volumeID, stagePath, targetPath))
	require.NoError(t, err)
	session, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, session.PublishedTargets, 1)

	fakeMounter, ok := ns.mounter.Interface.(*mount.FakeMounter)
	require.True(t, ok)
	filtered := make([]mount.MountPoint, 0, len(fakeMounter.MountPoints))
	for _, mountPoint := range fakeMounter.MountPoints {
		if mountPoint.Path == targetPath {
			continue
		}
		filtered = append(filtered, mountPoint)
	}
	fakeMounter.MountPoints = filtered

	err = ns.sharedFilesystemRecovery.recoverVolume(context.Background(), volumeID)
	require.NoError(t, err)

	check, err := ns.checkMountPoint(stagePath, targetPath, nil)
	require.NoError(t, err)
	assert.True(t, check.targetIsMountPoint)
}

func TestSharedFilesystemPublishRejectsStaleStageWithoutBinding(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	ns := getTestNodeServer(nil)
	ns.Driver.featureGates.CephFSSelfHealing = false
	volumeID, stagePath, targetPath := stageSharedFilesystemFixture(t, ns, "stale-stage-publish")

	originalDetect := detectSharedFilesystemMount
	detectSharedFilesystemMount = func(path string) error {
		if path == stagePath {
			return syscall.ENOTCONN
		}
		return originalDetect(path)
	}
	t.Cleanup(func() {
		detectSharedFilesystemMount = originalDetect
	})

	resp, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(volumeID, stagePath, targetPath))
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "stale shared filesystem staging target")

	check, checkErr := ns.checkMountPoint(stagePath, targetPath, nil)
	require.NoError(t, checkErr)
	assert.False(t, check.targetIsMountPoint)
}

func TestSharedFilesystemPublishRehydratesMissingSession(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	ns := getTestNodeServer(nil)
	volumeID, stagePath, targetPath := stageSharedFilesystemFixture(t, ns, "rehydrate")
	stageReq := newSharedFilesystemStageRequest(volumeID, stagePath, "fuse")
	require.NoError(t, ns.deleteSharedFilesystemSession(volumeID))
	var err error

	publishReq := newSharedFilesystemPublishRequest(volumeID, stagePath, targetPath)
	for key, value := range stageReq.GetPublishContext() {
		publishReq.PublishContext[key] = value
	}

	_, err = ns.NodePublishVolume(context.Background(), publishReq)
	require.NoError(t, err)

	session, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, "csi-node", session.UserID)
	require.Len(t, session.PublishedTargets, 1)
	assert.Equal(t, targetPath, session.PublishedTargets[0].TargetPath)
}

func TestSharedFilesystemRestageRehydratesStaleSession(t *testing.T) {
	for _, mountBecomesHealthy := range []bool{true, false} {
		name := "recovered"
		if !mountBecomesHealthy {
			name = "still-disconnected"
		}
		t.Run(name, func(t *testing.T) {
			withSharedFilesystemTestPaths(t)
			ns := getTestNodeServer(nil)
			ns.Driver.featureGates.CephFSSelfHealing = true
			id, stage, target := stageSharedFilesystemFixture(t, ns, "restage-missing-record")
			_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
			require.NoError(t, err)
			// A client can outlive a missing session record. Kubelet's adjacent
			// metadata and the existing mount identities still prove its targets.
			require.NoError(t, ns.deleteSharedFilesystemSession(id))
			healthy := false
			ns.sharedFS.probe = func(context.Context, string) error {
				if !healthy {
					return syscall.ENOTCONN
				}
				return nil
			}
			mountAttempts := 0
			originalFuse := ns.sharedFS.fuse
			ns.sharedFS.fuse = func(ctx context.Context, session sharedFilesystemSession, args []string) error {
				mountAttempts++
				persisted, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
				require.NoError(t, err)
				require.True(t, exists, "mount intent must be durable before remount")
				require.Len(t, persisted.PublishedTargets, 1)
				require.Equal(t, target, persisted.PublishedTargets[0].TargetPath)
				healthy = mountBecomesHealthy
				return originalFuse(ctx, session, args)
			}

			response, err := ns.NodeStageVolume(context.Background(), newSharedFilesystemStageRequest(id, stage, "fuse"))
			if mountBecomesHealthy {
				require.NoError(t, err)
				require.NotNil(t, response)
				require.NoError(t, ns.verifySharedFilesystemBind(stage, target))
			} else {
				require.Error(t, err, "a disconnected stage cannot be acknowledged as staged")
				require.Nil(t, response)
			}
			require.Equal(t, 1, mountAttempts, "restage must actually recover the disconnected mount")
			persisted, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
			require.NoError(t, err)
			require.True(t, exists)
			require.Len(t, persisted.PublishedTargets, 1, "discovered bind intent must survive recovery failures")
		})
	}
}

func TestSharedFilesystemReadonlyPublishOverridesWritableCapabilityFlags(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags []string
		keep  []string
	}{
		{name: "rw", flags: []string{"rw"}},
		{name: "comma-separated", flags: []string{"noexec,rw,nodev"}, keep: []string{"noexec", "nodev"}},
		{name: "duplicates", flags: []string{"ro", "rw", "ro"}},
		{name: "mixed-duplicates", flags: []string{"ro,rw,ro", "nosuid,rw"}, keep: []string{"nosuid"}},
	} {
		for _, partial := range []bool{false, true} {
			name := test.name + "/complete-bind"
			if partial {
				name = test.name + "/partial-bind"
			}
			t.Run(name, func(t *testing.T) {
				withSharedFilesystemTestPaths(t)
				ns := getTestNodeServer(nil)
				id, stage, target := stageSharedFilesystemFixture(t, ns, "readonly-capability")
				req := newSharedFilesystemPublishRequest(id, stage, target)
				req.Readonly = true
				req.VolumeCapability.GetMount().MountFlags = test.flags
				bind := ns.sharedFS.bind
				leaveWritable := partial
				ns.sharedFS.bind = func(ctx context.Context, stage, target string, options []string) error {
					// Model mount's effective ro/rw state, including comma-separated
					// options. Mountinfo reports that effective state, not the input.
					readonly := false
					var effective []string
					for _, argument := range options {
						for _, option := range strings.Split(argument, ",") {
							switch option = strings.TrimSpace(option); option {
							case "ro":
								readonly = true
							case "rw":
								readonly = false
							default:
								effective = append(effective, option)
							}
						}
					}
					if readonly && !leaveWritable {
						effective = append(effective, "ro")
					} else {
						effective = append(effective, "rw")
					}
					if err := bind(ctx, stage, target, effective); err != nil {
						return err
					}
					if leaveWritable {
						return syscall.EIO
					}
					return nil
				}

				_, err := ns.NodePublishVolume(context.Background(), req)
				if partial {
					require.Error(t, err)
					_, err = ns.NodePublishVolume(context.Background(), req)
					require.Error(t, err, "readonly retry cannot accept a writable partial bind")
					session, _, err := ns.sharedFilesystemRecovery.store.Load(id)
					require.NoError(t, err)
					health, err := ns.evaluateSharedFilesystemSession(context.Background(), session)
					require.NoError(t, err)
					require.Len(t, health.TargetsToRebind, 1)
					leaveWritable = false
					require.NoError(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id))
				} else {
					require.NoError(t, err)
				}
				actual, mounted, err := ns.mountPointForPath(target)
				require.NoError(t, err)
				require.True(t, mounted)
				require.Contains(t, actual.Opts, "ro", "explicit readonly must determine the effective bind permission")
				require.NotContains(t, actual.Opts, "rw")
				for _, option := range test.keep {
					require.Contains(t, actual.Opts, option)
				}
				_, err = ns.NodePublishVolume(context.Background(), req)
				require.NoError(t, err)
			})
		}
	}
}

func TestSharedFilesystemGarbageCollectSkipsWhenPodLookupUnknown(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	ns := getTestNodeServer(nil)
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api unavailable")
	})
	ns.Driver.kubeRuntime = &KubeRuntime{client: client, enabled: true}

	volumeID := "cephfs:test-gc-pod-lookup-unknown"
	session := sharedFilesystemSession{
		VolumeID:          volumeID,
		StagingTargetPath: filepath.Join(t.TempDir(), "globalmount"),
		PublishedTargets: []sharedFilesystemPublishedTarget{
			{TargetPath: "/var/lib/kubelet/pods/test-pod-uid/volumes/kubernetes.io~csi/pvc-test/mount"},
		},
	}
	require.NoError(t, ns.recordSharedFilesystemSession(session))

	collected, gcErr := ns.sharedFilesystemRecovery.garbageCollectOrphanedSession(context.Background(), session)
	require.NoError(t, gcErr)
	assert.False(t, collected)

	_, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestMountSharedFilesystemSessionRecreatesMissingStagePath(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	ns := getTestNodeServer(nil)
	volumeID, stagePath, _ := sharedFilesystemFixturePaths(t, ns, "missing-stage-path")

	_, err := ns.NodeStageVolume(context.Background(), newSharedFilesystemStageRequest(volumeID, stagePath, "fuse"))
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(stagePath))
	_, err = os.Stat(stagePath)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))

	session, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, ns.mountSharedFilesystemSession(context.Background(), session))

	info, err := os.Stat(stagePath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestStageSharedFilesystemKernelMounterRequiresFeatureGate(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	ns := getTestNodeServer(nil)
	volumeID, stagePath, _ := sharedFilesystemFixturePaths(t, ns, "kernel-gate")
	_, err := ns.NodeStageVolume(context.Background(), newSharedFilesystemStageRequest(volumeID, stagePath, "kernel"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cephfsKernelMounts=true")
}

func TestStageSharedFilesystemKernelMounterSucceedsWhenEnabled(t *testing.T) {
	withSharedFilesystemTestPaths(t)

	procFile := filepath.Join(t.TempDir(), "filesystems")
	require.NoError(t, os.WriteFile(procFile, []byte("nodev\tceph\n"), 0o644))
	sharedFilesystemProcFilesystemsPath = procFile

	ns := getTestNodeServer(nil)
	volumeID, stagePath, _ := sharedFilesystemFixturePaths(t, ns, "kernel-success")
	ns.Driver.featureGates.CephFSKernelMounts = true

	resp, err := ns.NodeStageVolume(context.Background(), newSharedFilesystemStageRequest(volumeID, stagePath, "kernel"))
	require.NoError(t, err)
	assert.Equal(t, &csi.NodeStageVolumeResponse{}, resp)

	session, exists, err := ns.sharedFilesystemRecovery.store.Load(volumeID)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, sharedFilesystemMounterKernel, session.Mounter)

	_, err = os.Stat(sharedCephFSSecretPath(stagePath))
	assert.NoError(t, err)
}

func TestHostSupportsCephKernelClient(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "filesystems")

	require.NoError(t, os.WriteFile(tempFile, []byte("nodev\text4\n"), 0o644))
	supported, err := hostSupportsCephKernelClient(tempFile)
	require.NoError(t, err)
	assert.False(t, supported)

	require.NoError(t, os.WriteFile(tempFile, []byte("nodev\tceph\n"), 0o644))
	supported, err = hostSupportsCephKernelClient(tempFile)
	require.NoError(t, err)
	assert.True(t, supported)
}

func withSharedFilesystemTestPaths(t *testing.T) {
	t.Helper()

	originalSessionRoot := sharedFilesystemSessionRootPath
	originalProcFilesystems := sharedFilesystemProcFilesystemsPath
	sharedFilesystemSessionRootPath = filepath.Join(t.TempDir(), "cephfs-sessions")
	sharedFilesystemProcFilesystemsPath = "/proc/filesystems"

	t.Cleanup(func() {
		sharedFilesystemSessionRootPath = originalSessionRoot
		sharedFilesystemProcFilesystemsPath = originalProcFilesystems
	})
}

func newSharedFilesystemStageRequest(volumeID, stagePath, mounter string) *csi.NodeStageVolumeRequest {
	subpath := "/kubernetes/dynamic/test"
	if metadata, err := opennebula.DecodeSharedVolumeID(volumeID); err == nil {
		subpath = metadata.Subpath
	}
	return &csi.NodeStageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagePath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: map[string]string{
			storageClassParamCephFSMounter: mounter,
		},
		PublishContext: map[string]string{
			sharedPublishContextShareBackend:   "cephfs",
			sharedPublishContextCephFSMonitors: "mon1,mon2",
			sharedPublishContextCephFSFSName:   "cephfs-prod",
			sharedPublishContextCephFSSubpath:  subpath,
			sharedPublishContextCephFSReadonly: "false",
		},
		Secrets: map[string]string{
			sharedNodeStageSecretUserIDKey:  "csi-node",
			sharedNodeStageSecretUserKeyKey: "super-secret",
		},
	}
}

func newSharedFilesystemPublishRequest(volumeID, stagePath, targetPath string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagePath,
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		PublishContext: map[string]string{
			sharedPublishContextShareBackend: "cephfs",
		},
	}
}

func TestSharedFilesystemPublishRehydratesStaleSessionWithExistingBinds(t *testing.T) {
	for _, mountBecomesHealthy := range []bool{true, false} {
		name := "recovered"
		if !mountBecomesHealthy {
			name = "still-disconnected"
		}
		t.Run(name, func(t *testing.T) {
			withSharedFilesystemTestPaths(t)
			ns := getTestNodeServer(nil)
			ns.Driver.featureGates.CephFSSelfHealing = true
			id, stage, target := stageSharedFilesystemFixture(t, ns, "publish-missing-record")
			_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(id, stage, target))
			require.NoError(t, err)
			require.NoError(t, ns.deleteSharedFilesystemSession(id))
			healthy := false
			ns.sharedFS.probe = func(context.Context, string) error {
				if !healthy {
					return syscall.ENOTCONN
				}
				return nil
			}
			mountAttempts := 0
			originalFuse := ns.sharedFS.fuse
			ns.sharedFS.fuse = func(ctx context.Context, session sharedFilesystemSession, args []string) error {
				mountAttempts++
				persisted, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
				require.NoError(t, err)
				require.True(t, exists, "mount intent must be durable before remount")
				require.Len(t, persisted.PublishedTargets, 1)
				require.Equal(t, target, persisted.PublishedTargets[0].TargetPath)
				healthy = mountBecomesHealthy
				return originalFuse(ctx, session, args)
			}

			request := newSharedFilesystemPublishRequest(id, stage, target)
			for key, value := range newSharedFilesystemStageRequest(id, stage, "fuse").PublishContext {
				request.PublishContext[key] = value
			}
			response, err := ns.NodePublishVolume(context.Background(), request)
			if mountBecomesHealthy {
				require.NoError(t, err)
				require.NotNil(t, response)
				require.NoError(t, ns.verifySharedFilesystemBind(stage, target))
			} else {
				require.Error(t, err, "a disconnected stage cannot be acknowledged as published")
				require.Nil(t, response)
			}
			require.Equal(t, 1, mountAttempts, "publish must actually recover the disconnected mount")
			persisted, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
			require.NoError(t, err)
			require.True(t, exists)
			require.Len(t, persisted.PublishedTargets, 1, "discovered bind intent must survive recovery failures")
		})
	}
}

func TestSharedFilesystemReconstructionPreservesSiblingGenerations(t *testing.T) {
	for _, operation := range []string{"stage", "publish"} {
		for _, generation := range []string{"absent-stage", "new-stage", "ambiguous", "missing-metadata"} {
			t.Run(operation+"/"+generation, func(t *testing.T) {
				withSharedFilesystemTestPaths(t)
				ns := getTestNodeServer(nil)
				ns.Driver.featureGates.CephFSSelfHealing = true
				id, stage, first := stageSharedFilesystemFixture(t, ns, "siblings")
				otherID, otherStage, otherTarget := stageSharedFilesystemFixture(t, ns, "unrelated")
				_, err := ns.NodePublishVolume(context.Background(), newSharedFilesystemPublishRequest(otherID, otherStage, otherTarget))
				require.NoError(t, err)
				second := filepath.Join(filepath.Dir(filepath.Dir(first)), "pvc-sibling", "mount")
				require.NoError(t, os.MkdirAll(filepath.Dir(second), 0750))
				payload, err := json.Marshal(sharedFilesystemVolumeData{DriverName: DefaultDriverName, VolumeHandle: id, SpecVolID: "pvc-sibling", NodeName: ns.Driver.nodeID})
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(second), "vol_data.json"), payload, 0600))
				for _, target := range []string{first, second} {
					req := newSharedFilesystemPublishRequest(id, stage, target)
					req.Readonly = target == second
					_, err := ns.NodePublishVolume(context.Background(), req)
					require.NoError(t, err)
				}
				fakeMount := ns.mounter.Interface.(*mount.FakeMounter)
				ns.sharedFS.unmount = func(ctx context.Context, path string) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					before, err := fakeMount.List()
					if err != nil {
						return err
					}
					if err := fakeMount.Unmount(path); err != nil {
						return err
					}
					for i, remaining := range fakeMount.MountPoints {
						for _, original := range before {
							if remaining.Path == original.Path {
								fakeMount.MountPoints[i] = original
							}
						}
					}
					return nil
				}
				for i := range fakeMount.MountPoints {
					if fakeMount.MountPoints[i].Path == first || fakeMount.MountPoints[i].Path == second {
						fakeMount.MountPoints[i].Device = "old-superblock"
					}
				}
				if generation == "absent-stage" {
					require.NoError(t, ns.sharedFS.unmount(context.Background(), stage))
				}
				if generation == "ambiguous" {
					fakeMount.MountPoints = append(fakeMount.MountPoints, mount.MountPoint{Device: "duplicate", Path: second, Type: "fuse.ceph-fuse"})
				}
				if generation == "missing-metadata" {
					require.NoError(t, os.Remove(filepath.Join(filepath.Dir(second), "vol_data.json")))
				}
				require.NoError(t, ns.deleteSharedFilesystemSession(id))
				healthy := false
				ns.sharedFS.probe = func(_ context.Context, path string) error {
					if path == otherStage || path == otherTarget || healthy {
						return nil
					}
					return syscall.ENOTCONN
				}
				mounts := 0
				fuse := ns.sharedFS.fuse
				ns.sharedFS.fuse = func(ctx context.Context, session sharedFilesystemSession, args []string) error {
					mounts++
					persisted, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
					require.NoError(t, err)
					require.True(t, exists)
					require.Len(t, persisted.PublishedTargets, 2)
					for _, target := range persisted.PublishedTargets {
						if target.TargetPath == second {
							require.Contains(t, target.MountOptions, "ro")
						}
					}
					healthy = true
					return fuse(ctx, session, args)
				}
				before, err := ns.mounter.List()
				require.NoError(t, err)
				if operation == "stage" {
					_, err = ns.NodeStageVolume(context.Background(), newSharedFilesystemStageRequest(id, stage, "fuse"))
				} else {
					req := newSharedFilesystemPublishRequest(id, stage, first)
					for key, value := range newSharedFilesystemStageRequest(id, stage, "fuse").PublishContext {
						req.PublishContext[key] = value
					}
					_, err = ns.NodePublishVolume(context.Background(), req)
					if generation == "absent-stage" {
						require.Error(t, err)
						require.Zero(t, mounts)
						require.NoError(t, ns.sharedFilesystemRecovery.recoverVolume(context.Background(), id))
						_, err = ns.NodePublishVolume(context.Background(), req)
					}
				}
				if generation == "ambiguous" || generation == "missing-metadata" {
					require.Error(t, err)
					require.Zero(t, mounts)
					_, exists, err := ns.sharedFilesystemRecovery.store.Load(id)
					require.NoError(t, err)
					require.False(t, exists)
					after, err := ns.mounter.List()
					require.NoError(t, err)
					require.Equal(t, before, after)
					return
				}
				require.NoError(t, err)
				require.Equal(t, 1, mounts)
				require.NoError(t, ns.verifySharedFilesystemBind(stage, first))
				require.NoError(t, ns.verifySharedFilesystemBind(stage, second))
				require.NoError(t, ns.verifySharedFilesystemTargetFlags(sharedFilesystemPublishedTarget{TargetPath: second, MountOptions: []string{"ro"}}))
				require.NoError(t, ns.verifySharedFilesystemBind(otherStage, otherTarget))
				for _, original := range before {
					if original.Path == otherStage || original.Path == otherTarget {
						current, mounted, err := ns.mountPointForPath(original.Path)
						require.NoError(t, err)
						require.True(t, mounted)
						require.Equal(t, original, current)
					}
				}
			})
		}
	}
}
