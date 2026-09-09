package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"syscall"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

const (
	sharedPublishContextShareBackend   = "shareBackend"
	sharedPublishContextCephFSMonitors = "cephfsMonitors"
	sharedPublishContextCephFSFSName   = "cephfsFSName"
	sharedPublishContextCephFSSubpath  = "cephfsSubpath"
	sharedPublishContextCephFSReadonly = "cephfsReadonly"
	sharedPublishContextCephFSMounts   = "cephfsMountOptions"

	sharedNodeStageSecretUserIDKey  = "userID"
	sharedNodeStageSecretUserKeyKey = "userKey"

	sharedCephFSTempDirSuffix = ".cephfs"
	sharedCephFSKeyringFile   = "ceph.client.keyring"
)

func isSharedFilesystemRequest(volumeID string, publishContext map[string]string) bool {
	return opennebula.IsSharedFilesystemVolumeID(volumeID) || strings.TrimSpace(publishContext[sharedPublishContextShareBackend]) != ""
}

func (ns *NodeServer) handleSharedFilesystemStage(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, sharedFilesystemAttemptTimeout)
	defer cancel()
	ctx, release, lockErr := ns.lockSharedFilesystemOperation(ctx, req.GetVolumeId())
	if lockErr != nil {
		return nil, lockErr
	}
	defer release()

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}

	if _, ok := volumeCapability.GetAccessType().(*csi.VolumeCapability_Block); ok {
		return nil, status.Error(codes.InvalidArgument, "shared filesystem volumes require a filesystem volume capability")
	}

	publishContext := req.GetPublishContext()
	if strings.TrimSpace(publishContext[sharedPublishContextShareBackend]) != "cephfs" {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported shared filesystem backend %q", publishContext[sharedPublishContextShareBackend])
	}

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	session, err := sharedFilesystemSessionFromStageRequest(req)
	if err != nil {
		return nil, err
	}

	secrets := req.GetSecrets()
	userID := strings.TrimSpace(secrets[sharedNodeStageSecretUserIDKey])
	userKey := strings.TrimSpace(secrets[sharedNodeStageSecretUserKeyKey])
	if userID == "" || userKey == "" {
		return nil, status.Errorf(codes.InvalidArgument, "CephFS node staging requires secret keys %q and %q", sharedNodeStageSecretUserIDKey, sharedNodeStageSecretUserKeyKey)
	}

	if err := ns.verifySharedFilesystemStage(session.VolumeID, stagingTargetPath); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	keyringPath, secretFilePath, err := ensureSharedFilesystemCredentials(stagingTargetPath, userID, userKey)
	if err != nil {
		return nil, err
	}
	session.KeyringPath = keyringPath
	session.SecretFilePath = secretFilePath

	old, exists, err := ns.sharedFilesystemRecovery.store.Load(session.VolumeID)
	if err != nil {
		return nil, err
	}
	if exists {
		if old.Unstaging {
			return nil, status.Error(codes.FailedPrecondition, "volume is being unstaged")
		}
		if old.StagingTargetPath != session.StagingTargetPath {
			return nil, status.Error(codes.FailedPrecondition, "staging path differs from persisted session")
		}
		session.PublishedTargets = old.PublishedTargets
	}
	_, mounted, err := ns.mountPointForPath(stagingTargetPath)
	if err != nil {
		return nil, err
	}
	if !exists && mounted {
		// A preexisting client may have no session record. Reconstruct every
		// provable bind before recovery so remounting cannot abandon its targets.
		session.PublishedTargets, err = ns.discoverSharedFilesystemTargets(session.VolumeID, stagingTargetPath)
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
	}
	if err := ns.verifySharedFilesystemSession(ctx, session); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if mounted {
		if err := ns.sharedFS.probe(ctx, stagingTargetPath); err == nil {
			// Restaging must not discard bind targets recorded by prior publishes.
			if old, ok, loadErr := ns.sharedFilesystemRecovery.store.Load(session.VolumeID); loadErr != nil {
				return nil, loadErr
			} else if ok {
				session.PublishedTargets = old.PublishedTargets
			}
			if err := ns.recordSharedFilesystemSession(session); err != nil {
				return nil, err
			}
			return &csi.NodeStageVolumeResponse{}, nil
		} else if !isDisconnectedSharedFilesystemError(err) || !ns.Driver.featureGates.CephFSSelfHealing {
			return nil, status.Errorf(codes.FailedPrecondition, "stale CephFS mount detected at %s: %v", stagingTargetPath, err)
		}
		// Recovery treats an absent record as a completed unstage. Persist this
		// validated stage intent first, including any reconstructed bind targets.
		if err := ns.recordSharedFilesystemSession(session); err != nil {
			return nil, err
		}
		if err := ns.sharedFilesystemRecovery.recoverVolumeLocked(ctx, req.GetVolumeId()); err != nil {
			return nil, status.Errorf(codes.Unavailable, "CephFS stage recovery failed: %v", err)
		}
		if err := ns.confirmSharedFilesystemStage(ctx, session.VolumeID, stagingTargetPath); err != nil {
			return nil, status.Errorf(codes.Unavailable, "CephFS stage recovery did not establish a healthy mount: %v", err)
		}
		return &csi.NodeStageVolumeResponse{}, nil
	}

	if exists && len(session.PublishedTargets) > 0 {
		if err := ns.recordSharedFilesystemSession(session); err != nil {
			return nil, err
		}
		if err := ns.sharedFilesystemRecovery.recoverVolumeLocked(ctx, session.VolumeID); err != nil {
			return nil, status.Errorf(codes.Unavailable, "CephFS stage recovery failed: %v", err)
		}
		if err := ns.confirmSharedFilesystemStage(ctx, session.VolumeID, stagingTargetPath); err != nil {
			return nil, status.Errorf(codes.Unavailable, "CephFS stage recovery did not establish a healthy mount: %v", err)
		}
		return &csi.NodeStageVolumeResponse{}, nil
	}

	if err := ns.recordSharedFilesystemSession(session); err != nil {
		return nil, err
	}
	if err := ns.mountSharedFilesystemSession(ctx, session); err != nil {
		ns.Driver.metrics.RecordCephFSSubvolume("mount", "failure")
		return nil, err
	}
	if err := ns.recordSharedFilesystemSession(session); err != nil {
		return nil, err
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) handleSharedFilesystemUnstage(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, sharedFilesystemAttemptTimeout)
	defer cancel()
	ctx, release, lockErr := ns.lockSharedFilesystemOperation(ctx, req.GetVolumeId())
	if lockErr != nil {
		return nil, lockErr
	}
	defer release()

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	session, exists, err := ns.sharedFilesystemRecovery.store.Load(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if exists && session.StagingTargetPath != stagingTargetPath {
		return nil, status.Error(codes.FailedPrecondition, "staging path differs from persisted session")
	}
	if !exists {
		if _, mounted, err := ns.mountPointForPath(stagingTargetPath); err == nil && !mounted {
			if _, err := os.Lstat(stagingTargetPath); os.IsNotExist(err) {
				return &csi.NodeUnstageVolumeResponse{}, nil
			}
		}
		session = sharedFilesystemSession{VolumeID: req.GetVolumeId(), StagingTargetPath: stagingTargetPath}
	}
	if err := ns.verifySharedFilesystemStage(session.VolumeID, stagingTargetPath); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := ns.pruneSharedFilesystemUnpublishedTargets(ctx, &session); err != nil {
		return nil, err
	}
	if len(session.PublishedTargets) != 0 {
		return nil, status.Error(codes.FailedPrecondition, "volume still has published targets")
	}
	session.Unstaging = true
	if err := ns.recordSharedFilesystemSession(session); err != nil {
		return nil, err
	}
	if err := ns.finishSharedFilesystemUnstage(ctx, session); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *NodeServer) handleSharedFilesystemPublish(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, sharedFilesystemAttemptTimeout)
	defer cancel()
	ctx, release, lockErr := ns.lockSharedFilesystemOperation(ctx, req.GetVolumeId())
	if lockErr != nil {
		return nil, lockErr
	}
	defer release()

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	if _, ok := volumeCapability.GetAccessType().(*csi.VolumeCapability_Block); ok {
		return nil, status.Error(codes.InvalidArgument, "shared filesystem volumes require a filesystem volume capability")
	}

	stagingTargetPath := req.GetStagingTargetPath()
	targetPath := req.GetTargetPath()
	if stagingTargetPath == "" || targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path and target path are required")
	}

	if err := ns.ensureSharedFilesystemStageReady(ctx, req); err != nil {
		klog.V(0).ErrorS(err, "Shared filesystem staging target is not ready",
			"method", "handleSharedFilesystemPublish", "volumeID", req.GetVolumeId(),
			"stagingTargetPath", stagingTargetPath, "targetPath", targetPath)
		return nil, err
	}

	target := sharedFilesystemTargetFromPublishRequest(req)
	if err := ns.verifySharedFilesystemTargetMode(req.GetVolumeId(), targetPath, false); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := ns.updateSharedFilesystemPublishedTarget(req.GetVolumeId(), target); err != nil {
		return nil, err
	}
	if err := ns.sharedFilesystemRecovery.store.setUnpublishIntent(req.GetVolumeId(), targetPath, false); err != nil {
		return nil, err
	}
	if err := ns.publishSharedFilesystemTarget(ctx, stagingTargetPath, target); err != nil {
		if ns.sharedFilesystemRecovery != nil {
			ns.sharedFilesystemRecovery.enqueue(req.GetVolumeId(), "publish_target_failed")
		}
		klog.V(0).ErrorS(err, "Failed to publish shared filesystem volume",
			"method", "handleSharedFilesystemPublish", "stagingTargetPath", stagingTargetPath, "targetPath", targetPath, "accessType", reflect.TypeOf(volumeCapability.GetAccessType()).String())
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Error(codes.Internal, "failed to publish volume")
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) ensureSharedFilesystemStageReady(ctx context.Context, req *csi.NodePublishVolumeRequest) error {
	volumeID := strings.TrimSpace(req.GetVolumeId())
	stagingTargetPath := strings.TrimSpace(req.GetStagingTargetPath())
	if volumeID == "" || stagingTargetPath == "" {
		return status.Error(codes.InvalidArgument, "volume ID and staging target path are required")
	}

	if err := ns.verifySharedFilesystemStage(volumeID, stagingTargetPath); err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if _, err := ns.ensureSharedFilesystemSessionForPublish(req); err != nil {
		return status.Errorf(codes.FailedPrecondition, "cannot validate shared filesystem session before publish: %v", err)
	}

	stageCheck, err := ns.checkMountPoint("", stagingTargetPath, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to inspect shared filesystem staging target %s: %v", stagingTargetPath, err)
	}
	if !stageCheck.targetIsMountPoint {
		if ns.sharedFilesystemRecovery != nil {
			ns.sharedFilesystemRecovery.enqueue(volumeID, "publish_stage_missing")
		}
		return status.Errorf(codes.FailedPrecondition, "shared filesystem staging target %s is not mounted; restage the volume before publishing", stagingTargetPath)
	}
	if err := ns.sharedFS.probe(ctx, stagingTargetPath); err == nil {
		return nil
	} else if !isDisconnectedSharedFilesystemError(err) {
		return status.Errorf(codes.Internal, "failed to validate shared filesystem staging target %s: %v", stagingTargetPath, err)
	} else {
		if ns.Driver != nil && ns.Driver.metrics != nil {
			ns.Driver.metrics.RecordCephFSSubvolume("stale_mount_detected", "failure")
		}
		if ns.Driver != nil && ns.Driver.featureGates.CephFSSelfHealing && ns.sharedFilesystemRecovery != nil {
			if recoverErr := ns.sharedFilesystemRecovery.recoverVolumeLocked(ctx, volumeID); recoverErr != nil {
				ns.sharedFilesystemRecovery.enqueue(volumeID, "publish_stage_stale")
				return status.Errorf(codes.Unavailable, "stale shared filesystem staging target %s detected and recovery failed: %v", stagingTargetPath, recoverErr)
			}
			stageCheck, checkErr := ns.checkMountPoint("", stagingTargetPath, nil)
			if checkErr != nil {
				return status.Errorf(codes.Internal, "failed to inspect recovered shared filesystem staging target %s: %v", stagingTargetPath, checkErr)
			}
			if stageCheck.targetIsMountPoint {
				if staleErr := ns.sharedFS.probe(ctx, stagingTargetPath); staleErr == nil {
					return nil
				}
			}
		}
		if ns.sharedFilesystemRecovery != nil {
			ns.sharedFilesystemRecovery.enqueue(volumeID, "publish_stage_stale")
		}
		return status.Errorf(codes.Unavailable, "stale shared filesystem staging target %s detected; recovery queued and publish should be retried", stagingTargetPath)
	}
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		unique = append(unique, trimmed)
	}

	return unique
}

func (ns *NodeServer) publishSharedFilesystemTarget(ctx context.Context, stagingTargetPath string, target sharedFilesystemPublishedTarget) error {
	targetPath := strings.TrimSpace(target.TargetPath)
	if stagingTargetPath == "" || targetPath == "" {
		return status.Error(codes.InvalidArgument, "staging target path and target path are required")
	}

	mountCheck, err := ns.checkMountPoint(stagingTargetPath, targetPath, nil)
	if err != nil {
		return err
	}
	if mountCheck.targetIsMountPoint {
		if err := ns.sharedFS.probe(ctx, targetPath); err != nil {
			if ns.Driver != nil && ns.Driver.metrics != nil {
				ns.Driver.metrics.RecordCephFSSubvolume("stale_mount_detected", "failure")
			}
			return status.Errorf(codes.FailedPrecondition, "stale CephFS target detected at %s: %v", targetPath, err)
		}
		if err := ns.verifySharedFilesystemBind(stagingTargetPath, targetPath); err != nil {
			return err
		}
		return ns.verifySharedFilesystemTargetFlags(target)
	}

	if err := verifySharedFilesystemUnmountedLeaf(targetPath); err != nil {
		return err
	}
	if err := ns.sharedFS.mkdir(ctx, targetPath); err != nil {
		return err
	}
	options := append([]string{"bind"}, uniqueStrings(target.MountOptions)...)
	bindErr := ns.sharedFS.bind(ctx, stagingTargetPath, targetPath, options)
	// Inspect even a failed bind: its first syscall may have left a writable
	// target. Never accept that target on retry or mark it healthy in recovery.
	identityErr := ns.verifySharedFilesystemBind(stagingTargetPath, targetPath)
	flagsErr := ns.verifySharedFilesystemTargetFlags(target)
	if bindErr != nil {
		return fmt.Errorf("failed to bind shared filesystem target %s: %w", targetPath, bindErr)
	}
	if identityErr != nil {
		return identityErr
	}
	if flagsErr != nil {
		return flagsErr
	}
	return ns.sharedFS.probe(ctx, targetPath)
}

func (ns *NodeServer) mountSharedFilesystemSession(ctx context.Context, session sharedFilesystemSession) error {
	if err := ns.verifySharedFilesystemStage(session.VolumeID, session.StagingTargetPath); err != nil {
		return err
	}
	if err := ns.sharedFS.mkdir(ctx, session.StagingTargetPath); err != nil {
		return err
	}
	var err error
	switch session.Mounter {
	case sharedFilesystemMounterKernel:
		err = ns.mountSharedFilesystemKernel(ctx, session)
	case sharedFilesystemMounterFuse:
		err = ns.mountSharedFilesystemFuse(ctx, session)
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported CephFS mounter %q", session.Mounter)
	}
	if err != nil {
		return err
	}
	return ns.confirmSharedFilesystemStage(ctx, session.VolumeID, session.StagingTargetPath)
}

func (ns *NodeServer) confirmSharedFilesystemStage(ctx context.Context, volumeID, stagingTargetPath string) error {
	if err := ns.verifySharedFilesystemStage(volumeID, stagingTargetPath); err != nil {
		return err
	}
	if _, mounted, err := ns.mountPointForPath(stagingTargetPath); err != nil {
		return err
	} else if !mounted {
		return fmt.Errorf("CephFS stage was not mounted at the requested path")
	}
	return ns.sharedFS.probe(ctx, stagingTargetPath)
}

func (ns *NodeServer) mountSharedFilesystemFuse(ctx context.Context, session sharedFilesystemSession) error {
	confPath, err := ensureSharedFilesystemCephConf(session.StagingTargetPath)
	if err != nil {
		return err
	}
	args := []string{
		session.StagingTargetPath,
		"-c", confPath,
		"-m", strings.Join(session.Monitors, ","),
		"--id", session.UserID,
		"-k", session.KeyringPath,
		"--client_mountpoint", session.Subpath,
		"--client_fs", session.FSName,
	}
	if len(session.StageMountOptions) > 0 {
		args = append(args, "-o", strings.Join(uniqueStrings(session.StageMountOptions), ","))
	}

	if err := ns.sharedFS.fuse(ctx, session, args); err != nil {
		return status.Errorf(codes.Internal, "failed to mount CephFS volume: %v", err)
	}
	ns.Driver.metrics.RecordCephFSSubvolume("mount", "success")
	ns.recordSharedFilesystemTestStageMount(session)
	return nil
}

func (ns *NodeServer) mountSharedFilesystemKernel(ctx context.Context, session sharedFilesystemSession) error {
	if !ns.Driver.featureGates.CephFSKernelMounts {
		return status.Error(codes.FailedPrecondition, "CephFS kernel mounts require feature gate cephfsKernelMounts=true")
	}
	if err := sharedFilesystemKernelSupported(); err != nil {
		return status.Errorf(codes.FailedPrecondition, "CephFS kernel mount prerequisites failed: %v", err)
	}
	if _, err := ns.mounter.Exec.LookPath("mount.ceph"); err != nil {
		return status.Errorf(codes.FailedPrecondition, "mount.ceph helper is not available in PATH: %v", err)
	}
	if strings.TrimSpace(session.SecretFilePath) == "" {
		return status.Error(codes.FailedPrecondition, "CephFS kernel mounts require a secretfile path")
	}

	device := fmt.Sprintf("%s@.%s=%s", session.UserID, session.FSName, session.Subpath)
	options := []string{
		"mon_addr=" + sharedFilesystemKernelMonitorOption(session.Monitors),
		"secretfile=" + session.SecretFilePath,
	}
	options = append(options, uniqueStrings(session.StageMountOptions)...)
	args := []string{"-t", "ceph", device, session.StagingTargetPath, "-o", strings.Join(options, ",")}

	output, err := ns.sharedFS.run(ctx, "mount", args...)
	if err != nil {
		klog.V(0).ErrorS(err, "Failed to mount CephFS volume with kernel client",
			"method", "mountSharedFilesystemKernel", "stagingTargetPath", session.StagingTargetPath, "output", string(output))
		return status.Errorf(codes.Internal, "failed to mount CephFS volume with kernel client: %s", strings.TrimSpace(string(output)))
	}
	ns.Driver.metrics.RecordCephFSSubvolume("mount", "success")
	ns.recordSharedFilesystemTestStageMount(session)
	return nil
}

func (ns *NodeServer) recordSharedFilesystemTestStageMount(session sharedFilesystemSession) {
	fake, ok := ns.mounter.Interface.(*mount.FakeMounter)
	if !ok {
		return
	}
	for _, mountPoint := range fake.MountPoints {
		if mountPoint.Path == session.StagingTargetPath {
			return
		}
	}
	fsType := "fuse.ceph-fuse"
	if session.Mounter == sharedFilesystemMounterKernel {
		fsType = "ceph"
	}
	fake.MountPoints = append(fake.MountPoints, mount.MountPoint{
		Device: session.StagingTargetPath,
		Path:   session.StagingTargetPath,
		Type:   fsType,
		Opts:   uniqueStrings(session.StageMountOptions),
	})
}

func sharedCephFSKeyringDir(stagingTargetPath string) string {
	return stagingTargetPath + sharedCephFSTempDirSuffix
}

func detectStaleSharedFilesystemMount(stagingTargetPath string) error {
	entries, err := os.ReadDir(stagingTargetPath)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	return nil
}

var detectSharedFilesystemMount = detectStaleSharedFilesystemMount

func isDisconnectedSharedFilesystemError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.ENOTCONN) || strings.Contains(strings.ToLower(err.Error()), "transport endpoint is not connected")
}
