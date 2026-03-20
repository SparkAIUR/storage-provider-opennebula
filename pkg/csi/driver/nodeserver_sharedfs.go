package driver

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"

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

func (ns *NodeServer) handleSharedFilesystemStage(req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
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

	notMountPoint, err := ns.mounter.Interface.IsLikelyNotMountPoint(stagingTargetPath)
	if err == nil && !notMountPoint {
		if staleErr := detectStaleSharedFilesystemMount(stagingTargetPath); staleErr == nil {
			return &csi.NodeStageVolumeResponse{}, nil
		} else {
			ns.Driver.metrics.RecordCephFSSubvolume("stale_mount_detected", "failure")
			klog.Warningf("detected stale CephFS mount at %s: %v", stagingTargetPath, staleErr)
			if !ns.Driver.featureGates.CephFSSelfHealing {
				return nil, status.Errorf(codes.FailedPrecondition, "stale CephFS mount detected at %s: %v", stagingTargetPath, staleErr)
			}
			if err := mountCleanup(stagingTargetPath, ns); err != nil {
				return nil, err
			}
			_ = os.RemoveAll(sharedCephFSKeyringDir(stagingTargetPath))
			ns.Driver.metrics.RecordCephFSSubvolume("self_heal_remount", "attempted")
		}
	}

	if err := os.MkdirAll(stagingTargetPath, 0775); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create staging target path %s: %v", stagingTargetPath, err)
	}

	secrets := req.GetSecrets()
	userID := strings.TrimSpace(secrets[sharedNodeStageSecretUserIDKey])
	userKey := strings.TrimSpace(secrets[sharedNodeStageSecretUserKeyKey])
	if userID == "" || userKey == "" {
		return nil, status.Errorf(codes.InvalidArgument, "CephFS node staging requires secret keys %q and %q", sharedNodeStageSecretUserIDKey, sharedNodeStageSecretUserKeyKey)
	}

	keyringDir := sharedCephFSKeyringDir(stagingTargetPath)
	if err := os.MkdirAll(keyringDir, 0700); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create CephFS keyring directory: %v", err)
	}
	keyringPath := filepath.Join(keyringDir, sharedCephFSKeyringFile)
	keyringContents := []byte("[client." + userID + "]\n\tkey = " + userKey + "\n")
	if err := os.WriteFile(keyringPath, keyringContents, 0600); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write CephFS keyring file: %v", err)
	}

	monitors := strings.TrimSpace(publishContext[sharedPublishContextCephFSMonitors])
	fsName := strings.TrimSpace(publishContext[sharedPublishContextCephFSFSName])
	subpath := strings.TrimSpace(publishContext[sharedPublishContextCephFSSubpath])
	if monitors == "" || fsName == "" || subpath == "" {
		return nil, status.Error(codes.InvalidArgument, "CephFS publish context is incomplete")
	}

	mountOptions := splitCSV(publishContext[sharedPublishContextCephFSMounts])
	if volumeCapability.GetMount() != nil {
		mountOptions = append(mountOptions, volumeCapability.GetMount().GetMountFlags()...)
	}
	if strings.EqualFold(strings.TrimSpace(publishContext[sharedPublishContextCephFSReadonly]), "true") {
		mountOptions = append(mountOptions, "ro")
	}

	args := []string{
		stagingTargetPath,
		"-m", monitors,
		"--id", userID,
		"-k", keyringPath,
		"--client_mountpoint", subpath,
		"--client_fs", fsName,
	}
	if len(mountOptions) > 0 {
		args = append(args, "-o", strings.Join(uniqueStrings(mountOptions), ","))
	}

	output, err := ns.mounter.Exec.Command("ceph-fuse", args...).CombinedOutput()
	if err != nil {
		ns.Driver.metrics.RecordCephFSSubvolume("mount", "failure")
		_ = os.RemoveAll(keyringDir)
		klog.V(0).ErrorS(err, "Failed to mount CephFS volume",
			"method", "handleSharedFilesystemStage", "stagingTargetPath", stagingTargetPath, "output", string(output))
		return nil, status.Errorf(codes.Internal, "failed to mount CephFS volume: %s", strings.TrimSpace(string(output)))
	}
	ns.Driver.metrics.RecordCephFSSubvolume("mount", "success")

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) handleSharedFilesystemUnstage(req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	err := mountCleanup(stagingTargetPath, ns)
	if err != nil {
		return nil, err
	}

	_ = os.RemoveAll(sharedCephFSKeyringDir(stagingTargetPath))
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *NodeServer) handleSharedFilesystemPublish(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
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

	targetDir := targetPath
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		if err := os.MkdirAll(targetDir, 0750); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create target path %s: %v", targetDir, err)
		}
	}

	options := []string{"bind"}
	if req.GetReadonly() {
		options = append(options, "ro")
	}

	resp, err := ns.handleMountVolumePublish(stagingTargetPath, targetPath, volumeCapability, options)
	if err != nil {
		klog.V(0).ErrorS(err, "Failed to publish shared filesystem volume",
			"method", "handleSharedFilesystemPublish", "stagingTargetPath", stagingTargetPath, "targetPath", targetPath, "accessType", reflect.TypeOf(volumeCapability.GetAccessType()).String())
		return nil, status.Error(codes.Internal, "failed to publish volume")
	}

	return resp, nil
}

func mountCleanup(stagingTargetPath string, ns *NodeServer) error {
	err := mount.CleanupMountPoint(stagingTargetPath, ns.mounter.Interface, true)
	if err != nil {
		klog.V(0).ErrorS(err, "Failed to clean mount point of staging target path",
			"method", "handleSharedFilesystemUnstage", "stagingTargetPath", stagingTargetPath)
		return status.Error(codes.Internal, "failed to cleanup mount point of staging target path")
	}

	return nil
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
