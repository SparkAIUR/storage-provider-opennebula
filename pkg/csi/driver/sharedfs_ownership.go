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
	"strings"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	mount "k8s.io/mount-utils"
)

type sharedFilesystemVolumeData struct {
	DriverName   string `json:"driverName"`
	VolumeHandle string `json:"volumeHandle"`
	SpecVolID    string `json:"specVolID"`
	NodeName     string `json:"nodeName"`
}

// Kubelet stores this on the host filesystem beside, never inside, the mount.
func sharedFilesystemMetadata(path string) (sharedFilesystemVolumeData, error) {
	var data sharedFilesystemVolumeData
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return data, fmt.Errorf("noncanonical shared filesystem path %q", path)
	}
	parent := filepath.Dir(path)
	for part := parent; part != "/"; part = filepath.Dir(part) {
		info, err := os.Lstat(part)
		if err != nil {
			return data, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return data, fmt.Errorf("symlink in mount path %s", path)
		}
	}
	payload, err := os.ReadFile(filepath.Join(parent, "vol_data.json"))
	if err != nil {
		return data, err
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return data, err
	}
	if data.DriverName != DefaultDriverName || !opennebula.IsSharedFilesystemVolumeID(data.VolumeHandle) {
		return data, fmt.Errorf("mount metadata does not identify an OpenNebula CephFS volume at %s", path)
	}
	return data, nil
}

func sharedFilesystemStageVolumeID(path string) (string, error) {
	if filepath.Base(path) != "globalmount" {
		return "", fmt.Errorf("invalid staging path %s", path)
	}
	data, err := sharedFilesystemMetadata(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(data.VolumeHandle))
	parent := filepath.Base(filepath.Dir(path))
	if parent != hex.EncodeToString(sum[:]) && parent != data.VolumeHandle {
		return "", fmt.Errorf("staging directory does not match volume handle at %s", path)
	}
	return data.VolumeHandle, nil
}

func (ns *NodeServer) verifySharedFilesystemTarget(volumeID, path string) error {
	return ns.verifySharedFilesystemTargetMode(volumeID, path, true)
}

func (ns *NodeServer) verifySharedFilesystemTargetMode(volumeID, path string, allowAbsent bool) error {
	data, err := sharedFilesystemMetadata(path)
	if allowAbsent && os.IsNotExist(err) {
		// CSI unpublish is idempotent after kubelet removed the target. Missing
		// metadata never authorizes unmounting an existing mount or directory.
		_, mounted, listErr := ns.mountPointForPath(path)
		if listErr == nil && !mounted {
			if _, statErr := os.Lstat(path); os.IsNotExist(statErr) {
				return nil
			}
		}
	}
	if err != nil {
		return err
	}
	if filepath.Base(path) != "mount" || filepath.Base(filepath.Dir(filepath.Dir(path))) != "kubernetes.io~csi" || podUIDFromKubeletPath(path) == "" {
		return fmt.Errorf("invalid CSI pod target %s", path)
	}
	if data.VolumeHandle != volumeID || data.SpecVolID != filepath.Base(filepath.Dir(path)) || (data.NodeName != "" && data.NodeName != ns.Driver.nodeID) {
		return fmt.Errorf("volume ownership mismatch at %s", path)
	}
	infos, err := ns.sharedFS.mountInfo()
	if err != nil {
		return err
	}
	count := 0
	for _, target := range infos {
		if target.MountPoint != path {
			continue
		}
		count++
		if count > 1 || !isCephFSMount(target) {
			return fmt.Errorf("ambiguous target mount at %s", path)
		}
		for _, other := range infos {
			if other.MountPoint == path || filepath.Base(other.MountPoint) != "globalmount" || !sameSharedFilesystemMount(target, other) {
				continue
			}
			owner, err := sharedFilesystemStageVolumeID(other.MountPoint)
			if err != nil || owner != volumeID {
				return fmt.Errorf("target mount belongs to another volume")
			}
		}
	}
	if count == 0 {
		return verifySharedFilesystemUnmountedLeaf(path)
	}
	return nil
}

func sameSharedFilesystemMount(a, b mount.MountInfo) bool {
	return a.Major == b.Major && a.Minor == b.Minor && a.Root == b.Root && a.FsType == b.FsType
}

func isCephFSMount(info mount.MountInfo) bool {
	return info.FsType == "ceph" || info.FsType == "fuse.ceph-fuse"
}

func (ns *NodeServer) verifySharedFilesystemStage(volumeID, path string) error {
	id, err := sharedFilesystemStageVolumeID(path)
	if err != nil {
		return err
	}
	if id != volumeID {
		return fmt.Errorf("stage ownership mismatch at %s", path)
	}
	infos, err := ns.sharedFS.mountInfo()
	if err != nil {
		return err
	}
	count := 0
	for _, stage := range infos {
		if stage.MountPoint != path {
			continue
		}
		count++
		if count > 1 || !isCephFSMount(stage) {
			return fmt.Errorf("ambiguous staging mount")
		}
		for _, other := range infos {
			if other.MountPoint != path && filepath.Base(other.MountPoint) == "globalmount" && sameSharedFilesystemMount(stage, other) {
				return fmt.Errorf("stage mount identity belongs to another volume")
			}
		}
	}
	if count == 0 {
		return verifySharedFilesystemUnmountedLeaf(path)
	}
	return nil
}

func (ns *NodeServer) verifySharedFilesystemSession(ctx context.Context, session sharedFilesystemSession) error {
	return ns.verifySharedFilesystemSessionMode(ctx, session, false)
}

func (ns *NodeServer) verifySharedFilesystemSessionMode(ctx context.Context, session sharedFilesystemSession, allowAbsent bool) error {
	if err := ns.verifySharedFilesystemStage(session.VolumeID, session.StagingTargetPath); err != nil {
		return err
	}
	id, err := sharedFilesystemStageVolumeID(session.StagingTargetPath)
	if err != nil {
		return err
	}
	if id != session.VolumeID {
		return fmt.Errorf("stage ownership mismatch at %s", session.StagingTargetPath)
	}
	metadata, err := opennebula.DecodeSharedVolumeID(id)
	if err != nil {
		return err
	}
	if metadata.FSName != session.FSName || metadata.Subpath != session.Subpath {
		return fmt.Errorf("CephFS subpath does not match the volume handle")
	}
	if session.KeyringPath != sharedCephFSKeyringPath(session.StagingTargetPath) || session.SecretFilePath != sharedCephFSSecretPath(session.StagingTargetPath) {
		return fmt.Errorf("credential paths do not belong to the staging path")
	}
	infos, err := ns.sharedFS.mountInfo()
	if err != nil {
		return err
	}
	var stage *mount.MountInfo
	for i := range infos {
		if infos[i].MountPoint == session.StagingTargetPath {
			if stage != nil || !isCephFSMount(infos[i]) {
				return fmt.Errorf("ambiguous staging mount")
			}
			stage = &infos[i]
		}
	}
	for _, target := range session.PublishedTargets {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ns.verifySharedFilesystemTargetMode(session.VolumeID, target.TargetPath, allowAbsent); err != nil {
			return err
		}
		for _, info := range infos {
			if info.MountPoint != target.TargetPath {
				continue
			}
			if !isCephFSMount(info) {
				return fmt.Errorf("non-CephFS target mount at %s", target.TargetPath)
			}
			// A stale bind may refer to the previous superblock after a partial
			// recovery. It must never belong to another volume's stage.
			for _, other := range infos {
				if other.MountPoint != session.StagingTargetPath && filepath.Base(other.MountPoint) == "globalmount" && sameSharedFilesystemMount(info, other) {
					return fmt.Errorf("target %s belongs to another staging mount", target.TargetPath)
				}
			}
			if stage != nil && !sameSharedFilesystemMount(*stage, info) {
				if err := ns.sharedFS.probe(ctx, target.TargetPath); !isDisconnectedSharedFilesystemError(err) {
					return fmt.Errorf("target mount identity mismatch at %s", target.TargetPath)
				}
			}
		}
	}
	return nil
}

func (ns *NodeServer) discoverSharedFilesystemTargets(volumeID, stagePath string) ([]sharedFilesystemPublishedTarget, error) {
	infos, err := ns.sharedFS.mountInfo()
	if err != nil {
		return nil, err
	}
	var stage *mount.MountInfo
	for i := range infos {
		if infos[i].MountPoint == stagePath {
			stage = &infos[i]
		}
	}
	if err := ns.verifySharedFilesystemStage(volumeID, stagePath); err != nil {
		return nil, err
	}
	var targets []sharedFilesystemPublishedTarget
	for _, info := range infos {
		if info.MountPoint == stagePath || !strings.Contains(info.MountPoint, "/volumes/kubernetes.io~csi/") || !isCephFSMount(info) {
			continue
		}
		data, err := sharedFilesystemMetadata(info.MountPoint)
		if err != nil {
			return nil, err
		}
		if data.VolumeHandle != volumeID {
			if stage != nil && sameSharedFilesystemMount(*stage, info) {
				return nil, fmt.Errorf("stage mount has a target owned by another volume")
			}
			continue
		}
		if err := ns.verifySharedFilesystemTarget(volumeID, info.MountPoint); err != nil {
			return nil, err
		}
		options := append([]string(nil), info.MountOptions...)
		for _, option := range info.SuperOptions {
			if option == "ro" {
				options = append(options, option)
			}
		}
		targets = append(targets, sharedFilesystemPublishedTarget{TargetPath: info.MountPoint, MountOptions: sharedFilesystemMountOptionsFromMountPoint(mount.MountPoint{Opts: options})})
	}
	return normalizeSharedFilesystemPublishedTargets(targets), nil
}

func (ns *NodeServer) verifySharedFilesystemBind(stagePath, targetPath string) error {
	infos, err := ns.sharedFS.mountInfo()
	if err != nil {
		return err
	}
	var stages, targets []mount.MountInfo
	for _, info := range infos {
		if info.MountPoint == stagePath {
			stages = append(stages, info)
		}
		if info.MountPoint == targetPath {
			targets = append(targets, info)
		}
	}
	if len(stages) != 1 || len(targets) != 1 || !isCephFSMount(stages[0]) || !sameSharedFilesystemMount(stages[0], targets[0]) {
		return fmt.Errorf("bind mount ownership mismatch at %s", targetPath)
	}
	return nil
}

// This check is used only after mountinfo proves that the leaf is unmounted;
// its ancestors and adjacent metadata live on the trusted kubelet host path.
func verifySharedFilesystemUnmountedLeaf(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("unmounted leaf is not a real directory at %s", path)
	}
	return nil
}

var errSharedFilesystemTargetFlags = errors.New("target mount flags differ from request")

func (ns *NodeServer) verifySharedFilesystemTargetFlags(target sharedFilesystemPublishedTarget) error {
	infos, err := ns.sharedFS.mountInfo()
	if err != nil {
		return err
	}
	var actual *mount.MountInfo
	for i := range infos {
		if infos[i].MountPoint == target.TargetPath {
			if actual != nil {
				return fmt.Errorf("ambiguous target mount")
			}
			actual = &infos[i]
		}
	}
	if actual == nil {
		return fmt.Errorf("target mount is missing at %s", target.TargetPath)
	}
	// util-linux may perform bind and flag application as separate syscalls.
	// Last requested flag wins, matching mount's option semantics.
	requested := make(map[string]bool)
	for _, option := range target.MountOptions {
		switch option {
		case "ro", "nosuid", "nodev", "noexec":
			requested[option] = true
		case "rw":
			requested["ro"] = false
		case "suid":
			requested["nosuid"] = false
		case "dev":
			requested["nodev"] = false
		case "exec":
			requested["noexec"] = false
		}
	}
	present := make(map[string]bool)
	for _, option := range actual.MountOptions {
		present[option] = true
	}
	for _, option := range actual.SuperOptions {
		if option == "ro" {
			present["ro"] = true
		}
	}
	for flag, want := range requested {
		if present[flag] != want {
			return fmt.Errorf("%w: %s at %s", errSharedFilesystemTargetFlags, flag, target.TargetPath)
		}
	}
	return nil
}
