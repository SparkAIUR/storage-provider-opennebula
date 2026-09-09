package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func syncSharedFilesystemDirectory(path string) error {
	dir, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func durableSharedFilesystemWrite(tmp, target string, payload []byte) error {
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}
	return syncSharedFilesystemDirectory(filepath.Dir(target))
}

// Persist the kubelet's cleanup direction before touching mounts. Tombstones
// survive session reconstruction and failed session writes; only a subsequent
// explicit NodePublish may clear them.
func (s *sharedFilesystemSessionStore) unpublishIntentPath(volumeID, target string) string {
	volume := sha256.Sum256([]byte(volumeID))
	sum := sha256.Sum256([]byte(target))
	return filepath.Join(s.root, hex.EncodeToString(volume[:])+"-"+hex.EncodeToString(sum[:])+".unpublish")
}

func (s *sharedFilesystemSessionStore) setUnpublishIntent(volumeID, target string, unpublish bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.unpublishIntentPath(volumeID, target)
	if !unpublish {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return syncSharedFilesystemDirectory(s.root)
	}
	if err := os.MkdirAll(s.root, 0755); err != nil {
		return err
	}
	return durableSharedFilesystemWrite(path+".tmp", path, []byte("unpublish\n"))
}

func (s *sharedFilesystemSessionStore) isUnpublished(volumeID, target string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := os.Lstat(s.unpublishIntentPath(volumeID, target))
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func (ns *NodeServer) pruneSharedFilesystemUnpublishedTargets(ctx context.Context, session *sharedFilesystemSession) error {
	var active []sharedFilesystemPublishedTarget
	for _, target := range session.PublishedTargets {
		removed, err := ns.sharedFilesystemRecovery.store.isUnpublished(session.VolumeID, target.TargetPath)
		if err != nil {
			return err
		}
		if !removed {
			active = append(active, target)
			continue
		}
		if err := ns.verifySharedFilesystemTarget(session.VolumeID, target.TargetPath); err != nil {
			return err
		}
		if err := ns.cleanupSharedFilesystemPath(ctx, target.TargetPath); err != nil {
			return err
		}
	}
	if len(active) == len(session.PublishedTargets) {
		return nil
	}
	session.PublishedTargets = active
	return ns.recordSharedFilesystemSession(*session)
}

func (ns *NodeServer) confirmSharedFilesystemRecovery(ctx context.Context, session sharedFilesystemSession) error {
	health, err := ns.evaluateSharedFilesystemSession(ctx, session)
	if err != nil {
		return err
	}
	if health.RecoverStage || len(health.TargetsToRebind) != 0 {
		return fmt.Errorf("CephFS recovery did not establish healthy stage and targets")
	}
	return ns.verifySharedFilesystemSession(ctx, session)
}

func (ns *NodeServer) finishSharedFilesystemUnstage(ctx context.Context, session sharedFilesystemSession) error {
	if err := ns.pruneSharedFilesystemUnpublishedTargets(ctx, &session); err != nil {
		return err
	}
	if len(session.PublishedTargets) != 0 {
		return fmt.Errorf("volume still has published targets; unpublish them before unstaging")
	}
	if err := ns.verifySharedFilesystemStage(session.VolumeID, session.StagingTargetPath); err != nil {
		return err
	}
	// Check actual mount references too, including targets missing from the store.
	infos, err := ns.sharedFS.mountInfo()
	if err != nil {
		return err
	}
	for _, stage := range infos {
		if stage.MountPoint != session.StagingTargetPath {
			continue
		}
		for _, other := range infos {
			if other.MountPoint != stage.MountPoint && sameSharedFilesystemMount(stage, other) {
				return fmt.Errorf("stage still has a mounted reference at %s", other.MountPoint)
			}
		}
	}
	if err := ns.cleanupSharedFilesystemPath(ctx, session.StagingTargetPath); err != nil {
		return err
	}
	if err := ns.cleanupSharedFilesystemCredentials(ctx, session.StagingTargetPath); err != nil {
		return err
	}
	return ns.deleteSharedFilesystemSession(session.VolumeID)
}

func (ns *NodeServer) cleanupSharedFilesystemCredentials(ctx context.Context, stage string) error {
	dir := sharedCephFSKeyringDir(stage)
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("invalid credentials directory")
	}
	infos, err := ns.sharedFS.mountInfo()
	if err != nil {
		return err
	}
	for _, mount := range infos {
		if mount.MountPoint == dir || strings.HasPrefix(mount.MountPoint, dir+"/") {
			return fmt.Errorf("credentials directory contains a mount")
		}
	}
	for _, name := range []string{sharedCephFSKeyringFile, sharedCephFSSecretFile, sharedCephFSConfFile} {
		path := filepath.Join(dir, name)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refusing to remove nonregular credential file")
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return ns.sharedFS.rmdir(ctx, dir)
}
