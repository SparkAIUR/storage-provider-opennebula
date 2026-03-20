package opennebula

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	utilexec "k8s.io/utils/exec"
)

const (
	cephProvisionerSecretAdminIDKey  = "adminID"
	cephProvisionerSecretAdminKeyKey = "adminKey"
	cephFsCommandBin                 = "ceph"
)

type CephFSVolumeProvider struct {
	ctrl *goca.Controller
	exec utilexec.Interface
}

func NewCephFSVolumeProvider(client *OpenNebulaClient, exec utilexec.Interface) (*CephFSVolumeProvider, error) {
	if client == nil {
		return nil, fmt.Errorf("client reference is nil")
	}
	if exec == nil {
		return nil, fmt.Errorf("exec reference is nil")
	}

	return &CephFSVolumeProvider{
		ctrl: goca.NewController(client.Client),
		exec: exec,
	}, nil
}

func (p *CephFSVolumeProvider) CreateSharedVolume(ctx context.Context, req SharedVolumeRequest) (*SharedVolumeCreateResult, error) {
	if strings.TrimSpace(req.Name) == "" {
		return nil, fmt.Errorf("shared volume name cannot be empty")
	}

	candidates, err := p.resolveProvisioningDatastores(ctx, req.Selection)
	if err != nil {
		return nil, err
	}

	staticPath := sharedFilesystemPath(req.Parameters)
	overrideGroup := sharedFilesystemSubvolumeGroup(req.Parameters)
	var insufficientCapacity []string

	for _, datastore := range candidates {
		if datastore.CephFS == nil {
			return nil, &datastoreConfigError{message: fmt.Sprintf("datastore %d does not expose CephFS metadata", datastore.ID)}
		}

		metadata := SharedVolumeMetadata{
			DatastoreID:    datastore.ID,
			FSName:         datastore.CephFS.FSName,
			SubvolumeGroup: datastore.CephFS.SubvolumeGroup,
			Backend:        sharedBackendCephFS,
		}
		if overrideGroup != "" {
			metadata.SubvolumeGroup = overrideGroup
		}

		if staticPath != "" {
			metadata.Mode = SharedVolumeModeStatic
			metadata.Subpath, err = normalizeSharedSubpath(datastore.CephFS.RootPath, staticPath)
			if err != nil {
				return nil, err
			}

			volumeID, err := EncodeSharedVolumeID(metadata)
			if err != nil {
				return nil, err
			}

			return &SharedVolumeCreateResult{
				VolumeID:      volumeID,
				CapacityBytes: req.SizeBytes,
				Datastore:     datastore,
				Metadata:      metadata,
			}, nil
		}

		if req.SizeBytes > 0 && datastore.FreeBytes > 0 && datastore.FreeBytes < req.SizeBytes {
			insufficientCapacity = append(insufficientCapacity, strconv.Itoa(datastore.ID))
			continue
		}

		if metadata.SubvolumeGroup == "" {
			return nil, &datastoreConfigError{message: fmt.Sprintf("datastore %d is missing a CephFS subvolume group", datastore.ID)}
		}

		metadata.Mode = SharedVolumeModeDynamic
		metadata.SubvolumeName = sanitizeSubvolumeName(req.Name)

		if err := p.ensureSubvolume(ctx, datastore, metadata, req.SizeBytes, req.Secrets); err != nil {
			if IsDatastoreCapacityError(err) {
				insufficientCapacity = append(insufficientCapacity, strconv.Itoa(datastore.ID))
				continue
			}
			return nil, err
		}

		subpath, err := p.getSubvolumePath(ctx, datastore, metadata, req.Secrets)
		if err != nil {
			return nil, err
		}
		metadata.Subpath = cleanSharedPath(subpath)

		volumeID, err := EncodeSharedVolumeID(metadata)
		if err != nil {
			return nil, err
		}

		return &SharedVolumeCreateResult{
			VolumeID:      volumeID,
			CapacityBytes: req.SizeBytes,
			Datastore:     datastore,
			Metadata:      metadata,
		}, nil
	}

	if len(insufficientCapacity) > 0 {
		return nil, &datastoreCapacityError{
			message: fmt.Sprintf("none of the configured CephFS datastores had enough free capacity for %d bytes; attempted datastores: %s", req.SizeBytes, strings.Join(insufficientCapacity, ",")),
		}
	}

	return nil, &datastoreConfigError{message: "no eligible CephFS datastores were available for shared filesystem provisioning"}
}

func (p *CephFSVolumeProvider) DeleteSharedVolume(ctx context.Context, volumeID string, secrets map[string]string) error {
	metadata, datastore, err := p.validateSharedVolume(ctx, volumeID)
	if err != nil {
		return err
	}

	if metadata.Mode == SharedVolumeModeStatic {
		return nil
	}
	if metadata.SubvolumeName == "" {
		return &datastoreConfigError{message: fmt.Sprintf("shared volume %q is missing subvolume metadata", volumeID)}
	}

	args := []string{"fs", "subvolume", "rm", metadata.FSName, metadata.SubvolumeName}
	if metadata.SubvolumeGroup != "" {
		args = append(args, "--group_name", metadata.SubvolumeGroup)
	}

	output, err := p.runCephCommand(ctx, datastore, secrets, args...)
	if err != nil {
		if isCephNotFoundError(err, output) {
			return nil
		}
		return err
	}

	return nil
}

func (p *CephFSVolumeProvider) PublishSharedVolume(ctx context.Context, volumeID string, readonly bool) (map[string]string, error) {
	metadata, datastore, err := p.validateSharedVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}

	publishContext := map[string]string{
		publishContextShareBackend:   sharedBackendCephFS,
		publishContextCephFSMonitors: strings.Join(datastore.CephFS.Monitors, ","),
		publishContextCephFSFSName:   metadata.FSName,
		publishContextCephFSSubpath:  metadata.Subpath,
		publishContextCephFSReadonly: strconv.FormatBool(readonly),
	}
	if len(datastore.CephFS.MountOptions) > 0 {
		publishContext[publishContextCephFSMounts] = strings.Join(datastore.CephFS.MountOptions, ",")
	}

	return publishContext, nil
}

func (p *CephFSVolumeProvider) ValidateSharedVolume(ctx context.Context, volumeID string) (*SharedVolumeMetadata, error) {
	metadata, _, err := p.validateSharedVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (p *CephFSVolumeProvider) validateSharedVolume(ctx context.Context, volumeID string) (SharedVolumeMetadata, Datastore, error) {
	metadata, err := DecodeSharedVolumeID(volumeID)
	if err != nil {
		return SharedVolumeMetadata{}, Datastore{}, err
	}

	datastores, err := p.ctrl.Datastores().InfoContext(ctx)
	if err != nil {
		return SharedVolumeMetadata{}, Datastore{}, fmt.Errorf("failed to inspect datastores for shared volume %q: %w", volumeID, err)
	}

	sourceDatastore, err := findDatastoreByID(datastores.Datastores, metadata.DatastoreID)
	if err != nil {
		return SharedVolumeMetadata{}, Datastore{}, err
	}

	datastore := datastoreFromSchema(sourceDatastore)
	if datastore.CephFS == nil {
		return SharedVolumeMetadata{}, Datastore{}, &datastoreConfigError{message: fmt.Sprintf("datastore %d no longer validates as a CephFS datastore", metadata.DatastoreID)}
	}

	return metadata, datastore, nil
}

func (p *CephFSVolumeProvider) resolveProvisioningDatastores(ctx context.Context, selection DatastoreSelectionConfig) ([]Datastore, error) {
	datastores, err := p.ctrl.Datastores().InfoContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get datastores info: %w", err)
	}

	resolved, err := ResolveDatastores(datastores.Datastores, selection)
	if err != nil {
		return nil, err
	}

	ordered, err := OrderDatastores(resolved, selection.Policy)
	if err != nil {
		return nil, err
	}

	return ordered, nil
}

func (p *CephFSVolumeProvider) ensureSubvolume(ctx context.Context, datastore Datastore, metadata SharedVolumeMetadata, sizeBytes int64, secrets map[string]string) error {
	args := []string{"fs", "subvolume", "create", metadata.FSName, metadata.SubvolumeName}
	if metadata.SubvolumeGroup != "" {
		args = append(args, "--group_name", metadata.SubvolumeGroup)
	}
	if sizeBytes > 0 {
		args = append(args, "--size", strconv.FormatInt(sizeBytes, 10))
	}

	output, err := p.runCephCommand(ctx, datastore, secrets, args...)
	if err != nil {
		if isCephAlreadyExistsError(err, output) {
			return nil
		}
		return err
	}

	return nil
}

func (p *CephFSVolumeProvider) getSubvolumePath(ctx context.Context, datastore Datastore, metadata SharedVolumeMetadata, secrets map[string]string) (string, error) {
	args := []string{"fs", "subvolume", "getpath", metadata.FSName, metadata.SubvolumeName}
	if metadata.SubvolumeGroup != "" {
		args = append(args, "--group_name", metadata.SubvolumeGroup)
	}

	output, err := p.runCephCommand(ctx, datastore, secrets, args...)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(output)), nil
}

func (p *CephFSVolumeProvider) runCephCommand(_ context.Context, datastore Datastore, secrets map[string]string, args ...string) ([]byte, error) {
	if datastore.CephFS == nil {
		return nil, &datastoreConfigError{message: fmt.Sprintf("datastore %d does not expose CephFS metadata", datastore.ID)}
	}

	adminID := strings.TrimSpace(secrets[cephProvisionerSecretAdminIDKey])
	adminKey := strings.TrimSpace(secrets[cephProvisionerSecretAdminKeyKey])
	if adminID == "" || adminKey == "" {
		return nil, &datastoreConfigError{message: fmt.Sprintf("CephFS provisioning requires secret keys %q and %q", cephProvisionerSecretAdminIDKey, cephProvisionerSecretAdminKeyKey)}
	}

	keyFile, err := os.CreateTemp("", "cephfs-admin-key-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary CephFS key file: %w", err)
	}
	keyFilePath := keyFile.Name()
	defer os.Remove(keyFilePath)
	if _, err := keyFile.WriteString(adminKey); err != nil {
		keyFile.Close()
		return nil, fmt.Errorf("failed to write temporary CephFS key file: %w", err)
	}
	keyFile.Close()
	if err := os.Chmod(keyFilePath, 0600); err != nil {
		return nil, fmt.Errorf("failed to secure temporary CephFS key file: %w", err)
	}

	commandArgs := []string{"-m", strings.Join(datastore.CephFS.Monitors, ","), "--id", adminID, "--keyfile", keyFilePath}
	commandArgs = append(commandArgs, args...)

	cmd := p.exec.Command(cephFsCommandBin, commandArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("ceph command failed: %w", err)
	}

	return output, nil
}

func isCephAlreadyExistsError(err error, output []byte) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error() + " " + string(output))
	return strings.Contains(message, "already exists") || strings.Contains(message, "eexist")
}

func isCephNotFoundError(err error, output []byte) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error() + " " + string(output))
	return strings.Contains(message, "not found") || strings.Contains(message, "enoent")
}
