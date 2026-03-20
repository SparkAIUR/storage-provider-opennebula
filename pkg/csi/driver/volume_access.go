package driver

import (
	"fmt"
	"strings"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
)

func resolveVolumeAccessModel(capability *csi.VolumeCapability, allowedBackends []string) (opennebula.VolumeAccessModel, error) {
	if capability == nil || capability.AccessMode == nil {
		return "", fmt.Errorf("missing access mode")
	}

	mode := capability.AccessMode.Mode
	if isSharedFilesystemAccessMode(mode) {
		if _, ok := capability.GetAccessType().(*csi.VolumeCapability_Block); ok {
			return "", fmt.Errorf("access mode %q is not supported for block volumes; CephFS shared filesystem support requires a filesystem volume capability", mode.String())
		}

		profiles := backendCapabilityProfiles(allowedBackends)
		for _, profile := range profiles {
			if profile.SupportsFilesystemRWX {
				return opennebula.VolumeAccessModelSharedFS, nil
			}
		}

		return "", unsupportedRWXError(mode, profiles)
	}

	return opennebula.VolumeAccessModelDisk, nil
}

func validateAccessMode(volumeCapabilities []*csi.VolumeCapability, allowedBackends []string) error {
	supportedModes := make(map[csi.VolumeCapability_AccessMode_Mode]bool)
	for _, mode := range supportedAccessModes {
		supportedModes[mode] = true
	}

	backendProfiles := backendCapabilityProfiles(allowedBackends)
	for _, cap := range volumeCapabilities {
		if cap.AccessMode == nil {
			return fmt.Errorf("missing access mode")
		}

		mode := cap.AccessMode.Mode
		if supportedModes[mode] {
			continue
		}

		if isSharedFilesystemAccessMode(mode) {
			if _, err := resolveVolumeAccessModel(cap, allowedBackends); err != nil {
				return err
			}
			continue
		}

		return fmt.Errorf("unsupported access mode %q; supported modes are %s",
			mode.String(),
			strings.Join(supportedAccessModeNames(), ", "))
	}

	_ = backendProfiles
	return nil
}

func isSharedFilesystemAccessMode(mode csi.VolumeCapability_AccessMode_Mode) bool {
	switch mode {
	case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER:
		return true
	default:
		return false
	}
}
