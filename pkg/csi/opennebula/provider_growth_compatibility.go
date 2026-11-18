package opennebula

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/shared"
	vm "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
)

var supportedGrowthVersion = regexp.MustCompile(`^7\.4\.(0|[1-9][0-9]*)$`)

// authorizePersistentDiskGrowth is operation-specific. Probe and satisfied-capacity
// retries retain 7.4.0 support. No result is cached across growth attempts.
func (p *PersistentDiskVolumeProvider) authorizePersistentDiskGrowth(ctx context.Context, vmID, diskID, imageID int, vmName string) error {
	deny := func(reason string) error {
		return &datastoreConfigError{message: fmt.Sprintf("persistent disk growth requires verified OpenNebula 7.4.1+ metadata support for non-NONE transfer targets: %s", reason)}
	}
	observe := func() (string, error) {
		target, _, err := p.observeGrowthTarget(ctx, vmID, diskID, imageID, vmName)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		if err != nil {
			return "", deny(err.Error())
		}
		return target, nil
	}
	target, err := observe()
	if err != nil {
		return err
	}
	version, err := p.ctrl.SystemVersionContext(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		return deny(fmt.Sprintf("cannot authenticate provider version: %v", err))
	}
	if !supportedGrowthVersion.MatchString(version) {
		return deny(fmt.Sprintf("unqualified provider version %q; require canonical 7.4.patch", version))
	}
	patch, err := strconv.ParseUint(strings.TrimPrefix(version, "7.4."), 10, 64)
	if err != nil {
		return deny("provider patch version is out of range")
	}
	if target != "NONE" && patch < 1 {
		return deny(fmt.Sprintf("provider %s lacks persistent %s-target detach metadata support", version, target))
	}
	if vmName != "" {
		return p.verifyPublishMutation(ctx, vmID, imageID, vmName, func() error {
			current, err := observe()
			if err != nil {
				return err
			}
			if current != target {
				return deny("restoration disk transfer target changed")
			}
			return nil
		})
	}
	return nil
}

func (p *PersistentDiskVolumeProvider) observeGrowthTarget(ctx context.Context, vmID, diskID, imageID int, vmName string) (string, *shared.Disk, error) {
	restoring := vmName != ""
	var censusDisk VolumeDiskRecord
	if restoring {
		var err error
		censusDisk, err = p.verifyRestorationCensus(ctx, vmID, diskID, imageID)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return "", nil, ctxErr
			}
			return "", nil, err
		}
	}
	info, err := p.readResizeVM(ctx, vmID, restoring)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", nil, ctxErr
	}
	if err != nil {
		return "", nil, fmt.Errorf("cannot inspect VM identity: %w", err)
	}
	if info.ID != vmID {
		return "", nil, fmt.Errorf("VM identity changed")
	}
	state, lcm, err := info.State()
	if err != nil || ((state != vm.Active || lcm != vm.Running) && (state != vm.Poweroff || lcm != vm.LcmInit)) {
		return "", nil, fmt.Errorf("VM is not in a terminal eligible growth state")
	}
	var selected *shared.Disk
	if restoring {
		if censusDisk.NodeName != vmName {
			return "", nil, fmt.Errorf("restoration VM name differs from the census")
		}
		if err := verifyVMName(info, censusDisk.NodeName); err != nil {
			return "", nil, err
		}
		slot, disk, err := publishDiskFromVM(info, imageID, vmID)
		if err != nil || slot != diskID {
			return "", nil, fmt.Errorf("restoration attachment or disk slot is unverified")
		}
		observed, err := attachmentDiskRecord(info, info.Template.GetDisks(), *disk)
		if err != nil || observed != censusDisk {
			return "", nil, fmt.Errorf("restoration disk differs from the owner census")
		}
		selected = disk
	} else {
		for _, disk := range info.Template.GetDisks() {
			iid, ie := disk.GetI(shared.ImageID)
			did, de := disk.GetI(shared.DiskID)
			if ie != nil || de != nil || iid != imageID || did != diskID {
				continue
			}
			if selected != nil {
				return "", nil, fmt.Errorf("ambiguous attachment identity")
			}
			copyDisk := disk
			selected = &copyDisk
		}
	}
	if selected == nil {
		return "", nil, fmt.Errorf("attachment identity changed")
	}
	if previous, _ := selected.Get("SIZE_PREV"); strings.TrimSpace(previous) != "" {
		return "", nil, fmt.Errorf("disk growth remains pending")
	}
	if marker, _ := selected.Get("RESIZE"); marker != "" && marker != "NO" && marker != "0" && marker != "false" {
		return "", nil, fmt.Errorf("disk growth remains pending")
	}
	if !restoring {
		if err := p.verifyGrowthImage(ctx, imageID); err != nil {
			return "", nil, err
		}
	}
	persistent, _ := selected.Get("PERSISTENT")
	clone, _ := selected.Get("CLONE")
	tm, _ := selected.Get("TM_MAD")
	target, _ := selected.Get("LN_TARGET")
	if !strings.EqualFold(persistent, "YES") || !strings.EqualFold(clone, "NO") || strings.TrimSpace(tm) == "" {
		return "", nil, fmt.Errorf("disk persistence, clone mode or transfer driver is unknown or inconsistent")
	}
	target = strings.ToUpper(target)
	switch target {
	case "NONE", "SYSTEM", "SELF":
	default:
		return "", nil, fmt.Errorf("effective transfer target is unknown")
	}
	return target, selected, nil
}

func (p *PersistentDiskVolumeProvider) verifyGrowthImage(ctx context.Context, imageID int) error {
	image, err := p.readOrdinaryImageInfo(ctx, imageID)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		return fmt.Errorf("cannot inspect image persistence: %w", err)
	}
	if image.ID != imageID || image.Persistent == nil || *image.Persistent != 1 {
		return fmt.Errorf("image persistence is unknown or inconsistent")
	}
	return nil
}

func (p *PersistentDiskVolumeProvider) verifyRestorationCensus(ctx context.Context, vmID, diskID, imageID int) (VolumeDiskRecord, error) {
	image, err := p.readAttachmentImageInfo(ctx, imageID)
	if err != nil {
		return VolumeDiskRecord{}, err
	}
	if image.ID != imageID || image.Persistent == nil || *image.Persistent != 1 {
		return VolumeDiskRecord{}, fmt.Errorf("restoration image identity or persistence is unverified")
	}
	vms, err := p.readAttachmentVMs(ctx, imageID, vmID)
	if err != nil {
		return VolumeDiskRecord{}, err
	}
	metadata := VolumeAttachmentMetadata{ImageStateRaw: image.StateRaw, ImageRunningVMs: image.RunningVMs,
		ImageVMIDs: image.VMs.ID, RequestedNodeID: vmID, AttachedToRequestedNode: true}
	var selected VolumeDiskRecord
	for _, info := range vms {
		slot, disk, err := publishDiskFromVM(&info, imageID, info.ID)
		if errors.Is(err, ErrAttachmentNotFound) {
			continue
		}
		if err != nil {
			return VolumeDiskRecord{}, err
		}
		record, err := attachmentDiskRecord(&info, info.Template.GetDisks(), *disk)
		if err != nil {
			return VolumeDiskRecord{}, err
		}
		metadata.DiskRecords = append(metadata.DiskRecords, record)
		if info.ID == vmID && slot == diskID {
			selected = record
		}
	}
	if err := metadata.VerifyOwnership(); err != nil {
		return VolumeDiskRecord{}, err
	}
	if selected.NodeID == 0 {
		return VolumeDiskRecord{}, fmt.Errorf("restoration disk slot differs from the owner census")
	}
	return selected, nil
}
