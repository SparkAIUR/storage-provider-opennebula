package opennebula

import (
	"context"
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
func (p *PersistentDiskVolumeProvider) authorizePersistentDiskGrowth(ctx context.Context, vmID, diskID, imageID int, restoring bool) error {
	deny := func(reason string) error {
		return &datastoreConfigError{message: fmt.Sprintf("persistent disk growth requires verified OpenNebula 7.4.1+ metadata support for non-NONE transfer targets: %s", reason)}
	}
	if restoring {
		if err := p.verifyRestorationCensus(ctx, vmID, diskID, imageID); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return deny(err.Error())
		}
	}
	info, err := p.readResizeVM(ctx, vmID, restoring)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		return deny(fmt.Sprintf("cannot inspect VM identity: %v", err))
	}
	if info.ID != vmID {
		return deny("VM identity changed")
	}
	state, lcm, err := info.State()
	if err != nil || ((state != vm.Active || lcm != vm.Running) && (state != vm.Poweroff || lcm != vm.LcmInit)) {
		return deny("VM is not in a terminal eligible growth state")
	}
	var selected *shared.Disk
	if restoring {
		slot, disk, err := publishDiskFromVM(info, imageID, vmID)
		if err != nil || slot != diskID {
			return deny("restoration attachment or disk slot is unverified")
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
				return deny("ambiguous attachment identity")
			}
			copyDisk := disk
			selected = &copyDisk
		}
	}
	if selected == nil {
		return deny("attachment identity changed")
	}
	if previous, _ := selected.Get("SIZE_PREV"); strings.TrimSpace(previous) != "" {
		return deny("disk growth remains pending")
	}
	if marker, _ := selected.Get("RESIZE"); marker != "" && marker != "NO" && marker != "0" && marker != "false" {
		return deny("disk growth remains pending")
	}
	image, err := p.readResizeImage(ctx, imageID, restoring)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		return deny(fmt.Sprintf("cannot inspect image persistence: %v", err))
	}
	if image.ID != imageID || image.Persistent == nil || *image.Persistent != 1 {
		return deny("image persistence is unknown or inconsistent")
	}
	persistent, _ := selected.Get("PERSISTENT")
	clone, _ := selected.Get("CLONE")
	tm, _ := selected.Get("TM_MAD")
	target, _ := selected.Get("LN_TARGET")
	if !strings.EqualFold(persistent, "YES") || !strings.EqualFold(clone, "NO") || strings.TrimSpace(tm) == "" {
		return deny("disk persistence, clone mode or transfer driver is unknown or inconsistent")
	}
	target = strings.ToUpper(target)
	switch target {
	case "NONE", "SYSTEM", "SELF":
	default:
		return deny("effective transfer target is unknown")
	}
	// Upstream get_tm_target selects LN_TARGET only with CLONE=NO. In 7.4.0
	// trigger_disk_resize_success updates the persistent image only for NONE.
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
	return nil
}

func (p *PersistentDiskVolumeProvider) verifyRestorationCensus(ctx context.Context, vmID, diskID, imageID int) error {
	vms, err := p.readAttachmentVMs(ctx, imageID, vmID)
	if err != nil {
		return err
	}
	seen := false
	for _, info := range vms {
		if info.ID != vmID {
			continue
		}
		if seen {
			return fmt.Errorf("restoration VM identity is ambiguous")
		}
		slot, _, err := publishDiskFromVM(&info, imageID, vmID)
		if err != nil {
			return err
		}
		if slot != diskID {
			return fmt.Errorf("restoration disk slot differs from the VM census")
		}
		seen = true
	}
	if !seen {
		return fmt.Errorf("restoration VM identity is absent from the census")
	}
	return nil
}
