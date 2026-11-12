package opennebula

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/OpenNebula/one/src/oca/go/src/goca/parameters"
	img "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/shared"
	vm "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
)

const requestedSizeTag = "CSI_REQUESTED_SIZE_BYTES"

func effectiveImageSize(info *img.Image) (int64, error) {
	size := int64(info.Size) * sizeConversion
	if raw, err := info.Template.GetStr(requestedSizeTag); err == nil && raw != "" {
		recorded, e := strconv.ParseInt(raw, 10, 64)
		if e != nil || recorded <= 0 {
			return 0, fmt.Errorf("invalid verified requested capacity on image %d", info.ID)
		}
		if recorded > size {
			size = recorded
		}
	}
	return size, nil
}

// A disk's requested SIZE is optimistic while RESIZE/SIZE_PREV or resize LCM state remains.
func (p *PersistentDiskVolumeProvider) completedDiskSize(ctx context.Context, vmID, diskID, imageID int) (int64, error) {
	info, err := p.ctrl.VM(vmID).InfoContext(ctx, true)
	if err != nil {
		return 0, err
	}
	state, lcm, err := info.State()
	if err != nil {
		return 0, err
	}
	if strings.Contains(lcm.String(), "RESIZE") {
		return 0, nil
	}
	if !((state == vm.Active && lcm == vm.Running) || (state == vm.Poweroff && lcm == vm.LcmInit)) {
		return 0, fmt.Errorf("VM is not in an eligible resize-completion state: %s/%s", state, lcm)
	}
	for _, disk := range info.Template.GetDisks() {
		iid, e := disk.GetI(shared.ImageID)
		if e != nil || iid != imageID {
			continue
		}
		did, e := disk.GetI(shared.DiskID)
		if e != nil || did != diskID {
			continue
		}
		if prev, e := disk.Get("SIZE_PREV"); e == nil && strings.TrimSpace(prev) != "" {
			return 0, nil
		}
		if marker, e := disk.Get("RESIZE"); e == nil && marker != "" && marker != "NO" && marker != "0" && marker != "false" {
			return 0, nil
		}
		return diskSizeBytesFromTemplate(disk)
	}
	return 0, fmt.Errorf("resize identity changed: image %d vm %d disk %d", imageID, vmID, diskID)
}

func (p *PersistentDiskVolumeProvider) persistVerifiedSize(ctx context.Context, id int, size int64) error {
	info, err := p.ctrl.Image(id).InfoContext(ctx, true)
	if err != nil {
		return err
	}
	existing, err := effectiveImageSize(info)
	if err != nil {
		return err
	}
	if existing > size {
		size = existing
	}
	tpl := img.NewTemplate()
	tpl.AddPair(requestedSizeTag, strconv.FormatInt(size, 10))
	if err := p.ctrl.Image(id).UpdateContext(ctx, tpl.String(), parameters.Merge); err != nil {
		return err
	}
	info, err = p.ctrl.Image(id).InfoContext(ctx, true)
	if err != nil {
		return err
	}
	raw, err := info.Template.GetStr(requestedSizeTag)
	if err != nil || raw != strconv.FormatInt(size, 10) {
		return fmt.Errorf("verified size metadata readback failed for image %d", id)
	}
	return nil
}

func (p *PersistentDiskVolumeProvider) restoreVerifiedSize(ctx context.Context, volume string, vmID, imageID int, timeout time.Duration) error {
	info, err := p.ctrl.Image(imageID).InfoContext(ctx, true)
	if err != nil {
		return err
	}
	raw, err := info.Template.GetStr(requestedSizeTag)
	if err != nil || raw == "" {
		return nil
	}
	desired, err := effectiveImageSize(info)
	if err != nil {
		return err
	}
	foundVM, disk, attached, _, err := p.findAttachedVolumeDisk(ctx, imageID)
	if err != nil {
		return err
	}
	if !attached || foundVM != vmID {
		return fmt.Errorf("attachment changed before capacity verification")
	}
	actual, err := p.completedDiskSize(ctx, vmID, disk, imageID)
	if err != nil {
		return err
	}
	if actual < desired {
		_, err = p.ExpandVolume(ctx, volume, desired, false)
		return err
	}
	return nil
}
