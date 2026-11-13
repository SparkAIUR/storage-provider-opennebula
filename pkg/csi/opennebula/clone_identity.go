package opennebula

import (
	"context"
	"fmt"
	"strconv"

	"github.com/OpenNebula/one/src/oca/go/src/goca/parameters"
	img "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
)

const imageSelfTag = "CSI_IMAGE_ID"
const cloneParentTag = "CSI_CLONE_SOURCE_IMAGE_ID"

func imageIdentityNumber(info *img.Image, key string) (int, error) {
	raw, err := info.Template.GetStr(key)
	if err != nil || raw == "" {
		return 0, nil
	}
	id, err := strconv.Atoi(raw)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid %s on image %d", key, info.ID)
	}
	return id, nil
}

// OpenNebula clone_template copies the whole template. A child's inherited self
// marker is durable origin evidence even if its clone acknowledgement was lost.
// Normalize self and parent together before an image is used as a clone source.
func (p *PersistentDiskVolumeProvider) normalizeImageIdentity(ctx context.Context, info *img.Image) (*img.Image, error) {
	owner, err := info.Template.GetStr(ownerTag)
	if err != nil || owner != "csi.opennebula.io" {
		return nil, fmt.Errorf("image %d is not owned by this CSI driver", info.ID)
	}
	self, err := imageIdentityNumber(info, imageSelfTag)
	if err != nil {
		return nil, err
	}
	parent, err := imageIdentityNumber(info, cloneParentTag)
	if err != nil {
		return nil, err
	}
	if self == info.ID {
		return info, nil
	}
	if self > 0 {
		parent = self
	}
	if parent == info.ID {
		return nil, fmt.Errorf("image cannot be its own clone parent")
	}
	tpl := img.NewTemplate()
	if err := tpl.AddPair(imageSelfTag, strconv.Itoa(info.ID)); err != nil {
		return nil, err
	}
	if parent > 0 {
		if err := tpl.AddPair(cloneParentTag, strconv.Itoa(parent)); err != nil {
			return nil, err
		}
	}
	if err := p.ctrl.Image(info.ID).UpdateContext(ctx, tpl.String(), parameters.Merge); err != nil {
		return nil, err
	}
	current, err := p.ctrl.Image(info.ID).InfoContext(ctx, true)
	if err != nil {
		return nil, err
	}
	recorded, e := imageIdentityNumber(current, imageSelfTag)
	recordedParent, e2 := imageIdentityNumber(current, cloneParentTag)
	currentOwner, e3 := current.Template.GetStr(ownerTag)
	if e != nil || e2 != nil || e3 != nil || current.ID != info.ID || current.Name != info.Name || currentOwner != owner || recorded != info.ID || recordedParent != parent {
		return nil, fmt.Errorf("image identity normalization readback differs")
	}
	return current, nil
}

func (p *PersistentDiskVolumeProvider) verifiedCloneTarget(ctx context.Context, target *img.Image, sourceID int) (*img.Image, error) {
	if target.ID == sourceID {
		return nil, ErrCloneConflict
	}
	self, err := imageIdentityNumber(target, imageSelfTag)
	if err != nil {
		return nil, err
	}
	parent, err := imageIdentityNumber(target, cloneParentTag)
	if err != nil {
		return nil, err
	}
	if self != sourceID && (self != target.ID || parent != sourceID) {
		return nil, fmt.Errorf("%w: target has no durable clone origin", ErrCloneConflict)
	}
	current, err := p.normalizeImageIdentity(ctx, target)
	if err != nil {
		return nil, err
	}
	parent, err = imageIdentityNumber(current, cloneParentTag)
	if err != nil || parent != sourceID {
		return nil, ErrCloneConflict
	}
	return current, nil
}
