package opennebula

import (
	"context"
	"fmt"
	"math"
	"strings"

	img "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/shared"
	vm "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
)

// InspectSameNodeRetention does not repair or resize anything. It returns proof
// only for one persistent Ceph disk whose current image, owner census, and fresh
// VM observation agree. Kubernetes demand and object identity are caller gates.
func (p *PersistentDiskVolumeProvider) InspectSameNodeRetention(ctx context.Context, volume, node string) (*SameNodeRetentionEvidence, error) {
	deny := func(message string) (*SameNodeRetentionEvidence, error) {
		return nil, fmt.Errorf("cannot verify same-node Ceph retention for volume %s on node %s: %s", volume, node, message)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(volume) == "" || strings.TrimSpace(node) == "" {
		return deny("volume and node are required")
	}
	imageID, err := p.lookupImageID(ctx, volume)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return deny(fmt.Sprintf("live image lookup failed: %v", err))
	}
	if expected, pinned := ctx.Value(expectedImageIdentity{}).(int); pinned && expected != imageID {
		return deny("live image identity differs from the pinned image")
	}
	metadata, err := p.InspectVolumeAttachment(WithExpectedImageID(ctx, imageID), volume, node)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return deny(fmt.Sprintf("attachment inspection failed: %v", err))
	}
	if metadata == nil || metadata.ImageID <= 0 || metadata.ImageName != volume || metadata.RequestedNode != node || metadata.RequestedNodeID <= 0 || !metadata.AttachedToRequestedNode {
		return deny("attachment identity is incomplete")
	}
	if metadata.ImageRunningVMs != 1 || len(metadata.ImageVMIDs) != 1 || metadata.ImageVMIDs[0] != metadata.RequestedNodeID || len(metadata.ConflictingOwnerVMIDs()) != 0 {
		return deny("image does not have exactly the requested owner")
	}
	if len(metadata.DiskRecords) != 1 {
		return deny("VM disk ownership is ambiguous")
	}
	record := metadata.DiskRecords[0]
	if record.NodeID != metadata.RequestedNodeID || record.DiskID < 0 || strings.TrimSpace(record.Target) == "" || strings.TrimSpace(record.Serial) == "" {
		return deny("VM disk identity is incomplete")
	}
	image, err := p.ctrl.Image(metadata.ImageID).InfoContext(ctx, true)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return deny(fmt.Sprintf("image inspection failed: %v", err))
	}
	if image.ID != metadata.ImageID || image.Name != volume || image.Persistent == nil || *image.Persistent != 1 {
		return deny("image identity or persistence is inconsistent")
	}
	imageState, stateErr := image.State()
	if stateErr != nil || imageState != img.UsedPers || image.CloningOps != 0 {
		return deny("image is not stably in use as a persistent disk")
	}
	if image.Size <= 0 || int64(image.Size) > math.MaxInt64/sizeConversion {
		return deny("image capacity is invalid or out of range")
	}
	if image.RunningVMs != 1 || len(image.VMs.ID) != 1 || image.VMs.ID[0] != metadata.RequestedNodeID {
		return deny("image ownership changed during inspection")
	}
	effectiveSize, err := effectiveImageSize(image)
	if err != nil {
		return deny(err.Error())
	}
	imageSize := int64(image.Size) * sizeConversion
	if imageSize <= 0 || effectiveSize != imageSize {
		return deny("image capacity is unknown or requires restoration")
	}
	info, err := p.ctrl.VM(metadata.RequestedNodeID).InfoContext(ctx, true)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return deny(fmt.Sprintf("fresh VM inspection failed: %v", err))
	}
	if info.ID != metadata.RequestedNodeID {
		return deny("VM identity changed during inspection")
	}
	state, lcm, err := info.State()
	if err != nil || state != vm.Active || lcm != vm.Running {
		return deny("VM is not stably running")
	}
	diskID, selected, err := volumeDiskFromVM(info, metadata.ImageID, metadata.RequestedNodeID, true)
	if err != nil {
		return deny(err.Error())
	}
	target, targetErr := selected.Get(shared.TargetDisk)
	serial, serialErr := selected.Get(shared.Serial)
	if targetErr != nil || serialErr != nil || diskID != record.DiskID || target != record.Target || serial != record.Serial {
		return deny("fresh disk identity differs from the owner census")
	}
	persistent, _ := selected.Get("PERSISTENT")
	clone, _ := selected.Get("CLONE")
	transfer, _ := selected.Get("TM_MAD")
	targetMode, _ := selected.Get("LN_TARGET")
	if !strings.EqualFold(persistent, "YES") || !strings.EqualFold(clone, "NO") || !strings.EqualFold(transfer, "ceph") || !strings.EqualFold(targetMode, "NONE") {
		return deny("disk is not an eligible persistent Ceph attachment")
	}
	if err := p.verifyObservedAttachmentBackend(ctx, image, *selected, "ceph"); err != nil {
		return nil, fmt.Errorf("cannot verify same-node Ceph retention backend: %w", err)
	}
	diskSizeMB, sizeErr := selected.GetI(shared.Size)
	if sizeErr != nil || diskSizeMB != image.Size {
		return deny("raw disk capacity is unknown or inconsistent with the image")
	}
	completedSize, err := completedSelectedDiskSize(info, *selected)
	if err != nil {
		return deny(err.Error())
	}
	if completedSize <= 0 || completedSize != imageSize {
		return deny("disk capacity is pending, unknown or inconsistent with the image")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &SameNodeRetentionEvidence{VolumeID: volume, ImageID: metadata.ImageID, NodeID: metadata.RequestedNodeID, DiskID: record.DiskID, Target: record.Target, DeviceSerial: record.Serial, SizeBytes: completedSize}, nil
}

func (p *PersistentDiskVolumeProvider) VerifyAttachmentBackend(ctx context.Context, volume string, imageID, vmID int, expected string) error {
	image, err := p.ctrl.Image(imageID).InfoContext(ctx, true)
	if err != nil {
		return err
	}
	if image.ID != imageID || image.Name != volume {
		return fmt.Errorf("attachment image identity changed")
	}
	info, err := p.ctrl.VM(vmID).InfoContext(ctx, true)
	if err != nil {
		return err
	}
	_, disk, err := volumeDiskFromVM(info, imageID, vmID, true)
	if err != nil {
		return err
	}
	return p.verifyObservedAttachmentBackend(ctx, image, *disk, expected)
}

func (p *PersistentDiskVolumeProvider) verifyObservedAttachmentBackend(ctx context.Context, image *img.Image, disk shared.Disk, expected string) error {
	if image.DatastoreID == nil || *image.DatastoreID < 0 {
		return &datastoreConfigError{message: fmt.Sprintf("image %d has no valid datastore identity", image.ID)}
	}
	datastore, err := p.attachmentDatastore(ctx, *image.DatastoreID)
	if err != nil {
		return err
	}
	if expected == "" {
		expected = datastore.Backend
	}
	if datastore.Backend != expected {
		return fmt.Errorf("image %d datastore backend %q differs from %q", image.ID, datastore.Backend, expected)
	}
	transfer, _ := disk.Get("TM_MAD")
	transfer = strings.ToLower(strings.TrimSpace(transfer))
	if datastore.TMMad != "" && datastore.TMMad != transfer &&
		(datastore.TMMad == "shared" || datastore.TMMad == "qcow2" || transfer == "shared" || transfer == "qcow2") {
		return fmt.Errorf("image %d disk transfer driver %q differs from datastore transfer driver %q", image.ID, transfer, datastore.TMMad)
	}
	if attachmentTransferMatchesBackend(transfer, expected) {
		return nil
	}
	return fmt.Errorf("image %d disk transfer driver %q does not establish backend %q", image.ID, transfer, expected)
}

func attachmentTransferMatchesBackend(transfer, backend string) bool {
	switch strings.ToLower(strings.TrimSpace(transfer)) {
	case "ceph":
		return backend == "ceph"
	case "ssh", "shared", "qcow2", "fs_lvm", "fs_lvm_ssh", "local":
		return backend == "local"
	default:
		return false
	}
}
