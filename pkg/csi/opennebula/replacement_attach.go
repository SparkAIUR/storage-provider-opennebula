package opennebula

import (
	"context"
	"fmt"
	"strings"

	img "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/shared"
	vm "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
)

const attachAttemptField = "CSI_ATTACH_ATTEMPT"

type replacementAttachKey struct{}

func WithReplacementAttachAttempt(ctx context.Context, attempt string) context.Context {
	return context.WithValue(ctx, replacementAttachKey{}, attempt)
}

type ReplacementAttachInspector interface {
	InspectReplacementAttach(context.Context, string, string, int, int, string) (*VolumeDiskRecord, error)
}

func (p *PersistentDiskVolumeProvider) InspectReplacementAttach(ctx context.Context, volume, node string, imageID, vmID int, attempt string) (*VolumeDiskRecord, error) {
	deny := func() (*VolumeDiskRecord, error) {
		return nil, fmt.Errorf("%w: replacement Ceph attachment is unverified", ErrConflictingAttachmentEvidence)
	}
	version, err := p.ctrl.SystemVersionContext(ctx)
	if err != nil || version != "7.4.0" {
		return deny()
	}
	metadata, err := p.InspectVolumeAttachment(ctx, volume, node)
	if err != nil {
		return nil, err
	}
	if metadata.ImageID != imageID || metadata.RequestedNodeID != vmID {
		return deny()
	}
	image, err := p.readScopedImageInfo(ctx, imageID, false, "STATE", "VMS", "RUNNING_VMS", "CLONING_OPS")
	if err != nil {
		return nil, err
	}
	if image.ID != imageID || image.Name != volume || image.Persistent == nil || *image.Persistent != 1 ||
		image.CloningOps != 0 || image.DatastoreID == nil || *image.DatastoreID < 0 {
		return deny()
	}
	imageState, err := image.State()
	if err != nil {
		return nil, err
	}
	datastore, err := p.attachmentDatastore(ctx, *image.DatastoreID)
	if err != nil || datastore.Backend != "ceph" {
		return deny()
	}
	info, err := p.readVMInfo(ctx, vmID, vmDisks|vmState)
	if err != nil {
		return nil, err
	}
	vmState, lcmState, err := info.State()
	if err != nil || vmState != vm.Active || lcmState != vm.Running || info.Name != metadata.requestedVMName {
		return deny()
	}
	if imageState == img.Ready && metadata.ImageStateRaw == int(img.Ready) &&
		image.RunningVMs == 0 && len(image.VMs.ID) == 0 && metadata.ImageRunningVMs == 0 &&
		len(metadata.ImageVMIDs) == 0 && len(metadata.DiskRecords) == 0 {
		for _, disk := range info.Template.GetDisks() {
			id, present, err := diskImageReferenceForVolume(disk, imageID, true)
			if err != nil || (present && id == imageID) {
				return deny()
			}
		}
		return nil, ctx.Err()
	}
	if attempt == "" || imageState != img.UsedPers || metadata.ImageStateRaw != int(img.UsedPers) ||
		image.RunningVMs != 1 || len(image.VMs.ID) != 1 || image.VMs.ID[0] != vmID ||
		metadata.ImageRunningVMs != 1 || len(metadata.ImageVMIDs) != 1 || metadata.ImageVMIDs[0] != vmID ||
		len(metadata.DiskRecords) != 1 || !metadata.AttachedToRequestedNode {
		return deny()
	}
	slot, disk, err := publishDiskFromVM(info, imageID, vmID)
	if err != nil {
		return nil, err
	}
	marker := disk.GetPairs(attachAttemptField)
	if len(marker) != 1 || marker[0].Value != attempt {
		return deny()
	}
	vms, err := p.readAttachmentVMs(ctx, imageID, vmID)
	if err != nil {
		return nil, err
	}
	matches := 0
	for _, observedVM := range vms {
		for _, observedDisk := range observedVM.Template.GetDisks() {
			for _, pair := range observedDisk.GetPairs(attachAttemptField) {
				if pair.Value != attempt {
					continue
				}
				iid, imageErr := observedDisk.GetI(shared.ImageID)
				did, slotErr := observedDisk.GetI(shared.DiskID)
				if observedVM.ID != vmID || observedVM.Name != info.Name || imageErr != nil || iid != imageID || slotErr != nil || did != slot {
					return deny()
				}
				matches++
			}
		}
	}
	if matches != 1 {
		return deny()
	}
	if err := p.verifyObservedAttachmentBackend(ctx, image, *disk, "ceph"); err != nil {
		return nil, err
	}
	size, err := completedSelectedDiskSize(info, *disk)
	observed, recordErr := attachmentDiskRecord(info, info.Template.GetDisks(), *disk)
	if err != nil || size <= 0 || recordErr != nil || observed != metadata.DiskRecords[0] ||
		observed.Serial != fmt.Sprintf("onecsi-%d", imageID) || observed.Target == "" || observed.Target == "." ||
		observed.Target == ".." || strings.ContainsAny(observed.Target, "/\\ \t\r\n") {
		return deny()
	}
	targets := 0
	for _, candidate := range info.Template.GetDisks() {
		for _, field := range candidate.GetPairs(string(shared.TargetDisk)) {
			if strings.TrimSpace(field.Value) == observed.Target {
				targets++
			}
		}
	}
	if targets != 1 {
		return deny()
	}
	return &observed, ctx.Err()
}
