package opennebula

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	img "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/shared"
	vm "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
)

const attachAttemptField = "CSI_ATTACH_ATTEMPT"

type replacementAttachKey struct{}
type replacementDetachReadbackKey struct{}

type replacementDetachReadback struct {
	originalVMID   int
	originalVMName string
	recordAccepted func(context.Context, string, int, int, int) error
}

func WithReplacementDetachReadback(ctx context.Context, originalVMID int, originalVMName string, recordAccepted func(context.Context, string, int, int, int) error) context.Context {
	return context.WithValue(ctx, replacementDetachReadbackKey{}, replacementDetachReadback{originalVMID, originalVMName, recordAccepted})
}

type ReplacementAttachDispatch struct {
	attempt        string
	definitelyIdle bool
}

func WithReplacementAttachAttempt(ctx context.Context, attempt string) (context.Context, *ReplacementAttachDispatch) {
	dispatch := &ReplacementAttachDispatch{attempt: attempt}
	return context.WithValue(ctx, replacementAttachKey{}, dispatch), dispatch
}

func (d *ReplacementAttachDispatch) DefinitelyIdle() bool {
	return d != nil && d.definitelyIdle
}

type ReplacementAttachInspector interface {
	InspectReplacementRelease(context.Context, int, int, string) (string, error)
	InspectReplacementAttach(context.Context, string, string, int, int, string) (*VolumeDiskRecord, error)
	VerifyAcceptedReplacementDetach(ctx context.Context, volume, node string, imageID, vmID int, vmName string) error
}

func (p *PersistentDiskVolumeProvider) InspectReplacementAttach(ctx context.Context, volume, node string, imageID, vmID int, attempt string) (*VolumeDiskRecord, error) {
	return p.inspectReplacementAttach(ctx, volume, node, imageID, vmID, attempt, "")
}

func (p *PersistentDiskVolumeProvider) VerifyAcceptedReplacementDetach(ctx context.Context, volume, node string, imageID, vmID int, vmName string) error {
	_, err := p.inspectReplacementAttach(ctx, volume, node, imageID, vmID, "", vmName)
	return err
}

func (p *PersistentDiskVolumeProvider) inspectReplacementAttach(ctx context.Context, volume, node string, imageID, vmID int, attempt, expectedVMName string) (*VolumeDiskRecord, error) {
	version, err := p.ctrl.SystemVersionContext(ctx)
	if err != nil || version != "7.4.0" {
		return nil, fmt.Errorf("%w: replacement Ceph attach requires OpenNebula 7.4.0", ErrConflictingAttachmentEvidence)
	}
	deny := func() (*VolumeDiskRecord, error) {
		return nil, fmt.Errorf("%w: replacement Ceph attachment is unverified", ErrConflictingAttachmentEvidence)
	}
	metadata, err := p.InspectVolumeAttachment(ctx, volume, node)
	if err != nil {
		return nil, err
	}
	if metadata.ImageID != imageID || metadata.RequestedNodeID != vmID ||
		(expectedVMName != "" && metadata.requestedVMName != expectedVMName) {
		return deny()
	}
	image, err := p.readAttachmentImageInfo(ctx, imageID)
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
	vms, err := p.readAttachmentVMs(ctx, imageID, attachmentVMIdentity{id: vmID, name: info.Name, lookupName: node})
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
		observed.Serial != fmt.Sprintf("onecsi-%d", imageID) {
		return deny()
	}
	return &observed, ctx.Err()
}

var errReplacementVMSettling = errors.New("original Ceph VM release has not settled")

func (p *PersistentDiskVolumeProvider) InspectReplacementRelease(ctx context.Context, imageID, vmID int, vmName string) (string, error) {
	deny := func() (string, error) {
		return "", fmt.Errorf("%w: original Ceph VM release is unverified", ErrConflictingAttachmentEvidence)
	}
	if imageID <= 0 || vmID <= 0 {
		return deny()
	}
	vms, err := p.readAttachmentVMs(ctx, imageID, attachmentVMIdentity{}, vmID)
	if err != nil {
		return "", err
	}
	for _, original := range vms {
		if original.ID != vmID {
			continue
		}
		if vmName != "" && original.Name != vmName {
			return deny()
		}
		if _, _, err := volumeDiskFromVM(&original, imageID, vmID, true); !errors.Is(err, ErrAttachmentNotFound) {
			return deny()
		}
		info, err := p.readVMInfo(ctx, vmID, vmDisks|vmState)
		if err != nil || info.Name != original.Name {
			return deny()
		}
		state, lcm, err := info.State()
		if err != nil {
			return deny()
		}
		if _, _, err := volumeDiskFromVM(info, imageID, vmID, true); !errors.Is(err, ErrAttachmentNotFound) {
			return deny()
		}
		if (state == vm.Active && lcm == vm.Running) || (state == vm.Poweroff && lcm == vm.LcmInit) {
			return info.Name, ctx.Err()
		}
		if state == vm.Active {
			switch lcm {
			case vm.Boot, vm.BootPoweroff, vm.BootSuspended, vm.BootStopped, vm.BootUndeploy, vm.ShutdownPoweroff:
				return "", errReplacementVMSettling
			}
		}
		return deny()
	}
	_, err = p.readVMInfo(ctx, vmID, vmDisks|vmState)
	if IsAuthoritativeVolumeAbsence(err) && ctx.Err() == nil {
		return "", nil
	}
	return deny()
}

type ReplacementDestinationAbsenceVerifier interface {
	VerifyReplacementDestinationAbsent(ctx context.Context, volume, node string, imageID, vmID, sourceVMID int) error
}

type RetiredReplacementSourceVerifier interface {
	VerifyRetiredReplacementSource(ctx context.Context, volume, node string, imageID, vmID, originalVMID int, originalVMName string) error
}

func (p *PersistentDiskVolumeProvider) VerifyRetiredReplacementSource(ctx context.Context, volume, node string, imageID, vmID, originalVMID int, originalVMName string) error {
	denied := fmt.Errorf("%w: retired replacement source absence is unverified", ErrConflictingAttachmentEvidence)
	if imageID <= 0 || vmID <= 0 || node == "" || volume == "" {
		return denied
	}
	version, err := p.ctrl.SystemVersionContext(ctx)
	if err != nil || version != "7.4.0" {
		return denied
	}
	if originalVMID != 0 {
		if _, err := p.InspectReplacementRelease(ctx, imageID, originalVMID, originalVMName); err != nil {
			return err
		}
	}
	readReadyImage := func() (*img.Image, error) {
		image, err := p.readAttachmentImageInfo(ctx, imageID)
		if err != nil {
			return nil, err
		}
		if image.Name != volume || image.ID != imageID || image.DatastoreID == nil || *image.DatastoreID < 0 ||
			image.Persistent == nil || *image.Persistent != 1 || image.CloningOps != 0 ||
			image.StateRaw != int(img.Ready) || image.RunningVMs != 0 || len(image.VMs.ID) != 0 {
			return nil, denied
		}
		return image, nil
	}
	image, err := readReadyImage()
	if err != nil {
		return err
	}
	datastore, err := p.attachmentDatastore(ctx, *image.DatastoreID)
	if err != nil || datastore.Backend != "ceph" {
		return denied
	}
	rows, err := readVMPool(ctx, p.ctrl, true)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if slices.Contains(row.candidateIDs(), vmID) || slices.ContainsFunc(row.Names, func(name string) bool {
			return strings.EqualFold(name, node)
		}) {
			return denied
		}
	}
	vms, err := attachmentVMsFromRows(rows, imageID, attachmentVMIdentity{}, vmID, originalVMID)
	if err != nil {
		return err
	}
	for _, info := range vms {
		if _, _, err := volumeDiskFromVM(&info, imageID, info.ID, true); !errors.Is(err, ErrAttachmentNotFound) {
			return denied
		}
	}
	if _, err := p.readVMInfo(ctx, vmID, vmDisks); !IsAuthoritativeVolumeAbsence(err) {
		return denied
	}
	if liveID, err := p.lookupImageID(ctx, volume); err != nil || liveID != imageID {
		return denied
	}
	current, err := readReadyImage()
	if err != nil {
		return err
	}
	if *current.DatastoreID != *image.DatastoreID {
		return denied
	}
	return ctx.Err()
}

func (p *PersistentDiskVolumeProvider) VerifyReplacementDestinationAbsent(ctx context.Context, volume, node string, imageID, vmID, sourceVMID int) error {
	deny := func() error {
		return fmt.Errorf("%w: replacement destination disk absence is unverified", ErrConflictingAttachmentEvidence)
	}
	if imageID <= 0 || vmID <= 0 || vmID == sourceVMID || node == "" {
		return deny()
	}
	version, err := p.ctrl.SystemVersionContext(ctx)
	if err != nil || version != "7.4.0" {
		return deny()
	}
	datastore, err := p.InspectVolumeDatastore(ctx, volume, imageID)
	if err != nil || datastore.Backend != "ceph" {
		return deny()
	}
	metadata, err := p.inspectVolumeAttachment(ctx, volume, node, vmID, false)
	if err != nil {
		return err
	}
	present, err := attachmentPresence(metadata, volume, imageID, vmID)
	if err != nil || present {
		return deny()
	}
	switch len(metadata.DiskRecords) {
	case 0:
		if img.State(metadata.ImageStateRaw) != img.Ready {
			return deny()
		}
	case 1:
		if metadata.DiskRecords[0].NodeID != sourceVMID {
			return deny()
		}
	default:
		return deny()
	}
	return ctx.Err()
}
