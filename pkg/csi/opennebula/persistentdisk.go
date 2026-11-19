/*
Copyright 2025, OpenNebula Project, OpenNebula Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package opennebula

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	gocaerrors "github.com/OpenNebula/one/src/oca/go/src/goca/errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	"github.com/OpenNebula/one/src/oca/go/src/goca/parameters"
	datastoreSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/datastore"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
	img "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
	imk "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image/keys"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/shared"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	ownerTag            = "OWNER"
	sizeConversion      = 1024 * 1024
	fsTypeTag           = "FS"
	volumeUsedTimeout   = 30 * time.Second
	hotplugPollInterval = time.Second
)

type PersistentDiskVolumeProvider struct {
	ctrl             *goca.Controller
	bridgeIdentities CephBridgeIdentities
	hotplugPolicy    HotplugTimeoutPolicy
	hotplugPollWait  time.Duration
}

func NewPersistentDiskVolumeProvider(client *OpenNebulaClient, policy HotplugTimeoutPolicy, identities CephBridgeIdentities) (*PersistentDiskVolumeProvider, error) {
	if client == nil {
		return nil, fmt.Errorf("client reference is nil")
	}
	if policy.BaseTimeout <= 0 {
		policy.BaseTimeout = 60 * time.Second
	}
	if policy.MaxTimeout <= 0 || policy.MaxTimeout < policy.BaseTimeout {
		policy.MaxTimeout = policy.BaseTimeout
	}
	if policy.PollInterval <= 0 {
		policy.PollInterval = hotplugPollInterval
	}
	return &PersistentDiskVolumeProvider{
		ctrl:             goca.NewController(client.Client),
		bridgeIdentities: identities,
		hotplugPolicy:    policy,
		hotplugPollWait:  policy.PollInterval,
	}, nil
}

func (p *PersistentDiskVolumeProvider) CreateVolume(ctx context.Context, name string, size int64, owner string, immutable bool, fsType string, params map[string]string, selection DatastoreSelectionConfig) (*VolumeCreateResult, error) {
	if name == "" {
		return nil, fmt.Errorf("volume name cannot be empty")
	}

	if size <= 0 {
		return nil, fmt.Errorf("invalid volume size: must be greater than 0")
	}

	// size is in bytes and we need it in MB
	sizeMB := size / sizeConversion
	if sizeMB <= 0 {
		return nil, fmt.Errorf("invalid volume size: must be greater than 0 MB")
	}

	candidates, err := p.resolveProvisioningDatastores(ctx, selection)
	if err != nil {
		return nil, err
	}

	tpl := img.NewTemplate()
	tpl.Add(imk.Name, name)
	tpl.Add(imk.Size, int(sizeMB))
	tpl.Add(imk.Persistent, "YES")
	tpl.AddPair(ownerTag, owner)
	tpl.Add(imk.Type, string(image.Datablock))
	if immutable {
		tpl.Add(imk.PersistentType, "SHAREABLE")
	}
	if fsType != "" {
		tpl.Add(fsTypeTag, fsType)
	}

	if params != nil && params["devPrefix"] != "" {
		tpl.Add(imk.DevPrefix, params["devPrefix"])
	}

	var insufficientCapacity []string
	attemptedDatastores := make([]int, 0, len(candidates))
	for _, datastore := range candidates {
		attemptedDatastores = append(attemptedDatastores, datastore.ID)
		if datastore.FreeBytes < size {
			recordDatastoreProvisioningResult(datastore.ID, false)
			insufficientCapacity = append(insufficientCapacity, fmt.Sprintf("%d", datastore.ID))
			continue
		}

		if err := ProvisioningFeasible(ctx, p.ctrl, datastore, p.bridgeIdentities); err != nil {
			return nil, err
		}
		if selection.Authorize != nil {
			if err := selection.Authorize(ctx, datastore.ID); err != nil {
				return nil, err
			}
		}
		finishAttempt := beginDatastoreAttempt(datastore.ID)
		imageID, err := p.ctrl.Images().CreateContext(ctx, tpl.String(), uint(datastore.ID))
		finishAttempt()
		if err != nil {
			recordDatastoreProvisioningResult(datastore.ID, false)
			if isInsufficientCapacityError(err) {
				insufficientCapacity = append(insufficientCapacity, fmt.Sprintf("%d", datastore.ID))
				continue
			}

			return nil, fmt.Errorf("failed to create volume on datastore %d: %w", datastore.ID, err)
		}
		recordDatastoreProvisioningResult(datastore.ID, true)

		err = p.waitForResourceStatusContext(ctx, imageID, p.hotplugPolicy.BaseTimeout, p.volumeReadyContext)
		if err != nil {
			return nil, fmt.Errorf("failed to wait for volume readiness: %w", err)
		}

		return &VolumeCreateResult{
			Datastore:             datastore,
			CapacityBytes:         size,
			FallbackUsed:          len(attemptedDatastores) > 1,
			AttemptedDatastoreIDs: append([]int(nil), attemptedDatastores...),
		}, nil
	}

	return nil, &datastoreCapacityError{
		message: fmt.Sprintf("none of the configured datastores had enough free capacity for %d bytes; attempted datastores: %s", size, strings.Join(insufficientCapacity, ",")),
	}
}

func (p *PersistentDiskVolumeProvider) DeleteVolume(ctx context.Context, volume string) error {
	expectedID, pinned := ctx.Value(expectedImageIdentity{}).(int)
	vmID, _ := ctx.Value(expectedDeletionVMIdentity{}).(int)
	admission, _ := ctx.Value(volumeDeletionAuthorization{}).(deletionAdmission)
	volumeID, err := p.lookupImageID(ctx, volume)
	if errors.Is(err, ErrVolumeNotFound) {
		if pinned {
			return p.confirmVolumeDeletion(ctx, volume, expectedID, vmID, admission.verify)
		}
		if admission.verify != nil {
			if err := admission.verify(ctx, -1, false); err != nil {
				return err
			}
		}
		return ErrVolumeNotFound
	}
	if err != nil {
		return err
	}
	if pinned && volumeID != expectedID {
		return fmt.Errorf("live image differs from the expected deletion identity")
	}
	ctx = WithExpectedImageID(ctx, volumeID)
	if err := p.checkVolumeDeletion(ctx, volume, volumeID, vmID); err != nil {
		return err
	}
	if admission.prepare != nil {
		if err := admission.prepare(ctx, volumeID); err != nil {
			return err
		}
		if err := p.checkVolumeDeletion(ctx, volume, volumeID, vmID); err != nil {
			return err
		}
	}
	if admission.verify != nil {
		if err := admission.verify(ctx, volumeID, false); err != nil {
			return err
		}
	}
	if err := p.ctrl.Image(volumeID).DeleteContext(ctx); err != nil {
		return fmt.Errorf("delete image %d: %w", volumeID, err)
	}
	if err := p.waitForResourceStatusContext(ctx, volumeID, p.hotplugPolicy.BaseTimeout, p.volumeDeletedContext); err != nil {
		return err
	}
	return p.confirmVolumeDeletion(ctx, volume, volumeID, vmID, admission.verify)
}

func (p *PersistentDiskVolumeProvider) confirmVolumeDeletion(ctx context.Context, volume string, imageID, vmID int, verify func(context.Context, int, bool) error) error {
	if err := p.VerifyVolumeAbsent(ctx, volume, imageID, vmID); err != nil {
		return err
	}
	if verify != nil {
		return verify(ctx, imageID, true)
	}
	return ctx.Err()
}

func (p *PersistentDiskVolumeProvider) checkVolumeDeletion(ctx context.Context, volume string, volumeID, vmID int) error {
	info, err := p.readAttachmentImageInfo(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("cannot establish deletion authority for image %d: %w", volumeID, err)
	}
	if info.ID != volumeID || info.Name != volume {
		return fmt.Errorf("image information differs from the expected deletion identity")
	}
	state, err := info.State()
	if err != nil {
		return err
	}
	if !idleImageState(state) || info.RunningVMs != 0 {
		return fmt.Errorf("image %d is not in a deletable state: %v", volumeID, state)
	}
	if flag, _ := info.Template.GetStr("FORCE_DELETE"); strings.EqualFold(flag, "YES") || strings.EqualFold(flag, "true") || flag == "1" {
		return fmt.Errorf("image %d carries historical FORCE_DELETE; external cleanup verification required", volumeID)
	}
	if len(info.Snapshots.Snapshots) > 0 {
		return fmt.Errorf("image %d has dependent snapshots; source deletion is blocked", volumeID)
	}
	metadata, err := p.inspectVolumeAttachment(ctx, volume, "", vmID, false)
	if err != nil {
		return err
	}
	if metadata.ImageID != volumeID || metadata.ImageName != volume || !idleImageState(img.State(metadata.ImageStateRaw)) || len(metadata.DiskRecords) != 0 || metadata.ImageRunningVMs != 0 || len(metadata.ImageVMIDs) != 0 {
		return fmt.Errorf("image %d ownership is not consistently absent", volumeID)
	}
	if strings.TrimSpace(info.Source) == "" {
		return fmt.Errorf("image %d has no backend SOURCE; cleanup cannot be established", volumeID)
	}
	liveID, err := p.lookupImageID(ctx, volume)
	if err != nil {
		return fmt.Errorf("cannot verify live image identity before deletion: %w", err)
	}
	if liveID != volumeID {
		return fmt.Errorf("live image identity changed before deletion")
	}
	return ctx.Err()
}

func (p *PersistentDiskVolumeProvider) VerifyVolumeAbsent(ctx context.Context, volume string, imageID int, vmIDs ...int) error {
	if imageID < 0 {
		return fmt.Errorf("image identity is required to verify volume absence")
	}
	if _, err := p.ctrl.Image(imageID).InfoContext(ctx, true); !IsAuthoritativeVolumeAbsence(err) {
		if err != nil {
			return fmt.Errorf("cannot verify pinned image absence: %w", err)
		}
		return fmt.Errorf("pinned image still exists")
	}
	vms, err := p.readAttachmentVMs(ctx, imageID, attachmentVMIdentity{}, vmIDs...)
	if err != nil {
		return fmt.Errorf("cannot verify pinned image ownership: %w", err)
	}
	for _, info := range vms {
		for _, disk := range info.Template.GetDisks() {
			id, present, err := diskImageReferenceForVolume(disk, imageID, true)
			if err != nil {
				return err
			}
			if present && id == imageID {
				return fmt.Errorf("pinned image still has a VM disk reference")
			}
		}
	}
	for _, vmID := range vmIDs {
		if err := p.verifyPinnedVMDiskAbsent(ctx, imageID, vmID, vms); err != nil {
			return err
		}
	}
	if _, err := p.lookupImageID(ctx, volume); !errors.Is(err, ErrVolumeNotFound) {
		if err != nil {
			return err
		}
		return fmt.Errorf("volume handle now resolves to a live image")
	}
	return ctx.Err()
}

func (p *PersistentDiskVolumeProvider) verifyPinnedVMDiskAbsent(ctx context.Context, imageID, vmID int, census []vm.VM) error {
	if vmID <= 0 {
		return nil
	}
	info, err := p.readVMInfo(ctx, vmID, vmDisks)
	if IsAuthoritativeVolumeAbsence(err) && !slices.ContainsFunc(census, func(info vm.VM) bool { return info.ID == vmID }) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot verify pinned VM disk absence: %w", err)
	}
	for _, observed := range census {
		if observed.ID == vmID {
			if err := verifyVMName(info, observed.Name); err != nil {
				return err
			}
			break
		}
	}
	if _, _, err := volumeDiskFromVM(info, imageID, vmID, true); !errors.Is(err, ErrAttachmentNotFound) {
		if err != nil {
			return fmt.Errorf("pinned VM disk absence is unverified: %w", err)
		}
		return fmt.Errorf("pinned VM still has the image disk")
	}
	return nil
}

func (p *PersistentDiskVolumeProvider) VerifyVolumeAbsentFromVM(ctx context.Context, volume string, imageID, vmID int) error {
	if imageID <= 0 || vmID <= 0 {
		return fmt.Errorf("pinned image and VM identities are required")
	}
	metadata, err := p.inspectVolumeAttachment(WithExpectedImageID(ctx, imageID), volume, "", vmID, false)
	if err != nil {
		return err
	}
	metadata.RequestedNodeID = vmID
	if present, err := attachmentPresence(metadata, volume, imageID, vmID); err != nil {
		return err
	} else if present {
		return fmt.Errorf("pinned VM still owns the image")
	}
	liveID, err := p.lookupImageID(ctx, volume)
	if err != nil {
		return err
	}
	if liveID != imageID {
		return fmt.Errorf("live image identity changed while checking VM absence")
	}
	return ctx.Err()
}

func (p *PersistentDiskVolumeProvider) CloneVolume(ctx context.Context, name string, sourceVolume string, selection DatastoreSelectionConfig) (*VolumeCreateResult, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("clone name cannot be empty")
	}
	if strings.TrimSpace(sourceVolume) == "" {
		return nil, fmt.Errorf("source volume cannot be empty")
	}

	sourceVolumeID, _, err := p.VolumeExists(ctx, sourceVolume)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve source volume %q: %w", sourceVolume, err)
	}
	if sourceVolumeID == -1 {
		return nil, fmt.Errorf("source volume %q was not found", sourceVolume)
	}

	sourceImage, err := p.ctrl.Image(sourceVolumeID).InfoContext(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect source volume %q: %w", sourceVolume, err)
	}

	candidates, err := p.resolveProvisioningDatastores(ctx, selection)
	if err != nil {
		return nil, err
	}

	if name == sourceVolume {
		return nil, fmt.Errorf("%w: clone target equals source", ErrCloneConflict)
	}
	if existing, lookupErr := p.lookupImageID(ctx, name); lookupErr == nil {
		target, e := p.ctrl.Image(existing).InfoContext(ctx, true)
		if e != nil {
			return nil, e
		}
		if target.DatastoreID == nil {
			return nil, fmt.Errorf("clone target datastore unavailable")
		}
		for _, candidate := range candidates {
			if candidate.ID == *target.DatastoreID {
				if err := ProvisioningFeasible(ctx, p.ctrl, candidate, p.bridgeIdentities); err != nil {
					return nil, err
				}
				if selection.Authorize != nil {
					if e := selection.Authorize(ctx, candidate.ID); e != nil {
						return nil, e
					}
				}
				if _, e = p.verifiedCloneTarget(ctx, target, sourceVolumeID); e != nil {
					return nil, e
				}
				if err := p.waitForResourceStatusContext(ctx, existing, p.hotplugPolicy.BaseTimeout, p.volumeReadyContext); err != nil {
					return nil, err
				}
				target, e = p.ctrl.Image(existing).InfoContext(ctx, true)
				if e != nil {
					return nil, e
				}
				targetSize, e := effectiveImageSize(target)
				if e != nil {
					return nil, e
				}
				if targetSize < selection.CloneRequiredBytes || (selection.CloneLimitBytes > 0 && targetSize > selection.CloneLimitBytes) {
					return nil, ErrCloneCapacity
				}
				return &VolumeCreateResult{Datastore: candidate, CapacityBytes: targetSize, AttemptedDatastoreIDs: []int{candidate.ID}}, nil
			}
		}
		return nil, fmt.Errorf("%w: existing clone is outside selected datastores", ErrCloneConflict)
	} else if !errors.Is(lookupErr, ErrVolumeNotFound) {
		return nil, lookupErr
	}
	sourceSizeBytes, err := effectiveImageSize(sourceImage)
	if err != nil {
		return nil, err
	}
	if selection.CloneRequiredBytes > sourceSizeBytes || (selection.CloneLimitBytes > 0 && sourceSizeBytes > selection.CloneLimitBytes) {
		return nil, fmt.Errorf("%w: source capacity %d is outside requested clone range", ErrCloneCapacity, sourceSizeBytes)
	}
	attemptedDatastores := make([]int, 0, len(candidates))
	insufficientCapacity := make([]string, 0)
	for _, datastore := range candidates {
		attemptedDatastores = append(attemptedDatastores, datastore.ID)
		if datastore.FreeBytes < sourceSizeBytes {
			recordDatastoreProvisioningResult(datastore.ID, false)
			insufficientCapacity = append(insufficientCapacity, strconv.Itoa(datastore.ID))
			continue
		}

		if err := ProvisioningFeasible(ctx, p.ctrl, datastore, p.bridgeIdentities); err != nil {
			return nil, err
		}
		if selection.Authorize != nil {
			if err := selection.Authorize(ctx, datastore.ID); err != nil {
				return nil, err
			}
		}
		sourceImage, err = p.normalizeImageIdentity(ctx, sourceImage)
		if err != nil {
			return nil, err
		}
		finishAttempt := beginDatastoreAttempt(datastore.ID)
		cloneID, err := p.ctrl.Image(sourceVolumeID).CloneContext(ctx, name, datastore.ID)
		finishAttempt()
		if err != nil {
			recordDatastoreProvisioningResult(datastore.ID, false)
			if isInsufficientCapacityError(err) {
				insufficientCapacity = append(insufficientCapacity, strconv.Itoa(datastore.ID))
				continue
			}
			return nil, fmt.Errorf("failed to clone volume %q into datastore %d: %w", sourceVolume, datastore.ID, err)
		}
		target, e := p.ctrl.Image(cloneID).InfoContext(ctx, true)
		if e != nil {
			return nil, fmt.Errorf("clone outcome unresolved for image %d: %w", cloneID, e)
		}
		if target.ID == sourceVolumeID || target.Name != name {
			return nil, fmt.Errorf("clone target identity differs from request")
		}
		_, e = p.verifiedCloneTarget(ctx, target, sourceVolumeID)
		if e != nil {
			return nil, e
		}
		recordDatastoreProvisioningResult(datastore.ID, true)

		if err := p.waitForResourceStatusContext(ctx, cloneID, p.hotplugPolicy.BaseTimeout, p.volumeReadyContext); err != nil {
			return nil, fmt.Errorf("failed waiting for clone %d to become ready: %w", cloneID, err)
		}
		target, e = p.ctrl.Image(cloneID).InfoContext(ctx, true)
		if e != nil {
			return nil, e
		}
		targetSize, e := effectiveImageSize(target)
		if e != nil {
			return nil, e
		}
		if targetSize < selection.CloneRequiredBytes || (selection.CloneLimitBytes > 0 && targetSize > selection.CloneLimitBytes) {
			return nil, ErrCloneCapacity
		}

		return &VolumeCreateResult{
			Datastore:             datastore,
			CapacityBytes:         targetSize,
			FallbackUsed:          len(attemptedDatastores) > 1,
			AttemptedDatastoreIDs: append([]int(nil), attemptedDatastores...),
		}, nil
	}

	return nil, &datastoreCapacityError{
		message: fmt.Sprintf("none of the configured datastores had enough free capacity to clone %q; attempted datastores: %s", sourceVolume, strings.Join(insufficientCapacity, ",")),
	}
}

func (p *PersistentDiskVolumeProvider) CreateSnapshot(ctx context.Context, sourceVolume string, snapshotName string) (*VolumeSnapshot, error) {
	if strings.TrimSpace(sourceVolume) == "" {
		return nil, fmt.Errorf("source volume cannot be empty")
	}

	sourceVolumeID, _, err := p.VolumeExists(ctx, sourceVolume)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve source volume %q: %w", sourceVolume, err)
	}
	if sourceVolumeID == -1 {
		return nil, fmt.Errorf("source volume %q was not found", sourceVolume)
	}

	return nil, fmt.Errorf("new OpenNebula disk snapshots are unavailable until the complete backend snapshot lifecycle is qualified")
}

func (p *PersistentDiskVolumeProvider) DeleteSnapshot(ctx context.Context, handle string) error {
	imageID, snapshotID, err := DecodeVolumeSnapshotID(handle)
	if err != nil {
		return err
	}
	info, err := p.ctrl.Image(imageID).InfoContext(ctx, true)
	if err != nil {
		return err
	}
	state, err := info.State()
	if err != nil {
		return err
	}
	if state != img.Ready {
		return &datastoreConfigError{message: "source image is attached or not ready; existing snapshot deletion requires verified VM-side lifecycle and is not available in this mode"}
	}
	metadata, err := p.InspectVolumeAttachment(ctx, info.Name, "")
	if err != nil {
		return err
	}
	if metadata.ImageID != imageID || metadata.ImageRunningVMs != 0 || len(metadata.ImageVMIDs) != 0 || len(metadata.DiskRecords) != 0 {
		return &datastoreConfigError{message: "snapshot source ownership is inconsistent; no deletion performed"}
	}
	if _, err := imageSnapshotByID(info, snapshotID); err != nil {
		return nil
	}
	if err := p.ctrl.Image(imageID).Snapshot(snapshotID).DeleteContext(ctx); err != nil {
		return err
	}
	return p.waitForResourceStatusContext(ctx, imageID, p.hotplugPolicy.BaseTimeout, func(ctx context.Context, id int) (bool, error) {
		current, e := p.ctrl.Image(id).InfoContext(ctx, true)
		if e != nil {
			return false, e
		}
		state, e := current.State()
		if e != nil {
			return false, e
		}
		if state == img.Error {
			return false, fmt.Errorf("backend snapshot deletion failed")
		}
		_, e = imageSnapshotByID(current, snapshotID)
		return e != nil && state == img.Ready, nil
	})
}

func (p *PersistentDiskVolumeProvider) ListSnapshots(ctx context.Context, snapshotID string, sourceVolumeID string, maxEntries int32, startingToken string) ([]VolumeSnapshot, string, error) {
	snapshots := make([]VolumeSnapshot, 0)

	switch {
	case strings.TrimSpace(snapshotID) != "":
		imageID, imageSnapshotID, err := DecodeVolumeSnapshotID(snapshotID)
		if err != nil {
			return nil, "", err
		}

		imageInfo, err := p.ctrl.Image(imageID).InfoContext(ctx, true)
		if err != nil {
			return nil, "", fmt.Errorf("failed to inspect image %d while listing snapshot %q: %w", imageID, snapshotID, err)
		}
		snapshot, err := imageSnapshotByID(imageInfo, imageSnapshotID)
		if err != nil {
			return nil, "", err
		}
		snapshots = append(snapshots, convertImageSnapshot(imageInfo, snapshot))
	case strings.TrimSpace(sourceVolumeID) != "":
		imageID, _, err := p.VolumeExists(ctx, sourceVolumeID)
		if err != nil {
			return nil, "", fmt.Errorf("failed to resolve source volume %q: %w", sourceVolumeID, err)
		}
		if imageID == -1 {
			return nil, "", fmt.Errorf("source volume %q was not found", sourceVolumeID)
		}

		imageInfo, err := p.ctrl.Image(imageID).InfoContext(ctx, true)
		if err != nil {
			return nil, "", fmt.Errorf("failed to inspect source volume %q: %w", sourceVolumeID, err)
		}
		if state, e := imageInfo.State(); e != nil || state != img.Ready {
			return nil, "", &datastoreConfigError{message: "attached or transitional source snapshot inventory is incomplete; VM-side lifecycle inspection requires operator reconciliation"}
		}
		snapshots = append(snapshots, convertImageSnapshots(imageInfo)...)
	default:
		pool, err := p.ctrl.Images().InfoContext(ctx)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list images while enumerating snapshots: %w", err)
		}
		for _, imageInfo := range pool.Images {
			imageCopy := imageInfo
			snapshots = append(snapshots, convertImageSnapshots(&imageCopy)...)
		}
	}

	start := 0
	if strings.TrimSpace(startingToken) != "" {
		value, err := strconv.Atoi(startingToken)
		if err != nil || value < 0 {
			return nil, "", fmt.Errorf("invalid starting token %q", startingToken)
		}
		start = value
	}
	if start > len(snapshots) {
		return nil, "", fmt.Errorf("starting token %q is out of range", startingToken)
	}

	end := len(snapshots)
	nextToken := ""
	if maxEntries > 0 && start+int(maxEntries) < len(snapshots) {
		end = start + int(maxEntries)
		nextToken = strconv.Itoa(end)
	}

	return append([]VolumeSnapshot(nil), snapshots[start:end]...), nextToken, nil
}

func (p *PersistentDiskVolumeProvider) ExpandVolume(ctx context.Context, volume string, size int64, allowDetached bool) (int64, error) {
	if size <= 0 {
		return 0, fmt.Errorf("requested capacity must be positive")
	}
	id, current, err := p.volumeExists(ctx, volume, p.readOrdinaryImageInfo)
	if err != nil {
		return 0, err
	}
	if size < int64(current) {
		return 0, &datastoreConfigError{message: "shrinking volumes is unsupported"}
	}
	vmID, diskID, attached, _, err := p.findAttachedVolumeDisk(ctx, id)
	if err != nil {
		return 0, err
	}
	if !attached {
		if size == int64(current) {
			return int64(current), nil
		}
		return 0, &datastoreConfigError{message: "detached disk growth is unsupported; attach the volume and retry"}
	}
	return p.expandAttachedDisk(ctx, attachmentVMIdentity{id: vmID}, diskID, id, size)
}

func (p *PersistentDiskVolumeProvider) expandAttachedDisk(ctx context.Context, identity attachmentVMIdentity, diskID, id int, size int64) (int64, error) {
	vmID, vmName := identity.id, identity.name
	actual, err := p.completedDiskSize(ctx, vmID, diskID, id, vmName)
	if err != nil {
		return 0, err
	}
	if actual < size && actual > 0 {
		if err := p.authorizePersistentDiskGrowth(ctx, identity, diskID, id); err != nil {
			return 0, err
		}
		mb := size / sizeConversion
		if size%sizeConversion != 0 {
			mb++
		}
		if err := p.ctrl.VM(vmID).Disk(diskID).ResizeContext(ctx, strconv.FormatInt(mb, 10)); err != nil {
			return 0, fmt.Errorf("resize outcome uncertain; retry after observing completion: %w", err)
		}
	}
	var completed int64
	err = p.waitForResourceStatusContext(ctx, id, p.hotplugPolicy.MaxTimeout, func(ctx context.Context, _ int) (bool, error) {
		observed, e := p.completedDiskSize(ctx, vmID, diskID, id, vmName)
		if e != nil {
			return false, e
		}
		completed = observed
		return observed >= size, nil
	})
	if err != nil {
		return 0, err
	}
	if err := p.persistVerifiedSize(ctx, identity, diskID, id, completed); err != nil {
		return 0, err
	}
	return completed, nil
}

func (p *PersistentDiskVolumeProvider) AttachVolume(ctx context.Context, volume string, node string, immutable bool, params map[string]string) error {
	dispatch, _ := ctx.Value(replacementAttachKey{}).(*ReplacementAttachDispatch)
	attempt := ""
	if dispatch != nil {
		attempt = dispatch.attempt
		dispatch.definitelyIdle = true
	}

	nodeID, vmName, _, err := resolveNodeVM(ctx, p.ctrl, node)
	if err != nil || nodeID == -1 {
		return fmt.Errorf("failed to check if node exists: %w", err)
	}

	volumeID, _, err := p.VolumeExists(ctx, volume)
	if err != nil || volumeID == -1 {
		return fmt.Errorf("failed to check if volume exists: %w", err)
	}
	sizeBytes, err := p.ResolveVolumeSizeBytes(ctx, volume)
	if err != nil {
		return fmt.Errorf("failed to resolve volume size for attach of %s: %w", volume, err)
	}
	hotplugTimeout := p.ComputeHotplugTimeout(sizeBytes)

	report, err := p.inspectAttachmentEnvironment(ctx, volumeID, nodeID, vmName)
	if err != nil {
		return err
	}
	if report != nil {
		klog.V(1).InfoS("Validated volume attachment environment",
			"volumeID", volumeID,
			"nodeID", nodeID,
			"imageDatastoreID", report.ImageDatastore.ID,
			"imageDatastoreName", report.ImageDatastore.Name,
			"imageDatastoreType", report.ImageDatastore.Type,
			"backendProfile", report.ImageDatastore.Backend,
			"deploymentMode", report.DeploymentMode)
		if report.SystemDatastore != nil {
			klog.V(1).InfoS("Resolved target system datastore for attachment",
				"volumeID", volumeID,
				"nodeID", nodeID,
				"systemDatastoreID", report.SystemDatastore.ID,
				"systemDatastoreName", report.SystemDatastore.Name,
				"systemDatastoreTMMAD", report.SystemDatastore.TMMad)
		}
		for _, warning := range report.Warnings {
			klog.Warningf("volume %s attach validation warning: %s", volume, warning)
		}
	}
	var hostArtifactTarget *HostArtifactAttachmentTarget
	authorize, _ := ctx.Value(attachAuthorizationKey{}).(func(context.Context, *HostArtifactAttachmentTarget) error)
	disk := shared.NewDisk()
	disk.Add(shared.ImageID, volumeID)
	disk.Add(shared.Serial, fmt.Sprintf("onecsi-%d", volumeID))
	if attempt != "" {
		disk.Add(attachAttemptField, attempt)
	}
	addDiskParams(disk, params)
	if immutable {
		disk.Add("READONLY", "YES")
	}
	klog.V(1).InfoS("Computed dynamic hotplug timeout for attach",
		"volume", volume,
		"node", node,
		"sizeBytes", sizeBytes,
		"timeout", hotplugTimeout)

	diskTemplate := disk.String()
	attachDisk := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := p.verifyPublishMutation(ctx, nodeID, volumeID, vmName, func() error {
			var err error
			hostArtifactTarget, err = p.hostArtifactAttachmentTargetForResolved(ctx, volume, node, volumeID, nodeID, vmName, params, report)
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if err != nil {
				return status.Error(codes.Unavailable, "host artifact attachment target is unverified")
			}
			if authorize != nil {
				return authorize(ctx, hostArtifactTarget)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if dispatch != nil {
			dispatch.definitelyIdle = false
		}
		err = p.ctrl.VM(nodeID).DiskAttachContext(ctx, diskTemplate)
		if dispatch != nil {
			var refusal *gocaerrors.ResponseError
			authRefused := errors.As(err, &refusal) && strings.TrimSpace(refusal.Msg) != "" &&
				(refusal.Code == gocaerrors.OneAuthenticationError || refusal.Code == gocaerrors.OneAuthorizationError)
			dispatch.definitelyIdle = authRefused || isDefinitiveHotplugRefusal(err)
		}
		return err
	}

	attachErr := attachDisk(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if status.Code(attachErr) == codes.Unavailable || status.Code(attachErr) == codes.FailedPrecondition {
		return attachErr
	}
	if attachErr != nil && !isHotplugStateError(attachErr) {
		if attempt != "" {
			return status.Error(codes.Unavailable, "replacement attach outcome is unverified")
		}
		if conflict, ok := p.classifyHostArtifactConflict(ctx, nodeID, report, hostArtifactTarget, attachErr); ok {
			return conflict
		}
		return fmt.Errorf("failed to attach volume %s to node %s: %w",
			volume, node, attachErr)
	}

	var retryAttach func(context.Context) error
	if attachErr != nil {
		if attempt != "" {
			return status.Error(codes.Unavailable, "replacement attach outcome is unverified")
		}
		retryAttach = func(ctx context.Context) error {
			var err error
			report, err = p.inspectAttachmentEnvironment(ctx, volumeID, nodeID, vmName)
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if err != nil {
				return status.Error(codes.Unavailable, "retry attachment environment is unverified")
			}
			return attachDisk(ctx)
		}
	}

	if err := p.waitForAttachState(ctx, hotplugTimeout, p.hotplugPollWait, func(ctx context.Context) (bool, bool, error) {
		metadata, inspectErr := p.InspectVolumeAttachment(ctx, volume, node)
		if inspectErr != nil {
			return false, false, inspectErr
		}
		attached, inspectErr := attachmentPresence(metadata, volume, volumeID, nodeID)
		if inspectErr != nil {
			return false, false, inspectErr
		}
		ready, readyErr := p.nodeReadyContext(ctx, nodeID, vmName)
		if readyErr != nil {
			return attached, false, readyErr
		}
		return attached, ready, nil
	}, retryAttach, volume, node); err != nil {
		if status.Code(err) == codes.Unavailable || status.Code(err) == codes.FailedPrecondition || ctx.Err() != nil {
			return err
		}
		if conflict, ok := p.classifyHostArtifactConflict(ctx, nodeID, report, hostArtifactTarget, err); ok {
			return conflict
		}
		if vmError := strings.TrimSpace(p.latestVMError(ctx, nodeID)); vmError != "" {
			err = fmt.Errorf("%w (latest OpenNebula VM error: %s)", err, vmError)
		}
		if attachErr != nil {
			return fmt.Errorf("%w (initial attach error: %v)", err, attachErr)
		}
		return err
	}
	if attempt != "" {
		return nil
	}
	return p.restoreVolumeCapacity(ctx, volume, volumeID, attachmentVMIdentity{id: nodeID, name: vmName, lookupName: node})
}

func (p *PersistentDiskVolumeProvider) InspectHostArtifactAttachmentTarget(ctx context.Context, volume string, node string, params map[string]string) (*HostArtifactAttachmentTarget, error) {
	nodeID, vmName, _, err := resolveNodeVM(ctx, p.ctrl, node)
	if err != nil || nodeID == -1 {
		return nil, fmt.Errorf("failed to check if node exists: %w", err)
	}

	volumeID, _, err := p.VolumeExists(ctx, volume)
	if err != nil || volumeID == -1 {
		return nil, fmt.Errorf("failed to check if volume exists: %w", err)
	}

	report, err := p.inspectAttachmentEnvironment(ctx, volumeID, nodeID, vmName)
	if err != nil {
		return nil, err
	}
	return p.hostArtifactAttachmentTargetForResolved(ctx, volume, node, volumeID, nodeID, vmName, params, report)
}

func (p *PersistentDiskVolumeProvider) hostArtifactAttachmentTargetForResolved(ctx context.Context, volume, node string, volumeID, nodeID int, vmName string, params map[string]string, report *DatastoreEnvironmentReport) (*HostArtifactAttachmentTarget, error) {
	if !hostArtifactCandidateDeployment(report) {
		return nil, nil
	}
	vmInfo, err := p.readVMInfo(ctx, nodeID, vmDisks|vmHistory)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect node %d for host artifact target: %w", nodeID, err)
	}

	if err := verifyVMName(vmInfo, vmName); err != nil {
		return nil, err
	}

	history := latestHistoryRecord(vmInfo)
	if history == nil || history.DSID != report.SystemDatastore.ID {
		return nil, fmt.Errorf("%w: VM system datastore history changed", ErrConflictingAttachmentEvidence)
	}
	diskID := nextAvailableDiskID(vmInfo)
	target := nextAvailableDiskTarget(vmInfo, params)
	candidate := &HostArtifactAttachmentTarget{
		VolumeHandle: volume,
		ImageID:      volumeID,
		NodeName:     node,
		VMID:         nodeID,
		DiskID:       diskID,
		Target:       target,
	}
	if diskID > 0 {
		candidate.LVName = fmt.Sprintf("lv-one-%d-%d", nodeID, diskID)
	}
	if report != nil && report.SystemDatastore != nil {
		candidate.SystemDatastoreID = report.SystemDatastore.ID
		candidate.SystemDatastoreName = report.SystemDatastore.Name
		candidate.SystemDatastoreTM = report.SystemDatastore.TMMad
	}
	candidate.HostID = history.HID
	candidate.HostName = history.Hostname
	return candidate, nil
}

func (p *PersistentDiskVolumeProvider) classifyHostArtifactConflict(ctx context.Context, nodeID int, report *DatastoreEnvironmentReport, target *HostArtifactAttachmentTarget, cause error) (*HostArtifactConflictError, bool) {
	if !hostArtifactCandidateDeployment(report) {
		return nil, false
	}
	candidate := HostArtifactAttachmentTarget{}
	if target != nil {
		candidate = *target
	}
	if conflict, ok := HostArtifactConflictFromMessage(causeMessage(cause), candidate, cause); ok {
		return conflict, true
	}
	vmError := strings.TrimSpace(p.latestVMError(ctx, nodeID))
	if vmError == "" {
		return nil, false
	}
	return HostArtifactConflictFromMessage(vmError, candidate, cause)
}

func (p *PersistentDiskVolumeProvider) latestVMError(ctx context.Context, nodeID int) string {
	vmInfo, err := p.ctrl.VM(nodeID).InfoContext(ctx, true)
	if err != nil {
		return ""
	}
	if value, err := vmInfo.UserTemplate.GetStr("ERROR"); err == nil {
		return value
	}
	return ""
}

func hostArtifactCandidateDeployment(report *DatastoreEnvironmentReport) bool {
	if report == nil || report.SystemDatastore == nil {
		return false
	}
	tm := strings.ToLower(strings.TrimSpace(report.SystemDatastore.TMMad))
	return report.DeploymentMode == DeploymentModeSSH &&
		(strings.Contains(tm, "lvm") || strings.Contains(tm, "fs_lvm"))
}

func causeMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func nextAvailableDiskID(vmInfo *vm.VM) int {
	if vmInfo == nil {
		return 0
	}
	used := map[int]struct{}{}
	for _, disk := range vmInfo.Template.GetDisks() {
		if diskID, err := disk.GetI(shared.DiskID); err == nil && diskID >= 0 {
			used[diskID] = struct{}{}
		}
	}
	if raw, err := vmInfo.Template.GetStrFromVec("CONTEXT", "DISK_ID"); err == nil {
		if diskID, convErr := strconv.Atoi(strings.TrimSpace(raw)); convErr == nil && diskID >= 0 {
			used[diskID] = struct{}{}
		}
	}
	for diskID := 0; diskID < 512; diskID++ {
		if _, ok := used[diskID]; !ok {
			return diskID
		}
	}
	return len(used)
}

func nextAvailableDiskTarget(vmInfo *vm.VM, params map[string]string) string {
	prefix := "sd"
	if params != nil && strings.TrimSpace(params["devPrefix"]) != "" {
		prefix = strings.TrimSpace(params["devPrefix"])
	}
	used := map[int]struct{}{}
	if vmInfo != nil {
		for _, disk := range vmInfo.Template.GetDisks() {
			target, err := disk.Get(shared.TargetDisk)
			if err != nil {
				continue
			}
			if index, ok := linuxDiskTargetIndex(strings.TrimSpace(target), prefix); ok {
				used[index] = struct{}{}
			}
		}
	}
	for index := 0; index < 512; index++ {
		if _, ok := used[index]; !ok {
			return prefix + linuxDiskLetters(index)
		}
	}
	return ""
}

func linuxDiskTargetIndex(target, prefix string) (int, bool) {
	if prefix == "" || !strings.HasPrefix(target, prefix) {
		return 0, false
	}
	suffix := strings.TrimPrefix(target, prefix)
	if suffix == "" {
		return 0, false
	}
	index := 0
	for _, char := range suffix {
		if char < 'a' || char > 'z' {
			return 0, false
		}
		index = index*26 + int(char-'a'+1)
	}
	return index - 1, true
}

func linuxDiskLetters(index int) string {
	if index < 0 {
		index = 0
	}
	letters := ""
	for {
		remainder := index % 26
		letters = string(rune('a'+remainder)) + letters
		index = index/26 - 1
		if index < 0 {
			break
		}
	}
	return letters
}

func (p *PersistentDiskVolumeProvider) inspectAttachmentEnvironment(ctx context.Context, volumeID, nodeID int, vmName string) (*DatastoreEnvironmentReport, error) {
	imageInfo, err := p.readImageInfo(ctx, volumeID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect image %d before attach: %w", volumeID, err)
	}

	if imageInfo.DatastoreID == nil {
		return nil, &datastoreConfigError{message: fmt.Sprintf("volume %d does not expose a source datastore ID", volumeID)}
	}
	if imageInfo.ID != volumeID {
		return nil, fmt.Errorf("image identity changed before attach")
	}

	datastorePool, err := p.readDatastoreAuthorityPool(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect datastores before attach: %w", err)
	}

	imageDatastore, err := selectDatastoreAuthority(datastorePool, *imageInfo.DatastoreID)
	if err != nil {
		return nil, err
	}

	report := &DatastoreEnvironmentReport{
		ImageDatastore: datastoreFromSchema(imageDatastore),
		DeploymentMode: DeploymentModeUnknown,
	}

	vmInfo, err := p.readVMInfo(ctx, nodeID, vmHistory)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect node %d before attach: %w", nodeID, err)
	}

	if err := verifyVMName(vmInfo, vmName); err != nil {
		return nil, err
	}

	systemDatastoreID, err := latestHistoryDatastoreID(vmInfo)
	if err != nil {
		return nil, err
	}

	systemDatastore, err := selectDatastoreAuthority(datastorePool, systemDatastoreID)
	if err != nil {
		return nil, err
	}

	normalizedSystemDatastore := datastoreFromSchema(systemDatastore)
	report.SystemDatastore = &normalizedSystemDatastore
	report.DeploymentMode = resolveDeploymentMode(systemDatastore)

	if err := validateCompatibleSystemDatastore(imageDatastore, systemDatastoreID); err != nil {
		return nil, err
	}
	if err := ValidateImageSystemDatastoreCompatibility(imageDatastore, systemDatastore); err != nil {
		return nil, err
	}

	if report.ImageDatastore.Type != datastoreTypeCeph {
		return report, nil
	}

	if err := validateCephImageDatastore(imageDatastore); err != nil {
		return nil, err
	}

	switch report.DeploymentMode {
	case DeploymentModeCeph:
		if err := validateCephSystemDatastore(systemDatastore); err != nil {
			return nil, err
		}
		if err := compareCephConnectionIdentity(imageDatastore, systemDatastore); err != nil {
			return nil, err
		}
	case DeploymentModeSSH:
		report.Warnings = append(report.Warnings, "target VM is using an SSH system datastore; OpenNebula SSH mode limitations apply to Ceph-backed images")
	default:
		return nil, &datastoreConfigError{
			message: fmt.Sprintf("system datastore %d uses unsupported TM_MAD %q for ceph-backed volume %d", systemDatastore.ID, systemDatastore.TMMad, volumeID),
		}
	}

	return report, nil
}

func addDiskParams(disk *shared.Disk, params map[string]string) {
	for key, val := range params {
		if val == "" {
			continue
		}
		switch key {
		case "devPrefix":
			disk.Add(shared.DevPrefix, val)
		case "cache":
			disk.Add(shared.Cache, val)
		case "driver":
			disk.Add(shared.Driver, val)
		case "io":
			disk.Add(shared.IO, val)
		case "ioThread":
			disk.Add(shared.IOThread, val)
		case "virtioBLKQueues":
			disk.Add(shared.VirtioBLKQueues, val)
		case "totalBytesSec":
			disk.Add(shared.TotalBytesSec, val)
		case "readBytesSec":
			disk.Add(shared.ReadBytesSec, val)
		case "writeBytesSec":
			disk.Add(shared.WriteBytesSec, val)
		case "totalIOPSSec":
			disk.Add(shared.TotalIOPSSec, val)
		case "readIOPSSec":
			disk.Add(shared.ReadIOPSSec, val)
		case "writeIOPSSec":
			disk.Add(shared.WriteIOPSSec, val)
		case "totalBytesSecMax":
			disk.Add(shared.TotalBytesSecMax, val)
		case "readBytesSecMax":
			disk.Add(shared.ReadBytesSecMax, val)
		case "writeBytesSecMax":
			disk.Add(shared.WriteBytesSecMax, val)
		case "totalIOPSSecMax":
			disk.Add(shared.TotalIOPSSecMax, val)
		case "readIOPSSecMax":
			disk.Add(shared.ReadIOPSSecMax, val)
		case "writeIOPSSecMax":
			disk.Add(shared.WriteIOPSSecMax, val)
		case "totalBytesSecMaxLength":
			disk.Add(shared.TotalBytesSecMaxLength, val)
		case "readBytesSecMaxLength":
			disk.Add(shared.ReadBytesSecMaxLength, val)
		case "writeBytesSecMaxLength":
			disk.Add(shared.WriteBytesSecMaxLength, val)
		case "totalIOPSSecMaxLength":
			disk.Add(shared.TotalIOPSSecMaxLength, val)
		case "readIOPSSecMaxLength":
			disk.Add(shared.ReadIOPSSecMaxLength, val)
		case "writeIOPSSecMaxLength":
			disk.Add(shared.WriteIOPSSecMaxLength, val)
		case "sizeIOPSSec":
			disk.Add(shared.SizeIOPSSec, val)
		}
	}
}

func (p *PersistentDiskVolumeProvider) DetachVolume(ctx context.Context, volume, node string) error {
	_, err := p.DetachVolumeWithAuthority(ctx, volume, node, -1, "", nil, nil)
	return err
}

func (p *PersistentDiskVolumeProvider) DetachVolumeWithAuthority(ctx context.Context, volume, node string, expectedVMID int, expectedBackend string, verifyOwner DetachOwnerCheck, authorize DetachAuthorization) (bool, error) {
	nodeID, vmName, _, err := resolveNodeVM(ctx, p.ctrl, node)
	if err != nil || nodeID < 0 {
		return false, detachEvidenceError(ctx, "detach VM identity is unavailable", err)
	}

	if expectedVMID >= 0 && nodeID != expectedVMID {
		return false, detachEvidenceError(ctx, "detach VM identity changed", fmt.Errorf("expected VM %d, observed %d", expectedVMID, nodeID))
	}

	volumeID, _, err := p.VolumeExists(ctx, volume)
	if err != nil || volumeID < 0 {
		return false, detachEvidenceError(ctx, "detach image identity is unavailable", err)
	}
	diskID, sizeBytes, err := p.observeDetachDisk(ctx, volume, volumeID, nodeID, vmName, nil, "")
	if errors.Is(err, ErrAttachmentNotFound) {
		_, observedErr := p.getVolumeInNode(ctx, volumeID, nodeID, vmName, true, false)
		if errors.Is(observedErr, ErrAttachmentNotFound) {
			return false, nil
		}
		return false, detachEvidenceError(ctx, "detach attachment absence is unverified", observedErr)
	}
	if err != nil {
		return false, detachEvidenceError(ctx, "detach disk identity or completed capacity is unverified", err)
	}
	hotplugTimeout := p.ComputeHotplugTimeout(sizeBytes)

	readback, replacement := ctx.Value(replacementDetachReadbackKey{}).(replacementDetachReadback)
	accepted := false
	var authorityErr error
	deny := func(err error) error {
		authorityErr = err
		return errors.New("detach authorization denied")
	}
	detachDisk := func(ctx context.Context) error {
		currentNode, currentName, _, err := resolveNodeVM(ctx, p.ctrl, node)
		if err != nil || currentNode != nodeID || currentName != vmName {
			return deny(detachEvidenceError(ctx, "detach VM identity changed", fmt.Errorf("expected VM %d, observed %d: %v", nodeID, currentNode, err)))
		}
		var target string
		var ownershipErr error
		if verifyOwner != nil {
			target, ownershipErr = verifyOwner(ctx, volumeID, nodeID, diskID)
			if ownershipErr == nil && strings.TrimSpace(target) == "" {
				ownershipErr = detachEvidenceError(ctx, "detach census target is unavailable", nil)
			}
		}
		currentDisk, _, err := p.observeDetachDisk(ctx, volume, volumeID, nodeID, vmName, &expectedBackend, target)
		if errors.Is(err, ErrAttachmentNotFound) {
			_, observedErr := p.getVolumeInNode(ctx, volumeID, nodeID, vmName, true, false)
			if errors.Is(observedErr, ErrAttachmentNotFound) {
				return nil
			}
			return deny(detachEvidenceError(ctx, "detach attachment absence is unverified", observedErr))
		}
		if err != nil {
			return deny(detachEvidenceError(ctx, "detach disk identity or completed capacity is unverified", err))
		}
		if currentDisk != diskID {
			return deny(detachEvidenceError(ctx, "detach disk identity changed", fmt.Errorf("expected disk %d, observed %d", diskID, currentDisk)))
		}
		if ownershipErr != nil {
			return deny(ownershipErr)
		}
		liveID, err := p.lookupImageID(ctx, volume)
		if err != nil || liveID != volumeID {
			return deny(detachEvidenceError(ctx, "detach image identity changed or is unverified", err))
		}
		if authorize != nil {
			if err := authorize(ctx, volumeID, nodeID, diskID); err != nil {
				return deny(err)
			}
		}
		if err := ctx.Err(); err != nil {
			return deny(status.FromContextError(err).Err())
		}
		reply, err := p.ctrl.Client.VMDiskDetach(ctx, nodeID, diskID)
		if err != nil {
			return err
		}
		if replacement {
			if reply == nil || reply.BodyInt() != nodeID || readback.recordAccepted == nil {
				return deny(detachEvidenceError(ctx, "detach success reply is unverified", nil))
			}
			if err := readback.recordAccepted(ctx, vmName, volumeID, nodeID, diskID); err != nil {
				return deny(err)
			}
		}
		accepted = true
		return nil
	}
	detachErr := detachDisk(ctx)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if authorityErr != nil {
		return false, authorityErr
	}
	if detachErr != nil && !isDefinitiveHotplugRefusal(detachErr) {
		return false, fmt.Errorf("failed to detach volume %s from node %s: %w", volume, node, detachErr)
	}

	var retryDetach func(context.Context) error
	if detachErr != nil {
		retryDetach = detachDisk
	}
	if err := p.waitForDetachState(ctx, hotplugTimeout, p.hotplugPollWait, func(ctx context.Context) (bool, bool, error) {
		_, infoErr := p.getVolumeInNode(ctx, volumeID, nodeID, vmName, true, accepted)
		ownershipLag := accepted && errors.Is(infoErr, errAttachmentOwnershipLag)
		attached := infoErr == nil
		if !attached && !ownershipLag && !errors.Is(infoErr, ErrAttachmentNotFound) {
			return false, false, deny(detachEvidenceError(ctx, "detach readback is unverified", infoErr))
		}
		if accepted && !attached && readback.originalVMID != 0 {
			if _, err := p.InspectReplacementRelease(ctx, volumeID, readback.originalVMID, readback.originalVMName); err != nil {
				if errors.Is(err, errReplacementVMSettling) {
					return false, false, err
				}
				return false, false, deny(detachEvidenceError(ctx, "original VM release is unverified", err))
			}
		}
		if ownershipLag {
			return false, false, infoErr
		}
		ready, readyErr := p.nodeReadyContext(ctx, nodeID, vmName)
		if readyErr != nil {
			return attached, false, deny(detachEvidenceError(ctx, "detach VM state is unavailable", readyErr))
		}
		return attached, ready, nil
	}, retryDetach, volume, node); err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return false, parentErr
		}
		if detachErr != nil {
			klog.V(2).InfoS("Initial detach attempt failed", "volume", volume, "node", node, "err", detachErr)
		}
		var timeoutErr *HotplugTimeoutError
		if authorityErr != nil && !errors.As(err, &timeoutErr) {
			return false, authorityErr
		}
		return false, err
	}
	return accepted, nil
}

func detachEvidenceError(ctx context.Context, message string, cause error) error {
	if cause != nil {
		klog.V(2).InfoS("Provider detach evidence failed", "check", message, "err", cause)
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if IsDatastoreConfigError(cause) {
		return status.Error(codes.FailedPrecondition, message)
	}
	return status.Error(codes.Unavailable, message)
}

func (p *PersistentDiskVolumeProvider) ListVolumes(ctx context.Context, owner string, maxEntries int32, startingToken string) ([]string, error) {

	listingParams := []int{parameters.PoolWhoAll, -1, -1}

	if startingToken != "" {
		startIndex, err := strconv.Atoi(startingToken)
		if err != nil || startIndex < 0 {
			return nil, fmt.Errorf("invalid starting token: %w", err)
		}
		listingParams[1] = -int(startIndex) //pagination offset
	}

	if maxEntries < 0 {
		return nil, fmt.Errorf("maxEntries must be non-negative")
	}

	if maxEntries > 0 {
		listingParams[2] = int(maxEntries) // page size
	}

	images, err := p.ctrl.Images().InfoContext(ctx, listingParams...)
	if err != nil {
		return nil, fmt.Errorf("failed to list volumes: %w", err)
	}

	//Filter images by the owner tag
	var volumeIDs []string
	for _, img := range images.Images {
		imageOwner, err := img.Template.Get(ownerTag)
		if err == nil && imageOwner == owner {
			volumeIDs = append(volumeIDs, img.Name)
		}
	}
	return volumeIDs, nil
}

func (p *PersistentDiskVolumeProvider) GetCapacity(ctx context.Context, selection DatastoreSelectionConfig) (int64, error) {
	candidates, err := p.resolveProvisioningDatastores(ctx, selection)
	if err != nil {
		return 0, err
	}

	return SumDatastoreCapacity(candidates), nil
}

func (p *PersistentDiskVolumeProvider) ResolveProvisioningDatastores(ctx context.Context, selection DatastoreSelectionConfig) ([]Datastore, error) {
	return p.resolveProvisioningDatastores(ctx, selection)
}

func (p *PersistentDiskVolumeProvider) resolveProvisioningDatastores(ctx context.Context, selection DatastoreSelectionConfig) ([]Datastore, error) {
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

func (p *PersistentDiskVolumeProvider) findAttachedVolumeDisk(ctx context.Context, volumeID int) (int, int, bool, int64, error) {
	vmPool, err := p.ctrl.VMs().InfoContext(ctx)
	if err != nil {
		return 0, 0, false, 0, fmt.Errorf("failed to get vm info while locating attached volume %d: %w", volumeID, err)
	}

	var attachedVMID int
	var attachedDiskID int
	var attachedSizeBytes int64
	found := false
	for _, vmInfo := range vmPool.VMs {
		for _, disk := range vmInfo.Template.GetDisks() {
			diskImageID, diskErr := disk.GetI(shared.ImageID)
			if diskErr != nil || diskImageID != volumeID {
				continue
			}

			diskIDStr, diskIDErr := disk.Get(shared.DiskID)
			if diskIDErr != nil {
				return 0, 0, false, 0, fmt.Errorf("failed to determine disk ID for volume %d on vm %d: %w", volumeID, vmInfo.ID, diskIDErr)
			}
			diskID, convErr := strconv.Atoi(diskIDStr)
			if convErr != nil {
				return 0, 0, false, 0, fmt.Errorf("invalid disk ID for volume %d on vm %d: %w", volumeID, vmInfo.ID, convErr)
			}

			if found {
				return 0, 0, false, 0, &datastoreConfigError{
					message: fmt.Sprintf("volume %d is attached to multiple virtual machines; expansion currently requires a single attachment", volumeID),
				}
			}

			diskSizeBytes, sizeErr := diskSizeBytesFromTemplate(disk)
			if sizeErr != nil {
				return 0, 0, false, 0, fmt.Errorf("failed to determine attached size for volume %d on vm %d disk %d: %w", volumeID, vmInfo.ID, diskID, sizeErr)
			}

			attachedVMID = vmInfo.ID
			attachedDiskID = diskID
			attachedSizeBytes = diskSizeBytes
			found = true
		}
	}

	if !found {
		return 0, 0, false, 0, nil
	}

	return attachedVMID, attachedDiskID, true, attachedSizeBytes, nil
}

type diskImageConflict struct {
	imageID  int
	serialID int
}

func (*diskImageConflict) Error() string {
	return "CSI disk serial contradicts VM disk image identity"
}

func diskImageReference(disk shared.Disk, allowBlank bool) (int, bool, error) {
	raw, err := disk.Get(shared.ImageID)
	blank := err == nil && strings.TrimSpace(raw) == ""
	present := err == nil && !blank
	var id int
	if present {
		id, err = strconv.Atoi(raw)
		if err != nil || id < 0 {
			return 0, false, fmt.Errorf("malformed VM disk image identity")
		}
	}
	if !allowBlank {
		serial, _ := disk.Get(shared.Serial)
		if !present && (hasDiskBacking(disk, serial) || (blank && !isImageLessDisk(disk))) {
			return 0, false, fmt.Errorf("malformed VM disk image identity")
		}
		if suffix, ok := strings.CutPrefix(strings.TrimSpace(serial), "onecsi-"); ok {
			serialID, parseErr := strconv.Atoi(suffix)
			if parseErr == nil && serialID >= 0 && serialID != id {
				return 0, false, &diskImageConflict{imageID: id, serialID: serialID}
			}
		}
	}
	return id, present, nil
}

func isImageLessDisk(disk shared.Disk) bool {
	kind, _ := disk.Get("TYPE")
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "CDROM", "FS", "SWAP":
		return true
	default:
		volatile, _ := disk.Get("VOLATILE")
		return strings.EqualFold(strings.TrimSpace(volatile), "YES")
	}
}

func hasDiskBacking(disk shared.Disk, serial string) bool {
	if strings.HasPrefix(strings.TrimSpace(serial), "onecsi-") {
		return true
	}
	for _, key := range []shared.DiskKeys{"IMAGE", "IMAGE_UNAME", "IMAGE_UID", "SOURCE", "ORIGINAL_SOURCE"} {
		if value, _ := disk.Get(key); strings.TrimSpace(value) != "" {
			return true
		}
	}
	persistent, _ := disk.Get("PERSISTENT")
	return strings.TrimSpace(persistent) != "" && !strings.EqualFold(strings.TrimSpace(persistent), "NO")
}

func diskImageReferenceForVolume(disk shared.Disk, volumeID int, requireComplete bool) (int, bool, error) {
	if requireComplete {
		if err := validateAttachmentDiskFields(disk, volumeID); err != nil {
			return 0, false, err
		}
	}
	id, present, err := diskImageReference(disk, !requireComplete)
	var conflict *diskImageConflict
	if errors.As(err, &conflict) && conflict.imageID != volumeID && conflict.serialID != volumeID {
		return 0, false, nil
	}
	return id, present, err
}

func validateAttachmentDiskFields(disk shared.Disk, imageID int) error {
	seen := make(map[string]struct{})
	for _, pair := range disk.Pairs {
		switch key := pair.Key(); key {
		case "IMAGE_ID", "DISK_ID", "TARGET", "SERIAL", "PERSISTENT", "CLONE", "TM_MAD", "LN_TARGET",
			"SIZE", "SIZE_PREV", "RESIZE", "IMAGE", "IMAGE_UNAME", "IMAGE_UID", "SOURCE", "ORIGINAL_SOURCE", "TYPE", "VOLATILE":
			if _, duplicate := seen[key]; duplicate && !diskIdentifiesOtherImage(disk, imageID) {
				return fmt.Errorf("%w: repeated VM disk field %s", ErrConflictingAttachmentEvidence, key)
			}
			seen[key] = struct{}{}
		}
	}
	return nil
}

func diskIdentifiesOtherImage(disk shared.Disk, imageID int) bool {
	if imageID < 0 {
		return false
	}
	id, err := disk.GetI(shared.ImageID)
	if err != nil || id < 0 || id == imageID || !diskHasOnlyImageID(disk, id) {
		return false
	}
	for _, serial := range disk.GetPairs(string(shared.Serial)) {
		suffix, isCSI := strings.CutPrefix(strings.ToLower(strings.TrimSpace(serial.Value)), "onecsi-")
		serialID, err := strconv.Atoi(suffix)
		if isCSI && err == nil && serialID == imageID {
			return false
		}
	}
	return true
}

func diskHasOnlyImageID(disk shared.Disk, imageID int) bool {
	fields := disk.GetPairs(string(shared.ImageID))
	for _, field := range fields {
		id, err := strconv.Atoi(field.Value)
		if err != nil || id != imageID {
			return false
		}
	}
	return len(fields) != 0
}

func diskFieldsBelongToImage(disk shared.Disk, imageID int) bool {
	if !diskHasOnlyImageID(disk, imageID) {
		return false
	}
	for _, serial := range disk.GetPairs(string(shared.Serial)) {
		if suffix, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(serial.Value)), "onecsi-"); ok {
			id, err := strconv.Atoi(suffix)
			if err != nil || id != imageID {
				return false
			}
		}
	}
	return true
}

func validateUniqueVMDiskSlot(disks []shared.Disk, diskID int) error {
	found := false
	for _, disk := range disks {
		for _, slot := range disk.GetInts(string(shared.DiskID)) {
			if slot != diskID {
				continue
			}
			if found {
				return fmt.Errorf("selected VM disk slot is claimed by multiple records")
			}
			found = true
		}
	}
	return nil
}

func volumeDiskFromVM(info *vm.VM, imageID, vmID int, requireCompleteImageReferences bool) (int, *shared.Disk, error) {
	if info.ID != vmID {
		return 0, nil, fmt.Errorf("requested VM %d identity changed to %d", vmID, info.ID)
	}
	var selected *shared.Disk
	var diskID int
	disks := info.Template.GetDisks()
	for _, disk := range disks {
		id, present, err := diskImageReferenceForVolume(disk, imageID, requireCompleteImageReferences)
		if err != nil {
			return 0, nil, err
		}
		if !present || id != imageID {
			continue
		}
		if selected != nil {
			return 0, nil, fmt.Errorf("image %d has multiple disks on VM %d", imageID, vmID)
		}
		diskID, err = disk.GetI(shared.DiskID)
		if err != nil || diskID < 0 {
			return 0, nil, fmt.Errorf("invalid disk identity for image %d on VM %d: %v", imageID, vmID, err)
		}
		selected = &disk
	}
	if selected == nil {
		return 0, nil, fmt.Errorf("%w: image %d VM %d", ErrAttachmentNotFound, imageID, vmID)
	}
	if requireCompleteImageReferences {
		if err := validateUniqueVMDiskSlot(disks, diskID); err != nil {
			return 0, nil, err
		}
	}
	return diskID, selected, nil
}

func diskSizeBytesFromTemplate(disk shared.Disk) (int64, error) {
	sizeMB, err := disk.GetI(shared.Size)
	if err != nil {
		return 0, err
	}
	return int64(sizeMB) * sizeConversion, nil
}

type expectedImageIdentity struct{}

type expectedDeletionVMIdentity struct{}

type attachAuthorizationKey struct{}

func WithAttachAuthorization(ctx context.Context, authorize func(context.Context, *HostArtifactAttachmentTarget) error) context.Context {
	return context.WithValue(ctx, attachAuthorizationKey{}, authorize)
}

type publishMutationKey struct{}

type publishMutationAdmission struct {
	volume, node  string
	vmID, imageID int
	verifyNode    func(context.Context) error
}

func WithPublishMutationAuthorization(ctx context.Context, volume, node string, vmID, imageID int, verifyNode func(context.Context) error) context.Context {
	return context.WithValue(ctx, publishMutationKey{}, publishMutationAdmission{volume, node, vmID, imageID, verifyNode})
}

func (p *PersistentDiskVolumeProvider) verifyPublishMutation(ctx context.Context, vmID, imageID int, vmName string, verifyTarget func() error) error {
	admitted, ok := ctx.Value(publishMutationKey{}).(publishMutationAdmission)
	if !ok {
		return verifyTarget()
	}
	if admitted.vmID != vmID || admitted.imageID != imageID {
		return status.Error(codes.Unavailable, "publish mutation target differs from admission")
	}
	currentImage, err := p.lookupImageID(ctx, admitted.volume)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || currentImage != imageID {
		return status.Error(codes.Unavailable, "publish mutation image identity is unverified")
	}
	image, err := p.readImageInfo(ctx, imageID)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || image.ID != imageID || image.Name != admitted.volume {
		return status.Error(codes.Unavailable, "publish mutation image identity differs from admission")
	}
	currentVM, currentName, _, err := resolveNodeVM(ctx, p.ctrl, admitted.node)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || currentVM != vmID || (vmName != "" && vmName != currentName) {
		return status.Error(codes.Unavailable, "publish mutation VM identity is unverified")
	}
	if err := verifyTarget(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return admitted.verifyNode(ctx)
}

type volumeDeletionAuthorization struct{}

type deletionAdmission struct {
	prepare func(context.Context, int) error
	verify  func(context.Context, int, bool) error
}

func WithVolumeDeletionAuthorization(ctx context.Context, prepare func(context.Context, int) error, verify func(context.Context, int, bool) error) context.Context {
	return context.WithValue(ctx, volumeDeletionAuthorization{}, deletionAdmission{prepare: prepare, verify: verify})
}

func WithExpectedDeletionVMID(ctx context.Context, vmID int) context.Context {
	return context.WithValue(ctx, expectedDeletionVMIdentity{}, vmID)
}

func WithExpectedImageID(ctx context.Context, id int) context.Context {
	return context.WithValue(ctx, expectedImageIdentity{}, id)
}
func IsAuthoritativeVolumeAbsence(err error) bool {
	var response *gocaerrors.ResponseError
	return errors.As(err, &response) && response.Code == gocaerrors.OneNoExistsError
}

var ErrCloneCapacity = errors.New("unsupported clone capacity")
var ErrCloneConflict = errors.New("incompatible or uncertain clone target")

// These errors mean absence observed through a successful provider response, never transport failure.
var ErrVolumeNotFound = errors.New("volume not found")
var ErrAttachmentNotFound = errors.New("volume attachment not found")
var errAttachmentOwnershipLag = errors.New("attachment ownership has not converged")
var ErrConflictingAttachmentEvidence = errors.New("attachment evidence is contradictory or malformed")

func (p *PersistentDiskVolumeProvider) lookupImageID(ctx context.Context, name string) (int, error) {
	response, err := p.ctrl.Client.ImagePoolInfo(ctx, parameters.PoolWhoAll, -1, -1)
	if err != nil {
		return -1, err
	}
	var pool struct {
		XMLName xml.Name           `xml:"IMAGE_POOL"`
		Images  []imageIdentityXML `xml:"IMAGE"`
	}
	if err := xml.Unmarshal([]byte(response.Body()), &pool); err != nil {
		return -1, err
	}
	found := -1
	for _, v := range pool.Images {
		if slices.Contains(v.Names, name) {
			id, valid := v.imageID()
			if !valid || len(v.Names) != 1 {
				return -1, fmt.Errorf("invalid image identity for volume handle")
			}
			if found != -1 {
				return -1, fmt.Errorf("duplicate image name %q", name)
			}
			found = id
		}
	}
	if found < 0 {
		return -1, fmt.Errorf("%w: %s", ErrVolumeNotFound, name)
	}
	return found, nil
}

type imageIdentityXML struct {
	IDs   []string `xml:"ID"`
	Names []string `xml:"NAME"`
}

func (info imageIdentityXML) imageID() (int, bool) {
	if len(info.IDs) != 1 {
		return 0, false
	}
	id, err := strconv.Atoi(strings.TrimSpace(info.IDs[0]))
	return id, err == nil && id >= 0
}

type imageAuthorityField struct {
	XMLName  xml.Name
	Value    string                `xml:",chardata"`
	Children []imageAuthorityField `xml:",any"`
}

func validateImageAuthorityXML(data []byte, required ...string) error {
	var image struct {
		XMLName   xml.Name              `xml:"IMAGE"`
		Fields    []imageAuthorityField `xml:",any"`
		Templates []struct {
			Fields []imageAuthorityField `xml:",any"`
		} `xml:"TEMPLATE"`
	}
	if err := xml.Unmarshal(data, &image); err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, field := range image.Fields {
		key := field.XMLName.Local
		switch key {
		case "ID", "NAME", "PERSISTENT", "SIZE":
		case "STATE", "RUNNING_VMS", "CLONING_OPS", "DATASTORE_ID", "VMS", "SOURCE", "SNAPSHOTS":
		default:
			continue
		}
		if seen[key] {
			return fmt.Errorf("%w: repeated image field %s", ErrConflictingAttachmentEvidence, key)
		}
		if slices.Contains(required, key) {
			invalid := func() error {
				return fmt.Errorf("%w: required image field %s is invalid", ErrConflictingAttachmentEvidence, key)
			}
			if key == "VMS" {
				if strings.TrimSpace(field.Value) != "" {
					return invalid()
				}
				for _, child := range field.Children {
					id, err := strconv.Atoi(strings.TrimSpace(child.Value))
					if child.XMLName.Local != "ID" || len(child.Children) != 0 || err != nil || id < 0 {
						return invalid()
					}
				}
			} else {
				value, err := strconv.Atoi(strings.TrimSpace(field.Value))
				if len(field.Children) != 0 || err != nil || value < 0 {
					return invalid()
				}
			}
		}
		seen[key] = true
	}
	for _, field := range required {
		if !seen[field] {
			return fmt.Errorf("%w: required image field %s is missing", ErrConflictingAttachmentEvidence, field)
		}
	}
	if len(image.Templates) > 1 {
		return fmt.Errorf("%w: repeated image field TEMPLATE", ErrConflictingAttachmentEvidence)
	}
	for _, template := range image.Templates {
		for _, field := range template.Fields {
			key := field.XMLName.Local
			if key != requestedSizeTag && key != "FORCE_DELETE" {
				continue
			}
			if seen[key] {
				return fmt.Errorf("%w: repeated image template field %s", ErrConflictingAttachmentEvidence, key)
			}
			seen[key] = true
		}
	}
	return nil
}

func (p *PersistentDiskVolumeProvider) readImageInfo(ctx context.Context, imageID int) (*img.Image, error) {
	return p.readScopedImageInfo(ctx, imageID)
}

func (p *PersistentDiskVolumeProvider) readAttachmentImageInfo(ctx context.Context, imageID int) (*img.Image, error) {
	info, err := p.readScopedImageInfo(ctx, imageID, "STATE", "RUNNING_VMS", "VMS", "CLONING_OPS")
	if err != nil {
		return nil, err
	}
	if _, err := info.State(); err != nil {
		return nil, fmt.Errorf("%w: image state is unknown", ErrConflictingAttachmentEvidence)
	}
	if info.CloningOps != 0 || info.RunningVMs != len(info.VMs.ID) {
		return nil, fmt.Errorf("%w: image ownership is inconsistent or unstable", ErrConflictingAttachmentEvidence)
	}
	owners := make(map[int]bool, len(info.VMs.ID))
	for _, id := range info.VMs.ID {
		if owners[id] {
			return nil, fmt.Errorf("%w: image owner is repeated", ErrConflictingAttachmentEvidence)
		}
		owners[id] = true
	}
	return info, verifyImageOwnershipState(img.State(info.StateRaw), info.RunningVMs)
}

func (p *PersistentDiskVolumeProvider) readOrdinaryImageInfo(ctx context.Context, imageID int) (*img.Image, error) {
	return p.ctrl.Image(imageID).InfoContext(ctx, true)
}

func (p *PersistentDiskVolumeProvider) readScopedImageInfo(ctx context.Context, imageID int, required ...string) (*img.Image, error) {
	response, err := p.ctrl.Client.ImageInfo(ctx, imageID, true)
	if err != nil {
		return nil, err
	}
	data := []byte(response.Body())
	if err := validateImageAuthorityXML(data, required...); err != nil {
		if len(required) != 0 && !errors.Is(err, ErrConflictingAttachmentEvidence) {
			return nil, fmt.Errorf("%w: malformed image authority XML: %v", ErrConflictingAttachmentEvidence, err)
		}
		return nil, err
	}
	var info struct {
		img.Image
		IDs []string `xml:"ID"`
	}
	if err := xml.Unmarshal(data, &info); err != nil {
		if len(required) != 0 {
			return nil, fmt.Errorf("%w: malformed image authority XML: %v", ErrConflictingAttachmentEvidence, err)
		}
		return nil, err
	}
	id, valid := (imageIdentityXML{IDs: info.IDs}).imageID()
	if !valid {
		id = -1
	}
	info.ID = id
	return &info.Image, nil
}

func (p *PersistentDiskVolumeProvider) VolumeExists(ctx context.Context, volume string) (int, int, error) {
	return p.volumeExists(ctx, volume, p.readImageInfo)
}

func (p *PersistentDiskVolumeProvider) volumeExists(ctx context.Context, volume string, readImage func(context.Context, int) (*img.Image, error)) (int, int, error) {
	id, known := ctx.Value(expectedImageIdentity{}).(int)
	var err error
	if !known {
		id, err = p.lookupImageID(ctx, volume)
	}
	if err != nil {
		return -1, -1, err
	}
	info, err := readImage(ctx, id)
	if err != nil {
		return -1, -1, err
	}
	if info.ID != id || info.Name != volume {
		return -1, -1, fmt.Errorf("image information differs from the expected ID or handle")
	}
	size, err := effectiveImageSize(info)
	if err != nil {
		return -1, -1, err
	}
	return id, int(size), nil
}

func openNebulaLookupNotFound(err error) bool {
	var response *gocaerrors.ResponseError
	return errors.Is(err, ErrVolumeNotFound) || errors.Is(err, ErrAttachmentNotFound) || (errors.As(err, &response) && response.Code == gocaerrors.OneNoExistsError)
}

func isInsufficientCapacityError(err error) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error())
	fragments := []string{
		"not enough space",
		"not enough free space",
		"insufficient space",
		"insufficient capacity",
		"no space left",
		"not enough capacity",
	}

	for _, fragment := range fragments {
		if strings.Contains(message, fragment) {
			return true
		}
	}

	return false
}

func isHotplugStateError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "wrong state hotplug")
}

func isDefinitiveHotplugRefusal(err error) bool {
	var refusal *gocaerrors.ResponseError
	return errors.As(err, &refusal) && refusal.Code == gocaerrors.OneActionError && isHotplugStateError(refusal)
}

func (p *PersistentDiskVolumeProvider) VolumeReadyWithTimeout(ctx context.Context, volumeID int) (bool, error) {
	err := p.waitForResourceStatusContext(ctx, volumeID, volumeUsedTimeout, p.volumeReadyContext)
	if err != nil {
		return false, fmt.Errorf("failed to wait for volume ready: %w", err)
	}
	return p.volumeReadyContext(ctx, volumeID)
}

func (p *PersistentDiskVolumeProvider) HotplugPolicy() HotplugTimeoutPolicy {
	return p.hotplugPolicy
}

func (p *PersistentDiskVolumeProvider) ComputeHotplugTimeout(sizeBytes int64) time.Duration {
	timeout := p.hotplugPolicy.BaseTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	if sizeBytes > 0 && p.hotplugPolicy.Per100GiB > 0 {
		sizeGi := int64(math.Ceil(float64(sizeBytes) / float64(sizeConversion*1024)))
		increments := int64(math.Ceil(float64(sizeGi) / 100.0))
		if increments < 1 {
			increments = 1
		}
		timeout += time.Duration(increments) * p.hotplugPolicy.Per100GiB
	}

	if p.hotplugPolicy.MaxTimeout > 0 && timeout > p.hotplugPolicy.MaxTimeout {
		timeout = p.hotplugPolicy.MaxTimeout
	}
	if timeout < p.hotplugPolicy.BaseTimeout {
		timeout = p.hotplugPolicy.BaseTimeout
	}

	return timeout
}

func (p *PersistentDiskVolumeProvider) ResolveVolumeSizeBytes(ctx context.Context, volume string) (int64, error) {
	volumeID, sizeBytes, err := p.VolumeExists(ctx, volume)
	if err != nil {
		return 0, err
	}
	if volumeID == -1 {
		return 0, fmt.Errorf("volume %s not found", volume)
	}
	if sizeBytes > 0 {
		return int64(sizeBytes), nil
	}
	return 0, fmt.Errorf("volume %s size is unavailable", volume)
}

func (p *PersistentDiskVolumeProvider) NodeExists(ctx context.Context, node string) (int, error) {
	vmID, _, err := ResolveNodeVMID(ctx, p.ctrl, node)
	if err != nil {
		return -1, fmt.Errorf("Failed to fetch VM: %w", err)
	}

	return vmID, nil
}

func (p *PersistentDiskVolumeProvider) NodeReady(ctx context.Context, node string) (bool, error) {
	nodeID, vmName, _, err := resolveNodeVM(ctx, p.ctrl, node)
	if err != nil {
		return false, err
	}
	return p.nodeReadyContext(ctx, nodeID, vmName)
}

func (p *PersistentDiskVolumeProvider) volumeReadyContext(ctx context.Context, volumeID int) (bool, error) {
	image, err := p.ctrl.Image(volumeID).InfoContext(ctx, true)
	if err != nil {
		return false, fmt.Errorf("failed to get Disk info: %w", err)
	}

	state, err := image.State()
	if err != nil {
		return false, fmt.Errorf("failed to get Disk state: %w", err)
	}

	return state == img.Ready, nil
}

func (p *PersistentDiskVolumeProvider) volumeDeleted(volumeID int) (bool, error) {
	return p.volumeDeletedContext(context.Background(), volumeID)
}
func (p *PersistentDiskVolumeProvider) volumeDeletedContext(ctx context.Context, volumeID int) (bool, error) {
	info, err := p.readScopedImageInfo(ctx, volumeID, "STATE")
	if err != nil {
		if openNebulaLookupNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if info.ID != volumeID {
		return false, fmt.Errorf("image identity is unverified during deletion readback")
	}
	state, err := info.State()
	if err != nil {
		return false, err
	}
	if state == img.Error {
		return false, fmt.Errorf("backend deletion failed for image %d", volumeID)
	}
	return false, nil
}

func (p *PersistentDiskVolumeProvider) nodeReadyContext(ctx context.Context, nodeID int, vmName string) (bool, error) {
	vmInfo, err := p.readVMInfo(ctx, nodeID, vmState)
	if err != nil {
		return false, fmt.Errorf("failed to get VM info: %w", err)
	}
	if err := verifyVMName(vmInfo, vmName); err != nil {
		return false, err
	}
	_, vmLCMState, err := vmInfo.State()
	if err != nil {
		return false, fmt.Errorf("failed to get VM state: %w", err)
	}

	return vmLCMState == vm.Running, nil
}

func (p *PersistentDiskVolumeProvider) waitForAttachState(ctx context.Context, timeout, pollInterval time.Duration, observe func(context.Context) (bool, bool, error), retryAttach func(context.Context) error, volume, node string) error {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if pollInterval <= 0 {
		pollInterval = time.Second
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	lastAttached := false
	lastReady := false
	retryPending := retryAttach != nil

	timedOut := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return &HotplugTimeoutError{
			Operation:            "attach",
			Volume:               volume,
			Node:                 node,
			Timeout:              timeout,
			LastObservedAttached: lastAttached,
			LastObservedReady:    lastReady,
			Cause: fmt.Errorf(
				"timed out after %s waiting for attach of volume %s on node %s (attached=%t ready=%t)",
				timeout,
				volume,
				node,
				lastAttached,
				lastReady,
			),
		}
	}

	for {
		if deadlineCtx.Err() != nil {
			return timedOut()
		}
		attached, ready, err := observe(deadlineCtx)
		if deadlineCtx.Err() != nil {
			return timedOut()
		}
		if err != nil {
			if retryPending || !errors.Is(err, errAttachmentOwnershipLag) {
				return fmt.Errorf("failed while waiting for attach of volume %s on node %s: %w", volume, node, err)
			}
			attached, ready = false, false
		}

		lastAttached = attached
		lastReady = ready
		if attached && ready && deadlineCtx.Err() == nil {
			return nil
		}

		if retryPending && ready && !attached && deadlineCtx.Err() == nil {
			retryErr := retryAttach(deadlineCtx)
			if deadlineCtx.Err() != nil {
				return timedOut()
			}
			if retryErr != nil {
				if !isHotplugStateError(retryErr) || status.Code(retryErr) == codes.Unavailable || status.Code(retryErr) == codes.FailedPrecondition {
					return fmt.Errorf("failed to retry attach of volume %s on node %s: %w", volume, node, retryErr)
				}
			} else {
				retryPending = false
			}
		}

		select {
		case <-deadlineCtx.Done():
			return timedOut()
		case <-ticker.C:
		}
	}
}

func (p *PersistentDiskVolumeProvider) waitForDetachState(ctx context.Context, timeout, pollInterval time.Duration, observe func(context.Context) (bool, bool, error), retryDetach func(context.Context) error, volume, node string) error {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if pollInterval <= 0 {
		pollInterval = time.Second
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	lastAttached := true
	lastReady := false
	retryPending := retryDetach != nil
	var settlementErr error
	timedOut := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		cause := fmt.Errorf("timed out after %s waiting for detach of volume %s on node %s (attached=%t ready=%t)",
			timeout, volume, node, lastAttached, lastReady)
		if settlementErr != nil {
			cause = fmt.Errorf("%w: %w", cause, settlementErr)
		}
		return &HotplugTimeoutError{
			Operation:            "detach",
			Volume:               volume,
			Node:                 node,
			Timeout:              timeout,
			LastObservedAttached: lastAttached,
			LastObservedReady:    lastReady,
			Cause:                cause,
		}
	}

	for {
		if deadlineCtx.Err() != nil {
			return timedOut()
		}
		attached, ready, err := observe(deadlineCtx)
		if deadlineCtx.Err() != nil {
			return timedOut()
		}
		if err != nil {
			if retryPending || (!errors.Is(err, errAttachmentOwnershipLag) && !errors.Is(err, errReplacementVMSettling)) {
				return fmt.Errorf("failed while waiting for detach of volume %s on node %s: %w", volume, node, err)
			}
			settlementErr = err
		} else {
			settlementErr = nil
			lastAttached = attached
			lastReady = ready
			if !attached {
				return nil
			}

			if retryPending && ready {
				retryErr := retryDetach(deadlineCtx)
				if deadlineCtx.Err() != nil {
					return timedOut()
				}
				if retryErr == nil {
					retryPending = false
				} else if !isDefinitiveHotplugRefusal(retryErr) {
					return fmt.Errorf("failed to retry detach of volume %s on node %s: %w", volume, node, retryErr)
				}
			}
		}

		select {
		case <-deadlineCtx.Done():
			return timedOut()
		case <-ticker.C:
		}
	}
}

func (p *PersistentDiskVolumeProvider) observeDetachDisk(ctx context.Context, volume string, volumeID, vmID int, vmName string, expectedBackend *string, target string) (int, int64, error) {
	image, err := p.readAttachmentImageInfo(ctx, volumeID)
	if err != nil {
		return 0, 0, err
	}
	size, err := effectiveImageSize(image)
	if err != nil || size <= 0 || volumeID < 0 || image.ID != volumeID || image.Name != volume {
		return 0, 0, fmt.Errorf("durable capacity or identity is unverified for image %d: %v", volumeID, err)
	}
	info, err := p.readVMInfo(ctx, vmID, vmDisks|vmState)
	if err != nil {
		return 0, 0, err
	}
	if err := verifyVMName(info, vmName); err != nil {
		return 0, 0, err
	}
	diskID, disk, err := volumeDiskFromVM(info, volumeID, vmID, true)
	if err != nil {
		return 0, 0, err
	}
	if !usedImageState(img.State(image.StateRaw)) || !slices.Contains(image.VMs.ID, vmID) {
		return 0, 0, fmt.Errorf("image state or ownership does not verify the selected detach disk")
	}
	if expectedBackend != nil {
		if err := p.verifyObservedAttachmentBackend(ctx, image, *disk, *expectedBackend); err != nil {
			return 0, 0, err
		}
	}
	if target != "" {
		currentTarget, err := verifiedDiskTarget(info.Template.GetDisks(), *disk)
		if err != nil {
			return 0, 0, err
		}
		if image.RunningVMs != 1 || len(image.VMs.ID) != 1 || image.VMs.ID[0] != vmID || strings.TrimSpace(currentTarget) != target {
			return 0, 0, fmt.Errorf("final image %d owner or VM %d disk target differs from the census", volumeID, vmID)
		}
	}
	completed, err := completedSelectedDiskSize(info, *disk)
	if err != nil || completed <= 0 {
		return 0, 0, &datastoreConfigError{message: fmt.Sprintf("completed capacity is not verified for image %d on VM %d: %v", volumeID, vmID, err)}
	}
	if completed != size {
		return 0, 0, &datastoreConfigError{message: fmt.Sprintf("disk capacity %d differs from durable image capacity %d; verify expansion and persist capacity before detaching", completed, size)}
	}
	return diskID, completed, nil
}

func (p *PersistentDiskVolumeProvider) waitForResourceStatusContext(parent context.Context, volumeID int, timeout time.Duration, check func(context.Context, int) (bool, error)) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	interval := p.hotplugPollWait
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ready, err := check(ctx, volumeID)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func idleImageState(state img.State) bool {
	return state == img.Ready || state == img.Error
}

func usedImageState(state img.State) bool {
	return state == img.Used || state == img.UsedPers
}

// Absence must be a successful, consistent provider observation, never a lookup error.
func attachmentPresence(metadata *VolumeAttachmentMetadata, volume string, imageID, nodeID int) (bool, error) {
	if metadata == nil || metadata.VolumeHandle != volume || metadata.ImageID != imageID || metadata.RequestedNodeID != nodeID || nodeID <= 0 {
		return false, fmt.Errorf("attachment identity could not be verified")
	}
	if err := metadata.VerifyOwnership(); err != nil {
		return false, err
	}
	return metadata.AttachedToRequestedNode, nil
}

func (metadata *VolumeAttachmentMetadata) VerifyOwnership() error {
	owners := make(map[int]bool, len(metadata.DiskRecords))
	for _, disk := range metadata.DiskRecords {
		if disk.NodeID <= 0 || strings.TrimSpace(disk.Target) == "" || owners[disk.NodeID] {
			return fmt.Errorf("%w: VM disk ownership is invalid or ambiguous", ErrConflictingAttachmentEvidence)
		}
		owners[disk.NodeID] = true
	}
	if metadata.ImageRunningVMs != len(owners) || len(metadata.ImageVMIDs) != len(owners) {
		return fmt.Errorf("%w: %w: image usage differs from observed VM attachments", ErrConflictingAttachmentEvidence, errAttachmentOwnershipLag)
	}
	if owners[metadata.RequestedNodeID] != metadata.AttachedToRequestedNode {
		return fmt.Errorf("%w: requested-node attachment observations contradict one another", ErrConflictingAttachmentEvidence)
	}
	for _, id := range metadata.ImageVMIDs {
		if !owners[id] {
			return fmt.Errorf("%w: %w: image usage contradicts observed VM attachments", ErrConflictingAttachmentEvidence, errAttachmentOwnershipLag)
		}
		delete(owners, id)
	}
	return verifyImageOwnershipState(img.State(metadata.ImageStateRaw), metadata.ImageRunningVMs)
}

func verifyImageOwnershipState(state img.State, owners int) error {
	if owners == 0 && !idleImageState(state) || owners > 0 && !usedImageState(state) {
		if state == img.Ready || usedImageState(state) {
			return fmt.Errorf("%w: %w: image state disagrees with ownership", ErrConflictingAttachmentEvidence, errAttachmentOwnershipLag)
		}
		return fmt.Errorf("%w: image state disagrees with attachment ownership", ErrConflictingAttachmentEvidence)
	}
	return nil
}

func (p *PersistentDiskVolumeProvider) GetVolumeInNode(ctx context.Context, volumeID int, nodeID int) (string, error) {
	return p.getVolumeInNode(ctx, volumeID, nodeID, "", false, false)
}

func (p *PersistentDiskVolumeProvider) getVolumeInNode(ctx context.Context, volumeID, nodeID int, vmName string, requireCompleteImageReferences, acceptedDetach bool) (string, error) {
	vmInfo, err := p.readVMInfo(ctx, nodeID, vmDisks)
	if err != nil {
		return "", fmt.Errorf("failed to get VM info: %w", err)
	}

	if vmName != "" {
		if err := verifyVMName(vmInfo, vmName); err != nil {
			return "", err
		}
	}

	var target string
	disks := vmInfo.Template.GetDisks()
	for _, disk := range disks {
		if !requireCompleteImageReferences {
			if err := validateAttachmentDiskFields(disk, volumeID); err != nil {
				return "", err
			}
		}
		diskImageID, present, err := diskImageReferenceForVolume(disk, volumeID, requireCompleteImageReferences)
		if err != nil {
			return "", err
		}
		if present && diskImageID == volumeID {
			if target != "" {
				return "", fmt.Errorf("image %d has multiple disks on VM %d", volumeID, nodeID)
			}
			if requireCompleteImageReferences {
				diskID, err := disk.GetI(shared.DiskID)
				if err != nil || diskID < 0 {
					return "", fmt.Errorf("attachment disk slot is unverified")
				}
				if err := validateUniqueVMDiskSlot(disks, diskID); err != nil {
					return "", err
				}
			}
			target, err = verifiedDiskTarget(disks, disk)
			if err != nil {
				return "", err
			}
		}
	}
	if target != "" {
		return target, nil
	}
	if !requireCompleteImageReferences {
		for _, disk := range disks {
			if _, _, err := diskImageReferenceForVolume(disk, volumeID, true); err != nil {
				return "", err
			}
		}
	}
	info, err := p.readImageInfo(ctx, volumeID)
	if err != nil {
		return "", err
	}
	if info.ID != volumeID || strings.TrimSpace(info.Name) == "" {
		return "", fmt.Errorf("image identity is unverified while checking attachment absence")
	}
	metadata, err := p.inspectVolumeAttachment(ctx, info.Name, vmInfo.Name, 0, acceptedDetach)
	_, replacement := ctx.Value(replacementDetachReadbackKey{}).(replacementDetachReadback)
	if acceptedDetach && replacement && metadata != nil && len(metadata.DiskRecords) != 0 {
		return "", fmt.Errorf("%w: replacement detach has additional disk owners", ErrConflictingAttachmentEvidence)
	}
	if err != nil {
		if acceptedDetach && errors.Is(err, errAttachmentOwnershipLag) && !detachOwnershipLagMatches(metadata, volumeID, nodeID, vmInfo.Name) {
			return "", fmt.Errorf("%w: detach ownership differs from the admitted VM", ErrConflictingAttachmentEvidence)
		}
		return "", err
	}
	if metadata.requestedVMName != vmInfo.Name {
		return "", fmt.Errorf("%w: VM %d name differs across observations", ErrConflictingAttachmentEvidence, nodeID)
	}
	present, err := attachmentPresence(metadata, info.Name, volumeID, nodeID)
	if err != nil {
		return "", err
	}
	if present {
		return "", fmt.Errorf("contradictory VM observations")
	}
	return "", fmt.Errorf("%w: image %d node %d", ErrAttachmentNotFound, volumeID, nodeID)
}

func detachOwnershipLagMatches(metadata *VolumeAttachmentMetadata, imageID, nodeID int, vmName string) bool {
	if metadata == nil || metadata.ImageID != imageID || metadata.RequestedNodeID != nodeID ||
		metadata.requestedVMName != vmName || metadata.AttachedToRequestedNode ||
		metadata.ImageRunningVMs != len(metadata.ImageVMIDs) || !usedImageState(img.State(metadata.ImageStateRaw)) {
		return false
	}
	if len(metadata.ImageVMIDs) == 0 {
		return len(metadata.DiskRecords) == 0
	}
	if len(metadata.ImageVMIDs) != len(metadata.DiskRecords)+1 || !slices.Contains(metadata.ImageVMIDs, nodeID) {
		return false
	}
	return !slices.ContainsFunc(metadata.DiskRecords, func(disk VolumeDiskRecord) bool {
		return !slices.Contains(metadata.ImageVMIDs, disk.NodeID)
	})
}

func (p *PersistentDiskVolumeProvider) InspectVolumeLookup(ctx context.Context, volume string, requestedNode string) (*VolumeLookupResult, error) {
	result := &VolumeLookupResult{
		Status:        VolumeLookupNotFound,
		VolumeHandle:  strings.TrimSpace(volume),
		RequestedNode: strings.TrimSpace(requestedNode),
	}

	if strings.TrimSpace(requestedNode) != "" {
		nodeID, resolution, err := ResolveNodeVMID(ctx, p.ctrl, requestedNode)
		if err == nil {
			result.RequestedNodeID = nodeID
			result.NodeResolution = resolution
		}
	}

	imgID, err := p.lookupImageID(ctx, volume)
	if err != nil {
		if errors.Is(err, ErrVolumeNotFound) {
			result.Status = VolumeLookupNotFound
			result.Message = fmt.Sprintf("OpenNebula image record for volume %s is missing", volume)
			return result, nil
		}
		return nil, fmt.Errorf("failed to inspect volume %s by name: %w", volume, err)
	}

	imageInfo, err := p.readImageInfo(ctx, imgID)
	if err != nil {
		if openNebulaLookupNotFound(err) {
			result.Status = VolumeLookupProviderInconsistent
			result.ImageID = imgID
			result.Message = fmt.Sprintf("OpenNebula image %d for volume %s resolved by name but could not be loaded", imgID, volume)
			return result, nil
		}
		return nil, fmt.Errorf("failed to inspect OpenNebula image %d for volume %s: %w", imgID, volume, err)
	}
	if imageInfo.ID != imgID || imageInfo.Name != volume {
		result.Status = VolumeLookupProviderInconsistent
		result.Message = "image lookup returned a different identity"
		return result, nil
	}

	result.Status = VolumeLookupPresent
	result.ImageID = imgID
	result.SizeBytes = int64(imageInfo.Size) * sizeConversion
	metadata, metadataErr := p.InspectVolumeAttachment(ctx, volume, requestedNode)
	if metadataErr == nil && metadata.ImageID == imgID {
		result.AttachmentMetadata = metadata
	}
	return result, nil
}

func (p *PersistentDiskVolumeProvider) InspectVolumeDisk(ctx context.Context, imageID, vmID int) (VolumeDiskRecord, error) {
	info, err := p.readVMInfo(ctx, vmID, vmDisks)
	if err != nil {
		return VolumeDiskRecord{}, err
	}
	_, disk, err := publishDiskFromVM(info, imageID, vmID)
	if err != nil {
		return VolumeDiskRecord{}, err
	}
	image, err := p.readAttachmentImageInfo(ctx, imageID)
	if err != nil {
		return VolumeDiskRecord{}, err
	}
	if image.ID != imageID || len(image.VMs.ID) != 1 || image.VMs.ID[0] != vmID {
		return VolumeDiskRecord{}, fmt.Errorf("%w: image ownership contradicts the selected VM disk", ErrConflictingAttachmentEvidence)
	}
	return attachmentDiskRecord(info, info.Template.GetDisks(), *disk)
}

func publishDiskFromVM(info *vm.VM, imageID, vmID int) (int, *shared.Disk, error) {
	disks := info.Template.GetDisks()
	for _, candidate := range disks {
		if _, err := attachmentDiskImage(candidate, imageID); errors.Is(err, ErrConflictingAttachmentEvidence) {
			return 0, nil, err
		}
	}
	slot, disk, err := volumeDiskFromVM(info, imageID, vmID, false)
	if err != nil {
		return 0, nil, err
	}
	if err := validateUniqueVMDiskSlot(disks, slot); err != nil {
		return 0, nil, fmt.Errorf("%w: %v", ErrConflictingAttachmentEvidence, err)
	}
	return slot, disk, nil
}

func attachmentDiskImage(disk shared.Disk, imageID int) (bool, error) {
	id, present, err := diskImageReferenceForVolume(disk, imageID, true)
	if err != nil {
		if errors.Is(err, ErrConflictingAttachmentEvidence) {
			return false, err
		}
		if _, permissivePresent, permissiveErr := diskImageReference(disk, true); permissiveErr == nil && !permissivePresent {
			serial, _ := disk.Get(shared.Serial)
			if !hasDiskBacking(disk, serial) {
				return false, err
			}
		}
		return false, fmt.Errorf("%w: %v", ErrConflictingAttachmentEvidence, err)
	}
	return present && id == imageID, nil
}

func verifiedDiskTarget(disks []shared.Disk, selected shared.Disk) (string, error) {
	target, _ := selected.Get(shared.TargetDisk)
	name := strings.TrimPrefix(target, "/dev/")
	if name == "" || name == "." || name == ".." || strings.IndexFunc(name, func(r rune) bool {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-', r == '.':
			return false
		default:
			return true
		}
	}) >= 0 {
		return "", fmt.Errorf("%w: selected VM disk target is invalid", ErrConflictingAttachmentEvidence)
	}
	matches := 0
	for _, disk := range disks {
		for _, field := range disk.GetPairs(string(shared.TargetDisk)) {
			if strings.TrimPrefix(strings.TrimSpace(field.Value), "/dev/") == name {
				matches++
			}
		}
	}
	if matches != 1 {
		return "", fmt.Errorf("%w: selected VM disk target is ambiguous", ErrConflictingAttachmentEvidence)
	}
	return target, nil
}

func attachmentDiskRecord(info *vm.VM, disks []shared.Disk, disk shared.Disk) (VolumeDiskRecord, error) {
	slot, err := disk.GetI(shared.DiskID)
	if err != nil || slot < 0 {
		return VolumeDiskRecord{}, fmt.Errorf("%w: disk slot is invalid", ErrConflictingAttachmentEvidence)
	}
	if err := validateUniqueVMDiskSlot(disks, slot); err != nil {
		return VolumeDiskRecord{}, fmt.Errorf("%w: %v", ErrConflictingAttachmentEvidence, err)
	}
	target, err := verifiedDiskTarget(disks, disk)
	if err != nil {
		return VolumeDiskRecord{}, err
	}
	serial, _ := disk.Get(shared.Serial)
	readOnly := disk.GetPairs("READONLY")
	return VolumeDiskRecord{NodeName: info.Name, NodeID: info.ID, DiskID: slot, Target: target, Serial: serial,
		ReadOnly: len(readOnly) == 1 && readOnly[0].Value == "YES", ReplacementMarked: len(disk.GetPairs(attachAttemptField)) != 0}, nil
}

func (p *PersistentDiskVolumeProvider) InspectVolumeAttachment(ctx context.Context, volume string, node string) (*VolumeAttachmentMetadata, error) {
	return p.inspectVolumeAttachment(ctx, volume, node, 0, false)
}

func (p *PersistentDiskVolumeProvider) inspectVolumeAttachment(ctx context.Context, volume, node string, expectedAbsentVMID int, acceptedDetach bool) (*VolumeAttachmentMetadata, error) {
	readImage := func(ctx context.Context, id int) (*img.Image, error) {
		info, err := p.readAttachmentImageInfo(ctx, id)
		if acceptedDetach && errors.Is(err, errAttachmentOwnershipLag) &&
			info != nil && info.RunningVMs == 0 && usedImageState(img.State(info.StateRaw)) {
			return info, nil
		}
		return info, err
	}
	volumeID, _, err := p.volumeExists(ctx, volume, readImage)
	if err != nil {
		return nil, fmt.Errorf("failed to check if volume exists while inspecting attachment metadata: %w", err)
	}
	if volumeID == -1 {
		return nil, fmt.Errorf("volume %s was not found while inspecting attachment metadata", volume)
	}

	requestedNodeID, requestedVMName := -1, ""
	if strings.TrimSpace(node) != "" {
		requestedNodeID, requestedVMName, _, err = resolveNodeVM(ctx, p.ctrl, node)
		if err != nil {
			return nil, fmt.Errorf("failed to check if node exists while inspecting attachment metadata: %w", err)
		}
	}

	imageInfo, err := readImage(ctx, volumeID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect image %d attachment metadata: %w", volumeID, err)
	}
	if imageInfo.ID != volumeID || imageInfo.Name != volume {
		return nil, fmt.Errorf("%w: image attachment metadata returned a different identity", ErrConflictingAttachmentEvidence)
	}

	imageState := fmt.Sprintf("UNKNOWN_%d", imageInfo.StateRaw)
	if parsed, stateErr := imageInfo.StateString(); stateErr == nil {
		imageState = parsed
	}

	metadata := &VolumeAttachmentMetadata{
		VolumeHandle:    volume,
		ImageID:         imageInfo.ID,
		ImageName:       imageInfo.Name,
		ImageState:      imageState,
		ImageStateRaw:   imageInfo.StateRaw,
		ImageRunningVMs: imageInfo.RunningVMs,
		ImageVMIDs:      append([]int(nil), imageInfo.VMs.ID...),
		RequestedNode:   strings.TrimSpace(node),
		RequestedNodeID: requestedNodeID,
		requestedVMName: requestedVMName,
	}

	vms, err := p.readAttachmentVMs(ctx, volumeID, attachmentVMIdentity{id: requestedNodeID, name: requestedVMName, lookupName: node}, expectedAbsentVMID)
	if err != nil {
		return nil, fmt.Errorf("failed to list virtual machines while inspecting volume %d attachment metadata: %w", volumeID, err)
	}
	var incompleteDiskEvidence error
	for _, vmInfo := range vms {
		disks := vmInfo.Template.GetDisks()
		seen := false
		for _, disk := range disks {
			matches, err := attachmentDiskImage(disk, volumeID)
			if err != nil {
				if errors.Is(err, ErrConflictingAttachmentEvidence) {
					return nil, err
				}
				incompleteDiskEvidence = err
			}
			if !matches {
				continue
			}
			record, err := attachmentDiskRecord(&vmInfo, disks, disk)
			if err != nil {
				return nil, err
			}
			if requestedNodeID > 0 && vmInfo.ID == requestedNodeID {
				if seen {
					return nil, fmt.Errorf("%w: image %d has multiple disks on VM %d", ErrConflictingAttachmentEvidence, volumeID, vmInfo.ID)
				}
				seen = true
				metadata.AttachedToRequestedNode = true
			}
			metadata.DiskRecords = append(metadata.DiskRecords, record)
		}
	}

	if incompleteDiskEvidence != nil {
		return nil, incompleteDiskEvidence
	}
	if err := metadata.VerifyOwnership(); err != nil {
		return metadata, err
	}
	if err := p.verifyPinnedVMDiskAbsent(ctx, volumeID, expectedAbsentVMID, vms); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (p *PersistentDiskVolumeProvider) ListCurrentAttachments(ctx context.Context) ([]ObservedAttachment, VMNameInventory, error) {
	rows, err := readVMPool(ctx, p.ctrl, true)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list virtual machines: %w", err)
	}

	attachments := make([]ObservedAttachment, 0)
	vmNames := make(VMNameInventory, len(rows))
	conflictedImages := make(map[int]bool)
	incompleteImages := make(map[int]bool)
	counts := vmIdentityCounts(rows)
	for _, row := range rows {
		vmInfo, err := row.vm(vmDisks)
		if err == nil && counts[vmInfo.ID] > 1 {
			err = fmt.Errorf("%w: VM %d occurs more than once", ErrConflictingAttachmentEvidence, vmInfo.ID)
		}
		if err != nil {
			images, scoped := row.attachmentImages()
			if !scoped {
				return nil, nil, err
			}
			ids := row.candidateIDs()
			for _, name := range row.Names {
				vmNames[name] = append(vmNames[name], ids...)
				vmNames[name] = append(vmNames[name], -1, -1)
			}
			for _, imageID := range images {
				incompleteImages[imageID] = true
				attachments = append(attachments, ObservedAttachment{ImageID: imageID, NodeID: -1, DiskID: -1})
			}
			continue
		}
		vmNames[vmInfo.Name] = append(vmNames[vmInfo.Name], vmInfo.ID)
		for _, disk := range vmInfo.Template.GetDisks() {
			diskImageID, present, diskErr := diskImageReference(disk, false)
			if err := validateAttachmentDiskFields(disk, -1); err != nil {
				if diskErr != nil || !present || !diskFieldsBelongToImage(disk, diskImageID) {
					return nil, nil, err
				}
				incompleteImages[diskImageID] = true
			}
			var conflict *diskImageConflict
			if errors.As(diskErr, &conflict) {
				conflictedImages[conflict.imageID] = true
				conflictedImages[conflict.serialID] = true
				continue
			}
			if diskErr != nil {
				return nil, nil, diskErr
			}
			if !present {
				continue
			}
			diskID, diskIDErr := disk.GetI(shared.DiskID)
			if diskIDErr != nil {
				if !incompleteImages[diskImageID] {
					continue
				}
				diskID = -1
			}
			attachments = append(attachments, ObservedAttachment{
				ImageID: diskImageID, NodeName: vmInfo.Name, NodeID: vmInfo.ID, DiskID: diskID,
			})
		}
	}
	loadDatastorePool := sync.OnceValues(func() ([]datastoreAuthorityXML, error) {
		return p.readDatastoreAuthorityPool(ctx)
	})
	imageCache := map[int]struct {
		name    string
		backend string
	}{}
	for i := range attachments {
		attachment := &attachments[i]
		imageID := attachment.ImageID
		if conflictedImages[imageID] {
			continue
		}
		imageMeta, ok := imageCache[imageID]
		if !ok {
			imageInfo, imageErr := p.readImageInfo(ctx, imageID)
			if IsAuthoritativeVolumeAbsence(imageErr) || errors.Is(imageErr, ErrConflictingAttachmentEvidence) {
				conflictedImages[imageID] = true
				continue
			}
			if imageErr != nil {
				return nil, nil, fmt.Errorf("failed to inspect image %d while listing attachments: %w", imageID, imageErr)
			}
			if imageInfo.ID != imageID || strings.TrimSpace(imageInfo.Name) == "" {
				conflictedImages[imageID] = true
				klog.V(2).InfoS("Skipping attachment with unverified image identity", "imageID", imageID)
				continue
			}
			imageMeta.name = imageInfo.Name
			if imageInfo.DatastoreID != nil {
				pool, backendErr := loadDatastorePool()
				if backendErr == nil {
					var datastore Datastore
					datastore, backendErr = attachmentDatastoreFromPool(pool, *imageInfo.DatastoreID)
					imageMeta.backend = datastore.Backend
				}
				if backendErr != nil {
					klog.V(2).InfoS("Attachment inventory backend is unknown", "imageID", imageID, "err", backendErr)
				}
			}
			imageCache[imageID] = imageMeta
		}
		attachment.VolumeHandle = imageMeta.name
		attachment.Backend = imageMeta.backend
		if incompleteImages[imageID] {
			attachment.DiskID = -1
		}
	}
	attachments = slices.DeleteFunc(attachments, func(a ObservedAttachment) bool { return conflictedImages[a.ImageID] })
	return attachments, vmNames, nil
}

func (p *PersistentDiskVolumeProvider) InspectVolumeDatastore(ctx context.Context, volume string, imageID int) (Datastore, error) {
	image, err := p.readImageInfo(ctx, imageID)
	if err != nil {
		return Datastore{}, err
	}
	if imageID < 0 || image.ID != imageID || image.Name != volume || image.DatastoreID == nil || *image.DatastoreID < 0 {
		return Datastore{}, fmt.Errorf("image or datastore identity is unverified")
	}
	return p.attachmentDatastore(ctx, *image.DatastoreID)
}

func (p *PersistentDiskVolumeProvider) attachmentDatastore(ctx context.Context, datastoreID int) (Datastore, error) {
	pool, err := p.readDatastoreAuthorityPool(ctx)
	if err != nil {
		return Datastore{}, fmt.Errorf("cannot inspect attachment datastore %d: %w", datastoreID, err)
	}
	return attachmentDatastoreFromPool(pool, datastoreID)
}

func attachmentDatastoreFromPool(datastores []datastoreAuthorityXML, datastoreID int) (Datastore, error) {
	datastore, err := selectDatastoreAuthority(datastores, datastoreID)
	if err != nil {
		return Datastore{}, err
	}
	observed := datastoreFromSchema(datastore)
	if observed.Category != string(datastoreSchema.Image) {
		return Datastore{}, &datastoreConfigError{message: fmt.Sprintf("datastore %d is not an image datastore", datastoreID)}
	}
	for _, category := range []string{strings.TrimSpace(datastore.Type), getDatastoreAttribute(datastore, "TYPE")} {
		if category != "" && category != "-" && normalizeDatastoreCategory(datastoreSchema.Datastore{Type: category}) != string(datastoreSchema.Image) {
			return Datastore{}, &datastoreConfigError{message: fmt.Sprintf("datastore %d has conflicting image datastore categories", datastoreID)}
		}
	}
	switch backend := normalizeAllowedDatastoreType(observed.DSMad); observed.DSMad {
	case "ceph", "fs", "local", "fs_lvm", "fs_lvm_ssh":
		if observed.Backend != backend || (observed.TMMad != "" && !attachmentTransferMatchesBackend(observed.TMMad, backend)) {
			return Datastore{}, &datastoreConfigError{message: fmt.Sprintf("datastore %d drivers disagree with backend %q", datastoreID, observed.Backend)}
		}
		if backend == "ceph" {
			rootType := strings.ToUpper(strings.TrimSpace(datastore.DiskType))
			templateRaw, _ := datastore.Template.GetStr(cephAttrDiskType)
			templateType := strings.ToUpper(strings.TrimSpace(templateRaw))
			if observed.TMMad != "ceph" ||
				(rootType == "" && templateType == "") ||
				(rootType != "" && rootType != "3" && rootType != cephDiskTypeRBD) ||
				(templateType != "" && templateType != cephDiskTypeRBD) {
				return Datastore{}, &datastoreConfigError{message: fmt.Sprintf("datastore %d lacks consistent Ceph RBD evidence", datastoreID)}
			}
		}
		return observed, nil
	default:
		return Datastore{}, &datastoreConfigError{message: fmt.Sprintf("datastore %d has unsupported attachment backend %q", datastoreID, observed.Backend)}
	}
}

func findDatastoreByID(pool []datastoreSchema.Datastore, id int) (datastoreSchema.Datastore, error) {
	for _, ds := range pool {
		if ds.ID == id {
			return ds, nil
		}
	}

	return datastoreSchema.Datastore{}, &datastoreConfigError{message: fmt.Sprintf("datastore %d was not found", id)}
}

func latestHistoryDatastoreID(vmInfo *vm.VM) (int, error) {
	if vmInfo == nil {
		return 0, &datastoreConfigError{message: "target VM information is not available"}
	}

	latest := latestHistoryRecord(vmInfo)

	if latest == nil {
		return 0, &datastoreConfigError{message: fmt.Sprintf("target VM %d does not expose a system datastore history record", vmInfo.ID)}
	}

	return latest.DSID, nil
}

func latestHistoryRecord(vmInfo *vm.VM) *vm.HistoryRecord {
	if vmInfo == nil {
		return nil
	}
	var latest *vm.HistoryRecord
	for idx := range vmInfo.HistoryRecords {
		record := &vmInfo.HistoryRecords[idx]
		if record.DSID < 0 {
			continue
		}
		if latest == nil || record.SEQ > latest.SEQ {
			latest = record
		}
	}
	return latest
}

func imageSnapshotByID(imageInfo *img.Image, snapshotID int) (shared.Snapshot, error) {
	for _, snapshot := range imageInfo.Snapshots.Snapshots {
		if snapshot.ID == snapshotID {
			return snapshot, nil
		}
	}

	return shared.Snapshot{}, fmt.Errorf("snapshot %d was not found in image %d", snapshotID, imageInfo.ID)
}

func convertImageSnapshots(imageInfo *img.Image) []VolumeSnapshot {
	converted := make([]VolumeSnapshot, 0, len(imageInfo.Snapshots.Snapshots))
	for _, snapshot := range imageInfo.Snapshots.Snapshots {
		converted = append(converted, convertImageSnapshot(imageInfo, snapshot))
	}
	return converted
}

func convertImageSnapshot(imageInfo *img.Image, snapshot shared.Snapshot) VolumeSnapshot {
	return VolumeSnapshot{
		SnapshotID:     EncodeVolumeSnapshotID(imageInfo.ID, snapshot.ID),
		SourceVolumeID: imageInfo.Name,
		CreationTime:   time.Unix(int64(snapshot.Date), 0).UTC(),
		SizeBytes:      int64(snapshot.Size) * sizeConversion,
		ReadyToUse:     true,
	}
}
