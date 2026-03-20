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

package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

var (
	controllerCapabilityTypes = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}

	supportedAccessModes = []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
	}
)

type ControllerServer struct {
	driver                   *Driver
	volumeProvider           opennebula.OpenNebulaVolumeProvider
	sharedFilesystemProvider opennebula.SharedFilesystemProvider
	csi.UnimplementedControllerServer
}

func NewControllerServer(d *Driver, vp opennebula.OpenNebulaVolumeProvider, sharedProvider opennebula.SharedFilesystemProvider) *ControllerServer {
	return &ControllerServer{
		driver:                   d,
		volumeProvider:           vp,
		sharedFilesystemProvider: sharedProvider,
	}
}

func (s *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.V(1).InfoS("CreateVolume called", "req", protosanitizer.StripSecrets(req))

	name := req.GetName()
	if name == "" {
		klog.V(0).ErrorS(nil, "method", "CreateVolume", "CreateVolume called with empty volume name")
		return nil, status.Error(codes.InvalidArgument, "missing volume name")
	}

	volumeCapabilities := req.GetVolumeCapabilities()
	if len(volumeCapabilities) == 0 {
		klog.V(0).ErrorS(nil, "method", "CreateVolume", "CreateVolume called with empty or nil volume capabilities")
		return nil, status.Error(codes.InvalidArgument, "missing volume capabilities")
	}

	requiredBytes := req.GetCapacityRange().GetRequiredBytes()
	if requiredBytes == 0 {
		requiredBytes = DefaultVolumeSizeBytes
	}

	rawParams := req.GetParameters()
	selection, err := s.driver.GetDatastoreSelectionConfig(rawParams)
	if err != nil {
		klog.V(0).ErrorS(err, "Invalid datastore configuration", "method", "CreateVolume", "params", rawParams)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := validateAccessMode(volumeCapabilities, selection.AllowedTypes); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	accessMode := volumeCapabilities[0].GetAccessMode()
	if accessMode == nil {
		klog.V(0).ErrorS(nil, "method", "CreateVolume", "CreateVolume called with empty access mode")
		return nil, status.Error(codes.InvalidArgument, "missing access mode")
	}

	volumeAccessModel, err := resolveVolumeAccessModel(volumeCapabilities[0], selection.AllowedTypes)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	response := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      name,
			CapacityBytes: requiredBytes,
		},
	}

	params := filterProvisioningParams(rawParams)
	fsType := getRequestedFSType(rawParams, volumeCapabilities[0])
	immutableVolume := accessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY

	response.Volume.VolumeContext = params

	if volumeAccessModel == opennebula.VolumeAccessModelSharedFS {
		if s.sharedFilesystemProvider == nil {
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}

		result, sharedErr := s.sharedFilesystemProvider.CreateSharedVolume(ctx, opennebula.SharedVolumeRequest{
			Name:       name,
			SizeBytes:  requiredBytes,
			Selection:  selection,
			Parameters: rawParams,
			Secrets:    req.GetSecrets(),
		})
		if sharedErr != nil {
			switch {
			case opennebula.IsDatastoreConfigError(sharedErr):
				return nil, status.Error(codes.InvalidArgument, sharedErr.Error())
			case opennebula.IsDatastoreCapacityError(sharedErr):
				return nil, status.Error(codes.ResourceExhausted, sharedErr.Error())
			}
			klog.V(0).ErrorS(sharedErr, "Failed to create shared filesystem volume",
				"method", "CreateVolume", "volumeName", name, "requiredBytes", requiredBytes,
				"policy", selection.Policy, "datastores", selection.Identifiers)
			return nil, status.Error(codes.Internal, "failed to create volume")
		}

		response.Volume.VolumeId = result.VolumeID
		response.Volume.CapacityBytes = result.CapacityBytes
		klog.V(1).InfoS("Shared filesystem volume created successfully",
			"method", "CreateVolume", "volumeName", name, "requiredBytes", requiredBytes,
			"policy", selection.Policy, "datastores", selection.Identifiers,
			"selectedDatastoreID", result.Datastore.ID, "selectedDatastoreName", result.Datastore.Name,
			"backend", result.Metadata.Backend, "mode", result.Metadata.Mode)

		return response, nil
	}

	volumeID, volumeSize, _ := s.volumeProvider.VolumeExists(ctx, name)
	if volumeID != -1 {
		if int64(volumeSize) == requiredBytes {
			klog.V(3).InfoS("Volume already exists with the same size",
				"method", "CreateVolume", "volumeID", volumeID, "requiredSize", requiredBytes)
			return response, nil
		}
		klog.V(0).ErrorS(nil, "Volume with the same name already exists with different size",
			"method", "CreateVolume", "volumeID", volumeID, "existingSize", volumeSize, "requiredSize", requiredBytes)
		return nil, status.Error(codes.AlreadyExists,
			"volume with the same name already exists with different size")

	}

	result, err := s.volumeProvider.CreateVolume(ctx, name, requiredBytes, DefaultDriverName, immutableVolume, fsType, params, selection)
	if err != nil {
		switch {
		case opennebula.IsDatastoreConfigError(err):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case opennebula.IsDatastoreCapacityError(err):
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		klog.V(0).ErrorS(err, "Failed to create volume",
			"method", "CreateVolume", "volumeName", name, "requiredBytes", requiredBytes,
			"defaultDriverName", DefaultDriverName, "immutableVolume", immutableVolume,
			"policy", selection.Policy, "datastores", selection.Identifiers, "params", params)
		return nil, status.Error(codes.Internal, "failed to create volume")
	}

	klog.V(1).InfoS("Volume created successfully",
		"method", "CreateVolume", "volumeName", name, "requiredBytes",
		requiredBytes, "defaultDriverName", DefaultDriverName, "immutableVolume", immutableVolume,
		"policy", selection.Policy, "datastores", selection.Identifiers,
		"selectedDatastoreID", result.Datastore.ID, "selectedDatastoreName", result.Datastore.Name,
		"params", params)

	return response, nil
}

func (s *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(1).InfoS("DeleteVolume called", "req", protosanitizer.StripSecrets(req))

	if req.VolumeId == "" {
		klog.V(0).ErrorS(nil, "method", "DeleteVolume", "DeleteVolume called with empty volume ID")
		return nil, status.Error(codes.InvalidArgument, "missing volume ID")
	}

	klog.V(3).InfoS("Deleting volume", "method", "DeleteVolume", "volumeID", req.VolumeId)

	if opennebula.IsSharedFilesystemVolumeID(req.VolumeId) {
		if s.sharedFilesystemProvider == nil {
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}

		err := s.sharedFilesystemProvider.DeleteSharedVolume(ctx, req.VolumeId, req.GetSecrets())
		if err != nil {
			switch {
			case opennebula.IsDatastoreConfigError(err):
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			case opennebula.IsDatastoreCapacityError(err):
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}
			klog.V(0).ErrorS(err, "Failed to delete shared filesystem volume", "method", "DeleteVolume", "volumeID", req.VolumeId)
			return nil, status.Error(codes.FailedPrecondition, "failed to delete volume")
		}

		klog.V(1).InfoS("Shared filesystem volume deleted successfully", "method", "DeleteVolume", "volumeID", req.VolumeId)
		return &csi.DeleteVolumeResponse{}, nil
	}

	err := s.volumeProvider.DeleteVolume(ctx, req.VolumeId)
	if err != nil {
		klog.V(0).ErrorS(err, "Failed to delete volume", "method", "DeleteVolume", "volumeID", req.VolumeId)
		return nil, status.Error(codes.FailedPrecondition, "failed to delete volume")
	}

	klog.V(1).InfoS("Volume deleted successfully", "method", "DeleteVolume", "volumeID", req.VolumeId)
	return &csi.DeleteVolumeResponse{}, nil
}

// TODO: Process VolumeCapability, readonly
func (s *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.V(1).InfoS("ControllerPublishVolume called", "req", protosanitizer.StripSecrets(req))
	if req.VolumeId == "" {
		klog.V(0).ErrorS(nil, "method", "ControllerPublishVolume", "ControllerPublishVolume called with empty volume ID")
		return nil, status.Error(codes.InvalidArgument, "missing volume ID")
	}

	if req.NodeId == "" {
		klog.V(0).ErrorS(nil, "method", "ControllerPublishVolume", "ControllerPublishVolume called with empty node ID")
		return nil, status.Error(codes.InvalidArgument, "missing node ID")
	}

	if req.VolumeCapability == nil {
		klog.V(0).ErrorS(nil, "method", "ControllerPublishVolume", "ControllerPublishVolume called with empty volume capability")
		return nil, status.Error(codes.InvalidArgument, "missing volume capability")
	}

	if req.VolumeCapability.AccessMode == nil {
		klog.V(0).ErrorS(nil, "method", "ControllerPublishVolume", "ControllerPublishVolume called with empty access mode")
		return nil, status.Error(codes.InvalidArgument, "missing access mode")
	}

	immutableVolume := req.VolumeCapability.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY

	if opennebula.IsSharedFilesystemVolumeID(req.VolumeId) {
		if s.sharedFilesystemProvider == nil {
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}

		nodeID, err := s.volumeProvider.NodeExists(ctx, req.NodeId)
		if err != nil || nodeID == -1 {
			klog.V(0).ErrorS(err, "Node does not exist", "method", "ControllerPublishVolume", "nodeID", req.NodeId)
			return nil, status.Error(codes.NotFound, "node not found")
		}

		publishContext, err := s.sharedFilesystemProvider.PublishSharedVolume(ctx, req.VolumeId, req.GetReadonly() || immutableVolume)
		if err != nil {
			switch {
			case opennebula.IsDatastoreConfigError(err):
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			case opennebula.IsDatastoreCapacityError(err):
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}
			klog.V(0).ErrorS(err, "Failed to publish shared filesystem volume",
				"method", "ControllerPublishVolume", "volumeID", req.VolumeId, "nodeID", req.NodeId)
			return nil, status.Error(codes.Internal, "failed to publish volume")
		}

		_ = nodeID
		return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
	}

	volumeID, _, err := s.volumeProvider.VolumeExists(ctx, req.VolumeId)
	if err != nil || volumeID == -1 {
		klog.V(0).ErrorS(err, "Volume does not exist", "method", "ControllerPublishVolume", "volumeID", req.VolumeId)
		return nil, status.Error(codes.NotFound, "volume not found")
	}

	nodeID, err := s.volumeProvider.NodeExists(ctx, req.NodeId)
	if err != nil || nodeID == -1 {
		klog.V(0).ErrorS(err, "Node does not exist", "method", "ControllerPublishVolume", "nodeID", req.NodeId)
		return nil, status.Error(codes.NotFound, "node not found")
	}

	target, err := s.volumeProvider.GetVolumeInNode(ctx, volumeID, nodeID)
	if err == nil {
		klog.V(1).InfoS("Volume already attached to node",
			"method", "ControllerPublishVolume", "volumeID", volumeID, "nodeID", nodeID, "volumeName", target)
		return &csi.ControllerPublishVolumeResponse{
			PublishContext: map[string]string{
				"volumeName": target,
			},
		}, nil
	}

	// TODO: Validate VolumeCapability

	params := req.GetVolumeContext()
	klog.V(3).InfoS("Attaching volume to node",
		"method", "ControllerPublishVolume", "volumeID", volumeID, "nodeID", nodeID)
	err = s.volumeProvider.AttachVolume(ctx, req.VolumeId, req.NodeId, immutableVolume, params)
	if err != nil {
		switch {
		case opennebula.IsDatastoreConfigError(err):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case opennebula.IsDatastoreCapacityError(err):
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		klog.V(0).ErrorS(err, "Failed to attach volume",
			"method", "ControllerPublishVolume", "volumeID", req.VolumeId, "nodeID", req.NodeId)
		return nil, status.Error(codes.Internal, "failed to attach volume")
	}

	klog.V(3).InfoS("Checking if volume is attached",
		"method", "ControllerPublishVolume", "volumeID", volumeID, "nodeID", nodeID)
	target, err = s.volumeProvider.GetVolumeInNode(ctx, volumeID, nodeID)
	if err != nil {
		klog.V(0).ErrorS(err, "Failed to get volume in node",
			"method", "ControllerPublishVolume", "volumeID", volumeID, "nodeID", nodeID)
		return nil, status.Error(codes.Internal, "failed to get volume in node")
	}

	klog.V(1).InfoS("Volume attached successfully",
		"method", "ControllerPublishVolume", "volumeID", volumeID, "nodeID", nodeID, "volumeName", target)

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{
			"volumeName": target,
		},
	}, nil
}

func (s *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	klog.V(1).InfoS("ControllerUnpublishVolume called", "req", protosanitizer.StripSecrets(req))
	if req.VolumeId == "" {
		klog.V(0).ErrorS(nil, "method", "ControllerUnpublishVolume", "ControllerUnpublishVolume called with empty volume ID")
		return nil, status.Error(codes.InvalidArgument, "missing volume ID")
	}
	if req.NodeId == "" {
		klog.V(0).ErrorS(nil, "method", "ControllerUnpublishVolume", "ControllerUnpublishVolume called with empty node ID")
		return nil, status.Error(codes.InvalidArgument, "missing node ID")
	}

	if opennebula.IsSharedFilesystemVolumeID(req.VolumeId) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	volumeID, _, err := s.volumeProvider.VolumeExists(ctx, req.VolumeId)
	if err != nil || volumeID == -1 {
		klog.V(1).InfoS("Volume not found, skipping volume unpublish",
			"method", "ControllerUnpublishVolume", "volumeID", req.VolumeId)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	nodeID, err := s.volumeProvider.NodeExists(ctx, req.NodeId)
	if err != nil || nodeID == -1 {
		volumeReady, err := s.volumeProvider.VolumeReadyWithTimeout(volumeID)
		if err != nil {
			klog.V(0).ErrorS(err, "Failed to check if volume is used",
				"method", "ControllerUnpublishVolume", "volumeID", req.VolumeId)
			return nil, status.Error(codes.Internal, "failed to check if volume is used")
		}

		if !volumeReady {
			klog.V(0).ErrorS(err, "Node not found and volume is still in use",
				"method", "ControllerUnpublishVolume", "nodeID", req.NodeId)
			return nil, status.Error(codes.NotFound, "node not found and volume is still in use")
		}
		klog.V(1).InfoS("Node not found and volume not in use, skipping volume unpublish",
			"method", "ControllerUnpublishVolume", "nodeID", req.NodeId)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	_, err = s.volumeProvider.GetVolumeInNode(ctx, volumeID, nodeID)
	if err != nil {
		klog.V(1).InfoS("Volume does not exist in node, skipping unpublish",
			"method", "ControllerUnpublishVolume", "volumeID", req.VolumeId, "nodeID", req.NodeId)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	klog.V(3).InfoS("Detaching volume from node",
		"method", "ControllerUnpublishVolume", "volumeID", volumeID, "nodeID", nodeID)

	err = s.volumeProvider.DetachVolume(ctx, req.VolumeId, req.NodeId)
	if err != nil {
		klog.V(0).ErrorS(err, "Failed to detach volume",
			"method", "ControllerUnpublishVolume", "volumeID", req.VolumeId, "nodeID", req.NodeId)
		return nil, status.Error(codes.Internal, "failed to detach volume")
	}

	klog.V(1).InfoS("Volume detached successfully",
		"method", "ControllerUnpublishVolume", "volumeID", volumeID, "nodeID", nodeID)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (s *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	klog.V(1).InfoS("ValidateVolumeCapabilities called", "req", protosanitizer.StripSecrets(req))
	if req.VolumeId == "" {
		klog.V(0).ErrorS(nil, "method", "ValidateVolumeCapabilities", "ValidateVolumeCapabilities called with empty volume ID")
		return nil, status.Error(codes.InvalidArgument, "missing volume ID")
	}

	if len(req.VolumeCapabilities) == 0 {
		klog.V(0).ErrorS(nil, "method", "ValidateVolumeCapabilities", "ValidateVolumeCapabilities called with empty volume capabilities")
		return nil, status.Error(codes.InvalidArgument, "missing volume capabilities")
	}

	if opennebula.IsSharedFilesystemVolumeID(req.VolumeId) {
		if s.sharedFilesystemProvider == nil {
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}
		if _, err := s.sharedFilesystemProvider.ValidateSharedVolume(ctx, req.VolumeId); err != nil {
			klog.V(0).ErrorS(err, "Shared volume not found",
				"method", "ValidateVolumeCapabilities", "volumeID", req.VolumeId)
			return nil, status.Error(codes.NotFound, "volume not found")
		}
	} else {
		volumeID, _, err := s.volumeProvider.VolumeExists(ctx, req.VolumeId)
		if err != nil || volumeID == -1 {
			klog.V(0).ErrorS(err, "Volume not found",
				"method", "ValidateVolumeCapabilities", "volumeID", req.VolumeId)
			return nil, status.Error(codes.NotFound, "volume not found")
		}
	}

	if err := validateAccessMode(req.VolumeCapabilities, s.driver.getAllowedDatastoreTypes()); err != nil {
		klog.V(0).ErrorS(err, "Unsupported access mode",
			"method", "ValidateVolumeCapabilities", "volumeID", req.VolumeId, "volumeCapabilities", req.VolumeCapabilities)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	klog.V(1).InfoS("Volume capabilities validated successfully",
		"method", "ValidateVolumeCapabilities", "volumeID", req.VolumeId, "volumeCapabilities", req.VolumeCapabilities)

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}, nil
}

func (s *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(1).InfoS("ListVolumes called", "req", protosanitizer.StripSecrets(req))

	maxEntries := req.GetMaxEntries()
	if maxEntries < 0 {
		klog.V(0).ErrorS(nil, "Invalid max entries",
			"method", "ListVolumes", "maxEntries", maxEntries)
		return nil, status.Error(codes.Aborted, "invalid max_entries")
	}

	startingToken := req.GetStartingToken()
	if startingToken != "" {
		startIndex, err := strconv.Atoi(startingToken)
		if err != nil || startIndex < 0 {
			klog.V(0).ErrorS(err, "Invalid starting token",
				"method", "ListVolumes", "startingToken", startingToken)
			return nil, status.Error(codes.Aborted, "invalid starting_token")
		}
	}

	volumes, err := s.volumeProvider.ListVolumes(ctx, DefaultDriverName, maxEntries, startingToken)
	if err != nil {
		klog.V(0).ErrorS(err, "Failed to list volumes",
			"method", "ListVolumes", "error", err.Error())
		return nil, status.Error(codes.Internal, "failed to list volumes")
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, len(volumes))

	for _, volumeId := range volumes {
		volume := &csi.Volume{
			VolumeId: volumeId,
		}

		entry := &csi.ListVolumesResponse_Entry{
			Volume: volume,
		}

		entries = append(entries, entry)
	}

	klog.V(1).InfoS("Volumes listed successfully",
		"method", "ListVolumes", "volumeCount", len(entries), "startingToken", req.GetStartingToken())

	return &csi.ListVolumesResponse{
		Entries: entries,
	}, nil
}

func (s *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(1).InfoS("GetCapacity called", "req", protosanitizer.StripSecrets(req))

	selection, err := s.driver.GetDatastoreSelectionConfig(req.GetParameters())
	if err != nil {
		klog.V(0).ErrorS(err, "Invalid datastore configuration", "method", "GetCapacity")
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	availableCapacity, err := s.volumeProvider.GetCapacity(ctx, selection)
	if err != nil {
		switch {
		case opennebula.IsDatastoreConfigError(err):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case opennebula.IsDatastoreCapacityError(err):
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		klog.V(0).ErrorS(err, "Failed to get available capacity", "method", "GetCapacity")
		return nil, status.Error(codes.Internal, "failed to get capacity")
	}

	klog.V(1).InfoS("Available capacity retrieved successfully",
		"method", "GetCapacity", "availableCapacity", availableCapacity)
	return &csi.GetCapacityResponse{
		AvailableCapacity: availableCapacity,
	}, nil
}

func (s *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(1).InfoS("ControllerExpandVolume called", "req", protosanitizer.StripSecrets(req))

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume ID")
	}

	capacityRange := req.GetCapacityRange()
	if capacityRange == nil || capacityRange.GetRequiredBytes() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "missing required capacity range")
	}

	if opennebula.IsSharedFilesystemVolumeID(req.GetVolumeId()) {
		return nil, status.Error(codes.Unimplemented, "CephFS shared filesystem expansion is not supported in v0.4.0")
	}

	newSize, err := s.volumeProvider.ExpandVolume(ctx, req.GetVolumeId(), capacityRange.GetRequiredBytes())
	if err != nil {
		switch {
		case opennebula.IsDatastoreConfigError(err):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case opennebula.IsDatastoreCapacityError(err):
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}

		klog.V(0).ErrorS(err, "Failed to expand volume", "method", "ControllerExpandVolume", "volumeID", req.GetVolumeId())
		return nil, status.Error(codes.Internal, "failed to expand volume")
	}

	nodeExpansionRequired := true
	if capability := req.GetVolumeCapability(); capability != nil {
		if _, ok := capability.GetAccessType().(*csi.VolumeCapability_Block); ok {
			nodeExpansionRequired = false
		}
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         newSize,
		NodeExpansionRequired: nodeExpansionRequired,
	}, nil
}

// TODO: Implement methods specified in https://github.com/container-storage-interface/spec/blob/98819c45a37a67e0cd466bd02b813faf91af4e45/spec.md#controller-service-rpc
func (s *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(1).InfoS("ControllerGetCapabilities called", "req", protosanitizer.StripSecrets(req))

	capabilities := make([]*csi.ControllerServiceCapability, 0, len(controllerCapabilityTypes))
	for _, cap := range controllerCapabilityTypes {
		capabilities = append(capabilities, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: cap},
			},
		})
	}

	klog.V(1).InfoS("Controller capabilities retrieved successfully",
		"method", "ControllerGetCapabilities", "capabilitiesCount", len(capabilities))

	return &csi.ControllerGetCapabilitiesResponse{Capabilities: capabilities}, nil
}

func (s *ControllerServer) testConnectivity() {
	endpoint, _ := s.driver.PluginConfig.GetString(config.OpenNebulaRPCEndpointVar)
	credentials, ok := s.driver.PluginConfig.GetString(config.OpenNebulaCredentialsVar)
	if !ok {
		klog.V(0).ErrorS(nil, "Missing OpenNebula credentials", "method", "testConnectivity")
	}
	oneConfig := opennebula.OpenNebulaConfig{
		Endpoint:    endpoint,
		Credentials: credentials,
	}
	client := opennebula.NewClient(oneConfig)
	if err := client.Probe(context.TODO()); err != nil {
		klog.V(0).ErrorS(err, "Failed to connect to OpenNebula",
			"method", "testConnectivity", "endpoint", endpoint)
	}
	klog.V(3).InfoS("Successfully connected to OpenNebula",
		"method", "testConnectivity", "endpoint", endpoint)
}

func unsupportedRWXError(mode csi.VolumeCapability_AccessMode_Mode, profiles []opennebula.BackendCapabilityProfile) error {
	backends := unsupportedFilesystemRWXBackends(profiles)
	return fmt.Errorf(
		"access mode %q is not supported: this driver provisions VM-attached block disks and does not provide a shared-filesystem publish path; true ReadWriteMany requires a shared-filesystem backend such as NFS or CephFS. Current backends without filesystem RWX support: %s",
		mode.String(),
		strings.Join(backends, ", "),
	)
}

func backendCapabilityProfiles(backends []string) []opennebula.BackendCapabilityProfile {
	if len(backends) == 0 {
		backends = []string{"local", "ceph", "cephfs"}
	}

	profiles := make([]opennebula.BackendCapabilityProfile, 0, len(backends))
	seen := make(map[string]struct{}, len(backends))
	for _, backend := range backends {
		profile := opennebula.GetBackendCapabilityProfile(backend)
		if _, ok := seen[profile.Backend]; ok {
			continue
		}
		seen[profile.Backend] = struct{}{}
		profiles = append(profiles, profile)
	}

	return profiles
}

func unsupportedFilesystemRWXBackends(profiles []opennebula.BackendCapabilityProfile) []string {
	backends := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		if !profile.SupportsFilesystemRWX {
			backends = append(backends, profile.Backend)
		}
	}
	if len(backends) == 0 {
		return []string{"configured backends"}
	}

	return backends
}

func supportedAccessModeNames() []string {
	names := make([]string, 0, len(supportedAccessModes))
	for _, mode := range supportedAccessModes {
		names = append(names, mode.String())
	}

	return names
}
