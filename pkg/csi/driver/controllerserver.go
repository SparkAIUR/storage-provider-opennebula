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
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

var (
	controllerCapabilityTypes = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
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
	started := time.Now()
	backend := "unknown"
	klog.V(1).InfoS("CreateVolume called", "req", protosanitizer.StripSecrets(req))

	name := req.GetName()
	if name == "" {
		s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
		klog.V(0).ErrorS(nil, "method", "CreateVolume", "CreateVolume called with empty volume name")
		return nil, status.Error(codes.InvalidArgument, "missing volume name")
	}

	volumeCapabilities := req.GetVolumeCapabilities()
	if len(volumeCapabilities) == 0 {
		s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
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
		s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
		s.recordPVCWarningFromParams(ctx, rawParams, eventReasonDatastoreRejected, err.Error())
		klog.V(0).ErrorS(err, "Invalid datastore configuration", "method", "CreateVolume", "params", rawParams)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := validateAccessMode(volumeCapabilities, selection.AllowedTypes); err != nil {
		s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	accessMode := volumeCapabilities[0].GetAccessMode()
	if accessMode == nil {
		s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
		klog.V(0).ErrorS(nil, "method", "CreateVolume", "CreateVolume called with empty access mode")
		return nil, status.Error(codes.InvalidArgument, "missing access mode")
	}

	volumeAccessModel, err := resolveVolumeAccessModel(volumeCapabilities[0], selection.AllowedTypes)
	if err != nil {
		s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
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
	contentSource := req.GetVolumeContentSource()

	response.Volume.VolumeContext = params
	if contentSource != nil {
		response.Volume.ContentSource = contentSource
	}

	if volumeAccessModel == opennebula.VolumeAccessModelSharedFS {
		if s.sharedFilesystemProvider == nil {
			s.driver.metrics.RecordOperation("create_volume", "cephfs", "failed_precondition", time.Since(started))
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}
		backend = "cephfs"

		if contentSource != nil {
			var (
				result   *opennebula.SharedVolumeCreateResult
				cloneErr error
			)
			switch source := contentSource.Type.(type) {
			case *csi.VolumeContentSource_Volume:
				if !s.driver.featureGates.CephFSClones {
					s.driver.metrics.RecordOperation("create_volume", backend, "unimplemented", time.Since(started))
					return nil, status.Error(codes.Unimplemented, "CephFS volume cloning is disabled by feature gate")
				}
				result, cloneErr = s.sharedFilesystemProvider.CloneSharedVolume(ctx, opennebula.SharedVolumeCloneRequest{
					Name:           name,
					SizeBytes:      requiredBytes,
					Selection:      selection,
					Parameters:     rawParams,
					Secrets:        req.GetSecrets(),
					SourceVolumeID: source.Volume.GetVolumeId(),
				})
			case *csi.VolumeContentSource_Snapshot:
				if !opennebula.IsSharedFilesystemSnapshotID(source.Snapshot.GetSnapshotId()) {
					s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
					return nil, status.Error(codes.InvalidArgument, "shared filesystem restore requires a CephFS snapshot source")
				}
				if !s.driver.featureGates.CephFSSnapshots || !s.driver.featureGates.CephFSClones {
					s.driver.metrics.RecordOperation("create_volume", backend, "unimplemented", time.Since(started))
					return nil, status.Error(codes.Unimplemented, "CephFS snapshot restore is disabled by feature gate")
				}
				result, cloneErr = s.sharedFilesystemProvider.CloneSharedVolume(ctx, opennebula.SharedVolumeCloneRequest{
					Name:             name,
					SizeBytes:        requiredBytes,
					Selection:        selection,
					Parameters:       rawParams,
					Secrets:          req.GetSecrets(),
					SourceSnapshotID: source.Snapshot.GetSnapshotId(),
				})
			default:
				s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
				return nil, status.Error(codes.InvalidArgument, "unsupported shared filesystem content source")
			}

			if cloneErr != nil {
				switch {
				case opennebula.IsDatastoreConfigError(cloneErr):
					s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
					return nil, status.Error(codes.InvalidArgument, cloneErr.Error())
				case opennebula.IsDatastoreCapacityError(cloneErr):
					s.driver.metrics.RecordOperation("create_volume", backend, "resource_exhausted", time.Since(started))
					return nil, status.Error(codes.ResourceExhausted, cloneErr.Error())
				}
				s.driver.metrics.RecordOperation("create_volume", backend, "internal", time.Since(started))
				return nil, status.Error(codes.Internal, "failed to clone shared filesystem volume")
			}

			response.Volume.VolumeId = result.VolumeID
			response.Volume.CapacityBytes = result.CapacityBytes
			if s.driver.featureGates.TopologyAccessibility {
				response.Volume.AccessibleTopology = accessibleTopologyForDatastore(result.Datastore)
			}
			s.driver.metrics.RecordOperation("create_volume", backend, "success", time.Since(started))
			s.driver.metrics.RecordDatastoreSelection(string(selection.Policy), result.Datastore.Backend, result.Datastore.ID, "selected")
			s.driver.metrics.SetDatastoreCapacity(result.Datastore.Backend, result.Datastore.ID, result.Datastore.FreeBytes, result.Datastore.TotalBytes)
			s.recordPVCEventFromParams(ctx, rawParams, eventReasonCloneCreated, fmt.Sprintf("created CephFS clone in datastore %d from content source", result.Datastore.ID))
			if result.FallbackUsed {
				s.recordPVCEventFromParams(ctx, rawParams, eventReasonDatastoreFallback, fmt.Sprintf("fallback selected datastore %d after attempts %v", result.Datastore.ID, result.AttemptedDatastoreIDs))
			}
			s.recordPVCEventFromParams(ctx, rawParams, eventReasonDatastoreSelected, fmt.Sprintf("selected %s datastore %d (%s) with policy %s", result.Datastore.Backend, result.Datastore.ID, result.Datastore.Name, selection.Policy))
			s.annotatePlacementFromParams(ctx, rawParams, PlacementReport{
				Backend:                     result.Metadata.Backend,
				DatastoreID:                 result.Datastore.ID,
				DatastoreName:               result.Datastore.Name,
				SelectionPolicy:             string(selection.Policy),
				FallbackUsed:                result.FallbackUsed,
				CompatibilityAwareSelection: s.driver.featureGates.CompatibilityAwareSelection,
			})
			return response, nil
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
				s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
				s.recordPVCWarningFromParams(ctx, rawParams, eventReasonDatastoreRejected, sharedErr.Error())
				return nil, status.Error(codes.InvalidArgument, sharedErr.Error())
			case opennebula.IsDatastoreCapacityError(sharedErr):
				s.driver.metrics.RecordOperation("create_volume", backend, "resource_exhausted", time.Since(started))
				return nil, status.Error(codes.ResourceExhausted, sharedErr.Error())
			}
			s.driver.metrics.RecordOperation("create_volume", backend, "internal", time.Since(started))
			klog.V(0).ErrorS(sharedErr, "Failed to create shared filesystem volume",
				"method", "CreateVolume", "volumeName", name, "requiredBytes", requiredBytes,
				"policy", selection.Policy, "datastores", selection.Identifiers)
			return nil, status.Error(codes.Internal, "failed to create volume")
		}

		response.Volume.VolumeId = result.VolumeID
		response.Volume.CapacityBytes = result.CapacityBytes
		if s.driver.featureGates.TopologyAccessibility {
			response.Volume.AccessibleTopology = accessibleTopologyForDatastore(result.Datastore)
		}
		s.driver.metrics.RecordOperation("create_volume", backend, "success", time.Since(started))
		s.driver.metrics.RecordDatastoreSelection(string(selection.Policy), result.Datastore.Backend, result.Datastore.ID, "selected")
		s.driver.metrics.SetDatastoreCapacity(result.Datastore.Backend, result.Datastore.ID, result.Datastore.FreeBytes, result.Datastore.TotalBytes)
		if result.FallbackUsed {
			s.recordPVCEventFromParams(ctx, rawParams, eventReasonDatastoreFallback, fmt.Sprintf("fallback selected datastore %d after attempts %v", result.Datastore.ID, result.AttemptedDatastoreIDs))
		}
		s.recordPVCEventFromParams(ctx, rawParams, eventReasonDatastoreSelected, fmt.Sprintf("selected %s datastore %d (%s) with policy %s", result.Datastore.Backend, result.Datastore.ID, result.Datastore.Name, selection.Policy))
		s.annotatePlacementFromParams(ctx, rawParams, PlacementReport{
			Backend:                     result.Metadata.Backend,
			DatastoreID:                 result.Datastore.ID,
			DatastoreName:               result.Datastore.Name,
			SelectionPolicy:             string(selection.Policy),
			FallbackUsed:                result.FallbackUsed,
			CompatibilityAwareSelection: s.driver.featureGates.CompatibilityAwareSelection,
		})
		klog.V(1).InfoS("Shared filesystem volume created successfully",
			"method", "CreateVolume", "volumeName", name, "requiredBytes", requiredBytes,
			"policy", selection.Policy, "datastores", selection.Identifiers,
			"selectedDatastoreID", result.Datastore.ID, "selectedDatastoreName", result.Datastore.Name,
			"backend", result.Metadata.Backend, "mode", result.Metadata.Mode)

		return response, nil
	}
	backend = "disk"

	if contentSource != nil {
		switch source := contentSource.Type.(type) {
		case *csi.VolumeContentSource_Volume:
			result, err := s.volumeProvider.CloneVolume(ctx, name, source.Volume.GetVolumeId(), selection)
			if err != nil {
				switch {
				case opennebula.IsDatastoreConfigError(err):
					s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
					return nil, status.Error(codes.InvalidArgument, err.Error())
				case opennebula.IsDatastoreCapacityError(err):
					s.driver.metrics.RecordOperation("create_volume", backend, "resource_exhausted", time.Since(started))
					return nil, status.Error(codes.ResourceExhausted, err.Error())
				}
				s.driver.metrics.RecordOperation("create_volume", backend, "internal", time.Since(started))
				return nil, status.Error(codes.Internal, "failed to clone volume")
			}

			if result.CapacityBytes > 0 {
				response.Volume.CapacityBytes = result.CapacityBytes
			}
			if s.driver.featureGates.TopologyAccessibility {
				response.Volume.AccessibleTopology = accessibleTopologyForDatastore(result.Datastore)
			}
			s.driver.metrics.RecordOperation("create_volume", result.Datastore.Backend, "success", time.Since(started))
			s.recordPVCEventFromParams(ctx, rawParams, eventReasonCloneCreated, fmt.Sprintf("cloned source volume %s into datastore %d", source.Volume.GetVolumeId(), result.Datastore.ID))
			if result.FallbackUsed {
				s.recordPVCEventFromParams(ctx, rawParams, eventReasonDatastoreFallback, fmt.Sprintf("fallback selected datastore %d after attempts %v", result.Datastore.ID, result.AttemptedDatastoreIDs))
			}
			s.recordPVCEventFromParams(ctx, rawParams, eventReasonDatastoreSelected, fmt.Sprintf("selected %s datastore %d (%s) with policy %s", result.Datastore.Backend, result.Datastore.ID, result.Datastore.Name, selection.Policy))
			s.driver.metrics.RecordDatastoreSelection(string(selection.Policy), result.Datastore.Backend, result.Datastore.ID, "selected")
			s.driver.metrics.SetDatastoreCapacity(result.Datastore.Backend, result.Datastore.ID, result.Datastore.FreeBytes, result.Datastore.TotalBytes)
			s.annotatePlacementFromParams(ctx, rawParams, PlacementReport{
				Backend:                     result.Datastore.Backend,
				DatastoreID:                 result.Datastore.ID,
				DatastoreName:               result.Datastore.Name,
				SelectionPolicy:             string(selection.Policy),
				FallbackUsed:                result.FallbackUsed,
				CompatibilityAwareSelection: s.driver.featureGates.CompatibilityAwareSelection,
			})
			return response, nil
		case *csi.VolumeContentSource_Snapshot:
			s.driver.metrics.RecordOperation("create_volume", backend, "unimplemented", time.Since(started))
			return nil, status.Error(codes.Unimplemented, "restoring a volume from an OpenNebula image snapshot is not supported by the current backend")
		default:
			s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
			return nil, status.Error(codes.InvalidArgument, "unsupported volume content source")
		}
	}

	volumeID, volumeSize, _ := s.volumeProvider.VolumeExists(ctx, name)
	if volumeID != -1 {
		if int64(volumeSize) == requiredBytes {
			s.driver.metrics.RecordOperation("create_volume", backend, "success", time.Since(started))
			klog.V(3).InfoS("Volume already exists with the same size",
				"method", "CreateVolume", "volumeID", volumeID, "requiredSize", requiredBytes)
			return response, nil
		}
		s.driver.metrics.RecordOperation("create_volume", backend, "already_exists", time.Since(started))
		klog.V(0).ErrorS(nil, "Volume with the same name already exists with different size",
			"method", "CreateVolume", "volumeID", volumeID, "existingSize", volumeSize, "requiredSize", requiredBytes)
		return nil, status.Error(codes.AlreadyExists,
			"volume with the same name already exists with different size")

	}

	result, err := s.volumeProvider.CreateVolume(ctx, name, requiredBytes, DefaultDriverName, immutableVolume, fsType, params, selection)
	if err != nil {
		switch {
		case opennebula.IsDatastoreConfigError(err):
			s.driver.metrics.RecordOperation("create_volume", backend, "invalid_argument", time.Since(started))
			s.recordPVCWarningFromParams(ctx, rawParams, eventReasonDatastoreRejected, err.Error())
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case opennebula.IsDatastoreCapacityError(err):
			s.driver.metrics.RecordOperation("create_volume", backend, "resource_exhausted", time.Since(started))
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		s.driver.metrics.RecordOperation("create_volume", backend, "internal", time.Since(started))
		klog.V(0).ErrorS(err, "Failed to create volume",
			"method", "CreateVolume", "volumeName", name, "requiredBytes", requiredBytes,
			"defaultDriverName", DefaultDriverName, "immutableVolume", immutableVolume,
			"policy", selection.Policy, "datastores", selection.Identifiers, "params", params)
		return nil, status.Error(codes.Internal, "failed to create volume")
	}
	s.driver.metrics.RecordOperation("create_volume", result.Datastore.Backend, "success", time.Since(started))
	if result.CapacityBytes > 0 {
		response.Volume.CapacityBytes = result.CapacityBytes
	}
	if s.driver.featureGates.TopologyAccessibility {
		response.Volume.AccessibleTopology = accessibleTopologyForDatastore(result.Datastore)
	}
	s.driver.metrics.RecordDatastoreSelection(string(selection.Policy), result.Datastore.Backend, result.Datastore.ID, "selected")
	s.driver.metrics.SetDatastoreCapacity(result.Datastore.Backend, result.Datastore.ID, result.Datastore.FreeBytes, result.Datastore.TotalBytes)
	if result.FallbackUsed {
		s.recordPVCEventFromParams(ctx, rawParams, eventReasonDatastoreFallback, fmt.Sprintf("fallback selected datastore %d after attempts %v", result.Datastore.ID, result.AttemptedDatastoreIDs))
	}
	s.recordPVCEventFromParams(ctx, rawParams, eventReasonDatastoreSelected, fmt.Sprintf("selected %s datastore %d (%s) with policy %s", result.Datastore.Backend, result.Datastore.ID, result.Datastore.Name, selection.Policy))
	s.annotatePlacementFromParams(ctx, rawParams, PlacementReport{
		Backend:                     result.Datastore.Backend,
		DatastoreID:                 result.Datastore.ID,
		DatastoreName:               result.Datastore.Name,
		SelectionPolicy:             string(selection.Policy),
		FallbackUsed:                result.FallbackUsed,
		CompatibilityAwareSelection: s.driver.featureGates.CompatibilityAwareSelection,
	})

	klog.V(1).InfoS("Volume created successfully",
		"method", "CreateVolume", "volumeName", name, "requiredBytes",
		requiredBytes, "defaultDriverName", DefaultDriverName, "immutableVolume", immutableVolume,
		"policy", selection.Policy, "datastores", selection.Identifiers,
		"selectedDatastoreID", result.Datastore.ID, "selectedDatastoreName", result.Datastore.Name,
		"params", params)

	return response, nil
}

func (s *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	started := time.Now()
	klog.V(1).InfoS("DeleteVolume called", "req", protosanitizer.StripSecrets(req))

	if req.VolumeId == "" {
		s.driver.metrics.RecordOperation("delete_volume", "unknown", "invalid_argument", time.Since(started))
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
				s.driver.metrics.RecordOperation("delete_volume", "cephfs", "failed_precondition", time.Since(started))
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			case opennebula.IsDatastoreCapacityError(err):
				s.driver.metrics.RecordOperation("delete_volume", "cephfs", "resource_exhausted", time.Since(started))
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}
			s.driver.metrics.RecordOperation("delete_volume", "cephfs", "failed_precondition", time.Since(started))
			klog.V(0).ErrorS(err, "Failed to delete shared filesystem volume", "method", "DeleteVolume", "volumeID", req.VolumeId)
			return nil, status.Error(codes.FailedPrecondition, "failed to delete volume")
		}

		s.driver.metrics.RecordOperation("delete_volume", "cephfs", "success", time.Since(started))
		klog.V(1).InfoS("Shared filesystem volume deleted successfully", "method", "DeleteVolume", "volumeID", req.VolumeId)
		return &csi.DeleteVolumeResponse{}, nil
	}

	err := s.volumeProvider.DeleteVolume(ctx, req.VolumeId)
	if err != nil {
		s.driver.metrics.RecordOperation("delete_volume", "disk", "failed_precondition", time.Since(started))
		klog.V(0).ErrorS(err, "Failed to delete volume", "method", "DeleteVolume", "volumeID", req.VolumeId)
		return nil, status.Error(codes.FailedPrecondition, "failed to delete volume")
	}

	s.driver.metrics.RecordOperation("delete_volume", "disk", "success", time.Since(started))
	klog.V(1).InfoS("Volume deleted successfully", "method", "DeleteVolume", "volumeID", req.VolumeId)
	return &csi.DeleteVolumeResponse{}, nil
}

func (s *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	started := time.Now()
	klog.V(1).InfoS("CreateSnapshot called", "req", protosanitizer.StripSecrets(req))

	if strings.TrimSpace(req.GetName()) == "" {
		s.driver.metrics.RecordSnapshot("disk", "create", "invalid_argument")
		return nil, status.Error(codes.InvalidArgument, "snapshot name is required")
	}
	if strings.TrimSpace(req.GetSourceVolumeId()) == "" {
		s.driver.metrics.RecordSnapshot("disk", "create", "invalid_argument")
		return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
	}
	if opennebula.IsSharedFilesystemVolumeID(req.GetSourceVolumeId()) {
		if s.sharedFilesystemProvider == nil {
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}
		if !s.driver.featureGates.CephFSSnapshots {
			s.driver.metrics.RecordSnapshot("cephfs", "create", "unimplemented")
			return nil, status.Error(codes.Unimplemented, "CephFS snapshots are disabled by feature gate")
		}

		snapshot, err := s.sharedFilesystemProvider.CreateSharedSnapshot(ctx, req.GetSourceVolumeId(), req.GetName(), req.GetSecrets())
		if err != nil {
			s.driver.metrics.RecordSnapshot("cephfs", "create", "internal")
			return nil, status.Error(codes.Internal, err.Error())
		}

		s.driver.metrics.RecordSnapshot("cephfs", "create", "success")
		s.driver.metrics.RecordOperation("create_snapshot", "cephfs", "success", time.Since(started))
		return &csi.CreateSnapshotResponse{
			Snapshot: &csi.Snapshot{
				SizeBytes:      snapshot.SizeBytes,
				SnapshotId:     snapshot.SnapshotID,
				SourceVolumeId: snapshot.SourceVolumeID,
				CreationTime:   timestamppb.New(snapshot.CreationTime),
				ReadyToUse:     snapshot.ReadyToUse,
			},
		}, nil
	}

	snapshot, err := s.volumeProvider.CreateSnapshot(ctx, req.GetSourceVolumeId(), req.GetName())
	if err != nil {
		s.driver.metrics.RecordSnapshot("disk", "create", "internal")
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.driver.metrics.RecordSnapshot("disk", "create", "success")
	s.driver.metrics.RecordOperation("create_snapshot", "disk", "success", time.Since(started))

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SizeBytes:      snapshot.SizeBytes,
			SnapshotId:     snapshot.SnapshotID,
			SourceVolumeId: snapshot.SourceVolumeID,
			CreationTime:   timestamppb.New(snapshot.CreationTime),
			ReadyToUse:     snapshot.ReadyToUse,
		},
	}, nil
}

func (s *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	started := time.Now()
	klog.V(1).InfoS("DeleteSnapshot called", "req", protosanitizer.StripSecrets(req))

	if strings.TrimSpace(req.GetSnapshotId()) == "" {
		s.driver.metrics.RecordSnapshot("disk", "delete", "invalid_argument")
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}
	if opennebula.IsSharedFilesystemSnapshotID(req.GetSnapshotId()) {
		if s.sharedFilesystemProvider == nil {
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}
		if !s.driver.featureGates.CephFSSnapshots {
			s.driver.metrics.RecordSnapshot("cephfs", "delete", "unimplemented")
			return nil, status.Error(codes.Unimplemented, "CephFS snapshots are disabled by feature gate")
		}
		if err := s.sharedFilesystemProvider.DeleteSharedSnapshot(ctx, req.GetSnapshotId(), req.GetSecrets()); err != nil {
			s.driver.metrics.RecordSnapshot("cephfs", "delete", "internal")
			return nil, status.Error(codes.Internal, err.Error())
		}
		s.driver.metrics.RecordSnapshot("cephfs", "delete", "success")
		s.driver.metrics.RecordOperation("delete_snapshot", "cephfs", "success", time.Since(started))
		return &csi.DeleteSnapshotResponse{}, nil
	}

	if err := s.volumeProvider.DeleteSnapshot(ctx, req.GetSnapshotId()); err != nil {
		s.driver.metrics.RecordSnapshot("disk", "delete", "internal")
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.driver.metrics.RecordSnapshot("disk", "delete", "success")
	s.driver.metrics.RecordOperation("delete_snapshot", "disk", "success", time.Since(started))
	return &csi.DeleteSnapshotResponse{}, nil
}

func (s *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(1).InfoS("ListSnapshots called", "req", protosanitizer.StripSecrets(req))

	if opennebula.IsSharedFilesystemSnapshotID(req.GetSnapshotId()) || opennebula.IsSharedFilesystemVolumeID(req.GetSourceVolumeId()) {
		if s.sharedFilesystemProvider == nil {
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}
		if !s.driver.featureGates.CephFSSnapshots {
			return nil, status.Error(codes.Unimplemented, "CephFS snapshots are disabled by feature gate")
		}

		snapshots, nextToken, err := s.sharedFilesystemProvider.ListSharedSnapshots(ctx, req.GetSnapshotId(), req.GetSourceVolumeId(), req.GetMaxEntries(), req.GetStartingToken(), nil)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(snapshots))
		for _, snapshot := range snapshots {
			entries = append(entries, &csi.ListSnapshotsResponse_Entry{
				Snapshot: &csi.Snapshot{
					SizeBytes:      snapshot.SizeBytes,
					SnapshotId:     snapshot.SnapshotID,
					SourceVolumeId: snapshot.SourceVolumeID,
					CreationTime:   timestamppb.New(snapshot.CreationTime),
					ReadyToUse:     snapshot.ReadyToUse,
				},
			})
		}

		return &csi.ListSnapshotsResponse{
			Entries:   entries,
			NextToken: nextToken,
		}, nil
	}

	snapshots, nextToken, err := s.volumeProvider.ListSnapshots(ctx, req.GetSnapshotId(), req.GetSourceVolumeId(), req.GetMaxEntries(), req.GetStartingToken())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(snapshots))
	for _, snapshot := range snapshots {
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SizeBytes:      snapshot.SizeBytes,
				SnapshotId:     snapshot.SnapshotID,
				SourceVolumeId: snapshot.SourceVolumeID,
				CreationTime:   timestamppb.New(snapshot.CreationTime),
				ReadyToUse:     snapshot.ReadyToUse,
			},
		})
	}

	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

// TODO: Process VolumeCapability, readonly
func (s *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	started := time.Now()
	klog.V(1).InfoS("ControllerPublishVolume called", "req", protosanitizer.StripSecrets(req))
	if req.VolumeId == "" {
		s.driver.metrics.RecordOperation("controller_publish_volume", "unknown", "invalid_argument", time.Since(started))
		klog.V(0).ErrorS(nil, "method", "ControllerPublishVolume", "ControllerPublishVolume called with empty volume ID")
		return nil, status.Error(codes.InvalidArgument, "missing volume ID")
	}

	if req.NodeId == "" {
		s.driver.metrics.RecordOperation("controller_publish_volume", "unknown", "invalid_argument", time.Since(started))
		klog.V(0).ErrorS(nil, "method", "ControllerPublishVolume", "ControllerPublishVolume called with empty node ID")
		return nil, status.Error(codes.InvalidArgument, "missing node ID")
	}

	if req.VolumeCapability == nil {
		s.driver.metrics.RecordOperation("controller_publish_volume", "unknown", "invalid_argument", time.Since(started))
		klog.V(0).ErrorS(nil, "method", "ControllerPublishVolume", "ControllerPublishVolume called with empty volume capability")
		return nil, status.Error(codes.InvalidArgument, "missing volume capability")
	}

	if req.VolumeCapability.AccessMode == nil {
		s.driver.metrics.RecordOperation("controller_publish_volume", "unknown", "invalid_argument", time.Since(started))
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
			s.driver.metrics.RecordOperation("controller_publish_volume", "cephfs", "not_found", time.Since(started))
			klog.V(0).ErrorS(err, "Node does not exist", "method", "ControllerPublishVolume", "nodeID", req.NodeId)
			return nil, status.Error(codes.NotFound, "node not found")
		}

		publishContext, err := s.sharedFilesystemProvider.PublishSharedVolume(ctx, req.VolumeId, req.GetReadonly() || immutableVolume)
		if err != nil {
			switch {
			case opennebula.IsDatastoreConfigError(err):
				s.driver.metrics.RecordOperation("controller_publish_volume", "cephfs", "failed_precondition", time.Since(started))
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			case opennebula.IsDatastoreCapacityError(err):
				s.driver.metrics.RecordOperation("controller_publish_volume", "cephfs", "resource_exhausted", time.Since(started))
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}
			s.driver.metrics.RecordOperation("controller_publish_volume", "cephfs", "internal", time.Since(started))
			klog.V(0).ErrorS(err, "Failed to publish shared filesystem volume",
				"method", "ControllerPublishVolume", "volumeID", req.VolumeId, "nodeID", req.NodeId)
			return nil, status.Error(codes.Internal, "failed to publish volume")
		}

		s.driver.metrics.RecordOperation("controller_publish_volume", "cephfs", "success", time.Since(started))
		_ = nodeID
		return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
	}

	volumeID, _, err := s.volumeProvider.VolumeExists(ctx, req.VolumeId)
	if err != nil || volumeID == -1 {
		s.driver.metrics.RecordOperation("controller_publish_volume", "disk", "not_found", time.Since(started))
		klog.V(0).ErrorS(err, "Volume does not exist", "method", "ControllerPublishVolume", "volumeID", req.VolumeId)
		return nil, status.Error(codes.NotFound, "volume not found")
	}

	nodeID, err := s.volumeProvider.NodeExists(ctx, req.NodeId)
	if err != nil || nodeID == -1 {
		s.driver.metrics.RecordOperation("controller_publish_volume", "disk", "not_found", time.Since(started))
		klog.V(0).ErrorS(err, "Node does not exist", "method", "ControllerPublishVolume", "nodeID", req.NodeId)
		return nil, status.Error(codes.NotFound, "node not found")
	}

	target, err := s.volumeProvider.GetVolumeInNode(ctx, volumeID, nodeID)
	if err == nil {
		s.driver.metrics.RecordOperation("controller_publish_volume", "disk", "success", time.Since(started))
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
			s.driver.metrics.RecordOperation("controller_publish_volume", "disk", "failed_precondition", time.Since(started))
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case opennebula.IsDatastoreCapacityError(err):
			s.driver.metrics.RecordOperation("controller_publish_volume", "disk", "resource_exhausted", time.Since(started))
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		s.driver.metrics.RecordOperation("controller_publish_volume", "disk", "internal", time.Since(started))
		klog.V(0).ErrorS(err, "Failed to attach volume",
			"method", "ControllerPublishVolume", "volumeID", req.VolumeId, "nodeID", req.NodeId)
		return nil, status.Error(codes.Internal, "failed to attach volume")
	}

	klog.V(3).InfoS("Checking if volume is attached",
		"method", "ControllerPublishVolume", "volumeID", volumeID, "nodeID", nodeID)
	target, err = s.volumeProvider.GetVolumeInNode(ctx, volumeID, nodeID)
	if err != nil {
		s.driver.metrics.RecordOperation("controller_publish_volume", "disk", "internal", time.Since(started))
		klog.V(0).ErrorS(err, "Failed to get volume in node",
			"method", "ControllerPublishVolume", "volumeID", volumeID, "nodeID", nodeID)
		return nil, status.Error(codes.Internal, "failed to get volume in node")
	}

	s.driver.metrics.RecordOperation("controller_publish_volume", "disk", "success", time.Since(started))
	s.driver.metrics.RecordAttachValidation("disk", "attach", "success")
	s.recordPVCEventFromParams(ctx, req.GetVolumeContext(), eventReasonAttachValidated, fmt.Sprintf("validated and attached volume %s to node %s", req.GetVolumeId(), req.GetNodeId()))
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
	started := time.Now()
	klog.V(1).InfoS("GetCapacity called", "req", protosanitizer.StripSecrets(req))

	selection, err := s.driver.GetDatastoreSelectionConfig(req.GetParameters())
	if err != nil {
		s.driver.metrics.RecordOperation("get_capacity", "unknown", "invalid_argument", time.Since(started))
		klog.V(0).ErrorS(err, "Invalid datastore configuration", "method", "GetCapacity")
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	availableCapacity, err := s.volumeProvider.GetCapacity(ctx, selection)
	if err != nil {
		switch {
		case opennebula.IsDatastoreConfigError(err):
			s.driver.metrics.RecordOperation("get_capacity", "disk", "invalid_argument", time.Since(started))
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case opennebula.IsDatastoreCapacityError(err):
			s.driver.metrics.RecordOperation("get_capacity", "disk", "resource_exhausted", time.Since(started))
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		s.driver.metrics.RecordOperation("get_capacity", "disk", "internal", time.Since(started))
		klog.V(0).ErrorS(err, "Failed to get available capacity", "method", "GetCapacity")
		return nil, status.Error(codes.Internal, "failed to get capacity")
	}

	s.driver.metrics.RecordOperation("get_capacity", "disk", "success", time.Since(started))
	klog.V(1).InfoS("Available capacity retrieved successfully",
		"method", "GetCapacity", "availableCapacity", availableCapacity)
	return &csi.GetCapacityResponse{
		AvailableCapacity: availableCapacity,
	}, nil
}

func (s *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	started := time.Now()
	klog.V(1).InfoS("ControllerExpandVolume called", "req", protosanitizer.StripSecrets(req))

	if req.GetVolumeId() == "" {
		s.driver.metrics.RecordOperation("controller_expand_volume", "unknown", "invalid_argument", time.Since(started))
		return nil, status.Error(codes.InvalidArgument, "missing volume ID")
	}

	capacityRange := req.GetCapacityRange()
	if capacityRange == nil || capacityRange.GetRequiredBytes() <= 0 {
		s.driver.metrics.RecordOperation("controller_expand_volume", "unknown", "invalid_argument", time.Since(started))
		return nil, status.Error(codes.InvalidArgument, "missing required capacity range")
	}

	if opennebula.IsSharedFilesystemVolumeID(req.GetVolumeId()) {
		if s.sharedFilesystemProvider == nil {
			return nil, status.Error(codes.FailedPrecondition, "shared filesystem provider is not configured")
		}
		if !s.driver.featureGates.CephFSExpansion {
			s.driver.metrics.RecordOperation("controller_expand_volume", "cephfs", "unimplemented", time.Since(started))
			return nil, status.Error(codes.Unimplemented, "CephFS shared filesystem expansion is disabled by feature gate")
		}

		newSize, err := s.sharedFilesystemProvider.ExpandSharedVolume(ctx, req.GetVolumeId(), capacityRange.GetRequiredBytes(), req.GetSecrets())
		if err != nil {
			switch {
			case opennebula.IsDatastoreConfigError(err):
				s.driver.metrics.RecordOperation("controller_expand_volume", "cephfs", "failed_precondition", time.Since(started))
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			case opennebula.IsDatastoreCapacityError(err):
				s.driver.metrics.RecordOperation("controller_expand_volume", "cephfs", "resource_exhausted", time.Since(started))
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}

			s.driver.metrics.RecordOperation("controller_expand_volume", "cephfs", "internal", time.Since(started))
			return nil, status.Error(codes.Internal, "failed to expand CephFS volume")
		}

		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         newSize,
			NodeExpansionRequired: false,
		}, nil
	}

	newSize, err := s.volumeProvider.ExpandVolume(ctx, req.GetVolumeId(), capacityRange.GetRequiredBytes(), s.driver.featureGates.DetachedDiskExpansion)
	if err != nil {
		switch {
		case opennebula.IsDatastoreConfigError(err):
			s.driver.metrics.RecordOperation("controller_expand_volume", "disk", "failed_precondition", time.Since(started))
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case opennebula.IsDatastoreCapacityError(err):
			s.driver.metrics.RecordOperation("controller_expand_volume", "disk", "resource_exhausted", time.Since(started))
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}

		s.driver.metrics.RecordOperation("controller_expand_volume", "disk", "internal", time.Since(started))
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

func (s *ControllerServer) recordPVCEventFromParams(ctx context.Context, params map[string]string, reason, message string) {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil {
		return
	}
	s.driver.kubeRuntime.EmitPVCEvent(ctx, params[paramPVCNamespace], params[paramPVCName], reason, message)
}

func (s *ControllerServer) recordPVCWarningFromParams(ctx context.Context, params map[string]string, reason, message string) {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil {
		return
	}
	s.driver.kubeRuntime.EmitWarningEventOnPVC(ctx, params[paramPVCNamespace], params[paramPVCName], reason, message)
}

func (s *ControllerServer) annotatePlacementFromParams(ctx context.Context, params map[string]string, report PlacementReport) {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil {
		return
	}
	s.driver.kubeRuntime.AnnotatePVAsync(ctx, params[paramPVName], report)
}
