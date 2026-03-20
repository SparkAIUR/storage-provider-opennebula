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
	"errors"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	volumeSize    = 10 * 1024 * 1024
	datastoreSize = 100 * 1024 * 1024 * 1024
)

func getTestControllerServer(mockProvider *MockOpenNebulaVolumeProviderTestify) *ControllerServer {
	return getTestControllerServerWithAllowedTypes(mockProvider, &MockSharedFilesystemProviderTestify{}, "local")
}

func getTestControllerServerWithAllowedTypes(mockProvider *MockOpenNebulaVolumeProviderTestify, sharedProvider *MockSharedFilesystemProviderTestify, allowedTypes string) *ControllerServer {
	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.DefaultDatastoresVar, "100,101")
	pluginConfig.OverrideVal(config.AllowedDatastoreTypesVar, allowedTypes)

	driver := &Driver{
		name:               DefaultDriverName,
		version:            driverVersion,
		grpcServerEndpoint: DefaultGRPCServerEndpoint,
		nodeID:             "test-controller-id",
		PluginConfig:       pluginConfig,
	}

	return NewControllerServer(driver, mockProvider, sharedProvider)
}

func TestCreateVolume(t *testing.T) {
	const volumeSize = int64(1024 * 1024 * 1024) // 1GiB

	tcs := []struct {
		name                string
		createVolumeRequest *csi.CreateVolumeRequest
		expectResponse      *csi.CreateVolumeResponse
		expectError         bool
		setupMock           func(m *MockOpenNebulaVolumeProviderTestify)
	}{
		{
			name: "TestBasicVolumeCreation",
			createVolumeRequest: &csi.CreateVolumeRequest{
				Name: "test-volume",
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: volumeSize,
				},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{
								FsType: "",
							},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
				},
				Parameters: map[string]string{
					storageClassParamDatastoreIDs: "100",
					"type":                        "BLOCK",
				},
			},
			expectResponse: &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      "test-volume",
					CapacityBytes: volumeSize,
				},
			},
			expectError: false,
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("VolumeExists", mock.Anything, "test-volume").
					Return(-1, -1, nil)
				m.On(
					"CreateVolume",
					mock.Anything,
					"test-volume",
					volumeSize,
					mock.Anything,
					false,
					"",
					map[string]string{"type": "BLOCK"},
					opennebula.DatastoreSelectionConfig{
						Identifiers:  []string{"100"},
						Policy:       opennebula.DatastoreSelectionPolicyLeastUsed,
						AllowedTypes: []string{"local"},
					},
				).Return(&opennebula.VolumeCreateResult{Datastore: opennebula.Datastore{ID: 100, Name: "ds-100"}}, nil)
			},
		},
		{
			name: "TestVolumeCreationWithoutName",
			createVolumeRequest: &csi.CreateVolumeRequest{
				Name: "",
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: volumeSize,
				},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{
								FsType: "ext4",
							},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
				},
			},
			expectResponse: nil,
			expectError:    true,
			setupMock:      func(m *MockOpenNebulaVolumeProviderTestify) {},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := &MockOpenNebulaVolumeProviderTestify{}
			if tc.setupMock != nil {
				tc.setupMock(mockProvider)
			}

			cs := getTestControllerServer(mockProvider)
			response, err := cs.CreateVolume(context.Background(), tc.createVolumeRequest)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse.Volume.CapacityBytes, response.Volume.CapacityBytes)
				assert.Equal(t, tc.expectResponse.Volume.VolumeId, response.Volume.VolumeId)
			} else if !tc.expectError {
				assert.NotNil(t, response)
			}

			mockProvider.AssertExpectations(t)
		})
	}
}

func TestCreateVolumeCreatesSharedFilesystemVolumeForRWX(t *testing.T) {
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	sharedProvider := &MockSharedFilesystemProviderTestify{}
	cs := getTestControllerServerWithAllowedTypes(mockProvider, sharedProvider, "local,cephfs")

	sharedProvider.On("CreateSharedVolume", mock.Anything, opennebula.SharedVolumeRequest{
		Name:      "rwx-volume",
		SizeBytes: int64(1024 * 1024 * 1024),
		Selection: opennebula.DatastoreSelectionConfig{
			Identifiers:  []string{"100"},
			Policy:       opennebula.DatastoreSelectionPolicyLeastUsed,
			AllowedTypes: []string{"local", "cephfs"},
		},
		Parameters: map[string]string{
			storageClassParamDatastoreIDs: "100",
		},
		Secrets: map[string]string{
			"adminID":  "csi-admin",
			"adminKey": "super-secret",
		},
	}).Return(&opennebula.SharedVolumeCreateResult{
		VolumeID:      "cephfs:eyJiYWNrZW5kIjoiY2VwaGZzIiwiZGF0YXN0b3JlSUQiOjEwMCwiZnNOYW1lIjoiY2VwaGZzIiwibW9kZSI6ImR5bmFtaWMiLCJzdWJ2b2x1bWVHcm91cCI6ImNzaSIsInN1YnBhdGgiOiIvdm9sdW1lcy9jc2ktcHZjIn0",
		CapacityBytes: int64(1024 * 1024 * 1024),
		Datastore:     opennebula.Datastore{ID: 100, Name: "cephfs-file"},
		Metadata: opennebula.SharedVolumeMetadata{
			Backend: "cephfs",
			Mode:    opennebula.SharedVolumeModeDynamic,
		},
	}, nil)

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "rwx-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: int64(1024 * 1024 * 1024),
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
		Parameters: map[string]string{
			storageClassParamDatastoreIDs: "100",
		},
		Secrets: map[string]string{
			"adminID":  "csi-admin",
			"adminKey": "super-secret",
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, "cephfs:eyJiYWNrZW5kIjoiY2VwaGZzIiwiZGF0YXN0b3JlSUQiOjEwMCwiZnNOYW1lIjoiY2VwaGZzIiwibW9kZSI6ImR5bmFtaWMiLCJzdWJ2b2x1bWVHcm91cCI6ImNzaSIsInN1YnBhdGgiOiIvdm9sdW1lcy9jc2ktcHZjIn0", resp.GetVolume().GetVolumeId())
	mockProvider.AssertExpectations(t)
	sharedProvider.AssertExpectations(t)
}

func TestCreateVolumeRejectsReadWriteManyBlockVolume(t *testing.T) {
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	sharedProvider := &MockSharedFilesystemProviderTestify{}
	cs := getTestControllerServerWithAllowedTypes(mockProvider, sharedProvider, "local,cephfs")

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "rwx-block-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: int64(1024 * 1024 * 1024),
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Block{
					Block: &csi.VolumeCapability_BlockVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
		Parameters: map[string]string{
			storageClassParamDatastoreIDs: "100",
		},
	})

	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "filesystem volume capability")
	mockProvider.AssertExpectations(t)
	sharedProvider.AssertExpectations(t)
}

func TestCreateVolumeClonesFromSourceVolume(t *testing.T) {
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	sharedProvider := &MockSharedFilesystemProviderTestify{}
	cs := getTestControllerServerWithAllowedTypes(mockProvider, sharedProvider, "local")

	mockProvider.On("CloneVolume", mock.Anything, "clone-volume", "source-volume", opennebula.DatastoreSelectionConfig{
		Identifiers:  []string{"100"},
		Policy:       opennebula.DatastoreSelectionPolicyLeastUsed,
		AllowedTypes: []string{"local"},
	}).Return(&opennebula.VolumeCreateResult{
		Datastore:     opennebula.Datastore{ID: 100, Name: "fast-local", Backend: "local"},
		CapacityBytes: int64(1024 * 1024 * 1024),
	}, nil)

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "clone-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: int64(1024 * 1024 * 1024),
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
		Parameters: map[string]string{
			storageClassParamDatastoreIDs: "100",
		},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source-volume"},
			},
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, "clone-volume", resp.GetVolume().GetVolumeId())
	assert.Equal(t, int64(1024*1024*1024), resp.GetVolume().GetCapacityBytes())
	mockProvider.AssertExpectations(t)
}

func TestCreateVolumeRejectsSnapshotRestoreForDiskPath(t *testing.T) {
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	sharedProvider := &MockSharedFilesystemProviderTestify{}
	cs := getTestControllerServerWithAllowedTypes(mockProvider, sharedProvider, "local")

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "restore-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: int64(1024 * 1024 * 1024),
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
		Parameters: map[string]string{
			storageClassParamDatastoreIDs: "100",
		},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "image-snapshot:10:1"},
			},
		},
	})

	assert.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
	assert.Contains(t, err.Error(), "snapshot")
}

func TestDeleteVolume(t *testing.T) {
	tcs := []struct {
		name                string
		deleteVolumeRequest *csi.DeleteVolumeRequest
		setupMock           func(m *MockOpenNebulaVolumeProviderTestify)
		expectResponse      *csi.DeleteVolumeResponse
		expectError         bool
	}{
		{
			name: "TestBasicVolumeDeletion",
			deleteVolumeRequest: &csi.DeleteVolumeRequest{
				VolumeId: "test-volume-id",
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("DeleteVolume", mock.Anything, "test-volume-id").Return(nil)
			},
			expectResponse: &csi.DeleteVolumeResponse{},
			expectError:    false,
		},
		{
			name: "TestDeleteNonExistentVolume",
			deleteVolumeRequest: &csi.DeleteVolumeRequest{
				VolumeId: "non-existent-volume",
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("DeleteVolume", mock.Anything, "non-existent-volume").Return(nil)
			},
			expectResponse: &csi.DeleteVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(MockOpenNebulaVolumeProviderTestify)
			if tc.setupMock != nil {
				tc.setupMock(mockProvider)
			}

			cs := getTestControllerServer(mockProvider)
			response, err := cs.DeleteVolume(context.Background(), tc.deleteVolumeRequest)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else if !tc.expectError {
				assert.NotNil(t, response)
			}

			mockProvider.AssertExpectations(t)
		})
	}
}

func TestControllerPublishVolume(t *testing.T) {
	tcs := []struct {
		name                           string
		controllerPublishVolumeRequest *csi.ControllerPublishVolumeRequest
		setupMock                      func(m *MockOpenNebulaVolumeProviderTestify)
		expectResponse                 *csi.ControllerPublishVolumeResponse
		expectError                    bool
	}{
		{
			name: "TestBasicVolumeAttach",
			controllerPublishVolumeRequest: &csi.ControllerPublishVolumeRequest{
				VolumeId: "1234",
				NodeId:   "test-node-id",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("VolumeExists", mock.Anything, "1234").Return(1, 1, nil)
				m.On("NodeExists", mock.Anything, "test-node-id").Return(1, nil)
				m.On("GetVolumeInNode", mock.Anything, 1, 1).Once().Return("", errors.New("volume not attached to node"))
				m.On("AttachVolume", mock.Anything, "1234", "test-node-id", false, mock.Anything).Return(nil)
				m.On("GetVolumeInNode", mock.Anything, 1, 1).Once().Return("attached-volume", nil)
			},
			expectResponse: &csi.ControllerPublishVolumeResponse{
				PublishContext: map[string]string{
					"volumeName": "attached-volume",
				},
			},
			expectError: false,
		},
		{
			name: "TestCephAttachValidationFailureMapsToFailedPrecondition",
			controllerPublishVolumeRequest: &csi.ControllerPublishVolumeRequest{
				VolumeId: "1234",
				NodeId:   "test-node-id",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("VolumeExists", mock.Anything, "1234").Return(1, 1, nil)
				m.On("NodeExists", mock.Anything, "test-node-id").Return(1, nil)
				m.On("GetVolumeInNode", mock.Anything, 1, 1).Once().Return("", errors.New("volume not attached to node"))
				m.On("AttachVolume", mock.Anything, "1234", "test-node-id", false, mock.Anything).
					Return(opennebula.NewDatastoreConfigError("ceph datastore mismatch"))
			},
			expectResponse: nil,
			expectError:    true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(MockOpenNebulaVolumeProviderTestify)
			if tc.setupMock != nil {
				tc.setupMock(mockProvider)
			}

			cs := getTestControllerServer(mockProvider)
			response, err := cs.ControllerPublishVolume(context.Background(), tc.controllerPublishVolumeRequest)

			if tc.expectError {
				assert.Error(t, err)
				if tc.name == "TestCephAttachValidationFailureMapsToFailedPrecondition" {
					assert.Equal(t, codes.FailedPrecondition, status.Code(err))
				}
			} else {
				assert.NoError(t, err)
			}

			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else if !tc.expectError {
				assert.NotNil(t, response)
			}

			mockProvider.AssertExpectations(t)
		})
	}
}

func TestControllerPublishVolumeReturnsSharedFilesystemPublishContext(t *testing.T) {
	mockProvider := new(MockOpenNebulaVolumeProviderTestify)
	sharedProvider := new(MockSharedFilesystemProviderTestify)
	cs := getTestControllerServerWithAllowedTypes(mockProvider, sharedProvider, "local,cephfs")

	mockProvider.On("NodeExists", mock.Anything, "test-node-id").Return(1, nil)
	sharedProvider.On("PublishSharedVolume", mock.Anything, mock.Anything, false).Return(map[string]string{
		"shareBackend":   "cephfs",
		"cephfsMonitors": "mon1,mon2",
		"cephfsFSName":   "cephfs-prod",
		"cephfsSubpath":  "/kubernetes/dynamic/one-csi-demo",
		"cephfsReadonly": "false",
	}, nil)

	resp, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "cephfs:eyJiYWNrZW5kIjoiY2VwaGZzIiwiZGF0YXN0b3JlSUQiOjMwMCwiZnNOYW1lIjoiY2VwaGZzLXByb2QiLCJtb2RlIjoiZHluYW1pYyIsInN1YnZvbHVtZUdyb3VwIjoiY3NpIiwic3VicGF0aCI6Ii9rdWJlcm5ldGVzL2R5bmFtaWMvb25lLWNzaS1kZW1vIiwic3Vidm9sdW1lTmFtZSI6Im9uZS1jc2ktZGVtbyJ9",
		NodeId:   "test-node-id",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, "cephfs", resp.GetPublishContext()["shareBackend"])
	mockProvider.AssertExpectations(t)
	sharedProvider.AssertExpectations(t)
}

func TestDeleteVolumeRoutesSharedFilesystemVolumes(t *testing.T) {
	mockProvider := new(MockOpenNebulaVolumeProviderTestify)
	sharedProvider := new(MockSharedFilesystemProviderTestify)
	cs := getTestControllerServerWithAllowedTypes(mockProvider, sharedProvider, "local,cephfs")

	sharedProvider.On("DeleteSharedVolume", mock.Anything, mock.Anything, map[string]string{
		"adminID":  "csi-admin",
		"adminKey": "super-secret",
	}).Return(nil)

	resp, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: "cephfs:eyJiYWNrZW5kIjoiY2VwaGZzIiwiZGF0YXN0b3JlSUQiOjMwMCwiZnNOYW1lIjoiY2VwaGZzLXByb2QiLCJtb2RlIjoiZHluYW1pYyIsInN1YnZvbHVtZUdyb3VwIjoiY3NpIiwic3VicGF0aCI6Ii9rdWJlcm5ldGVzL2R5bmFtaWMvb25lLWNzaS1kZW1vIiwic3Vidm9sdW1lTmFtZSI6Im9uZS1jc2ktZGVtbyJ9",
		Secrets: map[string]string{
			"adminID":  "csi-admin",
			"adminKey": "super-secret",
		},
	})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockProvider.AssertExpectations(t)
	sharedProvider.AssertExpectations(t)
}

func TestListVolumes(t *testing.T) {
	tcs := []struct {
		name               string
		listVolumesRequest *csi.ListVolumesRequest
		setupMock          func(m *MockOpenNebulaVolumeProviderTestify)
		expectError        bool
		expectResponse     *csi.ListVolumesResponse
		expectVolumeCount  int
	}{
		{
			name: "TestListAllVolumes",
			listVolumesRequest: &csi.ListVolumesRequest{
				MaxEntries: 10,
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("ListVolumes", mock.Anything, "csi.opennebula.io", int32(10), "").Return([]string{"1"}, nil)
			},
			expectError: false,
			expectResponse: &csi.ListVolumesResponse{
				Entries: []*csi.ListVolumesResponse_Entry{
					{
						Volume: &csi.Volume{
							VolumeId: "1",
						},
					},
				},
			},
			expectVolumeCount: 1,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(MockOpenNebulaVolumeProviderTestify)
			if tc.setupMock != nil {
				tc.setupMock(mockProvider)
			}

			cs := getTestControllerServer(mockProvider)
			response, err := cs.ListVolumes(context.Background(), tc.listVolumesRequest)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, response)
				assert.Equal(t, tc.expectResponse, response)
				assert.Equal(t, tc.expectVolumeCount, len(response.Entries))
			}

			mockProvider.AssertExpectations(t)
		})
	}
}

func TestGetCapacity(t *testing.T) {
	tcs := []struct {
		name               string
		getCapacityRequest *csi.GetCapacityRequest
		setupMock          func(m *MockOpenNebulaVolumeProviderTestify)
		expectError        bool
		expectResponse     *csi.GetCapacityResponse
		expectCapacity     int64
	}{
		{
			name: "TestGetCapacity",
			getCapacityRequest: &csi.GetCapacityRequest{
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{
								FsType: "ext4",
							},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
				},
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On(
					"GetCapacity",
					mock.Anything,
					opennebula.DatastoreSelectionConfig{
						Identifiers:  []string{"100", "101"},
						Policy:       opennebula.DatastoreSelectionPolicyLeastUsed,
						AllowedTypes: []string{"local"},
					},
				).Return(int64(datastoreSize), nil)
			},
			expectError: false,
			expectResponse: &csi.GetCapacityResponse{
				AvailableCapacity: datastoreSize,
			},
			expectCapacity: datastoreSize,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(MockOpenNebulaVolumeProviderTestify)
			if tc.setupMock != nil {
				tc.setupMock(mockProvider)
			}

			cs := getTestControllerServer(mockProvider)
			response, err := cs.GetCapacity(context.Background(), tc.getCapacityRequest)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, response)
				assert.Equal(t, tc.expectResponse, response)
				assert.Equal(t, tc.expectCapacity, response.AvailableCapacity)
			}

			mockProvider.AssertExpectations(t)
		})
	}
}

func TestControllerExpandVolume(t *testing.T) {
	const expandedSize = int64(2 * 1024 * 1024 * 1024)

	tcs := []struct {
		name             string
		request          *csi.ControllerExpandVolumeRequest
		setupMock        func(m *MockOpenNebulaVolumeProviderTestify)
		expectError      bool
		expectNodeExpand bool
	}{
		{
			name: "FilesystemVolumeRequiresNodeExpansion",
			request: &csi.ControllerExpandVolumeRequest{
				VolumeId: "test-volume",
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: expandedSize,
				},
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
				},
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("ExpandVolume", mock.Anything, "test-volume", expandedSize).Return(expandedSize, nil)
			},
			expectNodeExpand: true,
		},
		{
			name: "BlockVolumeSkipsNodeExpansion",
			request: &csi.ControllerExpandVolumeRequest{
				VolumeId: "test-volume",
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: expandedSize,
				},
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
				},
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("ExpandVolume", mock.Anything, "test-volume", expandedSize).Return(expandedSize, nil)
			},
			expectNodeExpand: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(MockOpenNebulaVolumeProviderTestify)
			if tc.setupMock != nil {
				tc.setupMock(mockProvider)
			}

			cs := getTestControllerServer(mockProvider)
			response, err := cs.ControllerExpandVolume(context.Background(), tc.request)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, expandedSize, response.CapacityBytes)
				assert.Equal(t, tc.expectNodeExpand, response.NodeExpansionRequired)
			}

			mockProvider.AssertExpectations(t)
		})
	}
}

func TestCreateSnapshot(t *testing.T) {
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	cs := getTestControllerServer(mockProvider)
	now := time.Now().UTC()

	mockProvider.On("CreateSnapshot", mock.Anything, "test-volume", "snap-1").Return(&opennebula.VolumeSnapshot{
		SnapshotID:     "image-snapshot:10:1",
		SourceVolumeID: "test-volume",
		CreationTime:   now,
		SizeBytes:      int64(1024 * 1024 * 1024),
		ReadyToUse:     true,
	}, nil)

	resp, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-1",
		SourceVolumeId: "test-volume",
	})

	assert.NoError(t, err)
	assert.Equal(t, "image-snapshot:10:1", resp.GetSnapshot().GetSnapshotId())
	assert.Equal(t, "test-volume", resp.GetSnapshot().GetSourceVolumeId())
	mockProvider.AssertExpectations(t)
}

func TestDeleteSnapshot(t *testing.T) {
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	cs := getTestControllerServer(mockProvider)

	mockProvider.On("DeleteSnapshot", mock.Anything, "image-snapshot:10:1").Return(nil)

	resp, err := cs.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: "image-snapshot:10:1",
	})

	assert.NoError(t, err)
	assert.Equal(t, &csi.DeleteSnapshotResponse{}, resp)
	mockProvider.AssertExpectations(t)
}

func TestListSnapshots(t *testing.T) {
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	cs := getTestControllerServer(mockProvider)
	now := time.Now().UTC()

	mockProvider.On("ListSnapshots", mock.Anything, "", "test-volume", int32(10), "").Return([]opennebula.VolumeSnapshot{
		{
			SnapshotID:     "image-snapshot:10:1",
			SourceVolumeID: "test-volume",
			CreationTime:   now,
			SizeBytes:      int64(1024 * 1024 * 1024),
			ReadyToUse:     true,
		},
	}, "", nil)

	resp, err := cs.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: "test-volume",
		MaxEntries:     10,
	})

	assert.NoError(t, err)
	if assert.Len(t, resp.GetEntries(), 1) {
		assert.Equal(t, "image-snapshot:10:1", resp.GetEntries()[0].GetSnapshot().GetSnapshotId())
	}
	mockProvider.AssertExpectations(t)
}

func TestControllerUnpublishVolume(t *testing.T) {
	tcs := []struct {
		name                             string
		controllerUnpublishVolumeRequest *csi.ControllerUnpublishVolumeRequest
		setupMock                        func(m *MockOpenNebulaVolumeProviderTestify)
		expectResponse                   *csi.ControllerUnpublishVolumeResponse
		expectError                      bool
	}{
		{
			name: "TestBasicVolumeDetach",
			controllerUnpublishVolumeRequest: &csi.ControllerUnpublishVolumeRequest{
				VolumeId: "test-volume-id",
				NodeId:   "test-node-id",
			},
			setupMock: func(m *MockOpenNebulaVolumeProviderTestify) {
				m.On("VolumeExists", mock.Anything, "test-volume-id").Return(1, 1, nil)
				m.On("NodeExists", mock.Anything, "test-node-id").Return(42, nil)
				m.On("GetVolumeInNode", mock.Anything, 1, 42).Return("attached-target", nil)
				m.On("DetachVolume", mock.Anything, "test-volume-id", "test-node-id").Return(nil)
			},
			expectResponse: &csi.ControllerUnpublishVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(MockOpenNebulaVolumeProviderTestify)
			if tc.setupMock != nil {
				tc.setupMock(mockProvider)
			}

			cs := getTestControllerServer(mockProvider)
			response, err := cs.ControllerUnpublishVolume(context.Background(), tc.controllerUnpublishVolumeRequest)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else if !tc.expectError {
				assert.NotNil(t, response)
			}

			mockProvider.AssertExpectations(t)
		})
	}
}

type MockOpenNebulaVolumeProviderTestify struct {
	mock.Mock
}

type MockSharedFilesystemProviderTestify struct {
	mock.Mock
}

func (m *MockOpenNebulaVolumeProviderTestify) CreateVolume(ctx context.Context, name string, size int64, owner string, immutable bool, fsType string, params map[string]string, selection opennebula.DatastoreSelectionConfig) (*opennebula.VolumeCreateResult, error) {
	args := m.Called(ctx, name, size, owner, immutable, fsType, params, selection)
	if result := args.Get(0); result != nil {
		return result.(*opennebula.VolumeCreateResult), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockOpenNebulaVolumeProviderTestify) CloneVolume(ctx context.Context, name string, sourceVolume string, selection opennebula.DatastoreSelectionConfig) (*opennebula.VolumeCreateResult, error) {
	args := m.Called(ctx, name, sourceVolume, selection)
	if result := args.Get(0); result != nil {
		return result.(*opennebula.VolumeCreateResult), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockOpenNebulaVolumeProviderTestify) DeleteVolume(ctx context.Context, volume string) error {
	args := m.Called(ctx, volume)
	return args.Error(0)
}

func (m *MockOpenNebulaVolumeProviderTestify) ExpandVolume(ctx context.Context, volume string, size int64) (int64, error) {
	args := m.Called(ctx, volume, size)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockOpenNebulaVolumeProviderTestify) AttachVolume(ctx context.Context, volume string, node string, immutable bool, params map[string]string) error {
	args := m.Called(ctx, volume, node, immutable, params)
	return args.Error(0)
}

func (m *MockOpenNebulaVolumeProviderTestify) DetachVolume(ctx context.Context, volume string, node string) error {
	args := m.Called(ctx, volume, node)
	return args.Error(0)
}

func (m *MockOpenNebulaVolumeProviderTestify) ListVolumes(ctx context.Context, volume string, maxEntries int32, startingToken string) ([]string, error) {
	args := m.Called(ctx, volume, maxEntries, startingToken)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockOpenNebulaVolumeProviderTestify) GetCapacity(ctx context.Context, selection opennebula.DatastoreSelectionConfig) (int64, error) {
	args := m.Called(ctx, selection)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockOpenNebulaVolumeProviderTestify) VolumeExists(ctx context.Context, volume string) (int, int, error) {
	args := m.Called(ctx, volume)
	return args.Get(0).(int), args.Get(1).(int), args.Error(2)
}

func (m *MockOpenNebulaVolumeProviderTestify) CreateSnapshot(ctx context.Context, sourceVolume string, snapshotName string) (*opennebula.VolumeSnapshot, error) {
	args := m.Called(ctx, sourceVolume, snapshotName)
	if result := args.Get(0); result != nil {
		return result.(*opennebula.VolumeSnapshot), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockOpenNebulaVolumeProviderTestify) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	args := m.Called(ctx, snapshotID)
	return args.Error(0)
}

func (m *MockOpenNebulaVolumeProviderTestify) ListSnapshots(ctx context.Context, snapshotID string, sourceVolumeID string, maxEntries int32, startingToken string) ([]opennebula.VolumeSnapshot, string, error) {
	args := m.Called(ctx, snapshotID, sourceVolumeID, maxEntries, startingToken)
	return args.Get(0).([]opennebula.VolumeSnapshot), args.String(1), args.Error(2)
}

func (m *MockOpenNebulaVolumeProviderTestify) NodeExists(ctx context.Context, node string) (int, error) {
	args := m.Called(ctx, node)
	return args.Get(0).(int), args.Error(1)
}

func (m *MockOpenNebulaVolumeProviderTestify) GetVolumeInNode(ctx context.Context, volumeID int, nodeID int) (string, error) {
	args := m.Called(ctx, volumeID, nodeID)
	return args.Get(0).(string), args.Error(1)
}

func (m *MockOpenNebulaVolumeProviderTestify) VolumeReadyWithTimeout(volumeID int) (bool, error) {
	args := m.Called(volumeID)
	return args.Bool(0), args.Error(1)
}

func (m *MockSharedFilesystemProviderTestify) CreateSharedVolume(ctx context.Context, req opennebula.SharedVolumeRequest) (*opennebula.SharedVolumeCreateResult, error) {
	args := m.Called(ctx, req)
	if result := args.Get(0); result != nil {
		return result.(*opennebula.SharedVolumeCreateResult), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockSharedFilesystemProviderTestify) DeleteSharedVolume(ctx context.Context, volumeID string, secrets map[string]string) error {
	args := m.Called(ctx, volumeID, secrets)
	return args.Error(0)
}

func (m *MockSharedFilesystemProviderTestify) PublishSharedVolume(ctx context.Context, volumeID string, readonly bool) (map[string]string, error) {
	args := m.Called(ctx, volumeID, readonly)
	if result := args.Get(0); result != nil {
		return result.(map[string]string), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockSharedFilesystemProviderTestify) ValidateSharedVolume(ctx context.Context, volumeID string) (*opennebula.SharedVolumeMetadata, error) {
	args := m.Called(ctx, volumeID)
	if result := args.Get(0); result != nil {
		return result.(*opennebula.SharedVolumeMetadata), args.Error(1)
	}
	return nil, args.Error(1)
}
