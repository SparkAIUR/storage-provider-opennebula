package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

const (
	targetPath = "/tmp/target" // Example target path for publishing
)

func getTestNodeServer(mountPoints []string) *NodeServer {
	driver := &Driver{
		name:               DefaultDriverName,
		version:            driverVersion,
		grpcServerEndpoint: DefaultGRPCServerEndpoint,
		nodeID:             "test-node-id",
		maxVolumesPerNode:  30,
	}
	commandScriptArray := []testingexec.FakeCommandAction{}
	//TODO: Simulate real commands
	for i := 0; i < 10; i++ {
		commandScriptArray = append(commandScriptArray, func(cmd string, args ...string) exec.Cmd {
			return &testingexec.FakeCmd{
				Argv:           append([]string{cmd}, args...),
				Stdout:         nil,
				Stderr:         nil,
				DisableScripts: true, // Disable script checking for simplicity
			}
		})
	}
	mountPointList := []mount.MountPoint{}
	for _, mountPoint := range mountPoints {
		mountPointList = append(mountPointList, mount.MountPoint{
			Path: mountPoint,
		})
	}

	mounter := mount.NewSafeFormatAndMount(
		mount.NewFakeMounter(mountPointList), // using fake mounter implementation
		&testingexec.FakeExec{
			CommandScript: commandScriptArray,
		}, // using fake exec implementation
	)
	return NewNodeServer(driver, mounter)
}

func TestStageVolume(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeStageVolumeRequest
		expectResponse *csi.NodeStageVolumeResponse
		expectError    bool
	}{
		{
			name: "[SUCCESS] Test basic volume mount",
			request: &csi.NodeStageVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: tempDir,
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
				PublishContext: map[string]string{
					"volumeName": "zero",
				},
			},
			expectResponse: &csi.NodeStageVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{})
			response, err := ns.NodeStageVolume(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestStageSharedFilesystemVolume(t *testing.T) {
	tempDir := t.TempDir()
	ns := getTestNodeServer([]string{})

	resp, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "cephfs:eyJiYWNrZW5kIjoiY2VwaGZzIiwiZGF0YXN0b3JlSUQiOjMwMCwiZnNOYW1lIjoiY2VwaGZzLXByb2QiLCJtb2RlIjoic3RhdGljIiwic3VicGF0aCI6Ii9rdWJlcm5ldGVzL3N0YXRpYy9tb2RlbC1jYWNoZSJ9",
		StagingTargetPath: tempDir,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		PublishContext: map[string]string{
			"shareBackend":   "cephfs",
			"cephfsMonitors": "mon1,mon2",
			"cephfsFSName":   "cephfs-prod",
			"cephfsSubpath":  "/kubernetes/static/model-cache",
			"cephfsReadonly": "false",
		},
		Secrets: map[string]string{
			"userID":  "csi-node",
			"userKey": "super-secret",
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, &csi.NodeStageVolumeResponse{}, resp)
}

func TestUnstageVolume(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeUnstageVolumeRequest
		expectResponse *csi.NodeUnstageVolumeResponse
		expectError    bool
	}{
		{
			name: "[SUCCESS] Test correct staging target path",
			request: &csi.NodeUnstageVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: tempDir,
			},
			expectResponse: &csi.NodeUnstageVolumeResponse{},
			expectError:    false,
		},
		{
			name: "[SUCCESS] Test unmounted staging target path",
			request: &csi.NodeUnstageVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: "/tmp/nonexistent",
			},
			expectResponse: &csi.NodeUnstageVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeUnstageVolume(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestPublishVolume(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodePublishVolumeRequest
		expectResponse *csi.NodePublishVolumeResponse
		expectError    bool
	}{
		{
			name: "[SUCCESS] Test correct staging target path",
			request: &csi.NodePublishVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: tempDir,
				TargetPath:        targetPath,
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType:     "ext4",
							MountFlags: []string{"ro"},
						},
					},
				},
				PublishContext: map[string]string{
					"volumeName": "zero",
				},
			},
			expectResponse: &csi.NodePublishVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodePublishVolume(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestPublishSharedFilesystemVolume(t *testing.T) {
	tempDir := t.TempDir()
	ns := getTestNodeServer([]string{tempDir})

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "cephfs:eyJiYWNrZW5kIjoiY2VwaGZzIiwiZGF0YXN0b3JlSUQiOjMwMCwiZnNOYW1lIjoiY2VwaGZzLXByb2QiLCJtb2RlIjoiZHluYW1pYyIsInN1YnBhdGgiOiIva3ViZXJuZXRlcy9keW5hbWljL29uZS1jc2ktZGVtbyJ9",
		StagingTargetPath: tempDir,
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		PublishContext: map[string]string{
			"shareBackend":   "cephfs",
			"cephfsMonitors": "mon1,mon2",
			"cephfsFSName":   "cephfs-prod",
			"cephfsSubpath":  "/kubernetes/dynamic/one-csi-demo",
			"cephfsReadonly": "false",
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, &csi.NodePublishVolumeResponse{}, resp)
}

func TestUnpublishVolume(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeUnpublishVolumeRequest
		expectResponse *csi.NodeUnpublishVolumeResponse
		expectError    bool
	}{
		{
			name: "[SUCCESS] Test correct staging target path",
			request: &csi.NodeUnpublishVolumeRequest{
				VolumeId:   "test-volume-id",
				TargetPath: targetPath,
			},
			expectResponse: &csi.NodeUnpublishVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeUnpublishVolume(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestNodeGetVolumeStats(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeGetVolumeStatsRequest
		expectResponse *csi.NodeGetVolumeStatsResponse
		expectError    bool
	}{
		{
			name: "[ERROR] Test unimplemented",
			request: &csi.NodeGetVolumeStatsRequest{
				VolumeId: "test-volume-id",
			},
			expectResponse: &csi.NodeGetVolumeStatsResponse{},
			expectError:    true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeGetVolumeStats(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestNodeExpandVolume(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeExpandVolumeRequest
		expectResponse *csi.NodeExpandVolumeResponse
		expectError    bool
	}{
		{
			name: "[SUCCESS] Test block volume expansion",
			request: &csi.NodeExpandVolumeRequest{
				VolumeId:   "test-volume-id",
				VolumePath: tempDir,
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 2 * 1024 * 1024 * 1024,
				},
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
				},
			},
			expectResponse: &csi.NodeExpandVolumeResponse{
				CapacityBytes: 2 * 1024 * 1024 * 1024,
			},
			expectError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeExpandVolume(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestNodeGetCapabilities(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeGetCapabilitiesRequest
		expectResponse *csi.NodeGetCapabilitiesResponse
		expectError    bool
	}{
		{
			name:    "[Success] Test capabilities",
			request: &csi.NodeGetCapabilitiesRequest{},
			expectResponse: &csi.NodeGetCapabilitiesResponse{
				Capabilities: []*csi.NodeServiceCapability{
					{
						Type: &csi.NodeServiceCapability_Rpc{
							Rpc: &csi.NodeServiceCapability_RPC{
								Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
							},
						},
					},
					{
						Type: &csi.NodeServiceCapability_Rpc{
							Rpc: &csi.NodeServiceCapability_RPC{
								Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
							},
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeGetCapabilities(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestNodeGetInfo(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeGetInfoRequest
		expectResponse *csi.NodeGetInfoResponse
		expectError    bool
	}{
		{
			name:    "[Success] Test retrieved node info",
			request: &csi.NodeGetInfoRequest{},
			expectResponse: &csi.NodeGetInfoResponse{
				NodeId:            "test-node-id",
				MaxVolumesPerNode: 30,
			},
			expectError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeGetInfo(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}
