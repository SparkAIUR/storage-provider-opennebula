package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

func TestNodeExpandVolumeRejectsIncompleteGrowth(t *testing.T) {
	const requiredBytes int64 = 42949672960
	for _, tc := range []struct {
		name        string
		deviceSize  int64
		resized     bool
		resizeErr   error
		verifyErr   error
		code        codes.Code
		needsResize bool
	}{
		{"device one byte short despite configured tolerance", requiredBytes - 1, true, nil, nil, codes.DeadlineExceeded, false},
		{"device 512 MiB short despite configured tolerance", requiredBytes - 536870912, true, nil, nil, codes.DeadlineExceeded, false},
		{"filesystem geometry unavailable", requiredBytes, true, nil, errors.New("invalid superblock"), codes.Internal, false},
		{"resize command failed", requiredBytes, false, errors.New("resize failed"), nil, codes.Internal, false},
		{"unformatted device", requiredBytes, false, nil, nil, codes.FailedPrecondition, false},
		{"filesystem geometry incomplete despite full statfs", requiredBytes, true, nil, nil, codes.DeadlineExceeded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldStatfs, oldSleep, oldNow := nodeVolumePathFS, nodeDeviceSleep, nodeNow
			oldResize, oldNeedsResize, oldGOOS := nodeResizeFS, nodeNeedsResizeFS, nodeRuntimeGOOS
			t.Cleanup(func() {
				nodeVolumePathFS, nodeDeviceSleep, nodeNow = oldStatfs, oldSleep, oldNow
				nodeResizeFS, nodeNeedsResizeFS, nodeRuntimeGOOS = oldResize, oldNeedsResize, oldGOOS
			})
			volumePath := t.TempDir()
			device := filepath.Join(t.TempDir(), "device")
			require.NoError(t, os.WriteFile(device, nil, 0600))
			require.NoError(t, os.Truncate(device, tc.deviceSize))
			ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: volumePath, Device: device}})
			ns.Driver.PluginConfig.OverrideVal(config.NodeExpandVerifyTimeoutSecondsVar, 10)
			ns.Driver.PluginConfig.OverrideVal(config.NodeExpandSizeToleranceBytesVar, 1073741824)
			nodeRuntimeGOOS = "linux"
			now := time.Unix(0, 0)
			nodeNow = func() time.Time { return now }
			nodeDeviceSleep = func(d time.Duration) { now = now.Add(d) }
			filesystemBytes := int64(42158374912)
			if tc.needsResize {
				filesystemBytes = requiredBytes
			}
			nodeVolumePathFS = func(_ string, buf *unix.Statfs_t) error {
				buf.Bsize, buf.Blocks = 4096, uint64(filesystemBytes/4096)
				return nil
			}
			resizeCalls := 0
			nodeResizeFS = func(_ exec.Interface, _, _ string) (bool, error) {
				resizeCalls++
				return tc.resized, tc.resizeErr
			}
			nodeNeedsResizeFS = func(_ exec.Interface, _, _ string) (bool, error) { return tc.needsResize, tc.verifyErr }
			response, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
				VolumeId: "test-volume", VolumePath: volumePath, StagingTargetPath: volumePath,
				CapacityRange: &csi.CapacityRange{RequiredBytes: requiredBytes},
				VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
				}},
			})
			require.Nil(t, response)
			require.Equal(t, tc.code, status.Code(err))
			if tc.deviceSize < requiredBytes {
				require.Zero(t, resizeCalls, "never resize before the device reaches the full request")
			} else if tc.needsResize {
				require.GreaterOrEqual(t, resizeCalls, 2, "retry incomplete geometry even when statfs reports full capacity")
			} else {
				require.Equal(t, 1, resizeCalls)
			}
			t.Logf("CSI NodeExpandVolume fixture: required_bytes=%d device_bytes=%d statfs_bytes=%d configured_tolerance_bytes=1073741824 geometry_needs_resize=%t resize_calls=%d response=%v error=%v", requiredBytes, tc.deviceSize, filesystemBytes, tc.needsResize, resizeCalls, response, err)
		})
	}
}
