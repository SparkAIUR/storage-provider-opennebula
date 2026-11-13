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
		name              string
		deviceSize        int64
		resized           bool
		resizeErr         error
		cancelAfterResize bool
		code              codes.Code
		fullStatfs        bool
		statfsErr         error
	}{
		{"device one byte short despite configured tolerance", requiredBytes - 1, true, nil, false, codes.DeadlineExceeded, false, nil},
		{"device 512 MiB short despite configured tolerance", requiredBytes - 536870912, true, nil, false, codes.DeadlineExceeded, false, nil},
		// Actual format-observation rejection is additionally covered by the Linux mount-utils fixture.
		{"format observation unavailable", requiredBytes, false, errors.New("invalid superblock"), false, codes.Internal, false, nil},
		{"resize command failed", requiredBytes, false, errors.New("resize failed"), false, codes.Internal, false, nil},
		{"unformatted device", requiredBytes, false, nil, false, codes.FailedPrecondition, false, nil},
		// NeedResize=true is eligibility, not incomplete geometry. Preserve rejection
		// after successful Resize when cancellation invalidates the response instead.
		{"canceled after tool convergence despite full statfs", requiredBytes, true, nil, true, codes.Canceled, true, nil},
		{"statfs syscall failure remains fatal", requiredBytes, true, nil, false, codes.Internal, false, unix.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldStatfs, oldSleep, oldNow := nodeVolumePathFS, nodeExpandSleep, nodeNow
			oldResize, oldGOOS := nodeResizeFS, nodeRuntimeGOOS
			t.Cleanup(func() {
				nodeVolumePathFS, nodeExpandSleep, nodeNow = oldStatfs, oldSleep, oldNow
				nodeResizeFS, nodeRuntimeGOOS = oldResize, oldGOOS
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
			nodeExpandSleep = func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }
			filesystemBytes := int64(42158374912)
			if tc.fullStatfs {
				filesystemBytes = requiredBytes
			}
			nodeVolumePathFS = func(_ string, buf *unix.Statfs_t) error {
				buf.Bsize, buf.Blocks = 4096, uint64(filesystemBytes/4096)
				return tc.statfsErr
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resizeCalls := 0
			nodeResizeFS = func(_ exec.Interface, _, _ string) (bool, error) {
				resizeCalls++
				if tc.cancelAfterResize {
					cancel()
				}
				return tc.resized, tc.resizeErr
			}
			response, err := ns.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{
				VolumeId: "test-volume", VolumePath: volumePath, StagingTargetPath: volumePath,
				CapacityRange: &csi.CapacityRange{RequiredBytes: requiredBytes},
				VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
				}},
			})
			require.Nil(t, response)
			require.Equal(t, tc.code, status.Code(err))
			if tc.statfsErr != nil {
				require.ErrorContains(t, err, "failed to collect filesystem size")
				require.ErrorContains(t, err, tc.statfsErr.Error())
			}
			if tc.deviceSize < requiredBytes {
				require.Zero(t, resizeCalls, "never resize before the device reaches the full request")

			} else {
				require.Equal(t, 1, resizeCalls)
			}
			t.Logf("CSI NodeExpandVolume fixture: required_bytes=%d device_bytes=%d statfs_bytes=%d configured_tolerance_bytes=1073741824 full_statfs=%t resize_calls=%d response=%v error=%v", requiredBytes, tc.deviceSize, filesystemBytes, tc.fullStatfs, resizeCalls, response, err)
		})
	}
}
