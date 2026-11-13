package driver

import (
	"context"
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

// CSI's capacity and resize result must describe one backing device. Distinct
// sparse files stand in for devices; symlinks/hardlinks supply independent OS
// identity evidence. A successful resize of the wrong mount must be rejected.
func TestNodeExpandMountBinding(t *testing.T) {
	for _, scenario := range []string{
		"same device", "symlink alias", "hardlink alias", "no staging path",
		"different devices", "missing published mount", "missing staging mount",
		"empty published device", "empty staging device", "duplicate published mount", "duplicate staging mount", "unobservable published device",
		"unobservable staging device", "same unobservable device", "directory device",
		"published mount changes during wait", "staging mount changes during wait",
		"both mounts change during wait", "device alias changes during wait",
		"device disappears during wait", "published mount disappears during wait",
		"valid alias changes during wait",
	} {
		t.Run(scenario, func(t *testing.T) {
			oldOS, oldResize, oldFS, oldSleep, oldNow := nodeRuntimeGOOS, nodeResizeFS, nodeVolumePathFS, nodeExpandSleep, nodeNow
			t.Cleanup(func() {
				nodeRuntimeGOOS, nodeResizeFS, nodeVolumePathFS, nodeExpandSleep, nodeNow = oldOS, oldResize, oldFS, oldSleep, oldNow
			})
			nodeRuntimeGOOS = "linux"
			const requested = int64(8192)
			root := t.TempDir()
			device, other, alias := filepath.Join(root, "device"), filepath.Join(root, "other"), filepath.Join(root, "alias")
			for _, p := range []string{device, other} {
				require.NoError(t, os.WriteFile(p, nil, 0600))
				require.NoError(t, os.Truncate(p, requested))
			}
			stage, publish := filepath.Join(root, "stage"), filepath.Join(root, "publish")
			points := []mount.MountPoint{{Path: stage, Device: device}, {Path: publish, Device: device}}
			want := codes.FailedPrecondition
			switch scenario {
			case "same device", "no staging path", "valid alias changes during wait":
				want = codes.OK
			case "symlink alias":
				require.NoError(t, os.Symlink(device, alias))
				points[1].Device = alias
				want = codes.OK
			case "hardlink alias":
				require.NoError(t, os.Link(device, alias))
				points[1].Device = alias
				want = codes.OK
			case "duplicate published mount":
				points = append(points, points[1])
			case "duplicate staging mount":
				points = append(points, points[0])
			case "different devices":
				points[1].Device = other
			case "missing published mount":
				points = points[:1]
			case "missing staging mount":
				points = points[1:]
			case "empty published device":
				points[1].Device = ""
			case "empty staging device":
				points[0].Device = ""
			case "unobservable published device":
				points[1].Device = alias
			case "unobservable staging device":
				points[0].Device = alias
			case "same unobservable device":
				points[0].Device, points[1].Device = alias, alias
			case "directory device":
				points[0].Device, points[1].Device = root, root
			case "device alias changes during wait":
				require.NoError(t, os.Symlink(device, alias))
				points[0].Device, points[1].Device = alias, alias
			}
			ns := getTestNodeServerWithMountPoints(points)
			ns.Driver.PluginConfig.OverrideVal(config.NodeExpandVerifyTimeoutSecondsVar, 10)
			ns.Driver.PluginConfig.OverrideVal(config.NodeExpandRetryIntervalSecondsVar, 1)
			mounter := ns.mounter.Interface.(*mount.FakeMounter)
			resizeCalls, sleeps := 0, 0
			nodeResizeFS = func(_ exec.Interface, dev, target string) (bool, error) {
				resizeCalls++
				require.Equal(t, publish, target)
				actual, err := os.Stat(dev)
				require.NoError(t, err)
				expected, err := os.Stat(device)
				require.NoError(t, err)
				require.True(t, os.SameFile(expected, actual))
				return true, nil
			}
			nodeVolumePathFS = func(_ string, st *unix.Statfs_t) error { st.Bsize = 4096; st.Blocks = 1; return nil }
			now := time.Unix(0, 0)
			nodeNow = func() time.Time { return now }
			waiting := false
			switch scenario {
			case "published mount changes during wait", "staging mount changes during wait", "both mounts change during wait", "device alias changes during wait", "device disappears during wait", "published mount disappears during wait", "valid alias changes during wait":
				waiting = true
			}
			if waiting {
				require.NoError(t, os.Truncate(device, requested/2))
			}
			nodeExpandSleep = func(_ context.Context, d time.Duration) error {
				sleeps++
				require.Equal(t, 1, sleeps)
				now = now.Add(d)
				require.NoError(t, os.Truncate(device, requested))
				switch scenario {
				case "published mount changes during wait":
					mounter.MountPoints[1].Device = other
				case "staging mount changes during wait":
					mounter.MountPoints[0].Device = other
				case "both mounts change during wait":
					mounter.MountPoints[0].Device, mounter.MountPoints[1].Device = other, other
				case "device alias changes during wait":
					require.NoError(t, os.Remove(alias))
					require.NoError(t, os.Symlink(other, alias))
				case "device disappears during wait":
					require.NoError(t, os.Remove(device))
				case "published mount disappears during wait":
					mounter.MountPoints = mounter.MountPoints[:1]
				case "valid alias changes during wait":
					require.NoError(t, os.Symlink(device, alias))
					mounter.MountPoints[1].Device = alias
				}
				return nil
			}
			if scenario == "no staging path" {
				stage = ""
			}
			response, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: "binding-fixture", VolumePath: publish, StagingTargetPath: stage, CapacityRange: &csi.CapacityRange{RequiredBytes: requested}})
			require.Equal(t, want, status.Code(err), "%v", err)
			if want == codes.OK {
				require.Equal(t, requested, response.GetCapacityBytes())
				require.Equal(t, 1, resizeCalls)
			} else {
				require.Nil(t, response)
				require.Zero(t, resizeCalls, "reject before any filesystem mutation")
			}
			if waiting {
				require.Equal(t, 1, sleeps)
			}
		})
	}
}
