//go:build linux

package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

// Exercise the actual pinned Linux ResizeFs implementation. Only command output,
// statfs and the verification clock are fixtures; NeedResize and Resize are real.
func TestNodeExpandMountUtilsCompletion(t *testing.T) {
	for _, fsType := range []string{"xfs", "ext4", "ext3"} {
		t.Run(fsType, func(t *testing.T) {
			oldFS, oldSleep, oldNow := nodeVolumePathFS, nodeDeviceSleep, nodeNow
			t.Cleanup(func() { nodeVolumePathFS, nodeDeviceSleep, nodeNow = oldFS, oldSleep, oldNow })
			const requested = int64(2147483648)
			now := time.Unix(0, 0)
			nodeNow = func() time.Time { return now }
			nodeDeviceSleep = func(d time.Duration) { now = now.Add(d) }
			nodeVolumePathFS = func(_ string, st *unix.Statfs_t) error { st.Bsize = 4096; st.Blocks = 507904; return nil }
			volumePath := t.TempDir()
			ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: volumePath, Device: "/dev/null"}})
			ns.Driver.PluginConfig.OverrideVal(config.NodeExpandVerifyTimeoutSecondsVar, 10)
			ns.Driver.PluginConfig.OverrideVal(config.NodeExpandRetryIntervalSecondsVar, 2)
			resizeCalls := 0
			action := func(cmd string, args ...string) exec.Cmd {
				var output string
				switch cmd {
				case "blockdev":
					require.Len(t, args, 2)
					require.Equal(t, "/dev/null", args[1])
					switch args[0] {
					case "--getsize64":
						output = "2147483648\n"
					case "--getro":
						output = "0\n"
					default:
						t.Fatalf("unexpected blockdev args %v", args)
					}
				case "blkid":
					require.Equal(t, []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/null"}, args)
					output = "TYPE=" + fsType + "\n"
				case "xfs_growfs":
					require.Equal(t, "xfs", fsType)
					require.Equal(t, []string{"-d", volumePath}, args)
					resizeCalls++
					output = "data blocks changed\n"
				case "resize2fs":
					require.Contains(t, []string{"ext3", "ext4"}, fsType)
					require.Equal(t, []string{"/dev/null"}, args)
					resizeCalls++
					output = "The filesystem is now full-sized.\n"
				default:
					t.Fatalf("unexpected command %s %v", cmd, args)
				}
				return testingexec.InitFakeCmd(&testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) { return []byte(output), nil, nil }}}, cmd, args...)
			}
			fakeExec := &testingexec.FakeExec{}
			for range 128 {
				fakeExec.CommandScript = append(fakeExec.CommandScript, action)
			}
			ns.mounter.Exec = fakeExec
			// NeedResize remains true even after a successful resize command. It is
			// eligibility, not a post-completion geometry measurement in v0.36.6.
			resizer := mount.NewResizeFs(fakeExec)
			resized, err := resizer.Resize("/dev/null", volumePath)
			require.NoError(t, err)
			require.True(t, resized)
			eligible, err := resizer.NeedResize("/dev/null", volumePath)
			require.NoError(t, err)
			require.True(t, eligible)
			resizeCalls = 0
			response, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: "owned-fixture", VolumePath: volumePath, StagingTargetPath: volumePath, CapacityRange: &csi.CapacityRange{RequiredBytes: requested}, VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: fsType}}}})
			require.NoError(t, err, fmt.Sprintf("successful %s convergence must complete despite lower statfs and NeedResize=true", fsType))
			require.Equal(t, requested, response.GetCapacityBytes())
			require.Equal(t, 1, resizeCalls, "successful filesystem convergence must not loop")
		})
	}
}
