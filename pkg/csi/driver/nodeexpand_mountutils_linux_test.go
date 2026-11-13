//go:build linux

package driver

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
			oldFS, oldSleep, oldNow := nodeVolumePathFS, nodeExpandSleep, nodeNow
			t.Cleanup(func() { nodeVolumePathFS, nodeExpandSleep, nodeNow = oldFS, oldSleep, oldNow })
			const requested = int64(2147483648)
			now := time.Unix(0, 0)
			nodeNow = func() time.Time { return now }
			nodeExpandSleep = func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }
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

// CommandContext is observed independently of FakeExec's context-agnostic implementation.
type nodeExpandRecordingExec struct {
	*testingexec.FakeExec
	contexts []context.Context
}

func (e *nodeExpandRecordingExec) CommandContext(ctx context.Context, cmd string, args ...string) exec.Cmd {
	e.contexts = append(e.contexts, ctx)
	return e.Command(cmd, args...)
}

func TestNodeExpandMountUtilsSafety(t *testing.T) {
	const requested = int64(2147483648)
	for _, tc := range []struct {
		name, format                        string
		sizes                               []int64
		resizeError, formatError, statError bool
		cancelAt                            string
		want                                codes.Code
		wantResize, wantSleep               int
	}{
		{name: "short device reaches target", format: "xfs", sizes: []int64{requested / 2, requested}, wantResize: 1, wantSleep: 1},
		{name: "permanently short device", format: "xfs", sizes: []int64{requested - 4096}, want: codes.DeadlineExceeded, wantSleep: 5},
		{name: "resize tool fails", format: "xfs", resizeError: true, want: codes.Internal, wantResize: 1},
		{name: "ext resize tool fails", format: "ext4", resizeError: true, want: codes.Internal, wantResize: 1},
		{name: "unformatted returns false", format: "", want: codes.FailedPrecondition},
		{name: "unsupported format", format: "vfat", want: codes.Internal},
		{name: "unknown partition format", format: "partition", want: codes.Internal},
		{name: "format observation fails", formatError: true, want: codes.Internal},
		{name: "deadline during resize", format: "xfs", cancelAt: "deadline-resize", want: codes.DeadlineExceeded, wantResize: 1},
		{name: "deadline during device read", format: "xfs", cancelAt: "deadline-device", want: codes.DeadlineExceeded},
		{name: "cancel after resize", format: "xfs", cancelAt: "resize", want: codes.Canceled, wantResize: 1},
		{name: "cancel after device read", format: "xfs", cancelAt: "device", want: codes.Canceled},
		{name: "cancel after statfs", format: "ext4", cancelAt: "statfs", want: codes.Canceled, wantResize: 1},
		{name: "cancel while waiting for device", format: "xfs", sizes: []int64{requested / 2}, cancelAt: "wait", want: codes.Canceled, wantSleep: 1},
		{name: "statfs diagnostic error fails", format: "xfs", statError: true, want: codes.Internal, wantResize: 1},
		{name: "btrfs existing resize support", format: "btrfs", wantResize: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldFS, oldSleep, oldNow := nodeVolumePathFS, nodeExpandSleep, nodeNow
			t.Cleanup(func() { nodeVolumePathFS, nodeExpandSleep, nodeNow = oldFS, oldSleep, oldNow })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			now := time.Unix(0, 0)
			nodeNow = func() time.Time { return now }
			sleepCalls := 0
			nodeExpandSleep = func(ctx context.Context, d time.Duration) error {
				sleepCalls++
				now = now.Add(d)
				if tc.cancelAt == "wait" {
					cancel()
					return ctx.Err()
				}
				return nil
			}
			nodeVolumePathFS = func(_ string, st *unix.Statfs_t) error {
				if tc.cancelAt == "statfs" {
					cancel()
				}
				if tc.statError {
					return errors.New("statfs fixture failure")
				}
				st.Bsize = 4096
				st.Blocks = 507904
				return nil
			}
			volumePath := t.TempDir()
			ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: volumePath, Device: "/dev/null"}})
			ns.Driver.PluginConfig.OverrideVal(config.NodeExpandVerifyTimeoutSecondsVar, 10)
			ns.Driver.PluginConfig.OverrideVal(config.NodeExpandRetryIntervalSecondsVar, 2)
			resizeCalls, deviceReads := 0, 0
			action := func(cmd string, args ...string) exec.Cmd {
				var output string
				var commandErr error
				switch cmd {
				case "blockdev":
					require.Equal(t, []string{"--getsize64", "/dev/null"}, args)
					size := requested
					if len(tc.sizes) > 0 {
						index := min(deviceReads, len(tc.sizes)-1)
						size = tc.sizes[index]
					}
					deviceReads++
					output = fmt.Sprint(size)
					if tc.cancelAt == "device" {
						cancel()
					}
				case "blkid":
					require.Equal(t, []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/null"}, args)
					if tc.format != "" {
						output = "TYPE=" + tc.format + "\n"
					}
					if tc.format == "partition" {
						output = "PTTYPE=gpt\n"
					}
					if tc.formatError {
						commandErr = errors.New("format fixture failure")
					}
				case "xfs_growfs", "resize2fs", "btrfs":
					switch cmd {
					case "xfs_growfs":
						require.Equal(t, []string{"-d", volumePath}, args)
					case "resize2fs":
						require.Equal(t, []string{"/dev/null"}, args)
					case "btrfs":
						require.Equal(t, []string{"filesystem", "resize", "max", volumePath}, args)
					}
					resizeCalls++
					if tc.resizeError {
						commandErr = errors.New("resize fixture failure")
						output = "tool refused growth"
					}
				default:
					t.Fatalf("unexpected command %s %v", cmd, args)
				}
				return testingexec.InitFakeCmd(&testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) {
					if (tc.cancelAt == "deadline-resize" && cmd == "xfs_growfs") || (tc.cancelAt == "deadline-device" && cmd == "blockdev") {
						<-ctx.Done()
						return nil, nil, ctx.Err()
					}
					if tc.cancelAt == "resize" && (cmd == "xfs_growfs" || cmd == "resize2fs") {
						cancel()
					}
					return []byte(output), nil, commandErr
				}}}, cmd, args...)
			}
			commands := &nodeExpandRecordingExec{FakeExec: &testingexec.FakeExec{}}
			for range 128 {
				commands.CommandScript = append(commands.CommandScript, action)
			}
			ns.mounter.Exec = commands
			// Start the deadline after fixture setup so CPU contention cannot
			// expire it before the intended external command is reached.
			if tc.cancelAt == "deadline-resize" || tc.cancelAt == "deadline-device" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
			}
			response, err := ns.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: "owned-fixture", VolumePath: volumePath, StagingTargetPath: volumePath, CapacityRange: &csi.CapacityRange{RequiredBytes: requested}, VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"}}}})
			require.Equal(t, tc.want, status.Code(err), "%v", err)
			if tc.want == codes.OK {
				require.Equal(t, requested, response.GetCapacityBytes())
			} else {
				require.Nil(t, response)
			}
			require.Equal(t, tc.wantResize, resizeCalls)
			require.Equal(t, tc.wantSleep, sleepCalls)
			if tc.wantResize > 0 {
				require.NotEmpty(t, commands.contexts, "mount-utils commands must use RPC context")
				for _, observed := range commands.contexts {
					_, bounded := observed.Deadline()
					require.True(t, bounded)
					if tc.cancelAt != "" {
						require.Error(t, observed.Err())
					}
				}
			}
			if tc.resizeError {
				require.Contains(t, err.Error(), "tool refused growth")
			}
		})
	}
}
