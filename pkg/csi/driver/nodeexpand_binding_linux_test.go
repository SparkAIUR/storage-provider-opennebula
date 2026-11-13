//go:build linux

package driver

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

// Real mount-utils must not acknowledge XFS's successful no-op on device B
// using device A's larger capacity. Device nodes are external-boundary fixtures.
func TestNodeExpandMountUtilsBinding(t *testing.T) {
	for _, scenario := range []string{"different devices", "valid alias", "kernel block aliases", "kernel btrfs anonymous identity", "kernel btrfs valid no staging", "kernel btrfs valid symlink alias", "kernel btrfs device count mismatch", "kernel btrfs zero fsid", "kernel btrfs missing dev", "kernel btrfs nonblock member", "kernel btrfs mount device changes during read", "kernel btrfs mount mismatch", "kernel btrfs both stale aliases", "kernel btrfs stale no staging", "kernel btrfs filesystem changes during read", "kernel btrfs filesystem changes during wait", "kernel btrfs missing membership", "kernel btrfs extra membership", "kernel btrfs malformed membership", "kernel btrfs maxid mismatch", "kernel btrfs observation error", "kernel btrfs canceled observation", "kernel btrfs deadline observation", "kernel published mount mismatch", "kernel staging mount mismatch", "kernel mount identity unavailable", "kernel mount changes during capacity read", "mount changes during capacity read", "alias changes during capacity read"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			alias := filepath.Join(root, "alias")
			require.NoError(t, os.Symlink("/dev/null", alias))
			publish := filepath.Join(root, "publish")
			require.NoError(t, os.Mkdir(publish, 0700))
			stage := filepath.Join(root, "stage")
			require.NoError(t, os.Mkdir(stage, 0700))
			stageDevice := "/dev/null"
			publishedDevice := alias
			mountChanged := false
			btrfsFSChanged := false
			capacityReads := 0
			if strings.HasPrefix(scenario, "kernel ") {
				stageDevice, publishedDevice = filepath.Join(root, "block-a"), filepath.Join(root, "block-b")
				for _, device := range []string{stageDevice, publishedDevice} {
					if err := unix.Mknod(device, unix.S_IFBLK|0600, int(unix.Mkdev(7, func() uint32 {
						if scenario == "kernel btrfs both stale aliases" || scenario == "kernel btrfs stale no staging" {
							return 252
						}
						return 253
					}()))); err != nil {
						if err == unix.EPERM {
							t.Skip("block-node alias fixture requires mknod permission")
						}
						require.NoError(t, err)
					}
				}
				a, err := os.Stat(stageDevice)
				require.NoError(t, err)
				b, err := os.Stat(publishedDevice)
				require.NoError(t, err)
				require.False(t, os.SameFile(a, b), "distinct nodes must be accepted by kernel device identity, not inode")
				oldStat := nodeVolumePathStat
				t.Cleanup(func() { nodeVolumePathStat = oldStat })
				// These are synthetic mount observations. The backing nodes
				// above are real distinct OS nodes with kernel ID 7:253.
				nodeVolumePathStat = func(target string) (os.FileInfo, error) {
					if scenario == "kernel mount identity unavailable" {
						return nil, os.ErrNotExist
					}
					info, err := os.Stat(target)
					if err != nil {
						return nil, err
					}
					st := *info.Sys().(*syscall.Stat_t)
					st.Dev = unix.Mkdev(7, 253)
					if strings.Contains(scenario, "btrfs") {
						st.Dev = unix.Mkdev(0, 253)
					}
					if (scenario == "kernel published mount mismatch" && target == publish) || (scenario == "kernel staging mount mismatch" && target == stage) || mountChanged || (scenario == "kernel btrfs mount mismatch" && target == publish) {
						st.Dev = unix.Mkdev(7, 252)
						if scenario == "kernel btrfs mount device changes during read" {
							st.Dev = unix.Mkdev(0, 252)
						}
					}
					return nodeExpandMountFileInfo{FileInfo: info, state: &st}, nil
				}

			}
			if scenario == "kernel btrfs valid symlink alias" {
				require.NoError(t, os.Remove(alias))
				require.NoError(t, os.Symlink(stageDevice, alias))
				publishedDevice = alias
			}
			if scenario == "different devices" {
				publishedDevice = "/dev/zero"
			}
			oldSleep, oldNow := nodeExpandSleep, nodeNow
			t.Cleanup(func() { nodeExpandSleep, nodeNow = oldSleep, oldNow })
			now := time.Unix(0, 0)
			nodeNow = func() time.Time { return now }
			nodeExpandSleep = func(_ context.Context, d time.Duration) error {
				now = now.Add(d)
				if scenario == "kernel btrfs filesystem changes during wait" {
					btrfsFSChanged = true
				}
				return nil
			}
			ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: stage, Device: stageDevice}, {Path: publish, Device: publishedDevice}})
			mounter := ns.mounter.Interface.(*mount.FakeMounter)
			if scenario == "kernel block aliases" {
				for i := range mounter.MountPoints {
					mounter.MountPoints[i].Type = "xfs"
				}
			}
			if strings.Contains(scenario, "btrfs") {
				for i := range mounter.MountPoints {
					mounter.MountPoints[i].Type = "btrfs"
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			if scenario == "kernel btrfs deadline observation" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
			}
			defer cancel()
			if strings.Contains(scenario, "btrfs") {
				oldInfo, oldDirectory := nodeBtrfsFilesystemInfo, nodeBtrfsDevicesDirectory
				t.Cleanup(func() { nodeBtrfsFilesystemInfo, nodeBtrfsDevicesDirectory = oldInfo, oldDirectory })
				sysfs := filepath.Join(root, "sysfs")
				require.NoError(t, os.Mkdir(sysfs, 0700))
				member := filepath.Join(root, "kernel-block")
				require.NoError(t, os.Mkdir(member, 0700))
				subsystem := "/sys/class/block"
				if scenario == "kernel btrfs nonblock member" {
					subsystem = root
				}
				require.NoError(t, os.Symlink(subsystem, filepath.Join(member, "subsystem")))
				text := "7:253\n"
				if scenario == "kernel btrfs malformed membership" {
					text = "07:253\n"
				}
				if scenario != "kernel btrfs missing dev" {
					require.NoError(t, os.WriteFile(filepath.Join(member, "dev"), []byte(text), 0600))
				}
				if scenario != "kernel btrfs missing membership" {
					require.NoError(t, os.Symlink(member, filepath.Join(sysfs, "block-a")))
				}
				if scenario == "kernel btrfs extra membership" {
					require.NoError(t, os.Symlink(member, filepath.Join(sysfs, "block-b")))
				}
				nodeBtrfsDevicesDirectory = func(string) string { return sysfs }
				// Synthetic kernel FS_INFO and sysfs observations; the capacity
				// source nodes and major/minor stats are real OS block nodes.
				nodeBtrfsFilesystemInfo = func(observationCtx context.Context, target string) (nodeBtrfsKernelInfo, error) {
					require.Contains(t, []string{publish, stage}, target)
					_, bounded := observationCtx.Deadline()
					require.True(t, bounded)
					if scenario == "kernel btrfs observation error" {
						return nodeBtrfsKernelInfo{}, errors.New("ioctl observation failed")
					}
					if scenario == "kernel btrfs canceled observation" {
						cancel()
					}
					if scenario == "kernel btrfs deadline observation" {
						<-observationCtx.Done()
						return nodeBtrfsKernelInfo{}, observationCtx.Err()
					}
					fsid := [16]byte{1}
					if btrfsFSChanged {
						fsid[0] = 2
					}
					if scenario == "kernel btrfs zero fsid" {
						fsid = [16]byte{}
					}
					devices := uint64(1)
					if scenario == "kernel btrfs device count mismatch" {
						devices = 2
					}
					filesystemDevice := unix.Mkdev(0, 253)
					if scenario == "kernel btrfs mount device changes during read" && mountChanged {
						filesystemDevice = unix.Mkdev(0, 252)
					}
					maxID := uint64(1)
					if scenario == "kernel btrfs maxid mismatch" {
						maxID = 2
					}
					return nodeBtrfsKernelInfo{filesystemDevice: filesystemDevice, filesystem: fsid, maxID: maxID, devices: devices}, nil
				}
			}
			commands := &testingexec.FakeExec{}
			resizeCalls := 0
			action := func(cmd string, args ...string) utilexec.Cmd {
				return testingexec.InitFakeCmd(&testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) {
					switch cmd {
					case "blockdev":
						capacityReads++
						if scenario == "kernel btrfs mount device changes during read" {
							mountChanged = true
						}
						if scenario == "kernel btrfs filesystem changes during read" {
							btrfsFSChanged = true
						}
						if scenario == "kernel btrfs filesystem changes during wait" && capacityReads == 1 {
							return []byte("1073741824\n"), nil, nil
						}
						if scenario == "kernel mount changes during capacity read" {
							mountChanged = true
						}
						require.Len(t, args, 2)
						require.Equal(t, "--getsize64", args[0])
						require.Contains(t, []string{stageDevice, publishedDevice}, args[1])
						if scenario == "mount changes during capacity read" {
							mounter.MountPoints[1].Device = "/dev/zero"
						}
						if scenario == "alias changes during capacity read" {
							require.NoError(t, os.Remove(alias))
							require.NoError(t, os.Symlink("/dev/zero", alias))
						}
						return []byte("2147483648\n"), nil, nil
					case "blkid":
						if strings.Contains(scenario, "btrfs") {
							return []byte("TYPE=btrfs\n"), nil, nil
						}
						return []byte("TYPE=xfs\n"), nil, nil
					case "btrfs":
						require.Equal(t, []string{"filesystem", "resize", "max", publish}, args)
						resizeCalls++
						return []byte("no change"), nil, nil
					case "xfs_growfs":
						require.Equal(t, []string{"-d", publish}, args)
						resizeCalls++
						return []byte("data size unchanged, skipping\n"), nil, nil
					default:
						t.Fatalf("unexpected command %s %v", cmd, args)
						return nil, nil, nil
					}
				}}}, cmd, args...)
			}
			for range 10 {
				commands.CommandScript = append(commands.CommandScript, action)
			}
			ns.mounter.Exec = commands
			if scenario == "kernel btrfs stale no staging" || scenario == "kernel btrfs valid no staging" {
				stage = ""
			}
			response, err := ns.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: "binding-fixture", VolumePath: publish, StagingTargetPath: stage, CapacityRange: &csi.CapacityRange{RequiredBytes: 2147483648}})
			if scenario == "valid alias" || scenario == "kernel block aliases" || scenario == "kernel btrfs anonymous identity" || scenario == "kernel btrfs valid no staging" || scenario == "kernel btrfs valid symlink alias" {
				require.NoError(t, err)
				require.Equal(t, int64(2147483648), response.GetCapacityBytes())
				require.Equal(t, 1, resizeCalls)
			} else {
				wantCode := codes.FailedPrecondition
				if scenario == "kernel btrfs canceled observation" {
					wantCode = codes.Canceled
				}
				if scenario == "kernel btrfs deadline observation" {
					wantCode = codes.DeadlineExceeded
				}
				require.Equal(t, wantCode, status.Code(err))
				require.Nil(t, response)
				require.Zero(t, resizeCalls)
			}
		})
	}
}

// Opt-in local image test. No provider, host device, loop mount or cluster is
// involved. CSI gRPC invokes the real pinned mount-utils and filesystem tools.
func TestNodeExpandLocalFilesystemRPC(t *testing.T) {
	if os.Getenv("ONE_CSI_LOCAL_FILESYSTEM_TEST") != "1" {
		t.Skip("set ONE_CSI_LOCAL_FILESYSTEM_TEST=1 with local e2fsprogs to exercise an ext4 image")
	}
	for _, tool := range []string{"mkfs.ext4", "blkid", "resize2fs"} {
		_, err := exec.LookPath(tool)
		require.NoError(t, err, "required local filesystem tool %s", tool)
	}
	root := t.TempDir()
	device, alias := filepath.Join(root, "ext4.img"), filepath.Join(root, "alias")
	require.NoError(t, os.WriteFile(device, nil, 0600))
	const initial = int64(64 << 20)
	const requested = int64(128 << 20)
	require.NoError(t, os.Truncate(device, initial))
	output, err := exec.Command("mkfs.ext4", "-F", "-b", "4096", "-O", "^64bit", device).CombinedOutput()
	require.NoError(t, err, "%s", output)
	// The external tool's explicit-size no-op is the independent oracle. If
	// the RPC acknowledged an ungrown filesystem, this command would grow it
	// and report growth instead. No filesystem geometry parser is involved.
	requireFullFilesystem := func() {
		output, err := exec.Command("resize2fs", "-f", device, "131072K").CombinedOutput()
		require.NoError(t, err, "%s", output)
		require.Contains(t, string(output), "Nothing to do!", "128 MiB must already be present before the verification command")
	}
	require.NoError(t, os.Truncate(device, requested))
	require.NoError(t, os.Symlink(device, alias))
	publish, stage := filepath.Join(root, "publish"), filepath.Join(root, "stage")
	require.NoError(t, os.Mkdir(publish, 0700))
	require.NoError(t, os.Mkdir(stage, 0700))
	ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: stage, Device: device}, {Path: publish, Device: alias}})
	ns.mounter.Exec = utilexec.New()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	csi.RegisterNodeServer(server, ns)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///local-filesystem", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	client := csi.NewNodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &csi.NodeExpandVolumeRequest{VolumeId: "local-image", VolumePath: publish, StagingTargetPath: stage, CapacityRange: &csi.CapacityRange{RequiredBytes: requested}}
	for attempt := range 2 {
		response, err := client.NodeExpandVolume(ctx, req)
		require.NoError(t, err)
		require.Equal(t, requested, response.GetCapacityBytes())
		requireFullFilesystem()
		t.Logf("actual local ext4 RPC attempt=%d image_bytes=%d explicit_size_verification=no-op", attempt+1, requested)
	}
	// A mismatched published image must fail without changing either image.
	other := filepath.Join(root, "other.img")
	require.NoError(t, os.WriteFile(other, nil, 0600))
	require.NoError(t, os.Truncate(other, initial))
	ns.mounter.Interface.(*mount.FakeMounter).MountPoints[1].Device = other
	response, err := client.NodeExpandVolume(ctx, req)
	require.Nil(t, response)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	f, err := os.Open(other)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	first := make([]byte, 4096)
	_, err = io.ReadFull(f, first)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 4096), first)
	requireFullFilesystem()
	t.Log("mismatched local image rejected before mutation")
}

// Real CommandContext must stop a resize child when cancellation arrives after
// mount-utils has read the format. The marker is the external mutation oracle.
func TestNodeExpandCancellationAfterFormatProcessBoundary(t *testing.T) {
	root := t.TempDir()
	device, marker := filepath.Join(root, "device"), filepath.Join(root, "resize-ran")
	require.NoError(t, os.WriteFile(device, make([]byte, 8192), 0600))
	publish := filepath.Join(root, "publish")
	ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: publish, Device: device}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commands := &nodeExpandCancelAfterProbeExec{Interface: utilexec.New(), cancel: cancel, marker: marker}
	ns.mounter.Exec = commands
	response, err := ns.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: "binding-fixture", VolumePath: publish, CapacityRange: &csi.CapacityRange{RequiredBytes: 8192}})
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Nil(t, response)
	require.True(t, commands.probed, "exercise cancellation after the format process actually exited")
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "the resize process must never mutate after cancellation")
}

type nodeExpandCancelAfterProbeExec struct {
	utilexec.Interface
	cancel context.CancelFunc
	marker string
	probed bool
}

func (e *nodeExpandCancelAfterProbeExec) CommandContext(ctx context.Context, cmd string, args ...string) utilexec.Cmd {
	if cmd == "blkid" {
		return nodeExpandAfterProbeCmd{Cmd: e.Interface.CommandContext(ctx, "sh", "-c", "printf 'TYPE=xfs\\n'"), after: func() { e.probed = true; e.cancel() }}
	}
	return e.Interface.CommandContext(ctx, "sh", "-c", "printf mutated > \"$1\"", "sh", e.marker)
}

type nodeExpandAfterProbeCmd struct {
	utilexec.Cmd
	after func()
}

func (c nodeExpandAfterProbeCmd) CombinedOutput() ([]byte, error) {
	output, err := c.Cmd.CombinedOutput()
	if err == nil {
		c.after()
	}
	return output, err
}

func TestNodeExpandMountUtilsBindingAcrossCommands(t *testing.T) {
	for _, format := range []string{"ext3", "ext4", "xfs", "btrfs"} {
		for _, boundary := range []string{"probe", "command creation", "resize", "statfs"} {
			for _, change := range []string{"stable no-op", "valid source alias", "valid mount alias", "source retargeted", "captured source retargeted", "source missing", "published source", "staging source", "published filesystem", "staging filesystem", "unknown observation", "canceled observation", "expired observation", "btrfs fsid"} {
				if change == "btrfs fsid" && format != "btrfs" {
					continue
				}
				t.Run(format+"/"+boundary+"/"+change, func(t *testing.T) {
					root := t.TempDir()
					device, equivalent, other := filepath.Join(root, "device"), filepath.Join(root, "equivalent"), filepath.Join(root, "other")
					for i, name := range []string{device, equivalent, other} {
						minor := uint32(253)
						if i == 2 {
							minor = 252
						}
						if err := unix.Mknod(name, unix.S_IFBLK|0600, int(unix.Mkdev(7, minor))); err != nil {
							if err == unix.EPERM {
								t.Skip("block-source boundary fixture requires mknod permission")
							}
							require.NoError(t, err)
						}
					}
					alias := filepath.Join(root, "alias")
					require.NoError(t, os.Symlink(device, alias))
					publish, stage := filepath.Join(root, "publish"), filepath.Join(root, "stage")
					require.NoError(t, os.Mkdir(publish, 0700))
					require.NoError(t, os.Mkdir(stage, 0700))
					ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: publish, Device: alias, Type: format}, {Path: stage, Device: device, Type: format}})
					mounter := ns.mounter.Interface.(*mount.FakeMounter)
					ctx, cancel := context.WithCancel(context.Background())
					if change == "expired observation" {
						cancel()
						ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
					}
					defer cancel()
					oldStat, oldFS := nodeVolumePathStat, nodeVolumePathFS
					oldInfo, oldDirectory := nodeBtrfsFilesystemInfo, nodeBtrfsDevicesDirectory
					t.Cleanup(func() {
						nodeVolumePathStat, nodeVolumePathFS = oldStat, oldFS
						nodeBtrfsFilesystemInfo, nodeBtrfsDevicesDirectory = oldInfo, oldDirectory
					})
					changed := false
					filesystemDevice := func(target string) uint64 {
						major, minor := uint32(7), uint32(253)
						if format == "btrfs" {
							major = 0
						}
						if changed && ((change == "published filesystem" && target == publish) || (change == "staging filesystem" && target == stage)) {
							minor = 252
						}
						return unix.Mkdev(major, minor)
					}
					nodeVolumePathStat = func(target string) (os.FileInfo, error) {
						if changed {
							switch change {
							case "unknown observation":
								return nil, os.ErrNotExist
							case "canceled observation":
								cancel()
								return nil, os.ErrNotExist
							case "expired observation":
								<-ctx.Done()
								return nil, os.ErrNotExist
							}
						}
						info, err := os.Stat(target)
						if err != nil {
							return nil, err
						}
						st := *info.Sys().(*syscall.Stat_t)
						st.Dev = filesystemDevice(target)
						return nodeExpandMountFileInfo{FileInfo: info, state: &st}, nil
					}
					if format == "btrfs" {
						sysfs, member := filepath.Join(root, "sysfs"), filepath.Join(root, "member")
						require.NoError(t, os.Mkdir(sysfs, 0700))
						require.NoError(t, os.Mkdir(member, 0700))
						require.NoError(t, os.Symlink("/sys/class/block", filepath.Join(member, "subsystem")))
						require.NoError(t, os.WriteFile(filepath.Join(member, "dev"), []byte("7:253\n"), 0600))
						require.NoError(t, os.Symlink(member, filepath.Join(sysfs, "device")))
						nodeBtrfsDevicesDirectory = func(string) string { return sysfs }
						nodeBtrfsFilesystemInfo = func(_ context.Context, target string) (nodeBtrfsKernelInfo, error) {
							fsid := [16]byte{1}
							if changed && change == "btrfs fsid" {
								fsid[0] = 2
							}
							return nodeBtrfsKernelInfo{filesystemDevice: filesystemDevice(target), filesystem: fsid, maxID: 1, devices: 1}, nil
						}
					}
					transition := func(at string) {
						if boundary != at {
							return
						}
						require.False(t, changed, "the external boundary must be exercised once")
						changed = true
						switch change {
						case "valid source alias", "source retargeted", "captured source retargeted", "source missing":
							require.NoError(t, os.Remove(alias))
							target := other
							if change == "valid source alias" {
								target = equivalent
							}
							if change != "source missing" {
								require.NoError(t, os.Symlink(target, alias))
							}
							if change == "captured source retargeted" {
								mounter.MountPoints[0].Device = equivalent
							}
						case "valid mount alias":
							mounter.MountPoints[0].Device, mounter.MountPoints[1].Device = equivalent, equivalent
						case "published source":
							mounter.MountPoints[0].Device = other
						case "staging source":
							mounter.MountPoints[1].Device = other
						}
					}
					statfsCalls, resizeCalls, probeCalls := 0, 0, 0
					nodeVolumePathFS = func(_ string, st *unix.Statfs_t) error {
						statfsCalls++
						transition("statfs")
						st.Bsize, st.Blocks = 4096, 1
						return nil
					}
					commands := &testingexec.FakeExec{}
					action := func(cmd string, args ...string) utilexec.Cmd {
						if cmd == "resize2fs" || cmd == "xfs_growfs" || cmd == "btrfs" {
							transition("command creation")
						}
						return testingexec.InitFakeCmd(&testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) {
							switch cmd {
							case "blockdev":
								require.Equal(t, []string{"--getsize64", alias}, args)
								return []byte("2147483648\n"), nil, nil
							case "blkid":
								require.Equal(t, alias, args[len(args)-1])
								probeCalls++
								transition("probe")
								return []byte("TYPE=" + format + "\n"), nil, nil
							case "resize2fs":
								require.Contains(t, []string{"ext3", "ext4"}, format)
								require.Equal(t, []string{alias}, args)
							case "xfs_growfs":
								require.Equal(t, "xfs", format)
								require.Equal(t, []string{"-d", publish}, args)
							case "btrfs":
								require.Equal(t, "btrfs", format)
								require.Equal(t, []string{"filesystem", "resize", "max", publish}, args)
							default:
								t.Fatalf("unexpected command %s %v", cmd, args)
							}
							resizeCalls++
							transition("resize")
							return []byte("Nothing to do!\n"), nil, nil
						}}}, cmd, args...)
					}
					for range 4 {
						commands.CommandScript = append(commands.CommandScript, action)
					}
					ns.mounter.Exec = commands
					response, err := ns.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: "command-boundary-fixture", VolumePath: publish, StagingTargetPath: stage, CapacityRange: &csi.CapacityRange{RequiredBytes: 2147483648}})
					require.True(t, changed, "exercise the selected external boundary")
					require.Equal(t, 1, probeCalls)
					valid := change == "stable no-op" || change == "valid source alias" || change == "valid mount alias"
					if valid {
						require.NoError(t, err)
						require.Equal(t, int64(2147483648), response.GetCapacityBytes())
					} else {
						want := codes.FailedPrecondition
						if change == "canceled observation" {
							want = codes.Canceled
						}
						if change == "expired observation" {
							want = codes.DeadlineExceeded
						}
						require.Equal(t, want, status.Code(err), "%v", err)
						require.Nil(t, response)
						if change == "captured source retargeted" {
							require.Contains(t, err.Error(), "resize source identity changed")
						}
					}
					wantResize, wantStatfs := 1, 1
					if !valid && boundary != "statfs" {
						wantStatfs = 0
						if boundary != "resize" {
							wantResize = 0
						}
					}
					require.Equal(t, wantResize, resizeCalls, "no mutating child may run after an invalid observation")
					require.Equal(t, wantStatfs, statfsCalls, "reject changed bindings immediately after resize")
				})
			}
		}
	}
}

// Override only the mount-stat external boundary; device-node identity is real.
type nodeExpandMountFileInfo struct {
	os.FileInfo
	state *syscall.Stat_t
}

func (f nodeExpandMountFileInfo) Sys() any { return f.state }

// This is a real read-only ioctl failure against a non-Btrfs local kernel
// filesystem. Positive mounted Btrfs observations remain boundary fixtures.
func TestNodeExpandBtrfsKernelObservation(t *testing.T) {
	_, err := readNodeBtrfsFilesystemInfo(context.Background(), "/proc/self")
	require.Error(t, err, "a procfs directory cannot provide Btrfs FS_INFO")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = readNodeBtrfsFilesystemInfo(ctx, "/proc/self")
	require.ErrorIs(t, err, context.Canceled)
}

// A mount observed as Btrfs must never use the regular-image or character-device
// capacity shortcuts, even when all source aliases consistently name B.
func TestNodeExpandBtrfsRejectsNonBlockSources(t *testing.T) {
	for _, scenario := range []string{"regular both aliases", "regular no staging", "character both aliases", "character no staging", "regular Btrfs staging only"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			image := filepath.Join(root, "image-b")
			require.NoError(t, os.WriteFile(image, nil, 0600))
			require.NoError(t, os.Truncate(image, 2147483648))
			source := image
			if strings.HasPrefix(scenario, "character") {
				source = "/dev/null"
			}
			stageSource, publishSource := filepath.Join(root, "stage-alias"), filepath.Join(root, "publish-alias")
			require.NoError(t, os.Symlink(source, stageSource))
			require.NoError(t, os.Symlink(source, publishSource))
			stage, publish := filepath.Join(root, "stage"), filepath.Join(root, "publish")
			require.NoError(t, os.Mkdir(stage, 0700))
			require.NoError(t, os.Mkdir(publish, 0700))
			publishType := "btrfs"
			if scenario == "regular Btrfs staging only" {
				publishType = "xfs"
			}
			ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: stage, Device: stageSource, Type: "btrfs"}, {Path: publish, Device: publishSource, Type: publishType}})
			commands := &testingexec.FakeExec{}
			capacityCalls, resizeCalls, allCalls := 0, 0, 0
			action := func(cmd string, args ...string) utilexec.Cmd {
				allCalls++
				return testingexec.InitFakeCmd(&testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) {
					switch cmd {
					case "blockdev":
						capacityCalls++
						return []byte("2147483648\n"), nil, nil
					case "blkid":
						return []byte("TYPE=btrfs\n"), nil, nil
					case "btrfs":
						resizeCalls++
						require.Equal(t, []string{"filesystem", "resize", "max", publish}, args)
						return []byte("resized"), nil, nil
					default:
						t.Fatalf("unexpected command %s %v", cmd, args)
						return nil, nil, nil
					}
				}}}, cmd, args...)
			}
			for range 16 {
				commands.CommandScript = append(commands.CommandScript, action)
			}
			ns.mounter.Exec = commands
			if strings.Contains(scenario, "no staging") {
				stage = ""
			}
			response, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: "btrfs-nonblock-fixture", VolumePath: publish, StagingTargetPath: stage, CapacityRange: &csi.CapacityRange{RequiredBytes: 2147483648}})
			t.Logf("capacity_calls=%d resize_calls=%d all_commands=%d response=%v error=%v", capacityCalls, resizeCalls, allCalls, response, err)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Nil(t, response)
			require.Zero(t, resizeCalls)
			require.Zero(t, capacityCalls)
			require.Zero(t, allCalls, "reject before format probes or capacity commands")
		})
	}
}

// Observed real filesystem types must not use the blank-type local-image
// contract to measure image B while the mounted filesystem is still A.
func TestNodeExpandObservedFilesystemRejectsNonBlockSources(t *testing.T) {
	for _, fsType := range []string{"ext3", "ext4", "xfs", "btrfs"} {
		for _, scenario := range []string{"regular both aliases", "regular no staging", "character both aliases", "character no staging", "regular staging type only"} {
			t.Run(fsType+"/"+scenario, func(t *testing.T) {
				root := t.TempDir()
				image := filepath.Join(root, "image-b")
				require.NoError(t, os.WriteFile(image, nil, 0600))
				require.NoError(t, os.Truncate(image, 2147483648))
				source := image
				if strings.HasPrefix(scenario, "character") {
					source = "/dev/null"
				}
				stageSource, publishSource := filepath.Join(root, "stage-alias"), filepath.Join(root, "publish-alias")
				require.NoError(t, os.Symlink(source, stageSource))
				require.NoError(t, os.Symlink(source, publishSource))
				stage, publish := filepath.Join(root, "stage"), filepath.Join(root, "publish")
				require.NoError(t, os.Mkdir(stage, 0700))
				require.NoError(t, os.Mkdir(publish, 0700))
				publishType := fsType
				if scenario == "regular staging type only" {
					publishType = ""
				}
				ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: stage, Device: stageSource, Type: fsType}, {Path: publish, Device: publishSource, Type: publishType}})
				commands := &testingexec.FakeExec{}
				capacityCalls, resizeCalls, allCalls := 0, 0, 0
				action := func(cmd string, args ...string) utilexec.Cmd {
					allCalls++
					return testingexec.InitFakeCmd(&testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) {
						switch cmd {
						case "blockdev":
							capacityCalls++
							require.Equal(t, []string{"--getsize64", publishSource}, args)
							return []byte("2147483648\n"), nil, nil
						case "blkid":
							return []byte("TYPE=" + fsType + "\n"), nil, nil
						case "resize2fs":
							require.Contains(t, []string{"ext3", "ext4"}, fsType)
							require.Equal(t, []string{publishSource}, args)
							resizeCalls++
						case "xfs_growfs":
							require.Equal(t, "xfs", fsType)
							require.Equal(t, []string{"-d", publish}, args)
							resizeCalls++
						case "btrfs":
							require.Equal(t, "btrfs", fsType)
							require.Equal(t, []string{"filesystem", "resize", "max", publish}, args)
							resizeCalls++
						default:
							t.Fatalf("unexpected command %s %v", cmd, args)
						}
						return []byte("tool convergence"), nil, nil
					}}}, cmd, args...)
				}
				for range 16 {
					commands.CommandScript = append(commands.CommandScript, action)
				}
				ns.mounter.Exec = commands
				if strings.Contains(scenario, "no staging") {
					stage = ""
				}
				response, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: "observed-fs-nonblock-fixture", VolumePath: publish, StagingTargetPath: stage, CapacityRange: &csi.CapacityRange{RequiredBytes: 2147483648}})
				t.Logf("capacity_calls=%d resize_calls=%d all_commands=%d response=%v error=%v", capacityCalls, resizeCalls, allCalls, response, err)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Nil(t, response)
				require.Zero(t, resizeCalls)
				require.Zero(t, capacityCalls)
				require.Zero(t, allCalls, "observed filesystem admission must precede capacity/format/resize commands")
			})
		}
	}
}
