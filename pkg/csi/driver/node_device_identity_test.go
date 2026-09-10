package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	utilexec "k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

func reviewKernelBlockDevice(t *testing.T) string {
	t.Helper()
	originalStat, originalRoot, originalRead := nodeDeviceStat, nodeSysDevBlockPath, nodeReadIdentityFile
	root := t.TempDir()
	kernel := filepath.Join(root, "class", "block", "testdisk")
	require.NoError(t, os.MkdirAll(filepath.Join(kernel, "device"), 0750))
	nodeSysDevBlockPath = filepath.Join(root, "dev", "block")
	require.NoError(t, os.MkdirAll(nodeSysDevBlockPath, 0750))
	deviceID := fmt.Sprintf("%d:%d", unix.Major(0x810), unix.Minor(0x810))
	require.NoError(t, os.Symlink(kernel, filepath.Join(nodeSysDevBlockPath, deviceID)))
	nodeDeviceStat = func(path string, stat *unix.Stat_t) error {
		if err := originalStat(path, stat); err != nil {
			return err
		}
		stat.Mode = unix.S_IFBLK | 0600
		stat.Rdev = 0x810
		return nil
	}
	t.Cleanup(func() {
		nodeDeviceStat, nodeSysDevBlockPath, nodeReadIdentityFile = originalStat, originalRoot, originalRead
	})
	return kernel
}

func reviewDeviceSerialFixture(t *testing.T, serialForPath func(string) string) *testingexec.FakeExec {
	t.Helper()
	reviewKernelBlockDevice(t)
	read := nodeReadIdentityFile
	nodeReadIdentityFile = func(path string) ([]byte, error) {
		if filepath.Base(path) == "serial" {
			return []byte(serialForPath(path)), nil
		}
		return read(path)
	}
	return reviewDeviceCommandExec("")
}

func reviewDeviceCommandExec(staleSerial string) *testingexec.FakeExec {
	exec := &testingexec.FakeExec{LookPathFunc: func(path string) (string, error) { return path, nil }}
	for i := 0; i < 512; i++ {
		exec.CommandScript = append(exec.CommandScript, func(command string, args ...string) utilexec.Cmd {
			output := ""
			if len(args) > 0 {
				device := args[len(args)-1]
				switch command {
				case "lsblk":
					if args[0] == "--json" {
						payload, _ := json.Marshal(map[string]any{"blockdevices": []map[string]string{{"path": device, "serial": staleSerial}}})
						output = string(payload)
					} else {
						output = staleSerial
					}
				case "udevadm":
					if args[0] == "info" {
						output = "DEVNAME=" + device + "\nDEVPATH=/devices/test/block/testdisk\nDEVTYPE=disk\nDISKSEQ=26\nMAJOR=8\nMINOR=16\nSUBSYSTEM=block\n"
						if staleSerial != "" {
							output += "ID_SERIAL=" + staleSerial + "\nID_SERIAL_SHORT=" + staleSerial + "\n"
						}
					}
				case "blkid":
					output = "TYPE=ext4\n"
				}
			}
			action := func() ([]byte, []byte, error) { return []byte(output), nil, nil }
			return &testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{action}, OutputScript: []testingexec.FakeAction{action}}
		})
	}
	return exec
}

func TestNodeDeviceResolverKernelSerial(t *testing.T) {
	for _, source := range []string{"alias", "by-id"} {
		for _, tc := range []struct {
			name, file, serial, staleSerial string
			payload                         []byte
		}{
			{name: "virtio", file: "serial", payload: []byte("onecsi-42\n"), serial: "onecsi-42"},
			{name: "virtio-mismatch-stale-udev", file: "serial", payload: []byte("onecsi-99\n"), serial: "onecsi-99", staleSerial: "onecsi-42"},
			{name: "virtio-current-over-stale-udev", file: "serial", payload: []byte("onecsi-42\n"), serial: "onecsi-42", staleSerial: "onecsi-99"},
			{name: "virtio-empty", file: "serial", payload: []byte("\n"), staleSerial: "onecsi-42"},
			{name: "virtio-invalid", file: "serial", payload: []byte("onecsi-42\x00"), staleSerial: "onecsi-42"},
			{name: "scsi", file: "device/vpd_pg80", payload: append([]byte{0, 0x80, 0, 9}, []byte("onecsi-42")...), serial: "onecsi-42"},
			{name: "scsi-mismatch-stale-udev", file: "device/vpd_pg80", payload: append([]byte{0, 0x80, 0, 9}, []byte("onecsi-99")...), serial: "onecsi-99", staleSerial: "onecsi-42"},
			{name: "scsi-truncated-header", file: "device/vpd_pg80", payload: []byte{0, 0x80, 0}, staleSerial: "onecsi-42"},
			{name: "scsi-truncated-payload", file: "device/vpd_pg80", payload: append([]byte{0, 0x80, 0, 10}, []byte("onecsi-42")...), staleSerial: "onecsi-42"},
			{name: "scsi-wrong-page", file: "device/vpd_pg80", payload: append([]byte{0, 0x83, 0, 9}, []byte("onecsi-42")...), staleSerial: "onecsi-42"},
			{name: "scsi-wrong-type", file: "device/vpd_pg80", payload: append([]byte{5, 0x80, 0, 9}, []byte("onecsi-42")...), staleSerial: "onecsi-42"},
			{name: "scsi-invalid-qualifier", file: "device/vpd_pg80", payload: append([]byte{0x20, 0x80, 0, 9}, []byte("onecsi-42")...), staleSerial: "onecsi-42"},
			{name: "scsi-trailing-data", file: "device/vpd_pg80", payload: append([]byte{0, 0x80, 0, 8}, []byte("onecsi-42")...), staleSerial: "onecsi-42"},
			{name: "scsi-empty", file: "device/vpd_pg80", payload: []byte{0, 0x80, 0, 0}, staleSerial: "onecsi-42"},
			{name: "scsi-control-byte", file: "device/vpd_pg80", payload: append([]byte{0, 0x80, 0, 10}, []byte("onecsi-42\x00")...), staleSerial: "onecsi-42"},
			{name: "unknown", staleSerial: "onecsi-42"},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				disk := withTestDiskPath(t)
				device := filepath.Join(disk, "sdd")
				require.NoError(t, os.WriteFile(device, nil, 0600))
				kernel := reviewKernelBlockDevice(t)
				if tc.file != "" {
					require.NoError(t, os.WriteFile(filepath.Join(kernel, tc.file), tc.payload, 0600))
				}
				if source == "by-id" {
					byID := filepath.Join(disk, "disk", "by-id", "virtio-onecsi-42")
					require.NoError(t, os.MkdirAll(filepath.Dir(byID), 0750))
					require.NoError(t, os.Symlink(device, byID))
				}
				cfg := config.LoadConfiguration()
				cfg.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
				cfg.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
				exec := reviewDeviceCommandExec(tc.staleSerial)
				resolver := NewNodeDeviceResolver(cfg, exec, func(string) []string { return []string{device} })
				path, result, err := resolver.Resolve(context.Background(), "vol-1", "sdd", map[string]string{publishContextDeviceSerial: "onecsi-42"}, 0)
				require.Equal(t, "onecsi-42", result.ExpectedSerial)
				require.Equal(t, tc.serial, observedDeviceSerial(nil, device))
				if tc.serial == "onecsi-42" {
					require.NoError(t, err)
					require.NotEmpty(t, path)
					require.Equal(t, "onecsi-42", resolver.cache["vol-1"].Serial)
				} else {
					require.Error(t, err)
					require.Empty(t, path)
					require.Empty(t, resolver.cache)
				}
			})
		}
	}
}

func TestNodeDeviceResolverRejectsChangedKernelDevice(t *testing.T) {
	for _, scenario := range []string{"regular-file", "character-device", "changed-inode", "changed-device-number", "retargeted-symlink"} {
		t.Run(scenario, func(t *testing.T) {
			disk := withTestDiskPath(t)
			device := filepath.Join(disk, "sdd")
			require.NoError(t, os.WriteFile(device, nil, 0600))
			kernel := reviewKernelBlockDevice(t)
			require.NoError(t, os.WriteFile(filepath.Join(kernel, "serial"), []byte("onecsi-42"), 0600))
			stat := nodeDeviceStat
			calls := 0
			nodeDeviceStat = func(path string, state *unix.Stat_t) error {
				if err := stat(path, state); err != nil {
					return err
				}
				calls++
				switch scenario {
				case "regular-file":
					state.Mode = unix.S_IFREG
				case "character-device":
					state.Mode = unix.S_IFCHR
				case "changed-inode":
					if calls > 1 {
						state.Ino++
					}
				case "changed-device-number":
					if calls > 1 {
						state.Rdev++
					}
				}
				return nil
			}
			if scenario == "retargeted-symlink" {
				other := filepath.Join(disk, "vdd")
				require.NoError(t, os.WriteFile(other, nil, 0600))
				alias := filepath.Join(disk, "alias")
				require.NoError(t, os.Symlink(device, alias))
				read := nodeReadIdentityFile
				nodeReadIdentityFile = func(path string) ([]byte, error) {
					require.NoError(t, os.Remove(alias))
					require.NoError(t, os.Symlink(other, alias))
					return read(path)
				}
				device = alias
			}
			serial, err := currentDeviceSerial(device)
			require.Error(t, err)
			require.Empty(t, serial)
		})
	}
}
