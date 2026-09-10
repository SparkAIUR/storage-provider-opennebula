package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeDeviceResolverPrefersByIDAndThenCache(t *testing.T) {
	diskPath := withTestDiskPath(t)
	require.NoError(t, os.MkdirAll(filepath.Join(diskPath, "disk", "by-id"), 0o755))
	devicePath := filepath.Join(diskPath, "vde")
	require.NoError(t, os.WriteFile(devicePath, []byte("test"), 0o644))
	byIDPath := filepath.Join(diskPath, "disk", "by-id", "virtio-onecsi-42")
	require.NoError(t, os.Symlink(devicePath, byIDPath))
	resolvedDevicePath, err := filepath.EvalSymlinks(devicePath)
	require.NoError(t, err)

	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
	pluginConfig.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
	resolver := NewNodeDeviceResolver(pluginConfig, reviewDeviceSerialFixture(t, func(string) string { return "onecsi-42" }), func(string) []string {
		return []string{filepath.Join(diskPath, "sde"), devicePath}
	})

	path, resolution, err := resolver.Resolve(context.Background(), "vol-1", "sde", map[string]string{
		publishContextDeviceSerial:      "onecsi-42",
		publishContextOpenNebulaImageID: "42",
	}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, resolvedDevicePath, path)
	assert.Equal(t, "by-id", resolution.ResolvedBy)

	path, resolution, err = resolver.Resolve(context.Background(), "vol-1", "sde", map[string]string{
		publishContextDeviceSerial:      "onecsi-42",
		publishContextOpenNebulaImageID: "42",
	}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, resolvedDevicePath, path)
	assert.Equal(t, "cache", resolution.ResolvedBy)
}

func TestNodeDeviceResolverInvalidatesCacheOnSerialMismatch(t *testing.T) {
	diskPath := withTestDiskPath(t)
	oldDevicePath := filepath.Join(diskPath, "vde")
	require.NoError(t, os.WriteFile(oldDevicePath, []byte("test"), 0o644))
	newDevicePath := filepath.Join(diskPath, "sde")
	require.NoError(t, os.WriteFile(newDevicePath, []byte("test"), 0o644))

	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
	pluginConfig.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
	resolver := NewNodeDeviceResolver(pluginConfig, reviewDeviceSerialFixture(t, func(string) string { return "onecsi-43" }), func(string) []string {
		return []string{newDevicePath}
	})
	resolver.remember("vol-1", "42", "onecsi-42", oldDevicePath, filepath.Join(diskPath, "disk", "by-id", "virtio-onecsi-42"))

	path, resolution, err := resolver.Resolve(context.Background(), "vol-1", "sde", map[string]string{
		publishContextDeviceSerial: "onecsi-43",
	}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, newDevicePath, path)
	assert.Equal(t, "exact", resolution.ResolvedBy)
}

func TestNodeDeviceResolverRejectsUnverifiedCandidates(t *testing.T) {
	for _, source := range []string{"alias", "alias-recovery", "by-id", "cache", "cache-without-context", "stale-by-id-cache"} {
		for _, observed := range []string{"onecsi-99", ""} {
			t.Run(source+"/serial="+observed, func(t *testing.T) {
				disk := withTestDiskPath(t)
				device := filepath.Join(disk, "sdd")
				require.NoError(t, os.WriteFile(device, nil, 0600))
				byID := filepath.Join(disk, "disk", "by-id", "virtio-onecsi-42")
				if source == "by-id" || source == "stale-by-id-cache" {
					require.NoError(t, os.MkdirAll(filepath.Dir(byID), 0750))
					require.NoError(t, os.Symlink(device, byID))
				}
				cfg := config.LoadConfiguration()
				cfg.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
				cfg.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
				serial := observed
				resolver := NewNodeDeviceResolver(cfg, reviewDeviceSerialFixture(t, func(string) string { return serial }), func(string) []string { return []string{device} })
				if source == "cache" || source == "cache-without-context" || source == "stale-by-id-cache" {
					resolver.remember("vol-1", "42", "onecsi-42", device, byID)
					if source == "stale-by-id-cache" {
						require.NoError(t, os.Remove(byID))
					}
				}
				if source == "alias-recovery" {
					stat := nodeVolumePathStat
					first := true
					nodeVolumePathStat = func(path string) (os.FileInfo, error) {
						if path == device && first {
							first = false
							return nil, os.ErrNotExist
						}
						return stat(path)
					}
				}
				publishContext := map[string]string{publishContextDeviceSerial: "onecsi-42"}
				if source == "cache-without-context" {
					delete(publishContext, publishContextDeviceSerial)
				}
				path, _, err := resolver.Resolve(context.Background(), "vol-1", "sdd", publishContext, 0)
				require.Error(t, err)
				require.Empty(t, path)
				if source == "cache" || source == "cache-without-context" || source == "stale-by-id-cache" {
					require.Equal(t, "onecsi-42", resolver.cache["vol-1"].Serial)
					require.Empty(t, resolver.cache["vol-1"].DevicePath)
					require.Empty(t, resolver.cache["vol-1"].ByIDPath)
				} else {
					require.Empty(t, resolver.cache)
				}
				serial = "onecsi-42"
				publishContext[publishContextDeviceSerial] = serial
				path, _, err = resolver.Resolve(context.Background(), "vol-1", "sdd", publishContext, 0)
				require.NoError(t, err)
				resolved, err := filepath.EvalSymlinks(device)
				require.NoError(t, err)
				actual, err := filepath.EvalSymlinks(path)
				require.NoError(t, err)
				require.Equal(t, resolved, actual)
				require.Equal(t, "onecsi-42", resolver.cache["vol-1"].Serial)
			})
		}
	}
}

func TestNodeDeviceResolverRetainedSerialAndRecovery(t *testing.T) {
	for _, scenario := range []string{"report-only", "conflicting-context", "alias-recovery", "identity-free-legacy"} {
		t.Run(scenario, func(t *testing.T) {
			disk := withTestDiskPath(t)
			device := filepath.Join(disk, "vdd")
			require.NoError(t, os.WriteFile(device, nil, 0600))
			cfg := config.LoadConfiguration()
			cfg.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
			cfg.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
			serialReads := 0
			resolver := NewNodeDeviceResolver(cfg, reviewDeviceSerialFixture(t, func(string) string {
				serialReads++
				if scenario == "alias-recovery" && serialReads > 1 {
					return "onecsi-42"
				}
				return "onecsi-99"
			}), func(string) []string { return []string{device} })
			publishContext := map[string]string{}
			expected := "onecsi-42"
			switch scenario {
			case "conflicting-context":
				publishContext[publishContextDeviceSerial] = "onecsi-99"
			case "identity-free-legacy":
				expected = ""
			}
			path, result, err := resolver.Resolve(context.Background(), "vol-1", "sdd", publishContext, 0, expected)
			if scenario == "report-only" || scenario == "conflicting-context" {
				require.Error(t, err)
				require.Empty(t, path)
				require.Empty(t, resolver.cache)
			} else {
				require.NoError(t, err)
				require.Equal(t, device, path)
				if scenario == "alias-recovery" {
					require.Equal(t, "alias-recovery", result.ResolvedBy)
				} else {
					require.Zero(t, serialReads)
				}
			}
		})
	}
}

func reviewDeviceResolutionClock(t *testing.T) {
	t.Helper()
	original := deviceResolverNow
	now := time.Now()
	deviceResolverNow = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	t.Cleanup(func() { deviceResolverNow = original })
}
