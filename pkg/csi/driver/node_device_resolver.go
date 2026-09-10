package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"
)

var (
	nodeByIDGlob      = filepath.Glob
	nodeEvalSymlinks  = filepath.EvalSymlinks
	nodeWriteFile     = os.WriteFile
	nodeReadDir       = os.ReadDir
	deviceResolverNow = func() time.Time { return time.Now() }
)

const (
	publishContextDeviceSerial      = "deviceSerial"
	publishContextOpenNebulaImageID = "opennebulaImageID"
)

type nodeDeviceCacheEntry struct {
	VolumeID        string
	ImageID         string
	Serial          string
	DevicePath      string
	ByIDPath        string
	LastConfirmedAt time.Time
}

type NodeDeviceResolver struct {
	exec               utilexec.Interface
	cacheEnabled       bool
	cacheTTL           time.Duration
	udevSettleTimeout  time.Duration
	rescanOnMiss       bool
	deviceCandidatesFn func(volumeName string) []string
	mu                 sync.RWMutex
	cache              map[string]nodeDeviceCacheEntry
	udevadmMissingLog  bool
}

func NewNodeDeviceResolver(cfg config.CSIPluginConfig, exec utilexec.Interface, candidatesFn func(volumeName string) []string) *NodeDeviceResolver {
	cacheEnabled, ok := cfg.GetBool(config.NodeDeviceCacheEnabledVar)
	if !ok {
		cacheEnabled = true
	}
	cacheTTLSeconds, ok := cfg.GetInt(config.NodeDeviceCacheTTLSecondsVar)
	if !ok || cacheTTLSeconds <= 0 {
		cacheTTLSeconds = 600
	}
	udevSettleTimeoutSeconds, ok := cfg.GetInt(config.NodeDeviceUdevSettleTimeoutSecondsVar)
	if !ok {
		udevSettleTimeoutSeconds = 10
	}
	rescanOnMiss, ok := cfg.GetBool(config.NodeDeviceRescanOnMissEnabledVar)
	if !ok {
		rescanOnMiss = true
	}
	return &NodeDeviceResolver{
		exec:               exec,
		cacheEnabled:       cacheEnabled,
		cacheTTL:           time.Duration(cacheTTLSeconds) * time.Second,
		udevSettleTimeout:  time.Duration(udevSettleTimeoutSeconds) * time.Second,
		rescanOnMiss:       rescanOnMiss,
		deviceCandidatesFn: candidatesFn,
		cache:              map[string]nodeDeviceCacheEntry{},
	}
}

func (r *NodeDeviceResolver) Resolve(ctx context.Context, volumeID, volumeName string, publishContext map[string]string, timeout time.Duration, expectedSerials ...string) (string, deviceResolutionResult, error) {
	started := deviceResolverNow()
	serial := strings.TrimSpace(publishContext[publishContextDeviceSerial])
	for _, expected := range expectedSerials {
		expected = strings.TrimSpace(expected)
		if expected == "" {
			continue
		}
		if serial != "" && !strings.EqualFold(serial, expected) {
			r.Invalidate(volumeID)
			return "", deviceResolutionResult{}, fmt.Errorf("conflicting expected device serials %q and %q", serial, expected)
		}
		serial = expected
	}
	if serial == "" && volumeID != "" {
		r.mu.RLock()
		serial = r.cache[volumeID].Serial
		r.mu.RUnlock()
	}
	imageID := strings.TrimSpace(publishContext[publishContextOpenNebulaImageID])
	if volumeID != "" {
		if path, ok := r.resolveFromCache(volumeID, serial); ok {
			return path.DevicePath, deviceResolutionResult{ResolvedBy: "cache", Latency: deviceResolverNow().Sub(started)}, nil
		}
	}

	if serial != "" {
		if path, ok := r.resolveByID(serial); ok {
			r.remember(volumeID, imageID, serial, path.DevicePath, path.ByIDPath)
			return path.DevicePath, deviceResolutionResult{ResolvedBy: "by-id", Latency: deviceResolverNow().Sub(started)}, nil
		}
	}

	if devicePath, result, err := r.resolveAlias(volumeName, serial, started); err == nil {
		r.remember(volumeID, imageID, serial, devicePath, "")
		return devicePath, result, nil
	}

	deadline := deviceResolverNow().Add(timeout)
	for {
		if timeout > 0 && deviceResolverNow().After(deadline) {
			break
		}
		r.runRecoverySequence(ctx)
		if serial != "" {
			if path, ok := r.resolveByID(serial); ok {
				r.remember(volumeID, imageID, serial, path.DevicePath, path.ByIDPath)
				return path.DevicePath, deviceResolutionResult{ResolvedBy: "by-id-recovery", Latency: deviceResolverNow().Sub(started)}, nil
			}
		}
		if devicePath, result, err := r.resolveAlias(volumeName, serial, started); err == nil {
			r.remember(volumeID, imageID, serial, devicePath, "")
			if result.ResolvedBy == "exact" {
				result.ResolvedBy = "exact-recovery"
			} else {
				result.ResolvedBy = "alias-recovery"
			}
			return devicePath, result, nil
		}
		if timeout <= 0 {
			break
		}
		nodeDeviceSleep(defaultNodeDevicePollPeriod)
	}

	candidates := []string{}
	if r.deviceCandidatesFn != nil {
		candidates = r.deviceCandidatesFn(volumeName)
	}
	if serial != "" {
		return "", deviceResolutionResult{}, fmt.Errorf("timed out after %s waiting for device path for volume %q (serial %s, checked %s)", timeout, volumeName, serial, strings.Join(candidates, ", "))
	}
	return "", deviceResolutionResult{}, fmt.Errorf("timed out after %s waiting for device path for volume %q (checked %s)", timeout, volumeName, strings.Join(candidates, ", "))
}

func (r *NodeDeviceResolver) Invalidate(volumeID string) {
	if r == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	r.mu.Lock()
	delete(r.cache, volumeID)
	r.mu.Unlock()
}

type resolvedDevicePath struct {
	DevicePath string
	ByIDPath   string
}

func (r *NodeDeviceResolver) resolveFromCache(volumeID, serial string) (resolvedDevicePath, bool) {
	if r == nil || !r.cacheEnabled || strings.TrimSpace(volumeID) == "" {
		return resolvedDevicePath{}, false
	}
	r.mu.RLock()
	entry, ok := r.cache[volumeID]
	r.mu.RUnlock()
	if !ok {
		return resolvedDevicePath{}, false
	}
	if r.cacheTTL > 0 && deviceResolverNow().Sub(entry.LastConfirmedAt) > r.cacheTTL {
		r.Invalidate(volumeID)
		return resolvedDevicePath{}, false
	}
	if serial != "" && entry.Serial != "" && !strings.EqualFold(strings.TrimSpace(serial), strings.TrimSpace(entry.Serial)) {
		r.Invalidate(volumeID)
		return resolvedDevicePath{}, false
	}
	if entry.DevicePath == "" {
		r.Invalidate(volumeID)
		return resolvedDevicePath{}, false
	}
	if _, err := nodeVolumePathStat(entry.DevicePath); err != nil {
		r.Invalidate(volumeID)
		return resolvedDevicePath{}, false
	}
	if serial != "" {
		if !deviceMatchesSerial(r.exec, entry.DevicePath, serial) {
			r.Invalidate(volumeID)
			return resolvedDevicePath{}, false
		}
	}
	return resolvedDevicePath{DevicePath: entry.DevicePath, ByIDPath: entry.ByIDPath}, true
}

func (r *NodeDeviceResolver) resolveByID(serial string) (resolvedDevicePath, bool) {
	if strings.TrimSpace(serial) == "" {
		return resolvedDevicePath{}, false
	}
	candidates, err := nodeByIDGlob(filepath.Join(defaultDiskPath, "disk", "by-id", "*"))
	if err != nil {
		return resolvedDevicePath{}, false
	}
	for _, candidate := range candidates {
		base := filepath.Base(candidate)
		if !strings.Contains(strings.ToLower(base), strings.ToLower(serial)) {
			continue
		}
		resolved, err := nodeEvalSymlinks(candidate)
		if err != nil {
			continue
		}
		if _, err := nodeVolumePathStat(resolved); err != nil {
			continue
		}
		if deviceMatchesSerial(r.exec, resolved, serial) {
			return resolvedDevicePath{DevicePath: resolved, ByIDPath: candidate}, true
		}
	}
	return resolvedDevicePath{}, false
}

func (r *NodeDeviceResolver) resolveAlias(volumeName, serial string, started time.Time) (string, deviceResolutionResult, error) {
	candidates := []string{}
	if r.deviceCandidatesFn != nil {
		candidates = r.deviceCandidatesFn(volumeName)
	}
	expected := pathJoin(defaultDiskPath, filepath.Base(volumeName))
	var lastErr error
	for _, candidate := range candidates {
		if _, err := nodeVolumePathStat(candidate); err == nil {
			if !deviceMatchesSerial(r.exec, candidate, serial) {
				continue
			}
			result := deviceResolutionResult{
				ResolvedBy: "exact",
				Latency:    deviceResolverNow().Sub(started),
			}
			if candidate != expected {
				result.ResolvedBy = "alias"
			}
			return candidate, result, nil
		} else if !os.IsNotExist(err) {
			lastErr = err
		}
	}
	if lastErr != nil {
		return "", deviceResolutionResult{}, lastErr
	}
	return "", deviceResolutionResult{}, fmt.Errorf("device not found")
}

func (r *NodeDeviceResolver) runRecoverySequence(ctx context.Context) {
	if r == nil || r.exec == nil {
		return
	}
	if r.udevSettleTimeout > 0 {
		timeoutSeconds := int(r.udevSettleTimeout / time.Second)
		if timeoutSeconds < 1 {
			timeoutSeconds = 1
		}
		if output, err := r.exec.CommandContext(ctx, "udevadm", "settle", fmt.Sprintf("--timeout=%d", timeoutSeconds)).CombinedOutput(); err != nil {
			if errors.Is(err, osexec.ErrNotFound) || strings.Contains(err.Error(), "executable file not found") {
				if r.markUdevadmMissingLogged() {
					klog.V(2).InfoS("udevadm is unavailable; device recovery will continue with SCSI rescan only", "err", err)
				}
			} else {
				klog.V(4).InfoS("udevadm settle failed during device recovery", "err", err, "output", strings.TrimSpace(string(output)))
			}
		}
	}
	if !r.rescanOnMiss {
		return
	}
	hostPaths, err := nodeByIDGlob("/sys/class/scsi_host/host*/scan")
	if err != nil {
		return
	}
	for _, hostPath := range hostPaths {
		if err := nodeWriteFile(hostPath, []byte("- - -"), 0o200); err != nil {
			klog.V(5).InfoS("SCSI rescan write failed", "path", hostPath, "err", err)
		}
	}
}

func (r *NodeDeviceResolver) markUdevadmMissingLogged() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.udevadmMissingLog {
		return false
	}
	r.udevadmMissingLog = true
	return true
}

func (r *NodeDeviceResolver) remember(volumeID, imageID, serial, devicePath, byIDPath string) {
	if r == nil || !r.cacheEnabled || strings.TrimSpace(volumeID) == "" || strings.TrimSpace(devicePath) == "" {
		return
	}
	r.mu.Lock()
	r.cache[volumeID] = nodeDeviceCacheEntry{
		VolumeID:        volumeID,
		ImageID:         imageID,
		Serial:          serial,
		DevicePath:      devicePath,
		ByIDPath:        byIDPath,
		LastConfirmedAt: deviceResolverNow().UTC(),
	}
	r.mu.Unlock()
}

func deviceMatchesSerial(exec utilexec.Interface, devicePath, serial string) bool {
	serial = strings.TrimSpace(serial)
	if serial == "" {
		return true
	}
	if exec == nil {
		return false
	}
	if output, err := exec.Command("lsblk", "-ndo", "SERIAL", devicePath).CombinedOutput(); err == nil && strings.TrimSpace(string(output)) != "" {
		return strings.EqualFold(strings.TrimSpace(string(output)), serial)
	}
	if output, err := exec.Command("udevadm", "info", "--query=property", "--name", devicePath).CombinedOutput(); err == nil {
		properties := map[string]string{}
		for _, line := range strings.Split(string(output), "\n") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				properties[parts[0]] = strings.TrimSpace(parts[1])
			}
		}
		observed := firstNonEmpty(properties["ID_SERIAL_SHORT"], properties["ID_SERIAL"])
		return observed != "" && strings.EqualFold(observed, serial)
	}
	return false
}

func pathJoin(parts ...string) string {
	return filepath.Join(parts...)
}
