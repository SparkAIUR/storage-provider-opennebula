//go:build linux

package driver

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Linux UAPI btrfs_ioctl_fs_info_args is a fixed 1024-byte read-only result.
// FS_INFO returns the filesystem of the opened mounted directory, not a UUID
// inferred from its mount-source path or a block-device superblock.
const nodeBtrfsFSInfoRequest = uintptr(0x8400941f)

type nodeBtrfsKernelInfo struct {
	filesystemDevice uint64
	filesystem       [16]byte
	maxID, devices   uint64
}

var (
	nodeBtrfsFilesystemInfo   = readNodeBtrfsFilesystemInfo
	nodeBtrfsDevicesDirectory = func(id string) string { return filepath.Join("/sys/fs/btrfs", id, "devices") }
)

func init() { nodeExpandBtrfsIdentity = observeNodeBtrfsIdentity }

func readNodeBtrfsFilesystemInfo(ctx context.Context, path string) (result nodeBtrfsKernelInfo, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nodeBtrfsKernelInfo{}, err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nodeBtrfsKernelInfo{}, err
	}
	defer func() {
		if e := unix.Close(fd); resultErr == nil && e != nil {
			resultErr = e
		}
	}()
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		return nodeBtrfsKernelInfo{}, err
	}
	var data [1024]byte
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), nodeBtrfsFSInfoRequest, uintptr(unsafe.Pointer(&data[0])))
	if err := ctx.Err(); err != nil {
		return nodeBtrfsKernelInfo{}, err
	}
	if errno != 0 {
		return nodeBtrfsKernelInfo{}, errno
	}
	result = nodeBtrfsKernelInfo{filesystemDevice: uint64(st.Dev), maxID: binary.NativeEndian.Uint64(data[0:8]), devices: binary.NativeEndian.Uint64(data[8:16])}
	copy(result.filesystem[:], data[16:32])
	return result, nil
}

func observeNodeBtrfsIdentity(ctx context.Context, path string) (nodeExpandBtrfsID, error) {
	info, err := nodeBtrfsFilesystemInfo(ctx, path)
	if contextErr := ctx.Err(); contextErr != nil {
		return nodeExpandBtrfsID{}, contextErr
	}
	if err != nil {
		return nodeExpandBtrfsID{}, err
	}
	// mount-utils invokes `btrfs filesystem resize max`, whose implicit devid
	// is 1. Other/multiple device layouts cannot identify its capacity source.
	if info.devices != 1 || info.maxID != 1 || info.filesystem == ([16]byte{}) {
		return nodeExpandBtrfsID{}, fmt.Errorf("btrfs device membership is missing or ambiguous")
	}
	id := fmt.Sprintf("%x-%x-%x-%x-%x", info.filesystem[:4], info.filesystem[4:6], info.filesystem[6:8], info.filesystem[8:10], info.filesystem[10:])
	directory := nodeBtrfsDevicesDirectory(id)
	dir, err := os.Open(directory)
	if err != nil {
		return nodeExpandBtrfsID{}, err
	}
	entries, readErr := dir.Readdir(2)
	closeErr := dir.Close()
	if contextErr := ctx.Err(); contextErr != nil {
		return nodeExpandBtrfsID{}, contextErr
	}
	if readErr != nil && readErr != io.EOF {
		return nodeExpandBtrfsID{}, readErr
	}
	if closeErr != nil {
		return nodeExpandBtrfsID{}, closeErr
	}
	if len(entries) != 1 || entries[0].Mode()&os.ModeSymlink == 0 {
		return nodeExpandBtrfsID{}, fmt.Errorf("btrfs sysfs membership is missing or ambiguous")
	}
	member := filepath.Join(directory, entries[0].Name())
	subsystem, err := os.Stat(filepath.Join(member, "subsystem"))
	if err != nil {
		return nodeExpandBtrfsID{}, err
	}
	blockClass, err := os.Stat("/sys/class/block")
	if err != nil || !os.SameFile(subsystem, blockClass) {
		return nodeExpandBtrfsID{}, fmt.Errorf("btrfs member is not a kernel block device")
	}
	file, err := os.Open(filepath.Join(member, "dev"))
	if err != nil {
		return nodeExpandBtrfsID{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, 65))
	closeErr = file.Close()
	if contextErr := ctx.Err(); contextErr != nil {
		return nodeExpandBtrfsID{}, contextErr
	}
	if readErr != nil {
		return nodeExpandBtrfsID{}, readErr
	}
	if closeErr != nil {
		return nodeExpandBtrfsID{}, closeErr
	}
	if len(data) > 64 {
		return nodeExpandBtrfsID{}, fmt.Errorf("btrfs block identity is oversized")
	}
	raw := strings.TrimSuffix(string(data), "\n")
	major, minor, found := strings.Cut(raw, ":")
	parse := func(value string) (uint64, error) {
		n, e := strconv.ParseUint(value, 10, 32)
		if e != nil || strconv.FormatUint(n, 10) != value {
			return 0, fmt.Errorf("invalid kernel block identity %q", raw)
		}
		return n, nil
	}
	if !found {
		return nodeExpandBtrfsID{}, fmt.Errorf("invalid kernel block identity %q", raw)
	}
	ma, err := parse(major)
	if err != nil {
		return nodeExpandBtrfsID{}, err
	}
	mi, err := parse(minor)
	if err != nil {
		return nodeExpandBtrfsID{}, err
	}
	device := unix.Mkdev(uint32(ma), uint32(mi))
	if device == 0 {
		return nodeExpandBtrfsID{}, fmt.Errorf("btrfs block identity is empty")
	}
	return nodeExpandBtrfsID{filesystemDevice: info.filesystemDevice, filesystem: info.filesystem, device: device}, nil
}
