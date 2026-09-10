package driver

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

var (
	nodeDeviceStat       = unix.Stat
	nodeSysDevBlockPath  = "/sys/dev/block"
	nodeReadIdentityFile = readDeviceIdentityFile
)

func readDeviceIdentityFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, 65540))
}

func currentDeviceSerial(devicePath string) (string, error) {
	resolved, err := nodeEvalSymlinks(devicePath)
	if err != nil {
		return "", err
	}
	var before unix.Stat_t
	if err := nodeDeviceStat(resolved, &before); err != nil {
		return "", err
	}
	if before.Mode&unix.S_IFMT != unix.S_IFBLK {
		return "", fmt.Errorf("device identity requires a block device")
	}
	deviceID := fmt.Sprintf("%d:%d", unix.Major(uint64(before.Rdev)), unix.Minor(uint64(before.Rdev)))
	kernelPath := filepath.Join(nodeSysDevBlockPath, deviceID)
	payload, err := nodeReadIdentityFile(filepath.Join(kernelPath, "serial"))
	var serial string
	if os.IsNotExist(err) {
		payload, err = nodeReadIdentityFile(filepath.Join(kernelPath, "device", "vpd_pg80"))
		if err == nil {
			serial, err = parseSCSISerialPage(payload)
		}
	} else if err == nil {
		serial, err = validateDeviceSerial(strings.TrimSpace(string(payload)))
	}
	if err != nil {
		return "", err
	}
	var after unix.Stat_t
	if err := nodeDeviceStat(devicePath, &after); err != nil {
		return "", err
	}
	if after.Mode&unix.S_IFMT != unix.S_IFBLK || before.Rdev != after.Rdev || before.Dev != after.Dev || before.Ino != after.Ino {
		return "", fmt.Errorf("block device changed during serial observation")
	}
	return serial, nil
}

func parseSCSISerialPage(page []byte) (string, error) {
	if len(page) < 4 || page[0] != 0 || page[1] != 0x80 || int(binary.BigEndian.Uint16(page[2:4])) != len(page)-4 {
		return "", fmt.Errorf("invalid SCSI unit serial VPD page")
	}
	return validateDeviceSerial(strings.Trim(string(page[4:]), " "))
}

func validateDeviceSerial(serial string) (string, error) {
	if serial == "" {
		return "", fmt.Errorf("device serial is empty")
	}
	for _, value := range []byte(serial) {
		if value < 0x20 || value > 0x7e {
			return "", fmt.Errorf("device serial contains invalid characters")
		}
	}
	return serial, nil
}
