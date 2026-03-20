package opennebula

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const snapshotIDPrefix = "image-snapshot:"

type VolumeSnapshot struct {
	SnapshotID     string
	SourceVolumeID string
	CreationTime   time.Time
	SizeBytes      int64
	ReadyToUse     bool
}

func EncodeVolumeSnapshotID(imageID, snapshotID int) string {
	return fmt.Sprintf("%s%d:%d", snapshotIDPrefix, imageID, snapshotID)
}

func DecodeVolumeSnapshotID(value string) (int, int, error) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, snapshotIDPrefix) {
		return 0, 0, &datastoreConfigError{message: fmt.Sprintf("invalid OpenNebula snapshot ID %q", value)}
	}

	payload := strings.TrimPrefix(trimmed, snapshotIDPrefix)
	imageIDRaw, snapshotIDRaw, ok := strings.Cut(payload, ":")
	if !ok {
		return 0, 0, &datastoreConfigError{message: fmt.Sprintf("invalid OpenNebula snapshot ID %q", value)}
	}

	imageID, err := strconv.Atoi(imageIDRaw)
	if err != nil {
		return 0, 0, &datastoreConfigError{message: fmt.Sprintf("invalid image ID in snapshot reference %q", value)}
	}
	snapshotID, err := strconv.Atoi(snapshotIDRaw)
	if err != nil {
		return 0, 0, &datastoreConfigError{message: fmt.Sprintf("invalid snapshot ID in snapshot reference %q", value)}
	}

	return imageID, snapshotID, nil
}
