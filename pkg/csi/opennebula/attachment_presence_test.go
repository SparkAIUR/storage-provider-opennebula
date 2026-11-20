package opennebula

import (
	"context"
	"fmt"
	img "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAttachRetryRequiresConsistentAbsence(t *testing.T) {
	for _, scenario := range []string{"absent", "attached", "ready-with-owner", "nil-metadata", "wrong-image", "wrong-vm", "missing-target", "another-vm", "image-usage", "image-vm-ids", "lookup-error"} {
		t.Run(scenario, func(t *testing.T) {
			metadata := &VolumeAttachmentMetadata{VolumeHandle: "volume", ImageID: 7, RequestedNodeID: 11, ImageStateRaw: int(img.Ready)}
			var lookupErr error
			switch scenario {
			case "nil-metadata":
				metadata = nil
			case "wrong-image":
				metadata.ImageID = 8
			case "wrong-vm":
				metadata.RequestedNodeID = 12
			case "missing-target":
				metadata.AttachedToRequestedNode = true
				metadata.DiskRecords = []VolumeDiskRecord{{NodeID: 11}}
			case "another-vm":
				metadata.ImageStateRaw = int(img.UsedPers)
				metadata.DiskRecords = []VolumeDiskRecord{{NodeID: 12, Target: "sdd"}}
				metadata.ImageVMIDs = []int{12}
				metadata.ImageRunningVMs = 1
			case "image-usage":
				metadata.ImageRunningVMs = 1
			case "image-vm-ids":
				metadata.ImageVMIDs = []int{12}
			case "attached", "ready-with-owner":
				if scenario == "attached" {
					metadata.ImageStateRaw = int(img.UsedPers)
				}
				metadata.ImageRunningVMs = 1
				metadata.ImageVMIDs = []int{11}
				metadata.AttachedToRequestedNode = true
				metadata.DiskRecords = []VolumeDiskRecord{{NodeID: 11, Target: "sdd"}}
			case "lookup-error":
				lookupErr = fmt.Errorf("VM lookup unavailable")
			}
			attaches := 0
			provider := &PersistentDiskVolumeProvider{}
			err := provider.waitForAttachState(context.Background(), time.Second, time.Millisecond, func(context.Context) (bool, bool, error) {
				if lookupErr != nil {
					return false, true, lookupErr
				}
				if attaches > 0 {
					return true, true, nil
				}
				attached, err := attachmentPresence(metadata, "volume", 7, 11)
				return attached, true, err
			}, func(context.Context) error { attaches++; return nil }, "volume", "node")
			switch scenario {
			case "absent", "another-vm":
				require.NoError(t, err)
				require.Equal(t, 1, attaches)
			case "attached":
				require.NoError(t, err)
				require.Zero(t, attaches)
			default:
				require.Error(t, err)
				require.Zero(t, attaches)
			}
		})
	}
}
