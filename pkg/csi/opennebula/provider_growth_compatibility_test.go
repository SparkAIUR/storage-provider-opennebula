package opennebula

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const persistentSystemDiskFacts = `<PERSISTENT>YES</PERSISTENT><CLONE>NO</CLONE><LN_TARGET>SYSTEM</LN_TARGET><TM_MAD>fs_lvm_ssh</TM_MAD><TM_MAD_SYSTEM>fs_lvm_ssh</TM_MAD_SYSTEM>`

type growthProtocolState struct {
	resizeCalls, metadataWrites, otherMutations, versionCalls atomic.Int32
	sizeMB, marker                                            atomic.Int64
}

func growthCompatibilityFixture(t *testing.T, version string, restoring bool) (*PersistentDiskVolumeProvider, *growthProtocolState) {
	t.Helper()
	st := &growthProtocolState{}
	st.sizeMB.Store(128)
	st.marker.Store(128 * sizeConversion)
	if restoring {
		st.marker.Store(256 * sizeConversion)
	}
	image := func() string {
		return fmt.Sprintf(`<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>8</STATE><SIZE>128</SIZE><PERSISTENT>1</PERSISTENT><UID>9</UID><GID>10</GID><TEMPLATE><CSI_REQUESTED_SIZE_BYTES>%d</CSI_REQUESTED_SIZE_BYTES><PROTECTED>YES</PROTECTED></TEMPLATE></IMAGE>`, st.marker.Load())
	}
	vmXML := func() string {
		return fmt.Sprintf(`<VM><ID>11</ID><NAME>node-test</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE><DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdb</TARGET><SIZE>%d</SIZE><ORIGINAL_SIZE>128</ORIGINAL_SIZE>%s</DISK></TEMPLATE></VM>`, st.sizeMB.Load(), persistentSystemDiskFacts)
	}
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		switch method {
		case "one.imagepool.info":
			return true, "<IMAGE_POOL>" + image() + "</IMAGE_POOL>", 0
		case "one.image.info":
			return true, image(), 0
		case "one.vmpool.info":
			return true, "<VM_POOL>" + vmXML() + "</VM_POOL>", 0
		case "one.vm.info":
			return true, vmXML(), 0
		case "one.system.version":
			st.versionCalls.Add(1)
			return true, version, 0
		case "one.vm.diskresize":
			st.resizeCalls.Add(1)
			st.sizeMB.Store(256)
			return true, "11", 0
		case "one.image.update":
			st.metadataWrites.Add(1)
			st.marker.Store(256 * sizeConversion)
			return true, "7", 0
		default:
			st.otherMutations.Add(1)
			return false, "unexpected operation", 0x800
		}
	})
	return p, st
}

func TestProviderGrowthCompatibility740RejectsBeforeMutation(t *testing.T) {
	for _, restoring := range []bool{false, true} {
		t.Run(fmt.Sprintf("restoration=%t", restoring), func(t *testing.T) {
			p, st := growthCompatibilityFixture(t, "7.4.0", restoring)
			beforeMarker := st.marker.Load()
			var err error
			if restoring {
				err = p.RestoreVolumeCapacity(context.Background(), "volume-test", 11, 7)
			} else {
				_, err = p.ExpandVolume(context.Background(), "volume-test", 256*sizeConversion, false)
			}
			require.Error(t, err, "7.4.0 cannot preserve persistent SYSTEM-target size through detach/reattach")
			require.Zero(t, st.resizeCalls.Load(), "reject before diskresize")
			require.Zero(t, st.metadataWrites.Load(), "reject before image metadata changes")
			require.Zero(t, st.otherMutations.Load(), "no attach/detach/allocate/update mutations")
			require.Equal(t, beforeMarker, st.marker.Load(), "durable capacity retained")
			require.EqualValues(t, 128, st.sizeMB.Load(), "attachment remains unchanged")
		})
	}
}
