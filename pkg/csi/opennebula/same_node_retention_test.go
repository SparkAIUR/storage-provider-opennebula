package opennebula

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type retentionProtocolCase struct {
	imageSize, diskSize, imageID, vmID, diskID, imageState                   int
	vmState, lcmState                                                        int
	imageName, serial, target, persistence, diskFacts, resize, requestedSize string
	imageOwners, extraVM, extraDisk, failMethod                              string
	beforeReply                                                              func(string)
}

func retentionProtocolFixture(t *testing.T, modify func(*retentionProtocolCase)) (*PersistentDiskVolumeProvider, *atomic.Int32) {
	t.Helper()
	cfg := retentionProtocolCase{imageSize: 1024, diskSize: 1024, imageID: 7, vmID: 11, diskID: 2, imageState: 8, vmState: 3, lcmState: 3, imageName: "volume-test", serial: "onecsi-7", target: "vdb", persistence: "1", diskFacts: persistentNoneDiskFacts, imageOwners: "<ID>11</ID>"}
	if modify != nil {
		modify(&cfg)
	}
	mutations := &atomic.Int32{}
	image := func() string {
		return fmt.Sprintf(`<IMAGE><ID>%d</ID><NAME>%s</NAME><STATE>%d</STATE><SIZE>%d</SIZE><PERSISTENT>%s</PERSISTENT><RUNNING_VMS>1</RUNNING_VMS><VMS>%s</VMS><TEMPLATE>%s</TEMPLATE></IMAGE>`, cfg.imageID, cfg.imageName, cfg.imageState, cfg.imageSize, cfg.persistence, cfg.imageOwners, cfg.requestedSize)
	}
	disk := func() string {
		return fmt.Sprintf(`<DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>%d</DISK_ID><TARGET>%s</TARGET><SERIAL>%s</SERIAL><SIZE>%d</SIZE>%s%s</DISK>`, cfg.diskID, cfg.target, cfg.serial, cfg.diskSize, cfg.diskFacts, cfg.resize)
	}
	vmXML := func() string {
		return fmt.Sprintf(`<VM><ID>%d</ID><NAME>node-test</NAME><STATE>%d</STATE><LCM_STATE>%d</LCM_STATE><TEMPLATE>%s%s</TEMPLATE></VM>`, cfg.vmID, cfg.vmState, cfg.lcmState, disk(), cfg.extraDisk)
	}
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		if cfg.beforeReply != nil {
			cfg.beforeReply(method)
		}
		if method == cfg.failMethod {
			return false, "read unavailable", 0x200
		}
		switch method {
		case "one.imagepool.info":
			return true, "<IMAGE_POOL>" + image() + "</IMAGE_POOL>", 0
		case "one.image.info":
			return true, image(), 0
		case "one.vmpool.info":
			return true, "<VM_POOL>" + vmXML() + cfg.extraVM + "</VM_POOL>", 0
		case "one.vm.info":
			return true, vmXML(), 0
		default:
			mutations.Add(1)
			return false, "unexpected mutation", 0x800
		}
	})
	return p, mutations
}

func TestSameNodeRetentionReadOnlyEvidence(t *testing.T) {
	p, mutations := retentionProtocolFixture(t, func(c *retentionProtocolCase) {
		c.extraDisk = `<DISK><DISK_ID>0</DISK_ID><TYPE>CDROM</TYPE></DISK>`
	})
	evidence, err := p.InspectSameNodeRetention(context.Background(), "volume-test", "node-test")
	require.NoError(t, err)
	require.Equal(t, &SameNodeRetentionEvidence{VolumeID: "volume-test", ImageID: 7, NodeID: 11, DiskID: 2, Target: "vdb", DeviceSerial: "onecsi-7", SizeBytes: 1024 * sizeConversion}, evidence)
	require.Zero(t, mutations.Load())
}

func TestSameNodeRetentionRejectsUncertainOrUnsafeEvidence(t *testing.T) {
	cases := map[string]func(*retentionProtocolCase){
		"unstable image":          func(c *retentionProtocolCase) { c.imageState = 9 },
		"image capacity overflow": func(c *retentionProtocolCase) { c.imageSize = 1 << 50 },
		"fresh VM identity changed": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.vmID = 12
				}
			}
		},
		"fresh serial changed": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.serial = "other-device"
				}
			}
		},
		"fresh disk ID changed": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.diskID = 3
				}
			}
		},
		"fresh target changed": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.target = "vdc"
				}
			}
		},
		"image lookup failure":   func(c *retentionProtocolCase) { c.failMethod = "one.imagepool.info" },
		"image read failure":     func(c *retentionProtocolCase) { c.failMethod = "one.image.info" },
		"owner census failure":   func(c *retentionProtocolCase) { c.failMethod = "one.vmpool.info" },
		"fresh VM failure":       func(c *retentionProtocolCase) { c.failMethod = "one.vm.info" },
		"image identity changed": func(c *retentionProtocolCase) { c.imageName = "replacement" },
		"nonpersistent":          func(c *retentionProtocolCase) { c.persistence = "0" },
		"unknown persistence":    func(c *retentionProtocolCase) { c.persistence = "" },
		"other image owner":      func(c *retentionProtocolCase) { c.imageOwners = "<ID>11</ID><ID>12</ID>" },
		"missing image owner":    func(c *retentionProtocolCase) { c.imageOwners = "" },
		"other VM owner": func(c *retentionProtocolCase) {
			c.extraVM = `<VM><ID>12</ID><NAME>other</NAME><TEMPLATE><DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>4</DISK_ID></DISK></TEMPLATE></VM>`
		},
		"malformed census image reference": func(c *retentionProtocolCase) {
			c.extraVM = `<VM><ID>12</ID><NAME>other</NAME><TEMPLATE><DISK><IMAGE_ID>unknown</IMAGE_ID><DISK_ID>4</DISK_ID></DISK></TEMPLATE></VM>`
		},
		"malformed fresh image reference": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.extraDisk = `<DISK><IMAGE_ID>unknown</IMAGE_ID><DISK_ID>3</DISK_ID></DISK>`
				}
			}
		},
		"duplicate disk": func(c *retentionProtocolCase) {
			c.extraDisk = `<DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>3</DISK_ID><TARGET>vdc</TARGET><SERIAL>onecsi-7</SERIAL><SIZE>1024</SIZE></DISK>`
		},
		"missing serial":         func(c *retentionProtocolCase) { c.serial = "" },
		"missing target":         func(c *retentionProtocolCase) { c.target = "" },
		"resize marker":          func(c *retentionProtocolCase) { c.resize = "<RESIZE>YES</RESIZE>" },
		"previous size marker":   func(c *retentionProtocolCase) { c.resize = "<SIZE_PREV>512</SIZE_PREV>" },
		"unstable VM":            func(c *retentionProtocolCase) { c.lcmState = 16 },
		"powered off VM":         func(c *retentionProtocolCase) { c.vmState = 8; c.lcmState = 0 },
		"disk capacity mismatch": func(c *retentionProtocolCase) { c.diskSize = 2048 },
		"zero disk capacity":     func(c *retentionProtocolCase) { c.diskSize = 0 },
		"verified capacity exceeds image": func(c *retentionProtocolCase) {
			c.requestedSize = "<CSI_REQUESTED_SIZE_BYTES>2147483648</CSI_REQUESTED_SIZE_BYTES>"
		},
		"invalid verified capacity": func(c *retentionProtocolCase) {
			c.requestedSize = "<CSI_REQUESTED_SIZE_BYTES>invalid</CSI_REQUESTED_SIZE_BYTES>"
		},
		"nonCeph driver": func(c *retentionProtocolCase) {
			c.diskFacts = strings.ReplaceAll(persistentNoneDiskFacts, "ceph", "fs_lvm_ssh")
		},
		"system target": func(c *retentionProtocolCase) {
			c.diskFacts = strings.Replace(persistentNoneDiskFacts, "<LN_TARGET>NONE</LN_TARGET>", "<LN_TARGET>SYSTEM</LN_TARGET>", 1)
		},
		"cloned disk": func(c *retentionProtocolCase) {
			c.diskFacts = strings.Replace(persistentNoneDiskFacts, "<CLONE>NO</CLONE>", "<CLONE>YES</CLONE>", 1)
		},
	}
	for name, modify := range cases {
		t.Run(name, func(t *testing.T) {
			p, mutations := retentionProtocolFixture(t, modify)
			evidence, err := p.InspectSameNodeRetention(context.Background(), "volume-test", "node-test")
			require.Error(t, err)
			require.Nil(t, evidence)
			require.Zero(t, mutations.Load())
		})
	}
}

func TestSameNodeRetentionCancellationNeverReturnsProof(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p, mutations := retentionProtocolFixture(t, func(c *retentionProtocolCase) {
		c.beforeReply = func(method string) {
			if method == "one.vm.info" {
				cancel()
			}
		}
	})
	evidence, err := p.InspectSameNodeRetention(ctx, "volume-test", "node-test")
	require.Error(t, err)
	require.Nil(t, evidence)
	require.Zero(t, mutations.Load())
}

func TestDiskInventoryRejectsMalformedImageReference(t *testing.T) {
	p, mutations := retentionProtocolFixture(t, func(c *retentionProtocolCase) {
		c.extraDisk = `<DISK><IMAGE_ID>unknown</IMAGE_ID><DISK_ID>3</DISK_ID></DISK>`
	})
	ctx := context.Background()
	disk, err := p.GetVolumeInNode(ctx, 7, 11)
	require.ErrorContains(t, err, "malformed VM disk image identity")
	require.Empty(t, disk)
	attachments, _, err := p.ListCurrentAttachments(ctx)
	require.ErrorContains(t, err, "malformed VM disk image identity")
	require.Empty(t, attachments)
	size, err := p.completedDiskSize(ctx, 11, 2, 7)
	require.ErrorContains(t, err, "malformed VM disk image identity")
	require.Zero(t, size)
	require.Zero(t, mutations.Load())
}
