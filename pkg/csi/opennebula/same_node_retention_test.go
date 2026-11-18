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
	imageOwners, extraVM, extraDisk, failMethod, datastoreBackend            string
	datastoreType, datastoreTemplateType, datastoreTransfer                  string
	datastoreDiskType, datastoreTemplateDiskType                             string
	missingDatastore, duplicateDatastore                                     bool
	beforeReply                                                              func(string)
}

func retentionProtocolFixture(t *testing.T, modify func(*retentionProtocolCase)) (*PersistentDiskVolumeProvider, *atomic.Int32) {
	t.Helper()
	cfg := retentionProtocolCase{imageSize: 1024, diskSize: 1024, imageID: 7, vmID: 11, diskID: 2, imageState: 8, vmState: 3, lcmState: 3, imageName: "volume-test", serial: "onecsi-7", target: "vdb", persistence: "1", diskFacts: persistentNoneDiskFacts, imageOwners: "<ID>11</ID>", datastoreBackend: "ceph", datastoreType: "IMAGE"}
	cfg.datastoreTransfer, cfg.datastoreTemplateDiskType = "ceph", "RBD"
	if modify != nil {
		modify(&cfg)
	}
	mutations := &atomic.Int32{}
	image := func() string {
		datastore := ""
		if cfg.datastoreBackend != "" {
			datastore = "<DATASTORE_ID>124</DATASTORE_ID>"
		}
		return fmt.Sprintf(`<IMAGE><ID>%d</ID><NAME>%s</NAME><STATE>%d</STATE><SIZE>%d</SIZE><PERSISTENT>%s</PERSISTENT>%s<RUNNING_VMS>1</RUNNING_VMS><CLONING_OPS>0</CLONING_OPS><VMS>%s</VMS><TEMPLATE>%s</TEMPLATE></IMAGE>`, cfg.imageID, cfg.imageName, cfg.imageState, cfg.imageSize, cfg.persistence, datastore, cfg.imageOwners, cfg.requestedSize)
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
		case "one.datastorepool.info":
			if cfg.missingDatastore {
				return true, "<DATASTORE_POOL/>", 0
			}
			datastore := fmt.Sprintf(`<DATASTORE><ID>124</ID><TYPE>%s</TYPE><DS_MAD>%s</DS_MAD><TM_MAD>%s</TM_MAD><DISK_TYPE>%s</DISK_TYPE><TEMPLATE><TYPE>%s</TYPE><DISK_TYPE>%s</DISK_TYPE></TEMPLATE></DATASTORE>`, cfg.datastoreType, cfg.datastoreBackend, cfg.datastoreTransfer, cfg.datastoreDiskType, cfg.datastoreTemplateType, cfg.datastoreTemplateDiskType)
			if cfg.duplicateDatastore {
				datastore += datastore
			}
			return true, "<DATASTORE_POOL>" + datastore + "</DATASTORE_POOL>", 0
		default:
			mutations.Add(1)
			return false, "unexpected mutation", 0x800
		}
	})
	return p, mutations
}

func TestSameNodeRetentionReadOnlyEvidence(t *testing.T) {
	p, mutations := retentionProtocolFixture(t, func(c *retentionProtocolCase) {
		c.extraDisk = `<DISK><DISK_ID>0</DISK_ID><TYPE>CDROM</TYPE><IMAGE_ID/><SOURCE/></DISK>`
		c.datastoreType, c.datastoreTemplateType = "0", "IMAGE_DS"
		c.datastoreDiskType = "3"
	})
	evidence, err := p.InspectSameNodeRetention(context.Background(), "volume-test", "node-test")
	require.NoError(t, err)
	require.Equal(t, &SameNodeRetentionEvidence{VolumeID: "volume-test", ImageID: 7, NodeID: 11, DiskID: 2, Target: "vdb", DeviceSerial: "onecsi-7", SizeBytes: 1_073_741_824}, evidence)
	require.Zero(t, mutations.Load())
}

func TestSameNodeRetentionRejectsUncertainOrUnsafeEvidence(t *testing.T) {
	cases := map[string]func(*retentionProtocolCase){
		"contradictory image datastore backend": func(c *retentionProtocolCase) { c.datastoreBackend = "fs" },
		"undeclared image datastore":            func(c *retentionProtocolCase) { c.datastoreBackend = "" },
		"declared datastore not found":          func(c *retentionProtocolCase) { c.missingDatastore = true },
		"declared datastore lookup failed":      func(c *retentionProtocolCase) { c.failMethod = "one.datastorepool.info" },
		"system datastore":                      func(c *retentionProtocolCase) { c.datastoreType = "SYSTEM" },
		"conflicting datastore categories":      func(c *retentionProtocolCase) { c.datastoreTemplateType = "SYSTEM_DS" },
		"conflicting datastore drivers":         func(c *retentionProtocolCase) { c.datastoreTransfer = "ssh" },
		"blank Ceph datastore transfer":         func(c *retentionProtocolCase) { c.datastoreTransfer = "" },
		"missing RBD disk type":                 func(c *retentionProtocolCase) { c.datastoreTemplateDiskType = "" },
		"conflicting root disk type":            func(c *retentionProtocolCase) { c.datastoreDiskType = "FILE" },
		"conflicting template disk type": func(c *retentionProtocolCase) {
			c.datastoreDiskType, c.datastoreTemplateDiskType = "3", "FILE"
		},
		"duplicate datastore ID":  func(c *retentionProtocolCase) { c.duplicateDatastore = true },
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
		"blank census image reference": func(c *retentionProtocolCase) {
			c.extraVM = `<VM><ID>12</ID><NAME>other</NAME><TEMPLATE><DISK><IMAGE_ID></IMAGE_ID><DISK_ID>4</DISK_ID></DISK></TEMPLATE></VM>`
		},
		"backed census disk without image ID": func(c *retentionProtocolCase) {
			c.extraVM = `<VM><ID>12</ID><NAME>other</NAME><TEMPLATE><DISK><DISK_ID>4</DISK_ID><IMAGE>volume-test</IMAGE><SOURCE>/ceph/disk</SOURCE><PERSISTENT>YES</PERSISTENT></DISK></TEMPLATE></VM>`
		},
		"backed fresh disk without image ID": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.extraDisk = `<DISK><DISK_ID>3</DISK_ID><IMAGE>volume-test</IMAGE><SOURCE>/ceph/disk</SOURCE><PERSISTENT>YES</PERSISTENT></DISK>`
				}
			}
		},
		"blank fresh image reference": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.extraDisk = `<DISK><IMAGE_ID></IMAGE_ID><DISK_ID>3</DISK_ID></DISK>`
				}
			}
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
		"slot collision in census": func(c *retentionProtocolCase) {
			c.extraDisk = `<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdc</TARGET></DISK>`
		},
		"slot collision in fresh VM with image-less disk": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.extraDisk = `<DISK><DISK_ID>2</DISK_ID><TYPE>CDROM</TYPE></DISK>`
				}
			}
		},
		"missing serial": func(c *retentionProtocolCase) { c.serial = "" },
		"missing target": func(c *retentionProtocolCase) { c.target = "" },
		"CSI serial in census lacks image ID": func(c *retentionProtocolCase) {
			c.extraVM = `<VM><ID>12</ID><NAME>other</NAME><TEMPLATE><DISK><DISK_ID>4</DISK_ID><SERIAL>onecsi-7</SERIAL></DISK></TEMPLATE></VM>`
		},
		"CSI serial in census conflicts with image ID": func(c *retentionProtocolCase) {
			c.extraVM = `<VM><ID>12</ID><NAME>other</NAME><TEMPLATE><DISK><DISK_ID>4</DISK_ID><IMAGE_ID>8</IMAGE_ID><SERIAL>onecsi-7</SERIAL></DISK></TEMPLATE></VM>`
		},
		"fresh disk serial lacks image ID": func(c *retentionProtocolCase) {
			c.beforeReply = func(method string) {
				if method == "one.vm.info" {
					c.extraDisk = `<DISK><DISK_ID>4</DISK_ID><SERIAL>onecsi-7</SERIAL></DISK>`
				}
			}
		},
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

func TestVerifyAttachmentBackendRequiresPositiveDatastoreAndDiskFacts(t *testing.T) {
	for _, tc := range []struct {
		name, datastore, datastoreTransfer, transfer, expected, failure string
		missingDatastore, failedRead                                    bool
	}{
		{name: "local via ssh", datastore: "fs", transfer: "ssh", expected: "local"},
		{name: "local via LVM", datastore: "fs", transfer: "fs_lvm_ssh", expected: "local"},
		{name: "local via shared", datastore: "fs", transfer: "shared", expected: "local"},
		{name: "filesystem datastore with shared transfer", datastore: "fs", datastoreTransfer: "shared", transfer: "shared", expected: "local"},
		{name: "filesystem datastore with qcow2 transfer", datastore: "fs", datastoreTransfer: "qcow2", transfer: "qcow2", expected: "local"},
		{name: "qcow2 disk conflicts with shared datastore", datastore: "fs", datastoreTransfer: "shared", transfer: "qcow2", expected: "local", failure: "disk transfer driver"},
		{name: "shared disk conflicts with qcow2 datastore", datastore: "fs", datastoreTransfer: "qcow2", transfer: "shared", expected: "local", failure: "disk transfer driver"},
		{name: "Ceph", datastore: "ceph", datastoreTransfer: "ceph", transfer: "ceph", expected: "ceph"},
		{name: "Ceph without datastore transfer", datastore: "ceph", transfer: "ceph", expected: "ceph", failure: "lacks consistent Ceph RBD evidence"},
		{name: "ambiguous ssh", datastore: "fs", transfer: "ssh", expected: "local", failedRead: true, failure: "cannot inspect attachment datastore"},
		{name: "missing declared datastore", datastore: "fs", transfer: "ssh", expected: "local", missingDatastore: true, failure: "was not found"},
		{name: "undeclared datastore", transfer: "ssh", expected: "local", failure: "no valid datastore identity"},
		{name: "unknown datastore", datastore: "unknown", transfer: "ssh", expected: "local", failure: "unsupported attachment backend"},
		{name: "unknown transfer driver", datastore: "fs", transfer: "custom", expected: "local", failure: "transfer driver"},
		{name: "blank transfer driver", datastore: "fs", expected: "local", failure: "transfer driver"},
		{name: "NFS is not a CSI backend", datastore: "fs", datastoreTransfer: "shared", transfer: "shared", expected: "nfs", failure: "differs from"},
		{name: "no NFS datastore driver alias", datastore: "nfs", datastoreTransfer: "shared", transfer: "shared", expected: "local", failure: "unsupported attachment backend"},
		{name: "no NAS datastore driver alias", datastore: "nas", datastoreTransfer: "shared", transfer: "shared", expected: "local", failure: "unsupported attachment backend"},
		{name: "conflicting filesystem datastore drivers", datastore: "fs", datastoreTransfer: "ceph", transfer: "shared", expected: "local", failure: "drivers disagree"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, mutations := retentionProtocolFixture(t, func(c *retentionProtocolCase) {
				c.datastoreBackend = tc.datastore
				c.datastoreTransfer = tc.datastoreTransfer
				c.missingDatastore = tc.missingDatastore
				c.diskFacts = "<TM_MAD>" + tc.transfer + "</TM_MAD>"
				if tc.failedRead {
					c.failMethod = "one.datastorepool.info"
				}
			})
			err := p.VerifyAttachmentBackend(context.Background(), "volume-test", 7, 11, tc.expected)
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, mutations.Load())
		})
	}
}

func TestDiskLookupDistinguishesBlankAndMalformedImageReferences(t *testing.T) {
	for _, imageID := range []string{"", "unknown"} {
		t.Run(fmt.Sprintf("image-reference=%q", imageID), func(t *testing.T) {
			p, mutations := retentionProtocolFixture(t, func(c *retentionProtocolCase) {
				c.extraDisk = fmt.Sprintf(`<DISK><IMAGE_ID>%s</IMAGE_ID><DISK_ID>3</DISK_ID></DISK>`, imageID)
			})
			ctx := context.Background()
			disk, err := p.GetVolumeInNode(ctx, 7, 11)
			if imageID == "" {
				require.NoError(t, err)
				require.Equal(t, "vdb", disk)
			} else {
				require.ErrorContains(t, err, "malformed VM disk image identity")
				require.Empty(t, disk)
			}
			disk, err = p.GetVolumeInNode(ctx, 8, 11)
			require.ErrorContains(t, err, "malformed VM disk image identity")
			require.NotErrorIs(t, err, ErrAttachmentNotFound)
			require.Empty(t, disk)
			attachments, _, err := p.ListCurrentAttachments(ctx)
			require.ErrorContains(t, err, "malformed VM disk image identity")
			require.Empty(t, attachments)
			size, err := p.completedDiskSize(ctx, 11, 2, 7, "")
			require.NoError(t, err)
			require.EqualValues(t, 1_073_741_824, size)
			require.Zero(t, mutations.Load())
		})
	}
}

func TestSameNodeRetentionScopesCSIImageConflict(t *testing.T) {
	for _, imageID := range []int{7, 8, 9} {
		t.Run(fmt.Sprint(imageID), func(t *testing.T) {
			p, mutations := retentionProtocolFixture(t, func(c *retentionProtocolCase) {
				c.imageID = imageID
				c.extraVM = `<VM><ID>12</ID><NAME>other</NAME><TEMPLATE>
<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>3</DISK_ID><SERIAL>onecsi-9</SERIAL></DISK>
<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>4</DISK_ID><SERIAL>onecsi-8</SERIAL></DISK>
<DISK><IMAGE_ID>9</IMAGE_ID><DISK_ID>5</DISK_ID><SERIAL>onecsi-9</SERIAL></DISK>
</TEMPLATE></VM>`
			})
			ctx := context.Background()
			evidence, err := p.InspectSameNodeRetention(ctx, "volume-test", "node-test")
			if imageID == 7 {
				require.NoError(t, err)
				require.Equal(t, 7, evidence.ImageID)
				attachments, nodes, err := p.ListCurrentAttachments(ctx)
				require.NoError(t, err)
				require.Len(t, attachments, 1)
				require.Equal(t, 7, attachments[0].ImageID)
				require.Equal(t, []int{12}, nodes["other"])
			} else {
				require.ErrorContains(t, err, "CSI disk serial contradicts VM disk image identity")
				require.Nil(t, evidence)
			}
			require.Zero(t, mutations.Load())
		})
	}
}
