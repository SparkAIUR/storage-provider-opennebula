package opennebula

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	"github.com/stretchr/testify/require"
)

const persistentSystemDiskFacts = `<PERSISTENT>YES</PERSISTENT><CLONE>NO</CLONE><LN_TARGET>SYSTEM</LN_TARGET><TM_MAD>fs_lvm_ssh</TM_MAD><TM_MAD_SYSTEM>fs_lvm_ssh</TM_MAD_SYSTEM>`

// Stock Ceph with a Ceph system datastore uses LN_TARGET=NONE in oned.conf.
const persistentNoneDiskFacts = `<PERSISTENT>YES</PERSISTENT><CLONE>NO</CLONE><LN_TARGET>NONE</LN_TARGET><TM_MAD>ceph</TM_MAD><TM_MAD_SYSTEM>ceph</TM_MAD_SYSTEM><DISK_TYPE>RBD</DISK_TYPE>`

type growthProtocolState struct {
	completedReads                                            atomic.Int32
	resizeCalls, metadataWrites, otherMutations, versionCalls atomic.Int32
	sizeMB, marker                                            atomic.Int64
}

type growthFixtureConfig struct {
	diskFacts, imagePersistent string
	initialMB                  int64
	versionReply               func() (bool, string, int)
	pendingReads               int32
	beforeObservation          func(string)
}

func growthCompatibilityFixture(t *testing.T, version string, restoring bool, options ...func(*growthFixtureConfig)) (*PersistentDiskVolumeProvider, *growthProtocolState) {
	t.Helper()
	cfg := growthFixtureConfig{diskFacts: persistentSystemDiskFacts, imagePersistent: "1", initialMB: 128, pendingReads: 1, versionReply: func() (bool, string, int) { return true, version, 0 }}
	for _, apply := range options {
		apply(&cfg)
	}
	st := &growthProtocolState{}
	st.sizeMB.Store(cfg.initialMB)
	st.marker.Store(128 * sizeConversion)
	if restoring {
		st.marker.Store(256 * sizeConversion)
	}
	image := func() string {
		persistenceXML := ""
		if cfg.imagePersistent != "" {
			persistenceXML = "<PERSISTENT>" + cfg.imagePersistent + "</PERSISTENT>"
		}
		return fmt.Sprintf(`<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>8</STATE><SIZE>128</SIZE>%s<UID>9</UID><GID>10</GID><TEMPLATE><CSI_REQUESTED_SIZE_BYTES>%d</CSI_REQUESTED_SIZE_BYTES><PROTECTED>YES</PROTECTED></TEMPLATE></IMAGE>`, persistenceXML, st.marker.Load())
	}
	vmXML := func() string {
		markers := ""
		if st.resizeCalls.Load() > 0 {
			if st.completedReads.Add(1) <= cfg.pendingReads {
				markers = "<RESIZE>YES</RESIZE><SIZE_PREV>128</SIZE_PREV>"
			}
		}
		return fmt.Sprintf(`<VM><ID>11</ID><NAME>node-test</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE><DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdb</TARGET><SIZE>%d</SIZE><ORIGINAL_SIZE>128</ORIGINAL_SIZE>%s%s</DISK></TEMPLATE></VM>`, st.sizeMB.Load(), cfg.diskFacts, markers)
	}
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		if cfg.beforeObservation != nil {
			cfg.beforeObservation(method)
		}
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
			return cfg.versionReply()
		case "one.vm.diskresize":
			st.resizeCalls.Add(1)
			st.sizeMB.Store(256)
			return true, "11", 0
		case "one.image.update":
			if st.resizeCalls.Load() > 0 {
				require.Greater(t, st.completedReads.Load(), cfg.pendingReads, "metadata only after completed provider readback")
			}
			st.metadataWrites.Add(1)
			st.marker.Store(256 * sizeConversion)
			return true, "7", 0
		default:
			st.otherMutations.Add(1)
			return false, "unexpected operation", 0x800
		}
	}, func(method string, raw []byte) {
		if method != "one.image.update" {
			return
		}
		var call struct {
			Params []struct {
				Value struct {
					Text string `xml:"string"`
				} `xml:"value"`
			} `xml:"params>param"`
		}
		require.NoError(t, xml.Unmarshal(raw, &call))
		require.Len(t, call.Params, 4)
		require.Contains(t, call.Params[2].Value.Text, requestedSizeTag)
		for _, protected := range []string{"PROTECTED", "UID", "GID", "PERSISTENT"} {
			require.NotContains(t, call.Params[2].Value.Text, protected)
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
			t.Logf("resize=%d metadataWrites=%d otherMutations=%d error=%v", st.resizeCalls.Load(), st.metadataWrites.Load(), st.otherMutations.Load(), err)
			require.Zero(t, st.resizeCalls.Load(), "reject before diskresize")
			require.Zero(t, st.metadataWrites.Load(), "reject before image metadata changes")
			require.Zero(t, st.otherMutations.Load(), "no attach/detach/allocate/update mutations")
			require.Equal(t, beforeMarker, st.marker.Load(), "durable capacity retained")
			require.EqualValues(t, 128, st.sizeMB.Load(), "attachment remains unchanged")
			require.Error(t, err, "7.4.0 cannot preserve persistent SYSTEM-target size through detach/reattach")
		})
	}
}

func TestProviderGrowthCompatibilityVersions(t *testing.T) {
	for _, tc := range []struct {
		version string
		allowed bool
	}{
		{"7.4.0", false}, {"7.4.1", true}, {"7.4.10", true}, {"7.4.01", false}, {"7.4.00", false}, {"7.4", false}, {"7.5.1", false}, {"8.0.1", false}, {"7.3.99", false}, {"7.4.1-rc.1", false}, {"7.4.1+vendor", false}, {"OpenNebula 7.4.1", false}, {"7.4.1 (vendor)", false}, {" 7.4.1", false}, {"7.4.1\n", false}, {"7.4.-1", false}, {"7.4.18446744073709551616", false}, {"", false},
	} {
		t.Run(tc.version, func(t *testing.T) {
			p, st := growthCompatibilityFixture(t, tc.version, false)
			size, err := p.ExpandVolume(context.Background(), "volume-test", 256*sizeConversion, false)
			require.EqualValues(t, 1, st.versionCalls.Load())
			if tc.allowed {
				require.NoError(t, err)
				require.EqualValues(t, 256*sizeConversion, size)
				require.EqualValues(t, 1, st.resizeCalls.Load())
				require.EqualValues(t, 1, st.metadataWrites.Load())
				require.GreaterOrEqual(t, st.completedReads.Load(), int32(2))
			} else {
				require.Error(t, err)
				require.True(t, IsDatastoreConfigError(err))
				require.Zero(t, st.resizeCalls.Load())
				require.Zero(t, st.metadataWrites.Load())
			}
			require.Zero(t, st.otherMutations.Load())
		})
	}
}

func TestProviderGrowthCompatibilityUnknownIdentity(t *testing.T) {
	for _, tc := range []struct{ name, facts, persistence string }{
		{"missing disk persistence", strings.ReplaceAll(persistentSystemDiskFacts, "<PERSISTENT>YES</PERSISTENT>", ""), "1"},
		{"nonpersistent disk", strings.ReplaceAll(persistentSystemDiskFacts, "<PERSISTENT>YES</PERSISTENT>", "<PERSISTENT>NO</PERSISTENT>"), "1"},
		{"missing clone mode", strings.ReplaceAll(persistentSystemDiskFacts, "<CLONE>NO</CLONE>", ""), "1"},
		{"contradictory clone mode", strings.ReplaceAll(persistentSystemDiskFacts, "<CLONE>NO</CLONE>", "<CLONE>YES</CLONE><CLONE_TARGET>NONE</CLONE_TARGET>"), "1"},
		{"missing effective target", strings.ReplaceAll(persistentSystemDiskFacts, "<LN_TARGET>SYSTEM</LN_TARGET>", ""), "1"},
		{"unknown effective target", strings.ReplaceAll(persistentSystemDiskFacts, "SYSTEM", "UNKNOWN"), "1"},
		{"missing transfer driver", strings.ReplaceAll(persistentSystemDiskFacts, "<TM_MAD>fs_lvm_ssh</TM_MAD>", ""), "1"},
		{"explicit cloned ephemeral class unqualified", `<CLONE>YES</CLONE><SAVE>NO</SAVE><CLONE_TARGET>SYSTEM</CLONE_TARGET><TM_MAD>ssh</TM_MAD>`, "0"},
		{"missing image persistence", persistentSystemDiskFacts, ""}, {"nonpersistent image", persistentSystemDiskFacts, "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, st := growthCompatibilityFixture(t, "7.4.1", false, func(c *growthFixtureConfig) { c.diskFacts = tc.facts; c.imagePersistent = tc.persistence })
			_, err := p.ExpandVolume(context.Background(), "volume-test", 256*sizeConversion, false)
			require.Error(t, err)
			require.True(t, IsDatastoreConfigError(err))
			require.Zero(t, st.resizeCalls.Load())
			require.Zero(t, st.metadataWrites.Load())
			require.Zero(t, st.otherMutations.Load())
		})
	}
}

func TestProviderGrowthCompatibilityUnaffectedAndIdempotent(t *testing.T) {
	t.Run("persistent NONE target retains 740 support", func(t *testing.T) {
		p, st := growthCompatibilityFixture(t, "7.4.0", false, func(c *growthFixtureConfig) {
			c.diskFacts = persistentNoneDiskFacts
		})
		size, err := p.ExpandVolume(context.Background(), "volume-test", 256*sizeConversion, false)
		require.NoError(t, err)
		require.EqualValues(t, 256*sizeConversion, size)
		require.EqualValues(t, 1, st.resizeCalls.Load())
	})
	for _, restore := range []bool{false, true} {
		t.Run(fmt.Sprintf("already satisfied restore=%t", restore), func(t *testing.T) {
			p, st := growthCompatibilityFixture(t, "7.4.0", restore, func(c *growthFixtureConfig) { c.initialMB = 256 })
			if restore {
				require.NoError(t, p.RestoreVolumeCapacity(context.Background(), "volume-test", 11, 7))
			} else {
				_, err := p.ExpandVolume(context.Background(), "volume-test", 256*sizeConversion, false)
				require.NoError(t, err)
			}
			require.Zero(t, st.resizeCalls.Load())
			require.Zero(t, st.versionCalls.Load())
			require.Zero(t, st.otherMutations.Load())
		})
	}
	t.Run("basic Probe unchanged", func(t *testing.T) {
		p, st := growthCompatibilityFixture(t, "7.4.0", false)
		client := &OpenNebulaClient{Client: p.ctrl.Client.(*goca.Client)}
		require.NoError(t, client.Probe(context.Background()))
		require.EqualValues(t, 1, st.versionCalls.Load())
		require.Zero(t, st.resizeCalls.Load())
	})
}

func TestProviderGrowthCompatibilityFreshVersionAndFailures(t *testing.T) {
	t.Run("fresh version each real growth", func(t *testing.T) {
		calls := 0
		p, st := growthCompatibilityFixture(t, "", false, func(c *growthFixtureConfig) {
			c.versionReply = func() (bool, string, int) {
				calls++
				if calls == 1 {
					return true, "7.4.0", 0
				}
				return true, "7.4.1", 0
			}
		})
		_, err := p.ExpandVolume(context.Background(), "volume-test", 256*sizeConversion, false)
		require.Error(t, err)
		require.Zero(t, st.resizeCalls.Load())
		_, err = p.ExpandVolume(context.Background(), "volume-test", 256*sizeConversion, false)
		require.NoError(t, err)
		require.EqualValues(t, 2, st.versionCalls.Load())
		require.EqualValues(t, 1, st.resizeCalls.Load())
	})
	for _, restoring := range []bool{false, true} {
		for _, mode := range []string{"fault", "cancel"} {
			t.Run(fmt.Sprintf("%s/restoration=%t", mode, restoring), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				p, st := growthCompatibilityFixture(t, "", restoring, func(c *growthFixtureConfig) {
					c.versionReply = func() (bool, string, int) {
						if mode == "cancel" {
							cancel()
							return true, "7.4.1", 0
						}
						return false, "authorization failed", 0x200
					}
				})
				beforeMarker := st.marker.Load()
				var err error
				if restoring {
					err = p.RestoreVolumeCapacity(ctx, "volume-test", 11, 7)
				} else {
					_, err = p.ExpandVolume(ctx, "volume-test", 256*sizeConversion, false)
				}
				require.Error(t, err)
				if mode == "cancel" {
					require.True(t, errors.Is(err, context.Canceled))
				}
				require.Zero(t, st.resizeCalls.Load())
				require.Zero(t, st.metadataWrites.Load())
				require.Zero(t, st.otherMutations.Load())
				require.EqualValues(t, beforeMarker, st.marker.Load())
				require.EqualValues(t, 1, st.versionCalls.Load())
			})
		}
	}
	t.Run("stalled completion cannot persist capacity", func(t *testing.T) {
		p, st := growthCompatibilityFixture(t, "7.4.1", false, func(c *growthFixtureConfig) { c.pendingReads = 100000 })
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err := p.ExpandVolume(ctx, "volume-test", 256*sizeConversion, false)
		require.Error(t, err)
		require.EqualValues(t, 1, st.resizeCalls.Load())
		require.Zero(t, st.metadataWrites.Load())
		require.EqualValues(t, 128*sizeConversion, st.marker.Load())
	})
}

// SYSTEM/SELF rows exercise explicit effective-target precedence, rather than
// claiming they are both stock fs_lvm_ssh defaults. NONE rows use stock Ceph.
func TestProviderGrowthCompatibilityEffectiveTargets(t *testing.T) {
	for _, tc := range []struct {
		target, version string
		allowed         bool
	}{
		{"SYSTEM", "7.4.0", false}, {"SELF", "7.4.0", false}, {"NONE", "7.4.0", true}, {"SYSTEM", "7.4.1", true}, {"SELF", "7.4.1", true}, {"NONE", "7.4.1", true},
	} {
		t.Run(tc.target+"/"+tc.version, func(t *testing.T) {
			p, st := growthCompatibilityFixture(t, tc.version, false, func(c *growthFixtureConfig) {
				c.diskFacts = strings.ReplaceAll(persistentSystemDiskFacts, "<LN_TARGET>SYSTEM</LN_TARGET>", "<LN_TARGET>"+tc.target+"</LN_TARGET>") + "<CLONE_TARGET>NONE</CLONE_TARGET>"
				if tc.target == "NONE" {
					c.diskFacts = persistentNoneDiskFacts + "<CLONE_TARGET>SYSTEM</CLONE_TARGET>"
				}
			})
			_, err := p.ExpandVolume(context.Background(), "volume-test", 256*sizeConversion, false)
			if tc.allowed {
				require.NoError(t, err)
				require.EqualValues(t, 1, st.resizeCalls.Load())
			} else {
				require.Error(t, err)
				require.Zero(t, st.resizeCalls.Load())
				require.Zero(t, st.metadataWrites.Load())
			}
		})
	}
}

func TestProviderGrowthCompatibilityObservationContextErrors(t *testing.T) {
	for _, restoring := range []bool{false, true} {
		for _, method := range []string{"one.vm.info", "one.image.info"} {
			for _, mode := range []string{"cancel", "deadline"} {
				t.Run(fmt.Sprintf("%s/%s/restoration=%t", method, mode, restoring), func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					expected := context.Canceled
					if mode == "deadline" {
						cancel()
						ctx, cancel = context.WithTimeout(context.Background(), time.Second)
						expected = context.DeadlineExceeded
					}
					defer cancel()
					selectedRead := int32(2)
					if restoring && method == "one.vm.info" {
						selectedRead = 4
					}
					var reads, laterObservations atomic.Int32
					var interrupted atomic.Bool
					p, st := growthCompatibilityFixture(t, "7.4.0", restoring, func(c *growthFixtureConfig) {
						c.diskFacts = persistentNoneDiskFacts
						c.beforeObservation = func(observed string) {
							if interrupted.Load() {
								laterObservations.Add(1)
							}
							if observed != method || reads.Add(1) != selectedRead {
								return
							}
							interrupted.Store(true)
							if mode == "cancel" {
								cancel()
							}
							<-ctx.Done()
						}
					})
					beforeMarker := st.marker.Load()
					var err error
					if restoring {
						err = p.RestoreVolumeCapacity(ctx, "volume-test", 11, 7)
					} else {
						_, err = p.ExpandVolume(ctx, "volume-test", 256*sizeConversion, false)
					}
					require.True(t, interrupted.Load(), "must reach the fresh compatibility observation")
					require.ErrorIs(t, err, expected)
					require.False(t, IsDatastoreConfigError(err))
					require.Zero(t, laterObservations.Load())
					require.Zero(t, st.versionCalls.Load())
					require.Zero(t, st.resizeCalls.Load())
					require.Zero(t, st.metadataWrites.Load())
					require.Zero(t, st.otherMutations.Load())
					require.Equal(t, beforeMarker, st.marker.Load())
					require.EqualValues(t, 128, st.sizeMB.Load())
				})
			}
		}
	}
}
