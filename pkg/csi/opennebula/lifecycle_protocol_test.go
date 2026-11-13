package opennebula

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The fixture exercises the actual GoCA XML-RPC transport, not a mocked provider.
func lifecycleFixture(t *testing.T, reply func(string) (bool, string, int), inspect ...func(string, []byte)) *PersistentDiskVolumeProvider {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call struct {
			Method string `xml:"methodName"`
		}
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, xml.NewDecoder(bytes.NewReader(raw)).Decode(&call))
		for _, observe := range inspect {
			observe(call.Method, raw)
		}
		ok, body, code := reply(call.Method)
		var escaped strings.Builder
		require.NoError(t, xml.EscapeText(&escaped, []byte(body)))
		b := 0
		if ok {
			b = 1
		}
		w.Header().Set("Content-Type", "text/xml")
		if ok && call.Method == "one.image.clone" {
			_, err := fmt.Fprintf(w, `<?xml version="1.0"?><methodResponse><params><param><value><array><data><value><boolean>1</boolean></value><value><int>%s</int></value><value><int>0</int></value></data></array></value></param></params></methodResponse>`, body)
			require.NoError(t, err)
			return
		}
		_, err = fmt.Fprintf(w, `<?xml version="1.0"?><methodResponse><params><param><value><array><data><value><boolean>%d</boolean></value><value><string>%s</string></value><value><int>%d</int></value></data></array></value></param></params></methodResponse>`, b, escaped.String(), code)
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	p, err := NewPersistentDiskVolumeProvider(NewClient(OpenNebulaConfig{Endpoint: server.URL, Credentials: "fixture:fixture"}), HotplugTimeoutPolicy{BaseTimeout: time.Second, MaxTimeout: time.Second, PollInterval: time.Millisecond})
	require.NoError(t, err)
	return p
}

func TestLifecycleLookupDNSIsUnknown(t *testing.T) {
	require.False(t, openNebulaLookupNotFound(fmt.Errorf("dial tcp: lookup unavailable.invalid: no such host")))
}

func TestLifecycleDeleteLookupFailureCannotSucceed(t *testing.T) {
	p := lifecycleFixture(t, func(method string) (bool, string, int) { return false, "authorization failed", 0x200 })
	require.Error(t, p.DeleteVolume(context.Background(), "volume-test"))
}

func TestLifecycleDeletionObservationUnknownAndPending(t *testing.T) {
	t.Run("unknown", func(t *testing.T) {
		p := lifecycleFixture(t, func(string) (bool, string, int) { return false, "authorization failed", 0x200 })
		absent, err := p.volumeDeleted(7)
		require.Error(t, err)
		require.False(t, absent)
	})
	t.Run("present remains pending", func(t *testing.T) {
		p := lifecycleFixture(t, func(string) (bool, string, int) {
			return true, `<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>1</STATE><SIZE>128</SIZE></IMAGE>`, 0
		})
		absent, err := p.volumeDeleted(7)
		require.NoError(t, err)
		require.False(t, absent)
	})
}

func TestLifecycleHistoricalForceNeverDeletes(t *testing.T) {
	mutations := 0
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		switch method {
		case "one.imagepool.info":
			return true, `<IMAGE_POOL><IMAGE><ID>7</ID><NAME>volume-test</NAME></IMAGE></IMAGE_POOL>`, 0
		case "one.image.info":
			return true, `<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>1</STATE><SIZE>128</SIZE><SOURCE>/fixture/disk</SOURCE><TEMPLATE><FORCE_DELETE>YES</FORCE_DELETE></TEMPLATE></IMAGE>`, 0
		default:
			mutations++
			return false, "unexpected mutation", 0x800
		}
	})
	require.ErrorContains(t, p.DeleteVolume(context.Background(), "volume-test"), "FORCE_DELETE")
	require.Zero(t, mutations)
}

func TestLifecycleDeleteUsesPinnedIDAndNonforce(t *testing.T) {
	deleted := false
	polls := 0
	var deleteArgs struct {
		Params []struct {
			Value struct {
				Int  int    `xml:"int"`
				I4   int    `xml:"i4"`
				I8   int    `xml:"i8"`
				Bool string `xml:"boolean"`
			} `xml:"value"`
		} `xml:"params>param"`
	}
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		switch method {
		case "one.imagepool.info":
			return true, `<IMAGE_POOL><IMAGE><ID>7</ID><NAME>volume-test</NAME></IMAGE></IMAGE_POOL>`, 0
		case "one.vmpool.info":
			return true, `<VM_POOL/>`, 0
		case "one.image.info":
			if deleted {
				polls++
				if polls > 1 {
					return false, "not present", 0x400
				}
			}
			return true, `<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>1</STATE><SIZE>128</SIZE><SOURCE>/fixture/disk</SOURCE><RUNNING_VMS>0</RUNNING_VMS></IMAGE>`, 0
		case "one.image.delete":
			deleted = true
			return true, "7", 0
		default:
			return false, "unexpected", 0x800
		}
	}, func(method string, raw []byte) {
		if method == "one.image.delete" {
			require.NoError(t, xml.Unmarshal(raw, &deleteArgs))
		}
	})
	require.NoError(t, p.DeleteVolume(context.Background(), "volume-test"))
	require.GreaterOrEqual(t, polls, 2)
	require.Len(t, deleteArgs.Params, 3)
	require.Equal(t, 7, deleteArgs.Params[1].Value.Int+deleteArgs.Params[1].Value.I4+deleteArgs.Params[1].Value.I8)
	require.Equal(t, "0", deleteArgs.Params[2].Value.Bool)
}

func TestLifecycleResizeOptimisticMetadataIsPending(t *testing.T) {
	for _, marker := range []string{`<RESIZE>YES</RESIZE>`, `<SIZE_PREV>128</SIZE_PREV>`} {
		t.Run(marker, func(t *testing.T) {
			p := lifecycleFixture(t, func(string) (bool, string, int) {
				return true, `<VM><ID>11</ID><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE><DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>2</DISK_ID><SIZE>256</SIZE>` + marker + `</DISK></TEMPLATE></VM>`, 0
			})
			size, err := p.completedDiskSize(context.Background(), 11, 2, 7)
			require.NoError(t, err)
			require.Zero(t, size)
		})
	}
}

func TestLifecycleResizeRejectsWrongStateOrIdentity(t *testing.T) {
	for _, vmXML := range []string{`<VM><ID>11</ID><STATE>3</STATE><LCM_STATE>16</LCM_STATE></VM>`, `<VM><ID>11</ID><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE><DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>2</DISK_ID><SIZE>256</SIZE></DISK></TEMPLATE></VM>`} {
		p := lifecycleFixture(t, func(string) (bool, string, int) { return true, vmXML, 0 })
		size, err := p.completedDiskSize(context.Background(), 11, 2, 7)
		require.Error(t, err)
		require.Zero(t, size)
	}
}

func TestLifecycleReattachRestoresVerifiedCapacityAndWaitsPending(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			desired := int64(256 * sizeConversion)
			resized := 0
			reads := 0
			image := fmt.Sprintf(`<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>8</STATE><SIZE>128</SIZE><TEMPLATE><CSI_REQUESTED_SIZE_BYTES>%d</CSI_REQUESTED_SIZE_BYTES></TEMPLATE></IMAGE>`, desired)
			vmXML := func(marker string, size int) string {
				return fmt.Sprintf(`<VM><ID>11</ID><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE><DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>2</DISK_ID><SIZE>%d</SIZE>%s</DISK></TEMPLATE></VM>`, size, marker)
			}
			p := lifecycleFixture(t, func(method string) (bool, string, int) {
				switch method {
				case "one.image.info":
					return true, image, 0
				case "one.imagepool.info":
					return true, "<IMAGE_POOL>" + image + "</IMAGE_POOL>", 0
				case "one.vmpool.info":
					return true, "<VM_POOL>" + vmXML("", 128) + "</VM_POOL>", 0
				case "one.vm.info":
					reads++
					if pending && reads <= 3 {
						return true, vmXML("<RESIZE>YES</RESIZE><SIZE_PREV>128</SIZE_PREV>", 256), 0
					}
					if resized > 0 || pending {
						return true, vmXML("", 256), 0
					}
					return true, vmXML("", 128), 0
				case "one.vm.diskresize":
					resized++
					return true, "11", 0
				case "one.image.update":
					return true, "7", 0
				default:
					t.Errorf("unexpected %s", method)
					return false, "unexpected", 0x800
				}
			})
			require.NoError(t, p.RestoreVolumeCapacity(context.Background(), "volume-test", 11, 7))
			require.GreaterOrEqual(t, reads, 3)
			if pending {
				require.Zero(t, resized)
			} else {
				require.Equal(t, 1, resized)
			}
		})
	}
}

func TestLifecycleMalformedVMImageCannotEstablishAbsence(t *testing.T) {
	p := lifecycleFixture(t, func(string) (bool, string, int) {
		return true, `<VM><ID>11</ID><TEMPLATE><DISK><IMAGE_ID>broken</IMAGE_ID></DISK></TEMPLATE></VM>`, 0
	})
	_, err := p.GetVolumeInNode(context.Background(), 7, 11)
	require.ErrorContains(t, err, "malformed")
	require.NotErrorIs(t, err, ErrAttachmentNotFound)
}

func TestLifecycleExistingSnapshotDeleteWaitsAndUsesSnapshotCapacity(t *testing.T) {
	deleted := false
	polls := 0
	mutations := 0
	snapshot := `<SNAPSHOTS><SNAPSHOT><ID>3</ID><NAME>old</NAME><SIZE>64</SIZE><DATE>1</DATE></SNAPSHOT></SNAPSHOTS>`
	image := func(snap string) string {
		return `<IMAGE><ID>7</ID><NAME>source</NAME><STATE>1</STATE><SIZE>256</SIZE><RUNNING_VMS>0</RUNNING_VMS>` + snap + `</IMAGE>`
	}
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		switch method {
		case "one.image.info":
			if deleted {
				polls++
				if polls > 1 {
					return true, image(""), 0
				}
			}
			return true, image(snapshot), 0
		case "one.imagepool.info":
			return true, "<IMAGE_POOL>" + image(snapshot) + "</IMAGE_POOL>", 0
		case "one.vmpool.info":
			return true, "<VM_POOL/>", 0
		case "one.image.snapshotdelete":
			deleted = true
			mutations++
			return true, "7", 0
		default:
			t.Errorf("unexpected %s", method)
			return false, "unexpected", 0x800
		}
	})
	handle := EncodeVolumeSnapshotID(7, 3)
	snapshots, _, err := p.ListSnapshots(context.Background(), handle, "", 0, "")
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.EqualValues(t, 64*sizeConversion, snapshots[0].SizeBytes)
	require.NoError(t, p.DeleteSnapshot(context.Background(), handle))
	require.GreaterOrEqual(t, polls, 2)
	require.Equal(t, 1, mutations)
}
