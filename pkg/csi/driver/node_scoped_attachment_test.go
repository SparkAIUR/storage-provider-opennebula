package driver

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type nodeAttachmentProtocol struct {
	hideDiskInfo      bool
	ignoreMarkerWrite bool
	mu                sync.Mutex
	disks             map[int]int
	marker            int64
	mutations         []string
	failMethod        string
	claimNew          bool
	ready             bool
	pending           bool
}

func nodeAttachmentProvider(t *testing.T) (*opennebula.PersistentDiskVolumeProvider, *nodeAttachmentProtocol) {
	t.Helper()
	state := &nodeAttachmentProtocol{disks: map[int]int{41: 256}, marker: 256 * 1024 * 1024}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call struct {
			Method string `xml:"methodName"`
			Params []struct {
				Value struct {
					Int    int    `xml:"int"`
					I4     int    `xml:"i4"`
					I8     int    `xml:"i8"`
					String string `xml:"string"`
				} `xml:"value"`
			} `xml:"params>param"`
		}
		require.NoError(t, xml.NewDecoder(r.Body).Decode(&call))
		number := func(i int) int { v := call.Params[i].Value; return v.Int + v.I4 + v.I8 }
		state.mu.Lock()
		defer state.mu.Unlock()
		vmXML := func(id int, direct bool) string {
			name := "node-old"
			if id == 42 {
				name = "node-new"
			}
			disk := ""
			if size, ok := state.disks[id]; ok && (!direct || !state.hideDiskInfo) {
				marker := ""
				if state.pending {
					marker = "<RESIZE>YES</RESIZE><SIZE_PREV>128</SIZE_PREV>"
				}
				disk = fmt.Sprintf(`<DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdb</TARGET><SIZE>%d</SIZE><SERIAL>onecsi-7</SERIAL>%s</DISK>`, size, marker)
			}
			return fmt.Sprintf(`<VM><ID>%d</ID><NAME>%s</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE>%s</TEMPLATE><HISTORY_RECORDS><HISTORY><SEQ>0</SEQ><DS_ID>200</DS_ID></HISTORY></HISTORY_RECORDS></VM>`, id, name, disk)
		}
		imageXML := func() string {
			ids := ""
			count := 0
			for _, id := range []int{41, 42} {
				if _, ok := state.disks[id]; ok || (id == 42 && state.claimNew) {
					ids += fmt.Sprintf("<ID>%d</ID>", id)
					count++
				}
			}
			imageState := 1
			if count > 0 && !state.ready {
				imageState = 8
			}
			tag := ""
			if state.marker > 0 {
				tag = fmt.Sprintf("<CSI_REQUESTED_SIZE_BYTES>%d</CSI_REQUESTED_SIZE_BYTES>", state.marker)
			}
			return fmt.Sprintf(`<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>%d</STATE><SIZE>128</SIZE><DATASTORE_ID>100</DATASTORE_ID><SOURCE>/fixture/disk</SOURCE><PERSISTENT>0</PERSISTENT><RUNNING_VMS>%d</RUNNING_VMS><VMS>%s</VMS><TEMPLATE>%s</TEMPLATE></IMAGE>`, imageState, count, ids, tag)
		}
		ok, body, code := 1, "", 0
		if call.Method == state.failMethod {
			ok, body, code = 0, "observation unavailable", 0x800
		} else {
			switch call.Method {
			case "one.imagepool.info":
				body = "<IMAGE_POOL>" + imageXML() + "</IMAGE_POOL>"
			case "one.image.info":
				body = imageXML()
			case "one.vmpool.info":
				body = "<VM_POOL>" + vmXML(41, false) + vmXML(42, false) + "</VM_POOL>"
			case "one.vm.info":
				body = vmXML(number(1), true)
			case "one.datastorepool.info":
				body = `<DATASTORE_POOL><DATASTORE><ID>100</ID><NAME>images</NAME><TYPE>IMAGE</TYPE><DS_MAD>fs</DS_MAD><TM_MAD>ssh</TM_MAD><STATE>0</STATE><TOTAL_MB>20000</TOTAL_MB><FREE_MB>10000</FREE_MB></DATASTORE><DATASTORE><ID>200</ID><NAME>system</NAME><TYPE>SYSTEM</TYPE><TM_MAD>ssh</TM_MAD><STATE>0</STATE></DATASTORE></DATASTORE_POOL>`
			case "one.vm.attach":
				id := number(1)
				require.Contains(t, call.Params[2].Value.String, `IMAGE_ID="7"`)
				state.disks[id] = 128
				state.mutations = append(state.mutations, fmt.Sprintf("attach:%d:%t", id, strings.Contains(call.Params[2].Value.String, `READONLY="YES"`)))
				body = strconv.Itoa(id)
			case "one.vm.detach":
				id := number(1)
				require.Equal(t, 2, number(2))
				delete(state.disks, id)
				state.mutations = append(state.mutations, fmt.Sprintf("detach:%d", id))
				body = strconv.Itoa(id)
			case "one.vm.diskresize":
				id := number(1)
				require.Equal(t, 2, number(2))
				size, err := strconv.Atoi(call.Params[3].Value.String)
				require.NoError(t, err)
				state.disks[id] = size
				state.mutations = append(state.mutations, fmt.Sprintf("resize:%d:%d", id, size))
				body = strconv.Itoa(id)
			case "one.image.update":
				require.Equal(t, 1, number(3))
				key, raw, found := strings.Cut(strings.TrimSpace(call.Params[2].Value.String), "=")
				require.True(t, found)
				require.Equal(t, "CSI_REQUESTED_SIZE_BYTES", key)
				value, err := strconv.Unquote(strings.TrimSpace(raw))
				require.NoError(t, err)
				verified, err := strconv.ParseInt(value, 10, 64)
				require.NoError(t, err)
				if !state.ignoreMarkerWrite {
					state.marker = verified
				}
				state.mutations = append(state.mutations, "marker:"+value)
				body = "7"
			default:
				t.Errorf("unexpected XML-RPC method %s", call.Method)
				ok, body, code = 0, "unexpected", 0x800
			}
		}
		var escaped strings.Builder
		require.NoError(t, xml.EscapeText(&escaped, []byte(body)))
		_, err := fmt.Fprintf(w, `<methodResponse><params><param><value><array><data><value><boolean>%d</boolean></value><value><string>%s</string></value><value><int>%d</int></value></data></array></value></param></params></methodResponse>`, ok, escaped.String(), code)
		require.NoError(t, err)
	}))
	t.Cleanup(endpoint.Close)
	provider, err := opennebula.NewPersistentDiskVolumeProvider(opennebula.NewClient(opennebula.OpenNebulaConfig{Endpoint: endpoint.URL, Credentials: "fixture:fixture"}), opennebula.HotplugTimeoutPolicy{BaseTimeout: time.Second, MaxTimeout: time.Second, PollInterval: time.Millisecond}, opennebula.CephBridgeIdentities{})
	require.NoError(t, err)
	return provider, state
}

func TestProviderNodeScopedAbsenceAndGlobalOwnership(t *testing.T) {
	for _, scenario := range []string{"other-owner", "contradictory-image", "failed-vm", "failed-pool"} {
		t.Run(scenario, func(t *testing.T) {
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			state.ready = true
			if scenario == "contradictory-image" {
				state.claimNew = true
			}
			if scenario == "failed-vm" {
				state.failMethod = "one.vm.info"
			}
			if scenario == "failed-pool" {
				state.failMethod = "one.vmpool.info"
			}
			state.mu.Unlock()
			_, err := provider.GetVolumeInNode(context.Background(), 7, 42)
			if scenario == "other-owner" {
				require.ErrorIs(t, err, opennebula.ErrAttachmentNotFound)
				require.ErrorContains(t, provider.DeleteVolume(context.Background(), "volume-test"), "ownership is not consistently absent")
			} else {
				require.Error(t, err)
				require.NotErrorIs(t, err, opennebula.ErrAttachmentNotFound)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			require.Empty(t, state.mutations)
		})
	}
}

func TestProviderRequestedVMCapacityWithMultipleAttachments(t *testing.T) {
	for _, size := range []int{128, 256} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			state.disks[42] = size
			state.mu.Unlock()
			require.NoError(t, provider.RestoreVolumeCapacity(context.Background(), "volume-test", 42, 7))
			_, err := provider.ExpandVolume(context.Background(), "volume-test", 512*1024*1024, false)
			require.Error(t, err)
			state.mu.Lock()
			require.Equal(t, 256, state.disks[42])
			require.Equal(t, 256, state.disks[41])
			state.mu.Unlock()
			require.NoError(t, provider.DetachVolume(context.Background(), "volume-test", "node-new"))
			state.mu.Lock()
			defer state.mu.Unlock()
			require.Equal(t, 256, state.disks[41])
			_, exists := state.disks[42]
			require.False(t, exists)
		})
	}
}

func TestControllerPublishRealProviderStickyMoveAndSharedReader(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%v", shared), func(t *testing.T) {
			provider, state := nodeAttachmentProvider(t)
			mode := corev1.ReadWriteOnce
			if shared {
				mode = corev1.ReadOnlyMany
			}
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{mode}, nil)
			pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().UTC().Add(time.Minute).Format(time.RFC3339)
			driver := newStickyTestDriver(t, pv, pvc)
			if !shared {
				require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{VolumeID: "volume-test", NodeID: "node-old", Backend: "local", StartedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Minute), GraceSeconds: 60}))
			}
			controller := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
			client := reviewControllerClient(t, controller)
			req := newPublishReq("volume-test", "node-new")
			if shared {
				req.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
				req.Readonly = true
			}
			response, err := client.ControllerPublishVolume(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, "vdb", response.GetPublishContext()["volumeName"])
			state.mu.Lock()
			require.Equal(t, 256, state.disks[42])
			if shared {
				require.Equal(t, 256, state.disks[41])
				require.NotContains(t, state.mutations, "detach:41")
				require.Contains(t, state.mutations, "attach:42:true")
			} else {
				_, exists := state.disks[41]
				require.False(t, exists)
				require.Contains(t, state.mutations, "detach:41")
				require.Contains(t, state.mutations, "attach:42:false")
			}
			state.mu.Unlock()
			if !shared {
				_, exists := driver.stickyAttachments.Get("volume-test")
				require.False(t, exists)
				current, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.NotContains(t, current.Annotations, annotationAllowCrossNodeUntil)
			}
		})
	}
}

func TestProviderRequestedVMCapacityWaitsForCompletion(t *testing.T) {
	provider, state := nodeAttachmentProvider(t)
	state.mu.Lock()
	state.disks[42] = 256
	state.pending = true
	state.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	require.Error(t, provider.RestoreVolumeCapacity(ctx, "volume-test", 42, 7))
	state.mu.Lock()
	defer state.mu.Unlock()
	require.Empty(t, state.mutations)
}
