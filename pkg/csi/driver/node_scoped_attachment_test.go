package driver

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	mount "k8s.io/mount-utils"
)

type nodeAttachmentProtocol struct {
	endpoint          string
	hideDiskInfo      bool
	rejectFirstDetach bool
	detachAttempts    int
	ignoreMarkerWrite bool
	mu                sync.Mutex
	disks             map[int]int
	vmNames           map[int]string
	diskSlot          string
	extraDiskBefore   string
	extraDiskByVM     map[int]string
	marker            int64
	mutations         []string
	failMethod        string
	claimNew          bool
	ceph              bool
	imageName         string
	diskTarget        string
	ownerCount        int
	ready             bool
	pending           bool
	version           string
}

// This fixture models durable shareable storage, not ephemeral cloned disks.
// Image.cc emits PERSISTENT=YES/CLONE=NO/SAVE=YES and SHAREABLE=YES for a
// persistent raw image with PERSISTENT_TYPE=SHAREABLE. The former PERSISTENT=0
// contradicted both that intent and its USED_PERS state.
func nodeAttachmentProvider(t *testing.T, observe ...func(string, context.Context)) (*opennebula.PersistentDiskVolumeProvider, *nodeAttachmentProtocol) {
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
		for _, observer := range observe {
			observer(call.Method, r.Context())
		}
		number := func(i int) int { v := call.Params[i].Value; return v.Int + v.I4 + v.I8 }
		state.mu.Lock()
		defer state.mu.Unlock()
		vmXML := func(id int, direct bool) string {
			name := "node-old"
			if id == 42 {
				name = "node-new"
			}
			if override := state.vmNames[id]; override != "" {
				name = override
			}
			disk := ""
			if size, ok := state.disks[id]; ok && (!direct || !state.hideDiskInfo) {
				marker := ""
				if state.pending {
					marker = "<RESIZE>YES</RESIZE><SIZE_PREV>128</SIZE_PREV>"
				}
				slot := firstNonEmpty(state.diskSlot, "2")
				disk = fmt.Sprintf(`<DISK><PERSISTENT>YES</PERSISTENT><CLONE>NO</CLONE><SAVE>YES</SAVE><SHAREABLE>YES</SHAREABLE><LN_TARGET>SYSTEM</LN_TARGET><TM_MAD>ssh</TM_MAD><TM_MAD_SYSTEM>ssh</TM_MAD_SYSTEM><IMAGE_ID>7</IMAGE_ID><DISK_ID>%s</DISK_ID><TARGET>%s</TARGET><SIZE>%d</SIZE><SERIAL>onecsi-7</SERIAL>%s</DISK>`, slot, firstNonEmpty(state.diskTarget, "vdb"), size, marker)
				if state.ceph {
					disk = strings.NewReplacer("<LN_TARGET>SYSTEM</LN_TARGET>", "<LN_TARGET>NONE</LN_TARGET>", "<TM_MAD>ssh</TM_MAD>", "<TM_MAD>ceph</TM_MAD>", "<TM_MAD_SYSTEM>ssh</TM_MAD_SYSTEM>", "<TM_MAD_SYSTEM>ceph</TM_MAD_SYSTEM>").Replace(disk)
				}
			}
			return fmt.Sprintf(`<VM><ID>%d</ID><NAME>%s</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE>%s</TEMPLATE><HISTORY_RECORDS><HISTORY><SEQ>0</SEQ><DS_ID>200</DS_ID></HISTORY></HISTORY_RECORDS></VM>`, id, name, state.extraDiskBefore+disk+state.extraDiskByVM[id])
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
			if state.ownerCount > 0 {
				count = state.ownerCount
			}
			return fmt.Sprintf(`<IMAGE><ID>7</ID><NAME>%s</NAME><STATE>%d</STATE><SIZE>128</SIZE><DATASTORE_ID>100</DATASTORE_ID><SOURCE>/fixture/disk</SOURCE><PERSISTENT>1</PERSISTENT><FORMAT>raw</FORMAT><RUNNING_VMS>%d</RUNNING_VMS><VMS>%s</VMS><TEMPLATE><PERSISTENT_TYPE>SHAREABLE</PERSISTENT_TYPE>%s</TEMPLATE></IMAGE>`, firstNonEmpty(state.imageName, "volume-test"), imageState, count, ids, tag)
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
				if state.ceph {
					body = strings.NewReplacer("<DS_MAD>fs</DS_MAD>", "<DS_MAD>ceph</DS_MAD>", "<TM_MAD>ssh</TM_MAD>", "<TM_MAD>ceph</TM_MAD>").Replace(body)
				}
			case "one.vm.attach":
				id := number(1)
				require.Contains(t, call.Params[2].Value.String, `IMAGE_ID="7"`)
				state.disks[id] = 128
				state.mutations = append(state.mutations, fmt.Sprintf("attach:%d:%t", id, strings.Contains(call.Params[2].Value.String, `READONLY="YES"`)))
				body = strconv.Itoa(id)
			case "one.vm.detach":
				id := number(1)
				require.Equal(t, 2, number(2))
				state.detachAttempts++
				if state.rejectFirstDetach && state.detachAttempts == 1 {
					ok, body, code = 0, "wrong state HOTPLUG: provider-private-disk-detail", 0x800
					break
				}
				delete(state.disks, id)
				state.mutations = append(state.mutations, fmt.Sprintf("detach:%d", id))
				body = strconv.Itoa(id)
			case "one.system.version":
				body = state.version
				if body == "" {
					body = "7.4.1"
				}
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
	state.endpoint = endpoint.URL
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
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
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

func TestControllerDetachRechecksReleaseAtProviderAttempt(t *testing.T) {
	for _, mode := range []string{"first-attempt", "retry", "late-observe", "late-repair-first", "late-repair-retry", "released-retry", "remote-readers"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			accessMode := corev1.ReadWriteOnce
			if mode == "remote-readers" {
				accessMode = corev1.ReadOnlyMany
			}
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{accessMode}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			for _, node := range []string{"node-new", "node-other"} {
				if mode == "remote-readers" {
					_, err := driver.kubeRuntime.client.CoreV1().Pods(pvc.Namespace).Create(ctx, activePodForPVC(node, pvc.Name, node), metav1.CreateOptions{})
					require.NoError(t, err)
				}
			}
			imageReads, detachCalls := 0, 0
			provider, state := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				inject := false
				switch method {
				case "one.image.info":
					imageReads++
					inject = mode == "first-attempt" && imageReads == 6
					if mode == "late-observe" && imageReads == 6 {
						enableObserveOnConfigMapRead(t, driver.kubeRuntime.client.(*fake.Clientset), pvc, 1)
					}
				case "one.vm.detach":
					detachCalls++
					inject = mode == "retry" && detachCalls == 1
				}
				if (mode == "late-repair-first" && method == "one.image.info" && imageReads == 6) ||
					(mode == "late-repair-retry" && method == "one.vm.detach" && detachCalls == 1) {
					persistProtectionAfterHostRead(t, driver.kubeRuntime.client.(*fake.Clientset), volumeRepairStateConfigMapName, "volume-test",
						`{"version":1,"volumeID":"volume-test","classification":"wrong_device_identity","message":"provider-private-repair-detail"}`)
				}
				if inject {
					_, err := driver.kubeRuntime.client.CoreV1().Pods(pvc.Namespace).Create(ctx,
						activePodForPVC("late-consumer", pvc.Name, "node-old"), metav1.CreateOptions{})
					require.NoError(t, err)
				}
			})
			state.mu.Lock()
			state.rejectFirstDetach = mode == "retry" || mode == "released-retry" || mode == "late-repair-retry"
			state.mu.Unlock()

			client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
			resp, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
			state.mu.Lock()
			defer state.mu.Unlock()
			switch mode {
			case "first-attempt", "retry", "late-observe", "late-repair-first", "late-repair-retry":
				require.Error(t, err)
				got := status.Convert(err)
				switch mode {
				case "late-repair-first", "late-repair-retry":
					require.Equal(t, codes.FailedPrecondition, got.Code())
					require.Equal(t, "volume repair is required before attachment changes", got.Message())
				case "first-attempt", "retry":
					require.Equal(t, codes.Unavailable, got.Code())
					require.Equal(t, "a current Pod still references the volume on the detach node", got.Message())
				}
				events, listErr := driver.kubeRuntime.client.CoreV1().Events(pvc.Namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, listErr)
				for _, event := range events.Items {
					require.NotContains(t, event.Message, "provider-private-")
				}
				require.Nil(t, resp)
				require.Equal(t, 256, state.disks[41])
				require.Empty(t, state.mutations)
				want := 0
				if mode == "retry" || mode == "late-repair-retry" {
					want = 1
				}
				require.Equal(t, want, state.detachAttempts)
				require.True(t, driver.volumeHistory.Snapshot()["volume-test"].LastSafeDetachTime.IsZero())
			default:
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.NotContains(t, state.disks, 41)
				require.Equal(t, []string{"detach:41"}, state.mutations)
			}
		})
	}
}

func TestControllerDetachRejectsDriftInFinalProviderObservation(t *testing.T) {
	for _, retry := range []bool{false, true} {
		for _, drift := range []string{"size", "resize", "slot", "duplicate-slot"} {
			t.Run(fmt.Sprintf("retry=%v/%s", retry, drift), func(t *testing.T) {
				ctx := context.Background()
				pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
				driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
				imageReads, attempts := 0, 0
				resolvedAttemptVM := false
				var protocol *nodeAttachmentProtocol
				provider, state := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					switch method {
					case "one.image.info":
						imageReads++
					case "one.vmpool.info":
						resolvedAttemptVM = imageReads >= 5 && (!retry || attempts > 0)
					case "one.vm.detach":
						attempts++
						resolvedAttemptVM = false
					case "one.vm.info":
						if !resolvedAttemptVM || imageReads < 6 || (retry && imageReads < 7) {
							return
						}
						protocol.mu.Lock()
						defer protocol.mu.Unlock()
						switch drift {
						case "size":
							protocol.disks[41] = 384
						case "resize":
							protocol.pending = true
						case "slot":
							protocol.diskSlot = "provider-private-malformed-slot"
						case "duplicate-slot":
							protocol.extraDiskByVM = map[int]string{41: `<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdc</TARGET></DISK>`}
						}
					}
				})
				protocol = state
				state.mu.Lock()
				state.rejectFirstDetach = retry
				state.mu.Unlock()
				client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
				response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
				require.Nil(t, response)
				wantCode := codes.FailedPrecondition
				if drift == "slot" || drift == "duplicate-slot" {
					wantCode = codes.Unavailable
				}
				require.Equal(t, wantCode, status.Code(err))
				require.Equal(t, "detach disk identity or completed capacity is unverified", status.Convert(err).Message())
				events, err := driver.kubeRuntime.client.CoreV1().Events(pvc.Namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				for _, event := range events.Items {
					require.NotContains(t, event.Message, "provider-private-")
				}
				state.mu.Lock()
				defer state.mu.Unlock()
				require.True(t, resolvedAttemptVM)
				require.Contains(t, state.disks, 41)
				require.Empty(t, state.mutations)
				if retry {
					require.Equal(t, 1, state.detachAttempts)
				} else {
					require.Zero(t, state.detachAttempts)
				}
				require.True(t, driver.volumeHistory.Snapshot()["volume-test"].LastSafeDetachTime.IsZero())
			})
		}
	}
}

func TestCephRestartCleanupUsesVerifiedVMMapping(t *testing.T) {
	for _, mode := range []string{"exact", "mixed-case", "ambiguous", "unavailable", "dissent"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			for i := range 30 {
				_, err := driver.kubeRuntime.client.CoreV1().Nodes().Create(ctx, newReadyNode(fmt.Sprintf("no-vm-%d", i), true), metav1.CreateOptions{})
				require.NoError(t, err)
			}
			var poolReads atomic.Int32
			var protocol *nodeAttachmentProtocol
			provider, state := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if method == "one.vmpool.info" {
					poolReads.Add(1)
				}
				if mode == "dissent" && method == "one.image.info" && poolReads.Load() == 2 {
					protocol.mu.Lock()
					protocol.vmNames[41], protocol.vmNames[42] = "other-vm", "node-old"
					protocol.mu.Unlock()
				}
			})
			protocol = state
			state.mu.Lock()
			state.ceph = true
			state.vmNames = map[int]string{}
			switch mode {
			case "mixed-case", "dissent":
				state.vmNames[41] = "NODE-OLD"
			case "ambiguous":
				state.vmNames[41], state.vmNames[42] = "NODE-OLD", "Node-Old"
			case "unavailable":
				state.failMethod = "one.vmpool.info"
			}
			state.mu.Unlock()
			reconciler := NewAttachmentReconciler(NewControllerServer(driver, provider, nil))
			reconciler.orphanGrace = time.Nanosecond
			err := reconciler.ReconcileOnce(ctx)
			require.EqualValues(t, 1, poolReads.Load())
			switch mode {
			case "unavailable":
				require.Error(t, err)
				require.Empty(t, reconciler.orphanSeen)
			case "ambiguous":
				require.NoError(t, err)
				require.Empty(t, reconciler.orphanSeen)
				require.NoError(t, reconciler.ReconcileOnce(ctx))
				require.EqualValues(t, 2, poolReads.Load())
			default:
				require.NoError(t, err)
				require.Len(t, reconciler.orphanSeen, 1)
				require.NoError(t, reconciler.ReconcileOnce(ctx))
			}

			state.mu.Lock()
			defer state.mu.Unlock()
			if mode == "exact" || mode == "mixed-case" {
				require.Equal(t, []string{"detach:41"}, state.mutations)
				require.NotContains(t, state.disks, 41)
			} else {
				require.Zero(t, state.detachAttempts)
				require.Contains(t, state.disks, 41)
			}
		})
	}
}

type detachCensusProbe struct {
	*opennebula.PersistentDiskVolumeProvider
	inDetach, inCensus atomic.Bool
	beforeDetach       func()
}

func (p *detachCensusProbe) InspectVolumeAttachment(ctx context.Context, volume, node string) (*opennebula.VolumeAttachmentMetadata, error) {
	p.inCensus.Store(true)
	defer p.inCensus.Store(false)
	return p.PersistentDiskVolumeProvider.InspectVolumeAttachment(ctx, volume, node)
}

func (p *detachCensusProbe) DetachVolumeWithAuthority(ctx context.Context, volume, node string, expectedVMID int, expectedBackend string, verifyOwner opennebula.DetachOwnerCheck, authorize opennebula.DetachAuthorization) (bool, error) {
	if p.beforeDetach != nil {
		p.beforeDetach()
	}
	p.inDetach.Store(true)
	defer p.inDetach.Store(false)
	return p.PersistentDiskVolumeProvider.DetachVolumeWithAuthority(ctx, volume, node, expectedVMID, expectedBackend, verifyOwner, authorize)
}

func TestCephBackgroundDetachUsesFreshSoleOwnerAndRelease(t *testing.T) {
	for _, mode := range []string{"remote-released", "late-pod", "resize", "retry-resize", "late-owner-list", "late-owner-count", "late-name", "late-target", "retry-owner-list", "parked-contradictory", "demand-disappeared"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node, newReadyNode("node-new", true))
			_, err := driver.volumeHistory.Upsert(ctx, "volume-test", func(record *VolumeHistoryRecord) {
				record.Backend = "ceph"
				record.LastSuccessfulPublishTime = time.Now().UTC().Add(-time.Hour)
				record.LastSuccessfulImageID = 7
				record.LastSuccessfulNodeName = node.Name
				record.LastSuccessfulNodeUID = string(node.UID)
				record.LastSuccessfulOpenNebulaVMID = 41
			})
			require.NoError(t, err)
			pods := driver.kubeRuntime.client.CoreV1().Pods(pvc.Namespace)
			if mode != "parked-contradictory" {
				_, err := pods.Create(ctx, activePodForPVC("remote-consumer", pvc.Name, "node-new"), metav1.CreateOptions{})
				require.NoError(t, err)
			}

			var probe *detachCensusProbe
			var protocol *nodeAttachmentProtocol
			var censusImageRead, injected, censusFinished atomic.Bool
			provider, state := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if !probe.inDetach.Load() {
					return
				}
				if !probe.inCensus.Load() {
					if !censusFinished.Load() {
						return
					}
					protocol.mu.Lock()
					defer protocol.mu.Unlock()
					if method == "one.image.info" {
						switch {
						case mode == "late-owner-list" || (mode == "retry-owner-list" && protocol.detachAttempts == 1):
							protocol.claimNew = true
							injected.Store(true)
						case mode == "late-owner-count":
							protocol.ownerCount = 2
							injected.Store(true)
						case mode == "late-name":
							protocol.imageName = "replacement-image-name"
							injected.Store(true)
						}
					}
					if method == "one.vm.info" && mode == "late-target" {
						protocol.diskTarget = "vdc"
						injected.Store(true)
					}
					return
				}
				if method == "one.image.info" {
					censusImageRead.Store(true)
				}
				if method != "one.vmpool.info" || !censusImageRead.Swap(false) {
					return
				}
				censusFinished.Store(true)
				protocol.mu.Lock()
				defer protocol.mu.Unlock()
				switch {
				case mode == "late-pod" && injected.CompareAndSwap(false, true):
					_, err := pods.Create(ctx, activePodForPVC("late-consumer", pvc.Name, "node-old"), metav1.CreateOptions{})
					require.NoError(t, err)
				case mode == "resize" || (mode == "retry-resize" && protocol.detachAttempts == 1):
					injected.Store(true)
					protocol.pending = true
				}
			})
			protocol = state
			probe = &detachCensusProbe{PersistentDiskVolumeProvider: provider}
			if mode == "demand-disappeared" {
				probe.beforeDetach = func() {
					require.NoError(t, pods.Delete(ctx, "remote-consumer", metav1.DeleteOptions{}))
					state.mu.Lock()
					defer state.mu.Unlock()
					state.claimNew = true
				}
			}
			state.mu.Lock()
			state.ceph = true
			state.rejectFirstDetach = strings.HasPrefix(mode, "retry-")
			state.claimNew = mode == "parked-contradictory"
			state.mu.Unlock()

			r := NewAttachmentReconciler(NewControllerServer(driver, probe, nil))
			r.orphanGrace = time.Nanosecond
			require.NoError(t, r.ReconcileOnce(ctx))
			require.Len(t, r.orphanSeen, 1)
			require.NoError(t, r.ReconcileOnce(ctx))
			events, err := driver.kubeRuntime.client.CoreV1().Events(pvc.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			state.mu.Lock()
			defer state.mu.Unlock()
			if mode == "remote-released" {
				require.Equal(t, []string{"detach:41"}, state.mutations)
				require.Len(t, events.Items, 1)
				require.Equal(t, eventReasonStuckAttachmentDetached, events.Items[0].Reason)
				return
			}
			require.Empty(t, state.mutations)
			require.Contains(t, state.disks, 41)
			require.Empty(t, events.Items)
			switch mode {
			case "late-pod", "resize", "late-owner-list", "late-owner-count", "late-name", "late-target":
				require.True(t, injected.Load())
				require.Zero(t, state.detachAttempts)
			case "retry-resize", "retry-owner-list":
				require.True(t, injected.Load())
				require.Equal(t, 1, state.detachAttempts)
			default:
				require.Zero(t, state.detachAttempts)
			}
		})
	}
}

func TestBackgroundDetachLeavesNonCephRemoteReadersAttached(t *testing.T) {
	for _, divergent := range []bool{false, true} {
		t.Run(fmt.Sprint(divergent), func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true), newReadyNode("node-new", true), activePodForPVC("reader", pvc.Name, "node-new"))
			if divergent {
				_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().Create(ctx, &storagev1.VolumeAttachment{
					ObjectMeta: metav1.ObjectMeta{Name: "reader-attachment", UID: "reader-va"},
					Spec: storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: "node-new",
						Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
				}, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			provider, state := nodeAttachmentProvider(t)
			r := NewAttachmentReconciler(NewControllerServer(driver, provider, nil))
			r.orphanGrace = time.Nanosecond
			require.NoError(t, r.ReconcileOnce(ctx))
			require.NoError(t, r.ReconcileOnce(ctx))
			require.Empty(t, r.orphanSeen)
			require.Empty(t, r.divergentSeen)
			state.mu.Lock()
			defer state.mu.Unlock()
			require.Zero(t, state.detachAttempts)
			require.Contains(t, state.disks, 41)
		})
	}
}

type disappearingDetachProvider struct {
	*opennebula.PersistentDiskVolumeProvider
	disappear func()
}

func (p disappearingDetachProvider) DetachVolumeWithAuthority(ctx context.Context, volume, node string, expectedVMID int, expectedBackend string, verifyOwner opennebula.DetachOwnerCheck, authorize opennebula.DetachAuthorization) (bool, error) {
	p.disappear()
	return p.PersistentDiskVolumeProvider.DetachVolumeWithAuthority(ctx, volume, node, expectedVMID, expectedBackend, verifyOwner, authorize)
}

func TestAttachmentReconcilerDistinguishesProviderNoOp(t *testing.T) {
	for _, disappear := range []bool{false, true} {
		t.Run(fmt.Sprint(disappear), func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			provider, state := nodeAttachmentProvider(t)
			r := NewAttachmentReconciler(NewControllerServer(driver, disappearingDetachProvider{provider, func() {
				if disappear {
					state.mu.Lock()
					defer state.mu.Unlock()
					delete(state.disks, 41)
				}
			}}, nil))
			r.orphanGrace = time.Nanosecond
			require.NoError(t, r.ReconcileOnce(ctx))
			require.NoError(t, r.ReconcileOnce(ctx))
			events, err := driver.kubeRuntime.client.CoreV1().Events(pvc.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			state.mu.Lock()
			defer state.mu.Unlock()
			require.NotContains(t, state.disks, 41)
			if disappear {
				require.Zero(t, state.detachAttempts)
				require.Empty(t, events.Items)
			} else {
				require.Equal(t, 1, state.detachAttempts)
				require.Len(t, events.Items, 1)
				require.Equal(t, eventReasonStuckAttachmentDetached, events.Items[0].Reason)
			}
		})
	}
}

func TestBackgroundDetachRejectsProviderBackendConflict(t *testing.T) {
	for _, mode := range []string{"census-conflict", "attempt-conflict"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true), newReadyNode("node-new", true))
			provider, protocol := nodeAttachmentProvider(t)
			probe := &detachCensusProbe{PersistentDiskVolumeProvider: provider}
			if mode == "census-conflict" {
				protocol.mu.Lock()
				protocol.ceph = true
				protocol.disks[42] = 256
				protocol.mu.Unlock()
			} else {
				probe.beforeDetach = func() {
					protocol.mu.Lock()
					defer protocol.mu.Unlock()
					protocol.ceph = true
				}
			}
			r := NewAttachmentReconciler(NewControllerServer(driver, probe, nil))
			r.orphanGrace = time.Nanosecond
			require.NoError(t, r.ReconcileOnce(ctx))
			if mode == "census-conflict" {
				require.Empty(t, r.orphanSeen)
			}
			require.NoError(t, r.ReconcileOnce(ctx))
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Zero(t, protocol.detachAttempts)
			require.Contains(t, protocol.disks, 41)
		})
	}
}

func TestProviderDetachRetryRespectsHotplugDeadline(t *testing.T) {
	ctx := context.Background()
	provider, protocol := nodeAttachmentProvider(t)
	protocol.mu.Lock()
	protocol.rejectFirstDetach = true
	protocol.mu.Unlock()
	authorizations := 0
	accepted, err := provider.DetachVolumeWithAuthority(ctx, "volume-test", "node-old", 41, "", nil, func(attempt context.Context, _, _, _ int) error {
		authorizations++
		if authorizations == 2 {
			select {
			case <-attempt.Done():
			case <-time.After(2 * time.Second):
				t.Error("retry authorization missed the hotplug deadline")
			}
		}
		return nil
	})
	var timeout *opennebula.HotplugTimeoutError
	require.ErrorAs(t, err, &timeout)
	require.False(t, accepted)
	require.Equal(t, 2, authorizations)
	protocol.mu.Lock()
	defer protocol.mu.Unlock()
	require.Equal(t, 1, protocol.detachAttempts)
	require.Contains(t, protocol.disks, 41)
}

func TestControllerUnpublishBindsAbsenceToHistoricalNode(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"same-node", "replacement-uid", "replacement-vm", "vm-changed-before-dispatch"} {
			t.Run(fmt.Sprintf("queued=%v/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				node := newReadyNode("node-old", true)
				historicalUID := node.UID
				if mode == "replacement-uid" {
					node.UID = "replacement-uid"
				}
				driver := newStickyTestDriver(t, pv, pvc, node, newReadyNode("node-new", true))
				_, err := driver.volumeHistory.Upsert(ctx, "volume-test", func(record *VolumeHistoryRecord) {
					record.Backend = "ceph"
					record.LastSuccessfulPublishTime = time.Now().UTC().Add(-time.Hour)
					record.LastSuccessfulImageID = 7
					record.LastSuccessfulNodeName = node.Name
					record.LastSuccessfulNodeUID = string(historicalUID)
					record.LastSuccessfulOpenNebulaVMID = 41
				})
				require.NoError(t, err)
				var protocol *nodeAttachmentProtocol
				var imageReads atomic.Int32
				provider, state := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if mode == "vm-changed-before-dispatch" && method == "one.image.info" && imageReads.Add(1) == 2 {
						protocol.mu.Lock()
						defer protocol.mu.Unlock()
						protocol.vmNames = map[int]string{41: "retired-vm", 42: "node-old"}
					}
				})
				protocol = state
				state.mu.Lock()
				state.ceph = true
				switch mode {
				case "replacement-vm":
					state.vmNames = map[int]string{41: "retired-vm", 42: "node-old"}
				case "same-node", "replacement-uid":
					delete(state.disks, 41)
				}
				state.mu.Unlock()
				controller := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, controller.validateHotplugQueueRequest)
				}
				client := reviewControllerClient(t, controller)
				response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
				history, _ := driver.volumeHistory.Get("volume-test")
				if mode == "same-node" {
					require.NoError(t, err)
					require.NotNil(t, response)
					require.False(t, history.LastSafeDetachTime.IsZero())
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, response)
					require.True(t, history.LastSafeDetachTime.IsZero())
				}
				state.mu.Lock()
				defer state.mu.Unlock()
				require.Empty(t, state.mutations)
				if strings.Contains(mode, "vm") {
					require.Contains(t, state.disks, 41)
				}
			})
		}
	}
}

func TestForegroundAndStickyDetachPreserveProviderNoOpOutcome(t *testing.T) {
	for _, action := range []string{"unpublish", "expiry", "expiry-recreated", "transfer"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true), newReadyNode("node-new", true))
			eventSequence := 0
			driver.kubeRuntime.client.(*fake.Clientset).PrependReactor("create", "events", func(action ktesting.Action) (bool, kruntime.Object, error) {
				event := action.(ktesting.CreateAction).GetObject().(*corev1.Event)
				eventSequence++
				event.Name = fmt.Sprintf("%s%d", event.GenerateName, eventSequence)
				return false, nil, nil
			})
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, action == "transfer")
			tracker := opennebula.NewAdaptiveTimeoutTracker(opennebula.AdaptiveTimeoutConfig{Enabled: true})
			driver.adaptiveTimeouts = tracker
			sticky := StickyAttachmentState{VolumeID: "volume-test", NodeID: "node-old", Backend: "local", PVCName: pvc.Name, PVCNamespace: pvc.Namespace, PVCUID: pvc.UID, StartedAt: time.Now().Add(-2 * time.Minute), ExpiresAt: time.Now().Add(time.Minute), GraceSeconds: 60}
			if strings.HasPrefix(action, "expiry") {
				sticky.ExpiresAt = time.Now().Add(-time.Minute)
			}
			if action != "unpublish" {
				require.NoError(t, driver.stickyAttachments.StartGrace(sticky))
			}
			provider, protocol := nodeAttachmentProvider(t)
			wrapped := disappearingDetachProvider{provider, func() {
				protocol.mu.Lock()
				defer protocol.mu.Unlock()
				delete(protocol.disks, 41)
			}}
			controller := NewControllerServer(driver, wrapped, nil)
			client := reviewControllerClient(t, controller)
			switch action {
			case "unpublish":
				_, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
				require.NoError(t, err)
			case "expiry":
				controller.expireStickyDetach(ctx, sticky)
			case "expiry-recreated":
				kube := driver.kubeRuntime.client.CoreV1().PersistentVolumeClaims(pvc.Namespace)
				require.NoError(t, kube.Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
				replacement := pvc.DeepCopy()
				replacement.UID = "replacement"
				replacement.Spec.VolumeName = ""
				_, err := kube.Create(ctx, replacement, metav1.CreateOptions{})
				require.NoError(t, err)
				require.NoError(t, driver.stickyAttachments.LoadFromConfigMap(ctx))
				protocol.mu.Lock()
				delete(protocol.disks, 41)
				protocol.mu.Unlock()
				controller.expireStickyDetach(ctx, sticky)
			case "transfer":
				_, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-new"))
				require.NoError(t, err)
			}
			events, err := driver.kubeRuntime.client.CoreV1().Events(pvc.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			sawAbsence := false
			for _, event := range events.Items {
				if event.Reason == eventReasonDetachGraceCancelled || event.Reason == eventReasonDetachGraceExpired {
					require.Contains(t, event.Message, "already absent")
					require.Equal(t, pvc.UID, event.InvolvedObject.UID)
					sawAbsence = true
				}
			}
			if action != "unpublish" {
				require.Equal(t, action != "expiry-recreated", sawAbsence)
				_, held := driver.stickyAttachments.Get("volume-test")
				require.False(t, held)
			}
			for _, recommendation := range tracker.Snapshot() {
				require.NotEqual(t, "detach", recommendation.Key.Operation)
			}
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Zero(t, protocol.detachAttempts)
		})
	}
}

func TestControllerForegroundRejectsUnknownOrConflictingProviderBackend(t *testing.T) {
	for _, mode := range []string{"local-rescue", "maintenance-hold", "queue-disabled-pause", "foreground", "queued-foreground", "sticky-transfer", "sticky-expiry", "explicit-manual", "unknown-local-rescue", "unknown-foreground"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			if mode == "explicit-manual" {
				pvc.Annotations[annotationRecoveryMode] = recoveryModeManual
			}
			pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true), newReadyNode("node-new", true))
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			_, err := driver.volumeHistory.Upsert(ctx, "volume-test", func(record *VolumeHistoryRecord) {
				record.Backend = "local"
				record.LastSuccessfulPublishTime = time.Now().UTC().Add(-time.Hour)
				record.LastSuccessfulImageID = 7
			})
			require.NoError(t, err)
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			pod := activePodForPVC("local-consumer", pvc.Name, "node-old")
			switch mode {
			case "local-rescue", "explicit-manual", "unknown-local-rescue":
				_, err := kube.CoreV1().Pods(pvc.Namespace).Create(ctx, pod, metav1.CreateOptions{})
				require.NoError(t, err)
			case "queue-disabled-pause":
				podReads := 0
				kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, kruntime.Object, error) {
					podReads++
					if podReads == 2 {
						require.NoError(t, kube.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
					}
					return false, nil, nil
				})
			case "maintenance-hold":
				driver.maintenanceMode = NewMaintenanceModeManager(driver, "default")
				driver.maintenanceMode.setState(true, false, false)
			}
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			if strings.HasPrefix(mode, "unknown-") {
				state.failMethod = "one.datastorepool.info"
			} else {
				state.ceph = true
			}
			state.mu.Unlock()
			server := NewControllerServer(driver, provider, nil)
			if mode == "queued-foreground" {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			client := reviewControllerClient(t, server)
			sticky := StickyAttachmentState{VolumeID: "volume-test", NodeID: "node-old", Backend: "local", PVCNamespace: pvc.Namespace, PVCName: pvc.Name, StartedAt: time.Now().Add(-2 * time.Minute), ExpiresAt: time.Now().Add(time.Minute), GraceSeconds: 60}
			switch mode {
			case "sticky-expiry":
				sticky.ExpiresAt = time.Now().Add(-time.Minute)
				require.NoError(t, driver.stickyAttachments.StartGrace(sticky))
				server.expireStickyDetach(ctx, sticky)
			case "sticky-transfer":
				require.NoError(t, driver.stickyAttachments.StartGrace(sticky))
				reply, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-new"))
				require.Nil(t, reply)
				require.Equal(t, codes.Unavailable, status.Code(err))
			default:
				reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
				if mode == "explicit-manual" {
					require.NoError(t, err)
					require.NotNil(t, reply)
				} else {
					require.Nil(t, reply)
					require.Equal(t, codes.Unavailable, status.Code(err))
					if mode == "foreground" || mode == "queued-foreground" || mode == "unknown-foreground" {
						require.Equal(t, "detach disk identity or completed capacity is unverified", status.Convert(err).Message())
					} else {
						require.Equal(t, "provider attachment backend is unverified", status.Convert(err).Message())
					}
				}
			}
			history, _ := driver.volumeHistory.Get("volume-test")
			require.Equal(t, mode != "explicit-manual", history.LastSafeDetachTime.IsZero())
			_, held := driver.stickyAttachments.Get("volume-test")
			require.Equal(t, strings.HasPrefix(mode, "sticky-"), held)
			state.mu.Lock()
			defer state.mu.Unlock()
			require.Zero(t, state.detachAttempts)
			require.Empty(t, state.mutations)
			require.Contains(t, state.disks, 41)
		})
	}
}

func TestCephReleasedPVRetainsAuthorityAcrossClaimRecreation(t *testing.T) {
	for _, mode := range []string{"released", "unreadable", "old-node-in-use", "claim-references-old-pv", "missing-claim-with-pod"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			pv, replacement := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			pv.Status.Phase = corev1.VolumeReleased
			replacement.UID = "replacement-claim"
			replacement.Spec.VolumeName = ""
			replacement.Status.Phase = corev1.ClaimPending
			replacement.Annotations[annotationRecoveryMode] = recoveryModeManual
			if mode == "claim-references-old-pv" {
				replacement.Spec.VolumeName = pv.Name
			}
			node := newReadyNode("node-old", true)
			if mode == "old-node-in-use" {
				node.Status.VolumesInUse = []corev1.UniqueVolumeName{"kubernetes.io/csi/" + DefaultDriverName + "^volume-test"}
			}
			driver := newStickyTestDriver(t, pv, replacement, node, activePodForPVC("replacement-pod", replacement.Name, node.Name))
			_, err := driver.volumeHistory.Upsert(ctx, "volume-test", func(record *VolumeHistoryRecord) {
				record.Backend = "ceph"
				record.LastSuccessfulPublishTime = time.Now().UTC().Add(-time.Hour)
				record.LastSuccessfulImageID = 7
				record.LastSuccessfulNodeName = node.Name
				record.LastSuccessfulNodeUID = string(node.UID)
				record.LastSuccessfulOpenNebulaVMID = 41
			})
			require.NoError(t, err)
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			if mode == "missing-claim-with-pod" {
				require.NoError(t, kube.CoreV1().PersistentVolumeClaims(replacement.Namespace).Delete(ctx, replacement.Name, metav1.DeleteOptions{}))
				kube.ClearActions()
			}
			if mode == "unreadable" {
				kube.PrependReactor("get", "persistentvolumeclaims", func(ktesting.Action) (bool, kruntime.Object, error) {
					return true, nil, errors.New("replacement claim observation unavailable")
				})
			}
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			state.ceph = true
			state.mu.Unlock()
			r := NewAttachmentReconciler(NewControllerServer(driver, provider, nil))
			r.orphanGrace = time.Nanosecond
			require.NoError(t, r.ReconcileOnce(ctx))
			require.NoError(t, r.ReconcileOnce(ctx))
			events, err := kube.CoreV1().Events(replacement.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			require.Empty(t, events.Items)
			for _, action := range kube.Actions() {
				if action.GetResource().Resource == "persistentvolumeclaims" || action.GetResource().Resource == "pods" {
					require.Contains(t, []string{"get", "list"}, action.GetVerb())
				}
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if mode == "released" {
				require.Equal(t, []string{"detach:41"}, state.mutations)
				require.NotContains(t, state.disks, 41)
			} else {
				require.Zero(t, state.detachAttempts)
				require.Contains(t, state.disks, 41)
			}
		})
	}
}

func TestProviderDetachPreservesParentCancellationDuringPollAndRetry(t *testing.T) {
	for _, phase := range []string{"poll", "retry"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var detachStarted atomic.Bool
			provider, protocol := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if method == "one.vm.detach" {
					detachStarted.Store(true)
				}
				if phase == "poll" && method == "one.vm.info" && detachStarted.Load() {
					cancel()
				}
			})
			protocol.mu.Lock()
			protocol.rejectFirstDetach = true
			protocol.mu.Unlock()
			authorizations := 0
			accepted, err := provider.DetachVolumeWithAuthority(ctx, "volume-test", "node-old", 41, "", nil, func(attempt context.Context, _, _, _ int) error {
				authorizations++
				if phase == "retry" && authorizations == 2 {
					cancel()
					<-attempt.Done()
				}
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
			var timeout *opennebula.HotplugTimeoutError
			require.False(t, errors.As(err, &timeout))
			require.False(t, accepted)
			require.True(t, detachStarted.Load())
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Equal(t, 1, protocol.detachAttempts)
			require.Empty(t, protocol.mutations)
		})
	}
}

func TestControllerUnpublishCancellationDoesNotPauseHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
	driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
	driver.PluginConfig.OverrideVal(config.NodeHotplugGuardFailureThresholdVar, 1)
	var detachStarted atomic.Bool
	provider, protocol := nodeAttachmentProvider(t, func(method string, _ context.Context) {
		if method == "one.vm.detach" {
			detachStarted.Store(true)
		}
		if method == "one.vm.info" && detachStarted.Load() {
			cancel()
		}
	})
	protocol.mu.Lock()
	protocol.rejectFirstDetach = true
	protocol.mu.Unlock()
	server := NewControllerServer(driver, provider, nil)
	reply, err := server.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Nil(t, reply)
	require.True(t, detachStarted.Load())
	require.Empty(t, driver.hotplugGuard.Snapshot())
	history, _ := driver.volumeHistory.Get("volume-test")
	require.True(t, history.LastSafeDetachTime.IsZero())
	protocol.mu.Lock()
	defer protocol.mu.Unlock()
	require.Equal(t, 1, protocol.detachAttempts)
	require.Empty(t, protocol.mutations)
}

func TestControllerQueuedUnpublishPreservesPendingAndCoalescedCancellation(t *testing.T) {
	for _, coalesced := range []bool{false, true} {
		t.Run(fmt.Sprintf("coalesced=%t", coalesced), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			provider, protocol := nodeAttachmentProvider(t)
			server := NewControllerServer(driver, provider, nil)
			queue := NewHotplugQueueManager(nil, "default", driver.metrics, 4*time.Second, 0)
			queue.Configure(true, 0, 0, 0, nil)
			driver.hotplugQueue = queue

			blockerCtx, release := context.WithCancel(ctx)
			defer release()
			started, blockerDone := make(chan struct{}), make(chan error, 1)
			go func() {
				blockerDone <- queue.Run(blockerCtx, "node-old", "test", "other-volume", hotplugQueuePriorityNormal, func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				})
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("queue blocker did not start")
			}

			req := &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"}
			unpublish := func(ctx context.Context) <-chan error {
				done := make(chan error, 1)
				go func() {
					_, err := server.ControllerUnpublishVolume(ctx, req)
					done <- err
				}()
				return done
			}
			waitForPending := func(wantCoalesced int) {
				require.Eventually(t, func() bool {
					pending := queue.Snapshot()["node-old"].Queued
					return len(pending) == 1 && pending[0].Volume == req.VolumeId && pending[0].Coalesced == wantCoalesced
				}, time.Second, time.Millisecond)
			}
			leaderCtx, cancelLeader := context.WithCancel(ctx)
			defer cancelLeader()
			leaderDone := unpublish(leaderCtx)
			waitForPending(0)
			if coalesced {
				followerCtx, cancelFollower := context.WithCancel(ctx)
				defer cancelFollower()
				followerDone := unpublish(followerCtx)
				waitForPending(1)
				cancelFollower()
				require.Equal(t, codes.Canceled, status.Code(<-followerDone))
				waitForPending(1)
			}
			cancelLeader()
			require.Equal(t, codes.Canceled, status.Code(<-leaderDone))
			release()
			require.ErrorIs(t, <-blockerDone, context.Canceled)
			events, err := driver.kubeRuntime.client.CoreV1().Events(pvc.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			require.Empty(t, events.Items)
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Zero(t, protocol.detachAttempts)
		})
	}
}

func TestControllerQueuedUnpublishDistinguishesCallerAndActiveDeadlines(t *testing.T) {
	for _, queueLimit := range []bool{false, true} {
		t.Run(fmt.Sprintf("queue-limit=%t", queueLimit), func(t *testing.T) {
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			driver.PluginConfig.OverrideVal(config.HotplugDiagnosticsEnabledVar, true)
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			kube.PrependReactor("create", "events", func(action ktesting.Action) (bool, kruntime.Object, error) {
				event := action.(ktesting.CreateAction).GetObject().(*corev1.Event)
				event.Name = event.Reason
				return false, nil, nil
			})

			var attempted, blocked atomic.Bool
			provider, protocol := nodeAttachmentProvider(t, func(method string, ctx context.Context) {
				if method == "one.vm.detach" {
					attempted.Store(true)
				}
				if method == "one.vm.info" && attempted.Load() && blocked.CompareAndSwap(false, true) {
					<-ctx.Done()
				}
			})
			protocol.mu.Lock()
			protocol.rejectFirstDetach = true
			protocol.mu.Unlock()

			server := NewControllerServer(driver, provider, nil)
			driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, time.Second, 0)
			callerTimeout, activeLimit := 500*time.Millisecond, 5*time.Second
			if queueLimit {
				callerTimeout, activeLimit = 5*time.Second, 500*time.Millisecond
			}
			driver.hotplugQueue.Configure(true, 0, 0, activeLimit, nil)
			ctx, cancel := context.WithTimeout(context.Background(), callerTimeout)
			defer cancel()
			reply, err := server.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
			require.Nil(t, reply)
			require.Equal(t, codes.DeadlineExceeded, status.Code(err))
			require.True(t, blocked.Load(), "deadline must interrupt provider readback")
			events, err := kube.CoreV1().Events(pvc.Namespace).List(context.Background(), metav1.ListOptions{})
			require.NoError(t, err)
			sawQueueTimeout := false
			for _, event := range events.Items {
				if event.Reason == eventReasonHotplugQueueTimeout {
					sawQueueTimeout = true
					require.Equal(t, pvc.UID, event.InvolvedObject.UID)
					require.Contains(t, event.Message, "executing hotplug queue request")
					require.NotContains(t, event.Message, "onecsi-7")
					require.NotContains(t, event.Message, "provider-private-disk-detail")
				}
			}
			require.Equal(t, queueLimit, sawQueueTimeout)
			require.Equal(t, queueLimit, server.loadHotplugObservation(context.Background(), "node-old") != nil)
			require.Empty(t, driver.hotplugGuard.Snapshot())
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Equal(t, 1, protocol.detachAttempts)
			require.Empty(t, protocol.mutations)
		})
	}
}

func TestControllerPublishDistinguishesBlankAndMalformedImageReferences(t *testing.T) {
	for _, imageID := range []string{"", "invalid"} {
		t.Run(fmt.Sprintf("image-reference=%q", imageID), func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			provider, protocol := nodeAttachmentProvider(t)
			protocol.mu.Lock()
			protocol.extraDiskBefore = fmt.Sprintf(`<DISK><DISK_ID>4</DISK_ID><IMAGE_ID>%s</IMAGE_ID></DISK>`, imageID)
			protocol.mu.Unlock()
			client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
			reply, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-old"))
			history, _ := driver.volumeHistory.Get("volume-test")
			if imageID == "" {
				require.NoError(t, err)
				require.Equal(t, "vdb", reply.PublishContext["volumeName"])
				require.False(t, history.LastSuccessfulPublishTime.IsZero())
			} else {
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "malformed VM disk image identity")
				require.Nil(t, reply)
				require.True(t, history.LastSuccessfulPublishTime.IsZero())
			}
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Empty(t, protocol.mutations)
		})
	}
}

func TestDetachNoOpRejectsReplacementVMWithoutHistory(t *testing.T) {
	for _, action := range []string{"unpublish", "queued-unpublish", "expiry", "transfer", "orphan-decision"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node, newReadyNode("node-new", true))
			provider, protocol := nodeAttachmentProvider(t)
			remap := func() {
				protocol.mu.Lock()
				defer protocol.mu.Unlock()
				protocol.vmNames = map[int]string{41: "retired-vm", 42: "node-old"}
			}
			server := NewControllerServer(driver, disappearingDetachProvider{provider, remap}, nil)
			if action == "queued-unpublish" {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, time.Second, 0)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			client := reviewControllerClient(t, server)
			sticky := StickyAttachmentState{VolumeID: "volume-test", NodeID: node.Name, Backend: "local", PVCName: pvc.Name, PVCNamespace: pvc.Namespace, PVCUID: pvc.UID,
				StartedAt: time.Now().Add(-2 * time.Minute), ExpiresAt: time.Now().Add(time.Minute), GraceSeconds: 60}
			if action == "expiry" || action == "transfer" {
				if action == "expiry" {
					sticky.ExpiresAt = time.Now().Add(-time.Minute)
				}
				require.NoError(t, driver.stickyAttachments.StartGrace(sticky))
			}
			switch action {
			case "expiry":
				server.expireStickyDetach(ctx, sticky)
			case "transfer":
				reply, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-new"))
				require.Nil(t, reply)
				require.Equal(t, codes.Unavailable, status.Code(err))
			case "orphan-decision":
				remap()
				err := NewAttachmentReconciler(server).detachObservedAttachment(ctx, opennebula.ObservedAttachment{
					VolumeHandle: "volume-test", ImageID: 7, NodeName: node.Name, NodeID: 41, DiskID: 2,
				}, pv.UID, node.UID, "local", false, hotplugQueuePriorityBackground)
				require.ErrorContains(t, err, "provider attachment identity changed since inventory")
				require.NotErrorIs(t, err, errAttachmentAbsent)
			default:
				reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: node.Name})
				require.Nil(t, reply)
				require.Equal(t, codes.Unavailable, status.Code(err))
			}
			history, _ := driver.volumeHistory.Get("volume-test")
			require.True(t, history.LastSafeDetachTime.IsZero())
			_, held := driver.stickyAttachments.Get("volume-test")
			require.Equal(t, action == "expiry" || action == "transfer", held)
			events, err := driver.kubeRuntime.client.CoreV1().Events(pvc.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			for _, event := range events.Items {
				require.NotContains(t, []string{eventReasonStuckAttachmentDetached, eventReasonDetachGraceExpired, eventReasonDetachGraceCancelled}, event.Reason)
			}
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Equal(t, "node-old", protocol.vmNames[42], "test must reach the replacement VM lookup")
			require.Contains(t, protocol.disks, 41)
			require.Empty(t, protocol.mutations)
			require.Zero(t, protocol.detachAttempts)
		})
	}
}

func TestDetachRechecksNodeMountUseAfterBindingReads(t *testing.T) {
	for _, lateMount := range []bool{false, true} {
		t.Run(fmt.Sprintf("late-mount=%t", lateMount), func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			node := newReadyNode("node-old", true)
			donePod := activePodForPVC("terminated", pvc.Name, node.Name)
			donePod.Status.Phase = corev1.PodSucceeded
			driver := newStickyTestDriver(t, pv, pvc, node, donePod)
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			provider, protocol := nodeAttachmentProvider(t)
			probe := &detachCensusProbe{PersistentDiskVolumeProvider: provider}
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			pvReads := 0
			kube.PrependReactor("get", "persistentvolumes", func(ktesting.Action) (bool, kruntime.Object, error) {
				if probe.inDetach.Load() {
					pvReads++
					if pvReads == 2 && lateMount {
						mounted := node.DeepCopy()
						mounted.Status.VolumesInUse = []corev1.UniqueVolumeName{"kubernetes.io/csi/" + DefaultDriverName + "^volume-test"}
						require.NoError(t, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), mounted, ""))
					}
				}
				return false, nil, nil
			})
			client := reviewControllerClient(t, NewControllerServer(driver, probe, nil))
			reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: node.Name})
			require.GreaterOrEqual(t, pvReads, 2)
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			if lateMount {
				require.Nil(t, reply)
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "detach node still reports the volume in use")
				require.Contains(t, protocol.disks, 41)
				require.Zero(t, protocol.detachAttempts)
			} else {
				require.NoError(t, err)
				require.NotNil(t, reply)
				require.Equal(t, []string{"detach:41"}, protocol.mutations)
			}
		})
	}
}

type delayedAbsenceProvider struct {
	*opennebula.PersistentDiskVolumeProvider
	afterAbsent func(context.Context, int)
}

func (p delayedAbsenceProvider) GetVolumeInNode(ctx context.Context, imageID, vmID int) (string, error) {
	target, err := p.PersistentDiskVolumeProvider.GetVolumeInNode(ctx, imageID, vmID)
	if errors.Is(err, opennebula.ErrAttachmentNotFound) {
		p.afterAbsent(ctx, vmID)
	}
	return target, err
}

func TestUnpublishRechecksInitialAbsenceAfterConcurrentPublish(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%t", queued), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true), activePodForPVC("writer", pvc.Name, "node-old"))
			provider, protocol := nodeAttachmentProvider(t)
			protocol.mu.Lock()
			delete(protocol.disks, 41)
			protocol.marker = 128 * 1024 * 1024
			protocol.mu.Unlock()
			absent, resume := make(chan struct{}), make(chan struct{})
			var first atomic.Bool
			wrapped := delayedAbsenceProvider{provider, func(ctx context.Context, _ int) {
				if first.CompareAndSwap(false, true) {
					close(absent)
					select {
					case <-resume:
					case <-ctx.Done():
					}
				}
			}}
			server := NewControllerServer(driver, wrapped, nil)
			if queued {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, time.Second, 0)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			client := reviewControllerClient(t, server)
			done := make(chan error, 1)
			go func() {
				_, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
				done <- err
			}()
			select {
			case <-absent:
			case <-ctx.Done():
				t.Fatal("Unpublish did not observe initial absence")
			}
			reply, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-old"))
			require.NoError(t, err)
			require.Equal(t, "vdb", reply.GetPublishContext()["volumeName"])
			close(resume)
			require.Equal(t, codes.Aborted, status.Code(<-done))
			history, _ := driver.volumeHistory.Get("volume-test")
			require.False(t, history.LastSuccessfulPublishTime.IsZero())
			require.True(t, history.LastSafeDetachTime.IsZero())
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Equal(t, []string{"attach:41:false"}, protocol.mutations)
		})
	}
}

func TestUnpublishJoiningStickyExpiryVerifiesOwnCompletion(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(fmt.Sprintf("absent=%t", absent), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			provider, protocol := nodeAttachmentProvider(t)
			server := NewControllerServer(driver, provider, nil)
			queue := NewHotplugQueueManager(nil, "default", driver.metrics, 4*time.Second, 0)
			queue.Configure(true, 0, 0, 0, nil)
			driver.hotplugQueue = queue
			blocker, release := context.WithCancel(ctx)
			defer release()
			started, blockerDone := make(chan struct{}), make(chan error, 1)
			go func() {
				blockerDone <- queue.Run(blocker, "node-old", "test", "other-volume", hotplugQueuePriorityNormal, func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				})
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("queue blocker did not start")
			}
			sticky := StickyAttachmentState{VolumeID: "volume-test", NodeID: "node-old", Backend: "local", StartedAt: time.Now().Add(-time.Minute), ExpiresAt: time.Now().Add(-time.Second)}
			require.NoError(t, driver.stickyAttachments.StartGrace(sticky))
			expiryDone := make(chan struct{})
			go func() {
				defer close(expiryDone)
				server.expireStickyDetach(ctx, sticky)
			}()
			waitForPending := func(coalesced int) {
				require.Eventually(t, func() bool {
					pending := queue.Snapshot()["node-old"].Queued
					return len(pending) == 1 && pending[0].Volume == sticky.VolumeID && pending[0].Coalesced == coalesced
				}, time.Second, time.Millisecond)
			}
			waitForPending(0)
			client := reviewControllerClient(t, server)
			unpublishDone := make(chan error, 1)
			go func() {
				_, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: sticky.VolumeID, NodeId: sticky.NodeID})
				unpublishDone <- err
			}()
			waitForPending(1)
			require.NoError(t, driver.stickyAttachments.Clear(sticky.VolumeID))
			if absent {
				protocol.mu.Lock()
				delete(protocol.disks, 41)
				protocol.mu.Unlock()
			}
			release()
			require.ErrorIs(t, <-blockerDone, context.Canceled)
			err := <-unpublishDone
			<-expiryDone
			if absent {
				require.NoError(t, err)
			} else {
				require.Equal(t, codes.Aborted, status.Code(err))
			}
			history, _ := driver.volumeHistory.Get(sticky.VolumeID)
			require.Equal(t, !absent, history.LastSafeDetachTime.IsZero())
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Zero(t, protocol.detachAttempts)
		})
	}
}

func TestUnpublishDeniesAbsenceWhenCSISerialIdentifiesTheDisk(t *testing.T) {
	for _, imageReference := range []string{"", "<IMAGE_ID>8</IMAGE_ID>"} {
		t.Run(fmt.Sprintf("image-reference=%q", imageReference), func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			provider, protocol := nodeAttachmentProvider(t)
			protocol.mu.Lock()
			delete(protocol.disks, 41)
			protocol.extraDiskBefore = "<DISK><DISK_ID>4</DISK_ID><TARGET>vdb</TARGET><SERIAL>onecsi-7</SERIAL>" + imageReference + "</DISK>"
			protocol.mu.Unlock()

			client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
			reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
			require.Nil(t, reply)
			require.Equal(t, codes.Unavailable, status.Code(err))
			_, err = provider.GetVolumeInNode(ctx, 7, 41)
			require.ErrorContains(t, err, "CSI disk serial contradicts VM disk image identity")
			attachments, _, err := provider.ListCurrentAttachments(ctx)
			require.NoError(t, err)
			require.Empty(t, attachments)
			require.Error(t, provider.DetachVolume(ctx, "volume-test", "node-old"))
			history, _ := driver.volumeHistory.Get("volume-test")
			require.True(t, history.LastSafeDetachTime.IsZero())
			protocol.mu.Lock()
			defer protocol.mu.Unlock()
			require.Zero(t, protocol.detachAttempts)
		})
	}
}

func TestUnpublishRejectsProviderWithoutAttemptAuthorization(t *testing.T) {
	ctx := context.Background()
	pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
	driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
	provider, protocol := nodeAttachmentProvider(t)
	wrapped := struct {
		opennebula.OpenNebulaVolumeProvider
		opennebula.AttachmentBackendVerifier
	}{provider, provider}
	client := reviewControllerClient(t, NewControllerServer(driver, wrapped, nil))
	reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
	require.Nil(t, reply)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "provider cannot authorize physical detach")
	protocol.mu.Lock()
	defer protocol.mu.Unlock()
	require.Zero(t, protocol.detachAttempts)
}

func TestStickyTransferRetriesAgainstConcurrentAbsentUnpublish(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%t", queued), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true), newReadyNode("node-new", true),
				activePodForPVC("new-owner", pvc.Name, "node-new"))
			sticky := StickyAttachmentState{VolumeID: "volume-test", NodeID: "node-old", Backend: "local", StartedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}
			require.NoError(t, driver.stickyAttachments.StartGrace(sticky))

			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			delete(wire.disks, 41)
			wire.marker = 128 * 1024 * 1024
			wire.mu.Unlock()
			atDestination, resume := make(chan struct{}), make(chan struct{})
			var paused atomic.Bool
			wrapped := delayedAbsenceProvider{provider, func(ctx context.Context, vmID int) {
				if vmID != 42 {
					return
				}
				release, acquired := driver.operationLocks.TryAcquire(controllerVolumeLockKey(sticky.VolumeID))
				if acquired {
					release()
					return
				}
				if paused.CompareAndSwap(false, true) {
					close(atDestination)
					select {
					case <-resume:
					case <-ctx.Done():
					}
				}
			}}
			server := NewControllerServer(driver, wrapped, nil)
			if queued {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, time.Second, 0)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			client := reviewControllerClient(t, server)
			publishDone := make(chan error, 1)
			go func() {
				_, err := client.ControllerPublishVolume(ctx, newPublishReq(sticky.VolumeID, "node-new"))
				publishDone <- err
			}()
			select {
			case <-atDestination:
			case <-ctx.Done():
				t.Fatal("Publish did not reach its destination attachment check")
			}

			unpublishDone := make(chan error, 1)
			go func() {
				_, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: sticky.VolumeID, NodeId: sticky.NodeID})
				unpublishDone <- err
			}()
			require.Eventually(t, func() bool {
				release, acquired := driver.operationLocks.TryAcquire(controllerNodeLockKey(sticky.NodeID))
				if acquired {
					release()
				}
				return !acquired
			}, time.Second, time.Millisecond)

			close(resume)
			err := <-publishDone
			require.Equal(t, codes.Aborted, status.Code(err))
			require.ErrorContains(t, err, "old attachment node is busy")
			require.NoError(t, <-unpublishDone)
			require.Contains(t, driver.stickyAttachments.Snapshot(), sticky.VolumeID)

			reply, err := client.ControllerPublishVolume(ctx, newPublishReq(sticky.VolumeID, "node-new"))
			require.NoError(t, err)
			require.Equal(t, "vdb", reply.GetPublishContext()["volumeName"])
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Equal(t, []string{"attach:42:false"}, wire.mutations)
		})
	}
}

func TestCephOrphanCleanupIgnoresUnrelatedCSIImageConflict(t *testing.T) {
	ctx := context.Background()
	pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true), newReadyNode("node-new", true))
	provider, wire := nodeAttachmentProvider(t)
	wire.mu.Lock()
	wire.ceph = true
	wire.disks[41] = 128
	wire.marker = 128 * 1024 * 1024
	wire.extraDiskByVM = map[int]string{42: `<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>2</DISK_ID><SERIAL>onecsi-9</SERIAL><TARGET>vdc</TARGET></DISK>`}
	wire.mu.Unlock()

	_, err := provider.GetVolumeInNode(ctx, 7, 42)
	require.ErrorIs(t, err, opennebula.ErrAttachmentNotFound)
	reconciler := NewAttachmentReconciler(NewControllerServer(driver, provider, nil))
	reconciler.orphanGrace = time.Nanosecond
	require.NoError(t, reconciler.ReconcileOnce(ctx))
	require.Len(t, reconciler.orphanSeen, 1)
	require.NoError(t, reconciler.ReconcileOnce(ctx))
	wire.mu.Lock()
	defer wire.mu.Unlock()
	require.Equal(t, []string{"detach:41"}, wire.mutations)
	require.Equal(t, 1, wire.detachAttempts)
}

type moveHistoryProvider struct {
	*opennebula.PersistentDiskVolumeProvider
	wire         *nodeAttachmentProtocol
	destination  string
	failedCensus atomic.Int32
}

func (p *moveHistoryProvider) InspectVolumeAttachment(ctx context.Context, volume, node string) (*opennebula.VolumeAttachmentMetadata, error) {
	p.wire.mu.Lock()
	_, moved := p.wire.disks[42]
	if node != p.destination || !moved {
		p.wire.mu.Unlock()
		return p.PersistentDiskVolumeProvider.InspectVolumeAttachment(ctx, volume, node)
	}
	p.wire.failMethod = "one.vmpool.info"
	p.wire.mu.Unlock()
	defer func() {
		p.wire.mu.Lock()
		p.wire.failMethod = ""
		p.wire.mu.Unlock()
	}()
	metadata, err := p.PersistentDiskVolumeProvider.InspectVolumeAttachment(ctx, volume, node)
	if err != nil {
		p.failedCensus.Add(1)
	}
	return metadata, err
}

func TestControllerPublishMoveClearsStaleAttachmentPinsWhenCensusFails(t *testing.T) {
	for _, sameNode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sameNode=%t", sameNode), func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			oldNode, newNode := newReadyNode("node-old", true), newReadyNode("node-new", true)
			driver := newStickyTestDriver(t, pv, pvc, oldNode, newNode)
			if sameNode {
				newNode = oldNode
			}
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			provider, wire := nodeAttachmentProvider(t)
			probe := &moveHistoryProvider{PersistentDiskVolumeProvider: provider, wire: wire, destination: newNode.Name}
			server := NewControllerServer(driver, probe, nil)
			client := reviewControllerClient(t, server)

			_, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", oldNode.Name))
			require.NoError(t, err)
			before, err := server.readAttachmentHistory(ctx, "volume-test")
			require.NoError(t, err)
			require.Equal(t, oldNode.Name, before.LastSuccessfulNodeName)
			require.Equal(t, 41, before.LastSuccessfulOpenNebulaVMID)
			require.Equal(t, 2, before.LastSuccessfulDiskID)
			_, err = client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: oldNode.Name})
			require.NoError(t, err)

			if sameNode {
				wire.mu.Lock()
				wire.vmNames = map[int]string{41: "retired-vm", 42: newNode.Name}
				wire.mu.Unlock()
			}

			_, err = client.ControllerPublishVolume(ctx, newPublishReq("volume-test", newNode.Name))
			require.NoError(t, err)
			require.EqualValues(t, 1, probe.failedCensus.Load())
			moved, err := server.readAttachmentHistory(ctx, "volume-test")
			require.NoError(t, err)
			require.Equal(t, newNode.Name, moved.LastSuccessfulNodeName)
			require.Equal(t, string(newNode.UID), moved.LastSuccessfulNodeUID)
			require.Equal(t, 7, moved.LastSuccessfulImageID)
			require.Equal(t, "vdb", moved.LastSuccessfulTarget)
			require.Equal(t, 42, moved.LastSuccessfulOpenNebulaVMID)
			require.Zero(t, moved.LastSuccessfulDiskID)

			_, err = client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: newNode.Name})
			require.NoError(t, err)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Contains(t, wire.mutations, "detach:41")
			require.Contains(t, wire.mutations, "attach:42:false")
			require.Contains(t, wire.mutations, "detach:42")
			require.Empty(t, wire.disks)
		})
	}
}

func TestCephStaticPublishStageAndRebindRetainThenReconcile(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%t", queued), func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
				annotationBackend: "ceph", annotationDatastoreID: "100",
			})
			pv.Status.Phase = corev1.VolumeBound
			pv.Spec.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("128Mi")}
			pvc.Status.Phase = corev1.ClaimBound
			pvc.Spec.AccessModes = pv.Spec.AccessModes
			pvc.Spec.Resources.Requests = pv.Spec.Capacity.DeepCopy()
			pvc.Status.Capacity = pv.Spec.Capacity.DeepCopy()
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node)
			driver.nodeID = node.Name
			driver.featureGates = defaultFeatureGates()
			driver.PluginConfig.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, true)
			driver.PluginConfig.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
			driver.PluginConfig.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.ceph, wire.version = true, "7.4.0"
			wire.disks[41], wire.marker = 128, 128*1024*1024
			wire.mu.Unlock()
			server := NewControllerServer(driver, provider, nil)
			if queued {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			client := reviewControllerClient(t, server)
			require.Empty(t, driver.volumeHistory.Snapshot())
			publish := newPublishReq(volume, node.Name)
			publish.VolumeContext = map[string]string{"example.test/static-attribute": "unrelated"}
			publish.VolumeCapability.GetMount().FsType = "ext4"
			published, err := client.ControllerPublishVolume(ctx, publish)
			require.NoError(t, err)
			require.Equal(t, "ceph", published.PublishContext[annotationBackend])
			publishedHistory, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, "ceph", publishedHistory.Backend)
			require.Equal(t, 7, publishedHistory.LastSuccessfulImageID)

			diskPath := withTestDiskPath(t)
			require.NoError(t, os.WriteFile(filepath.Join(diskPath, "vdb"), []byte("fixture"), 0o600))
			stageMount := mount.NewFakeMounter(nil)
			ns := NewNodeServer(driver, mount.NewSafeFormatAndMount(stageMount, reviewDeviceSerialFixture(t, func(string) string {
				return "onecsi-7"
			})))
			ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
			stageContext := cloneStringMap(published.PublishContext)
			if queued {
				stageContext[publishContextOpenNebulaImageID] = "8"
			}
			staged, err := ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
				VolumeId: volume, StagingTargetPath: t.TempDir(), VolumeCapability: publish.VolumeCapability, PublishContext: stageContext,
			})
			require.NoError(t, err)
			require.NotNil(t, staged)
			history, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, "ceph", history.Backend)
			require.Equal(t, 7, history.LastSuccessfulImageID)
			require.False(t, history.LastSuccessfulStageTime.IsZero())

			kube := driver.kubeRuntime.client
			require.NoError(t, kube.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{}))
			require.NoError(t, kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
			pv, pvc = pv.DeepCopy(), pvc.DeepCopy()
			pv.Name, pv.UID = "rebound-static-pv", "rebound-static-pv-uid"
			pvc.Name, pvc.UID = "rebound-static-pvc", "rebound-static-pvc-uid"
			pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.UID = pvc.Name, pvc.UID
			pvc.Spec.VolumeName = pv.Name
			delete(pv.Annotations, annotationBackend)
			pv.Spec.CSI.VolumeAttributes = map[string]string{annotationBackend: "Ceph"}
			_, err = kube.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
			require.NoError(t, err)
			_, err = kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
			require.NoError(t, err)

			pod := activePodForPVC("replacement", pvc.Name, node.Name)
			pod.UID = "static-consumer-uid"
			pod.Status.Phase = corev1.PodPending
			_, err = kube.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			require.NoError(t, err)
			metadata := cloneStringMap(published.PublishContext)
			metadata[paramPVName], metadata[paramPVCName] = pv.Name, pvc.Name
			deleting := metav1.Now()
			va := &storagev1.VolumeAttachment{
				ObjectMeta: metav1.ObjectMeta{Name: "rebound-static-va", UID: "rebound-static-va-uid", DeletionTimestamp: &deleting, Finalizers: []string{"external-attacher/" + DefaultDriverName}},
				Spec:       storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
				Status:     storagev1.VolumeAttachmentStatus{Attached: true, AttachmentMetadata: metadata},
			}
			_, err = kube.StorageV1().VolumeAttachments().Create(ctx, va, metav1.CreateOptions{})
			require.NoError(t, err)
			response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
			require.NoError(t, err)
			require.NotNil(t, response)
			retainedHistory, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, history, retainedHistory)
			wire.mu.Lock()
			mutations := append([]string(nil), wire.mutations...)
			wire.mu.Unlock()
			require.Empty(t, mutations)

			require.NoError(t, kube.StorageV1().VolumeAttachments().Delete(ctx, va.Name, metav1.DeleteOptions{}))
			require.NoError(t, kube.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
			reconciler := NewAttachmentReconciler(server)
			reconciler.orphanGrace = time.Nanosecond
			require.NoError(t, reconciler.ReconcileOnce(ctx))
			require.Len(t, reconciler.orphanSeen, 1)
			require.NoError(t, reconciler.ReconcileOnce(ctx))
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Equal(t, []string{"detach:41"}, wire.mutations)
			require.Empty(t, wire.disks)
		})
	}
}
