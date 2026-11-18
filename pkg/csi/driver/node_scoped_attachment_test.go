package driver

import (
	"context"
	"encoding/json"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	mount "k8s.io/mount-utils"
)

type nodeAttachmentProtocol struct {
	endpoint            string
	hideDiskInfo        bool
	rejectFirstDetach   bool
	rejectFirstAttach   bool
	abortFirstAttach    bool
	alterDirectImage    func(string) string
	loseAttachReply     bool
	loseDetachReply     bool
	attachDiskTemplate  string
	attachAttempt       string
	attachAttemptVM     int
	attachAttempts      int
	detachAttempts      int
	ignoreMarkerWrite   bool
	mu                  sync.Mutex
	disks               map[int]int
	vmNames             map[int]string
	diskSlot            string
	extraDiskBefore     string
	extraDiskByVM       map[int]string
	poolExtraDisk       string
	directDiskFields    string
	directVMFields      string
	alterDirectVM       func(string) string
	alterOriginalPoolVM func(string) string
	alterDatastorePool  func(string) string
	poolVMFields        map[int]string
	directImageFields   string
	imageTemplateFields string
	marker              int64
	mutations           []string
	failMethod          string
	claimNew            bool
	ceph                bool
	imageName           string
	zeroImage           bool
	omitPoolImageID     bool
	duplicateImage      bool
	missingImage        bool
	missingImageID      int
	allowImageDelete    bool
	hideImageName       bool
	omitOriginalVM      bool
	missingOriginalVM   bool
	replacementDisk     bool
	imageDatastoreID    int
	extraImages         string
	imageInfoByID       map[int]string
	prependImageID      bool
	imageTransfer       string
	diskTransfer        string
	diskTarget          string
	ownerCount          int
	ready               bool
	pending             bool
	version             string
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
		imageID := 7
		if state.zeroImage {
			imageID = 0
		}
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
				disk = fmt.Sprintf(`<DISK><PERSISTENT>YES</PERSISTENT><CLONE>NO</CLONE><SAVE>YES</SAVE><SHAREABLE>YES</SHAREABLE><LN_TARGET>SYSTEM</LN_TARGET><TM_MAD>ssh</TM_MAD><TM_MAD_SYSTEM>ssh</TM_MAD_SYSTEM><IMAGE_ID>%d</IMAGE_ID><DISK_ID>%s</DISK_ID><TARGET>%s</TARGET><SIZE>%d</SIZE><SERIAL>onecsi-%d</SERIAL>%s</DISK>`, imageID, slot, firstNonEmpty(state.diskTarget, "vdb"), size, imageID, marker)
				if slot == "missing" {
					disk = strings.Replace(disk, "<DISK_ID>missing</DISK_ID>", "", 1)
				}
				if state.ceph {
					disk = strings.NewReplacer("<LN_TARGET>SYSTEM</LN_TARGET>", "<LN_TARGET>NONE</LN_TARGET>", "<TM_MAD>ssh</TM_MAD>", "<TM_MAD>ceph</TM_MAD>", "<TM_MAD_SYSTEM>ssh</TM_MAD_SYSTEM>", "<TM_MAD_SYSTEM>ceph</TM_MAD_SYSTEM>").Replace(disk)
				}
				if state.diskTransfer != "" {
					disk = strings.Replace(disk, "<TM_MAD>ssh</TM_MAD>", "<TM_MAD>"+state.diskTransfer+"</TM_MAD>", 1)
				}
				if id == state.attachAttemptVM && state.attachAttempt != "" {
					disk = strings.TrimSuffix(disk, "</DISK>") + "<CSI_ATTACH_ATTEMPT>" + state.attachAttempt + "</CSI_ATTACH_ATTEMPT></DISK>"
				}
				if direct && id == 41 && state.directDiskFields != "" {
					disk = strings.TrimSuffix(disk, "</DISK>") + state.directDiskFields + "</DISK>"
				}
			}
			if id == 41 && state.replacementDisk {
				disk += "<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>3</DISK_ID><TARGET>vdc</TARGET><SERIAL>onecsi-8</SERIAL></DISK>"
			}
			if !direct && id == 41 {
				disk += state.poolExtraDisk
			}
			prefix := state.poolVMFields[id]
			if direct {
				prefix = ""
				if id == 41 {
					prefix = state.directVMFields
				}
			}
			result := fmt.Sprintf(`<VM>%s<ID>%d</ID><NAME>%s</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE>%s</TEMPLATE><HISTORY_RECORDS><HISTORY><SEQ>0</SEQ><DS_ID>200</DS_ID></HISTORY></HISTORY_RECORDS></VM>`, prefix, id, name, state.extraDiskBefore+disk+state.extraDiskByVM[id])
			if id == 41 && direct && state.alterDirectVM != nil {
				return state.alterDirectVM(result)
			}
			if id == 41 && !direct && state.alterOriginalPoolVM != nil {
				return state.alterOriginalPoolVM(result)
			}
			return result
		}
		imageXML := func() string {
			ids := ""
			count := 0
			for _, id := range []int{41, 42, 43} {
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
			datastoreID := state.imageDatastoreID
			if datastoreID == 0 {
				datastoreID = 100
			}
			return fmt.Sprintf(`<IMAGE><ID>%d</ID><NAME>%s</NAME><STATE>%d</STATE><SIZE>128</SIZE><DATASTORE_ID>%d</DATASTORE_ID><SOURCE>/fixture/disk</SOURCE><PERSISTENT>1</PERSISTENT><FORMAT>raw</FORMAT><RUNNING_VMS>%d</RUNNING_VMS><CLONING_OPS>0</CLONING_OPS><VMS>%s</VMS><TEMPLATE><PERSISTENT_TYPE>SHAREABLE</PERSISTENT_TYPE>%s</TEMPLATE></IMAGE>`, imageID, firstNonEmpty(state.imageName, "volume-test"), imageState, datastoreID, count, ids, tag)
		}
		ok, body, code := 1, "", 0
		if call.Method == state.failMethod {
			ok, body, code = 0, "observation unavailable", 0x800
		} else {
			switch call.Method {
			case "one.imagepool.info":
				body = "<IMAGE_POOL>"
				if !state.missingImage && !state.hideImageName {
					image := imageXML()
					if state.omitPoolImageID {
						image = strings.Replace(image, fmt.Sprintf("<ID>%d</ID>", imageID), "", 1)
					}
					body += image
				}
				if state.duplicateImage {
					body += fmt.Sprintf("<IMAGE><ID>8</ID><NAME>%s</NAME></IMAGE>", firstNonEmpty(state.imageName, "volume-test"))
				}
				body += state.extraImages + "</IMAGE_POOL>"
			case "one.image.info":
				body = imageXML()
				body = strings.Replace(body, "<IMAGE>", "<IMAGE>"+state.directImageFields, 1)
				body = strings.Replace(body, "<TEMPLATE>", "<TEMPLATE>"+state.imageTemplateFields, 1)
				if state.alterDirectImage != nil {
					body = state.alterDirectImage(body)
				}
				if state.prependImageID {
					body = strings.Replace(body, "<IMAGE>", "<IMAGE><ID>8</ID>", 1)
				}
				switch {
				case number(1) == 8 && state.duplicateImage:
					body = strings.Replace(body, "<ID>7</ID>", "<ID>8</ID>", 1)
				case state.missingImage || (state.missingImageID > 0 && number(1) == state.missingImageID):
					ok, body, code = 0, "image not found", 0x400
				case state.imageInfoByID[number(1)] != "":
					body = state.imageInfoByID[number(1)]
				}
			case "one.vmpool.info":
				body = "<VM_POOL>"
				if !state.omitOriginalVM {
					body += vmXML(41, false)
				}
				body += vmXML(42, false)
				if _, present := state.disks[43]; present || state.vmNames[43] != "" {
					body += vmXML(43, false)
				}
				body += "</VM_POOL>"
			case "one.vm.info":
				body = vmXML(number(1), true)
				if number(1) == 41 && state.missingOriginalVM {
					ok, body, code = 0, "VM not found", 0x400
				}
			case "one.datastorepool.info":
				body = `<DATASTORE_POOL><DATASTORE><ID>100</ID><NAME>images</NAME><TYPE>IMAGE</TYPE><DS_MAD>fs</DS_MAD><TM_MAD>ssh</TM_MAD><STATE>0</STATE><TOTAL_MB>20000</TOTAL_MB><FREE_MB>10000</FREE_MB></DATASTORE><DATASTORE><ID>200</ID><NAME>system</NAME><TYPE>SYSTEM</TYPE><TM_MAD>ssh</TM_MAD><STATE>0</STATE></DATASTORE></DATASTORE_POOL>`
				if state.ceph {
					const rbdTemplate = `<TEMPLATE><DISK_TYPE>RBD</DISK_TYPE><COMPATIBLE_SYS_DS>200</COMPATIBLE_SYS_DS><POOL_NAME>fixture</POOL_NAME><CEPH_HOST>mon.example.invalid</CEPH_HOST><CEPH_USER>csi</CEPH_USER><CEPH_SECRET>fixture</CEPH_SECRET><BRIDGE_LIST>bridge.example.invalid</BRIDGE_LIST></TEMPLATE>`
					body = strings.NewReplacer("<DS_MAD>fs</DS_MAD>", "<DS_MAD>ceph</DS_MAD>", "<TM_MAD>ssh</TM_MAD>", "<TM_MAD>ceph</TM_MAD>",
						"</DATASTORE>", rbdTemplate+"</DATASTORE>").Replace(body)
				}
				if state.imageTransfer != "" {
					current := "ssh"
					if state.ceph {
						current = "ceph"
					}
					body = strings.Replace(body, "<TM_MAD>"+current+"</TM_MAD>", "<TM_MAD>"+state.imageTransfer+"</TM_MAD>", 1)
				}
				if state.alterDatastorePool != nil {
					body = state.alterDatastorePool(body)
				}
			case "one.vm.attach":
				state.attachAttempts++
				if state.abortFirstAttach && state.attachAttempts == 1 {
					panic(http.ErrAbortHandler)
				}
				if state.rejectFirstAttach && state.attachAttempts == 1 {
					ok, body, code = 0, "wrong state HOTPLUG", 0x800
					break
				}
				id := number(1)
				require.Contains(t, call.Params[2].Value.String, `IMAGE_ID="7"`)
				state.attachDiskTemplate = call.Params[2].Value.String
				if _, tail, found := strings.Cut(state.attachDiskTemplate, `CSI_ATTACH_ATTEMPT="`); found {
					state.attachAttempt, _, _ = strings.Cut(tail, `"`)
					state.attachAttemptVM = id
				}
				state.disks[id] = 128
				state.mutations = append(state.mutations, fmt.Sprintf("attach:%d:%t", id, strings.Contains(call.Params[2].Value.String, `READONLY="YES"`)))
				if state.loseAttachReply {
					panic(http.ErrAbortHandler)
				}
				body = strconv.Itoa(id)
			case "one.vm.detach":
				id := number(1)
				require.Equal(t, firstNonEmpty(state.diskSlot, "2"), strconv.Itoa(number(2)))
				state.detachAttempts++
				if state.rejectFirstDetach && state.detachAttempts == 1 {
					ok, body, code = 0, "wrong state HOTPLUG: provider-private-disk-detail", 0x800
					break
				}
				delete(state.disks, id)
				if id == state.attachAttemptVM {
					state.attachAttempt = ""
				}
				state.mutations = append(state.mutations, fmt.Sprintf("detach:%d", id))
				if state.loseDetachReply {
					panic(http.ErrAbortHandler)
				}
				body = strconv.Itoa(id)
			case "one.system.version":
				body = state.version
				if body == "" {
					body = "7.4.1"
				}
			case "one.vm.diskresize":
				id := number(1)
				require.Equal(t, firstNonEmpty(state.diskSlot, "2"), strconv.Itoa(number(2)))
				size, err := strconv.Atoi(call.Params[3].Value.String)
				require.NoError(t, err)
				state.disks[id] = size
				state.mutations = append(state.mutations, fmt.Sprintf("resize:%d:%d", id, size))
				body = strconv.Itoa(id)
			case "one.image.delete":
				require.True(t, state.allowImageDelete, "unexpected provider image deletion")
				require.Equal(t, imageID, number(1))
				state.missingImage = true
				state.mutations = append(state.mutations, fmt.Sprintf("image-delete:%d", imageID))
				body = strconv.Itoa(imageID)
			case "one.image.update":
				require.Equal(t, imageID, number(1))
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
	for _, scenario := range []string{"other-owner", "ready-with-owner", "multi-reader", "contradictory-image", "failed-vm", "failed-pool"} {
		t.Run(scenario, func(t *testing.T) {
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			state.ready = scenario == "ready-with-owner"
			if scenario == "multi-reader" {
				state.disks[42] = 128
			}
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
			switch scenario {
			case "other-owner":
				require.ErrorIs(t, err, opennebula.ErrAttachmentNotFound)
				require.Error(t, provider.DeleteVolume(context.Background(), "volume-test"))
			case "multi-reader":
				require.NoError(t, err)
				metadata, err := provider.InspectVolumeAttachment(context.Background(), "volume-test", "node-new")
				require.NoError(t, err)
				require.ElementsMatch(t, []int{41, 42}, metadata.ImageVMIDs)
				require.Len(t, metadata.DiskRecords, 2)
			default:
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

func TestControllerUnpublishGenericEphemeralRelease(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"terminating-owned", "terminating-unverified", "released"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				node := newReadyNode("node-old", true)
				pod := activePodForPVC("consumer", pvc.Name, node.Name)
				pod.UID = "pod-uid"
				pod.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{
					VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{},
				}}
				deleting := metav1.Now()
				pod.DeletionTimestamp = &deleting
				pvc.Name = "consumer-data"
				pv.Spec.ClaimRef.Name = pvc.Name
				if mode != "terminating-unverified" {
					pvc.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(pod, corev1.SchemeGroupVersion.WithKind("Pod"))}
				}
				va := &storagev1.VolumeAttachment{
					ObjectMeta: metav1.ObjectMeta{Name: "deleting-va", UID: "va-uid", DeletionTimestamp: &deleting},
					Spec: storagev1.VolumeAttachmentSpec{
						Attacher: DefaultDriverName, NodeName: node.Name,
						Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name},
					},
					Status: storagev1.VolumeAttachmentStatus{Attached: true},
				}
				objects := []kruntime.Object{pv, pvc, va, node}
				if mode != "released" {
					objects = append(objects, pod)
				}
				driver := newStickyTestDriver(t, objects...)
				driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
				provider, wire := nodeAttachmentProvider(t)
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				reply, err := reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if mode == "released" {
					require.NoError(t, err)
					require.NotNil(t, reply)
					require.Equal(t, []string{"detach:41"}, wire.mutations)
					return
				}
				require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
				require.Nil(t, reply)
				require.Zero(t, wire.detachAttempts)
				require.Empty(t, wire.mutations)
			})
		}
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
		for _, mode := range []string{"same-node", "replacement-uid", "replacement-vm", "vm-changed-before-dispatch", "duplicate-image", "replacement-image", "late-duplicate-image", "missing-image", "missing-image-replacement-uid", "hidden-handle", "missing-image-retained-disk", "missing-image-unknown-ownership", "missing-image-malformed-disk", "missing-image-backed-disk-without-id", "missing-image-backed-disk-conflicting-serial", "owner-omitted", "owner-omitted-released", "owner-omitted-gone", "owner-omitted-unreadable", "retired-node", "live-image-retired-node", "live-image-retired-node-gone", "live-image-retired-node-attached", "live-image-unreadable-node", "missing-image-unreadable-node"} {
			t.Run(fmt.Sprintf("queued=%v/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				node := newReadyNode("node-old", true)
				historicalUID := node.UID
				if mode == "replacement-uid" || mode == "missing-image-replacement-uid" {
					node.UID = "replacement-uid"
				}
				driver := newStickyTestDriver(t, pv, pvc, node, newReadyNode("node-new", true))
				switch mode {
				case "retired-node", "live-image-retired-node", "live-image-retired-node-gone", "live-image-retired-node-attached":
					require.NoError(t, driver.kubeRuntime.client.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{}))
				case "missing-image-unreadable-node", "live-image-unreadable-node":
					driver.kubeRuntime.client.(*fake.Clientset).PrependReactor("get", "nodes", func(ktesting.Action) (bool, kruntime.Object, error) {
						return true, nil, fmt.Errorf("Node read unavailable")
					})
				}
				before, err := driver.volumeHistory.Upsert(ctx, "volume-test", func(record *VolumeHistoryRecord) {
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
					if mode == "late-duplicate-image" && method == "one.imagepool.info" && imageReads.Add(1) == 2 {
						protocol.mu.Lock()
						defer protocol.mu.Unlock()
						protocol.duplicateImage, protocol.replacementDisk = true, true
					}
				})
				protocol = state
				state.mu.Lock()
				state.ceph = true
				switch mode {
				case "replacement-vm":
					state.vmNames = map[int]string{41: "retired-vm", 42: "node-old"}
				case "same-node", "replacement-uid", "late-duplicate-image", "live-image-retired-node", "live-image-unreadable-node":
					delete(state.disks, 41)
				case "live-image-retired-node-gone":
					delete(state.disks, 41)
					state.omitOriginalVM, state.missingOriginalVM = true, true
				case "duplicate-image", "replacement-image", "missing-image", "missing-image-replacement-uid", "missing-image-unreadable-node":
					delete(state.disks, 41)
					state.missingImage = mode != "duplicate-image"
					state.duplicateImage = mode == "duplicate-image" || mode == "replacement-image"
					state.replacementDisk = state.duplicateImage
				case "missing-image-retained-disk", "missing-image-unknown-ownership":
					state.missingImage = true
					if mode == "missing-image-unknown-ownership" {
						state.failMethod = "one.vmpool.info"
					}
				case "missing-image-malformed-disk":
					delete(state.disks, 41)
					state.missingImage = true
					state.extraDiskBefore = "<DISK><IMAGE_ID>invalid</IMAGE_ID></DISK>"
				case "missing-image-backed-disk-without-id", "missing-image-backed-disk-conflicting-serial":
					delete(state.disks, 41)
					state.missingImage, state.omitOriginalVM = true, true
					facts := "<DISK_ID>2</DISK_ID><IMAGE>volume-test</IMAGE><SOURCE>/fixture/disk</SOURCE><PERSISTENT>YES</PERSISTENT>"
					if mode == "missing-image-backed-disk-conflicting-serial" {
						facts += "<SERIAL>onecsi-8</SERIAL>"
					}
					state.extraDiskByVM = map[int]string{41: "<DISK>" + facts + "</DISK>"}
				case "hidden-handle":
					state.hideImageName = true
				case "owner-omitted", "owner-omitted-released", "owner-omitted-gone", "owner-omitted-unreadable", "retired-node":
					state.missingImage, state.omitOriginalVM = true, true
					switch mode {
					case "owner-omitted-released", "owner-omitted-gone", "retired-node":
						delete(state.disks, 41)
						state.missingOriginalVM = mode != "owner-omitted-released"
					case "owner-omitted-unreadable":
						state.failMethod = "one.vm.info"
					}
				}
				state.mu.Unlock()
				controller := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, controller.validateHotplugQueueRequest)
				}
				client := reviewControllerClient(t, controller)
				response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
				history, historyErr := controller.readAttachmentHistory(ctx, "volume-test")
				require.NoError(t, historyErr)
				switch mode {
				case "same-node", "live-image-retired-node", "live-image-retired-node-gone":
					require.NoError(t, err)
					require.NotNil(t, response)
					require.False(t, history.LastSafeDetachTime.IsZero())
				case "missing-image", "owner-omitted-released", "owner-omitted-gone", "retired-node":
					require.NoError(t, err)
					require.NotNil(t, response)
					require.Equal(t, before, history)
				default:
					require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
					require.Nil(t, response)
					require.Equal(t, before, history)
				}
				state.mu.Lock()
				defer state.mu.Unlock()
				require.Empty(t, state.mutations)
				if strings.Contains(mode, "vm") || mode == "missing-image-retained-disk" || mode == "owner-omitted" {
					require.Contains(t, state.disks, 41)
				}
				if mode == "duplicate-image" || mode == "replacement-image" || mode == "late-duplicate-image" {
					require.True(t, state.replacementDisk)
				}
			})
		}
	}
}

func TestLiveImageZeroProvesOrdinaryAttachmentAbsence(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"unpublish", "omitted-id", "sticky-expiry", "stale-va"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "local"})
				pv.ResourceVersion = "1"
				node := newReadyNode("node-old", true)
				objects := []kruntime.Object{pv, pvc, node}
				va := &storagev1.VolumeAttachment{
					ObjectMeta: metav1.ObjectMeta{Name: "zero-image-va", UID: "zero-image-va-uid", ResourceVersion: "1"},
					Spec:       storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
					Status:     storagev1.VolumeAttachmentStatus{Attached: true, AttachmentMetadata: map[string]string{publishContextOpenNebulaImageID: "0", publishContextDeviceSerial: "onecsi-0"}},
				}
				if mode == "stale-va" {
					objects = append(objects, va)
				}
				driver := newStickyTestDriver(t, objects...)
				provider, wire := nodeAttachmentProvider(t)
				wire.mu.Lock()
				wire.zeroImage = true
				delete(wire.disks, 41)
				if mode == "omitted-id" {
					wire.imageInfoByID = map[int]string{0: `<IMAGE><NAME>volume-test</NAME><STATE>1</STATE><SIZE>128</SIZE><DATASTORE_ID>100</DATASTORE_ID></IMAGE>`}
				}
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				switch mode {
				case "sticky-expiry":
					state := StickyAttachmentState{VolumeID: volume, NodeID: node.Name, Backend: "local", StartedAt: time.Now().Add(-2 * time.Minute), ExpiresAt: time.Now().Add(-time.Minute)}
					require.NoError(t, driver.stickyAttachments.StartGrace(state))
					server.expireStickyDetach(ctx, state)
					_, held := driver.stickyAttachments.Get(volume)
					require.False(t, held)
				case "stale-va":
					r := NewAttachmentReconciler(server)
					r.staleVAGrace = time.Nanosecond
					require.NoError(t, r.ReconcileOnce(ctx))
					require.NoError(t, r.ReconcileOnce(ctx))
					_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(ctx, va.Name, metav1.GetOptions{})
					require.True(t, apierrors.IsNotFound(err), "%v", err)
				default:
					reply, err := reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
					if mode == "omitted-id" {
						require.Equal(t, codes.Unavailable, status.Code(err))
						require.Nil(t, reply)
					} else {
						require.NoError(t, err)
						require.NotNil(t, reply)
					}
				}
				history, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Zero(t, history.LastSuccessfulImageID)
				require.True(t, history.LastSuccessfulPublishTime.IsZero())
				require.Equal(t, mode == "unpublish" || mode == "sticky-expiry", !history.LastSafeDetachTime.IsZero())
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Empty(t, wire.mutations)
			})
		}
	}
}

func TestControllerPublishRejectsLiveImageZero(t *testing.T) {
	for _, inspected := range []bool{false, true} {
		t.Run(fmt.Sprintf("inspector=%t", inspected), func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node)
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.zeroImage = true
			wire.mu.Unlock()
			var volumeProvider opennebula.OpenNebulaVolumeProvider = provider
			if !inspected {
				volumeProvider = struct {
					opennebula.OpenNebulaVolumeProvider
				}{provider}
			}
			server := NewControllerServer(driver, volumeProvider, nil)
			reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.ErrorContains(t, err, "volume identity is unknown")
			require.Nil(t, reply)
			decision := server.evaluateVolumeReconcile(ctx, node.Name, "attach", volume)
			require.Equal(t, VolumeReconcilePause, decision.Action)
			history, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.True(t, history.LastSuccessfulPublishTime.IsZero())
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Empty(t, wire.mutations)
		})
	}
}

func TestControllerUnpublishRejectsLateAmbiguousImageName(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%t/retry=%t", queued, retry), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
				var protocol *nodeAttachmentProtocol
				provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if retry && method == "one.image.info" {
						protocol.mu.Lock()
						if protocol.detachAttempts == 1 {
							protocol.duplicateImage = true
						}
						protocol.mu.Unlock()
					}
				})
				protocol = wire
				wire.mu.Lock()
				wire.rejectFirstDetach = retry
				wire.mu.Unlock()
				probe := &detachCensusProbe{PersistentDiskVolumeProvider: provider}
				if !retry {
					probe.beforeDetach = func() {
						wire.mu.Lock()
						wire.duplicateImage = true
						wire.mu.Unlock()
					}
				}
				server := NewControllerServer(driver, probe, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				reply, err := reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
				require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
				require.ErrorContains(t, err, "detach image identity changed or is unverified")
				require.Nil(t, reply)
				history, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.True(t, history.LastSafeDetachTime.IsZero())
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.True(t, wire.duplicateImage)
				require.Empty(t, wire.mutations)
				require.Contains(t, wire.disks, 41)
				if retry {
					require.Equal(t, 1, wire.detachAttempts)
				} else {
					require.Zero(t, wire.detachAttempts)
				}
			})
		}
	}
}

func TestControllerUnpublishRejectsConflictingFinalVMAndDiskFields(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, tc := range []struct {
			name, diskFields, vmFields string
			alterVM                    func(string) string
			alterDatastore             func(string) string
		}{
			{name: "resize", diskFields: "<RESIZE>NO</RESIZE><RESIZE>YES</RESIZE>"},
			{name: "image", diskFields: "<IMAGE_ID>8</IMAGE_ID>"},
			{name: "slot", diskFields: "<DISK_ID>3</DISK_ID>"},
			{name: "vm-state", vmFields: "<STATE>1</STATE>"},
			{name: "vm-lcm-state", vmFields: "<LCM_STATE>14</LCM_STATE>"},
			{name: "vm-name", vmFields: "<NAME>different-vm</NAME>"},
			{name: "vm-id", vmFields: "<ID>42</ID>"},
			{name: "vm-template", vmFields: "<TEMPLATE/>"},
			{name: "missing-state", alterVM: func(body string) string { return strings.Replace(body, "<STATE>3</STATE>", "", 1) }},
			{name: "missing-lcm", alterVM: func(body string) string { return strings.Replace(body, "<LCM_STATE>3</LCM_STATE>", "", 1) }},
			{name: "missing-name", alterVM: func(body string) string { return strings.Replace(body, "<NAME>node-old</NAME>", "", 1) }},
			{name: "changed-name", alterVM: func(body string) string {
				return strings.Replace(body, "<NAME>node-old</NAME>", "<NAME>node-other</NAME>", 1)
			}},
			{name: "datastore-transfer", alterDatastore: func(body string) string {
				return strings.Replace(body, "<TM_MAD>ssh</TM_MAD>", "<TM_MAD>ceph</TM_MAD><TM_MAD>ssh</TM_MAD>", 1)
			}},
		} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, tc.name), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
				var protocol *nodeAttachmentProtocol
				var armed atomic.Bool
				var reads atomic.Int32
				provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if method == "one.vm.info" && armed.Load() && reads.Add(1) == 2 {
						protocol.mu.Lock()
						protocol.directDiskFields, protocol.directVMFields = tc.diskFields, tc.vmFields
						protocol.alterDirectVM, protocol.alterDatastorePool = tc.alterVM, tc.alterDatastore
						protocol.mu.Unlock()
					}
				})
				protocol = wire
				probe := &detachCensusProbe{PersistentDiskVolumeProvider: provider, beforeDetach: func() { armed.Store(true) }}
				server := NewControllerServer(driver, probe, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				reply, err := reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "detach disk identity or completed capacity is unverified")
				require.Nil(t, reply)
				history, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.True(t, history.LastSafeDetachTime.IsZero())
				require.EqualValues(t, 2, reads.Load())
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Equal(t, tc.diskFields, wire.directDiskFields)
				require.Equal(t, tc.vmFields, wire.directVMFields)
				require.Zero(t, wire.detachAttempts)
				require.Empty(t, wire.mutations)
				require.Contains(t, wire.disks, 41)
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
			replacement.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
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
			pod := activePodForPVC("replacement-pod", replacement.Name, node.Name)
			pod.CreationTimestamp = metav1.NewTime(replacement.CreationTimestamp.Add(time.Minute))
			driver := newStickyTestDriver(t, pv, replacement, node, pod)
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

func TestReleasedClaimDetachChecksOriginalConsumers(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, background := range []bool{false, true} {
			for _, mode := range []string{"original-terminating", "claim-absent", "unknown-age", "replacement-only", "ephemeral-replacement", "ephemeral-original", "ephemeral-ambiguous"} {
				t.Run(fmt.Sprintf("queued=%t/background=%t/%s", queued, background, mode), func(t *testing.T) {
					ctx := context.Background()
					const volume = "volume-test"
					pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
					pv.Status.Phase = corev1.VolumeReleased
					pvc.UID, pvc.Spec.VolumeName = "replacement-claim", "replacement-pv"
					pvc.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
					node := newReadyNode("node-old", true)
					pod := activePodForPVC("consumer", pvc.Name, node.Name)
					pod.CreationTimestamp = metav1.NewTime(pvc.CreationTimestamp.Add(-time.Minute))
					terminating := metav1.Now()
					pod.DeletionTimestamp = &terminating
					switch mode {
					case "replacement-only":
						pod.CreationTimestamp = metav1.NewTime(pvc.CreationTimestamp.Add(time.Minute))
					case "unknown-age":
						pod.CreationTimestamp = metav1.Time{}
					case "ephemeral-replacement", "ephemeral-original", "ephemeral-ambiguous":
						pod.UID = "consumer-pod"
						pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}}}
						pvc.Name, pv.Spec.ClaimRef.Name = "consumer-data", "consumer-data"
						if mode != "ephemeral-ambiguous" {
							ref := metav1.NewControllerRef(pod, corev1.SchemeGroupVersion.WithKind("Pod"))
							if mode == "ephemeral-original" {
								ref.UID = "new-pod"
							}
							pvc.OwnerReferences = []metav1.OwnerReference{*ref}
						}
					}
					replacementOnly := mode == "replacement-only" || mode == "ephemeral-replacement"
					objects := []kruntime.Object{pv, node, pod}
					if mode != "claim-absent" {
						objects = append(objects, pvc)
					}
					driver := newStickyTestDriver(t, objects...)
					provider, wire := nodeAttachmentProvider(t)
					wire.mu.Lock()
					wire.ceph = true
					wire.mu.Unlock()
					server := NewControllerServer(driver, provider, nil)
					if queued {
						driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
						driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
					}
					if background {
						r := NewAttachmentReconciler(server)
						r.orphanGrace = time.Nanosecond
						require.NoError(t, r.ReconcileOnce(ctx))
						require.NoError(t, r.ReconcileOnce(ctx))
					} else {
						reply, err := reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
						if replacementOnly {
							require.NoError(t, err)
							require.NotNil(t, reply)
						} else {
							if strings.HasPrefix(mode, "ephemeral-") {
								require.ErrorContains(t, err, "cannot verify Pod claim ownership")
							} else {
								require.ErrorContains(t, err, "a current Pod still references the volume")
							}
							require.Nil(t, reply)
						}
					}
					wire.mu.Lock()
					defer wire.mu.Unlock()
					if replacementOnly {
						require.Equal(t, []string{"detach:41"}, wire.mutations)
					} else {
						require.Zero(t, wire.detachAttempts)
						require.Contains(t, wire.disks, 41)
					}
				})
			}
		}
	}
}

func TestControllerPublishPrecheckUsesVerifiedVM(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, replaceVM := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%t/replaced=%t", queued, replaceVM), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				var protocol *nodeAttachmentProtocol
				directReads := 0
				provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if method != "one.vm.info" {
						return
					}
					protocol.mu.Lock()
					defer protocol.mu.Unlock()
					directReads++
					if replaceVM && directReads == 1 {
						protocol.vmNames = map[int]string{41: "retired-vm", 42: node.Name}
						protocol.disks[42] = 128
					} else if !replaceVM && directReads == 2 {
						protocol.disks[41] = 128
					}
				})
				protocol = wire
				wire.mu.Lock()
				wire.ceph, wire.marker, wire.diskTarget = true, 0, "vdc"
				delete(wire.disks, 41)
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, nil)
				}
				reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
				history, historyErr := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, historyErr)
				if replaceVM {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.ErrorContains(t, err, "publish attachment identity changed during precheck")
					require.Nil(t, reply)
					require.True(t, history.LastSuccessfulPublishTime.IsZero())
					require.Zero(t, history.LastSuccessfulOpenNebulaVMID)
				} else {
					require.NoError(t, err)
					require.Equal(t, "vdc", reply.GetPublishContext()["volumeName"])
					require.Equal(t, "7", reply.GetPublishContext()[publishContextOpenNebulaImageID])
					require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
					require.False(t, history.LastSuccessfulPublishTime.IsZero())
					require.Equal(t, 41, history.LastSuccessfulOpenNebulaVMID)
					require.Equal(t, "vdc", history.LastSuccessfulTarget)
					require.Equal(t, "onecsi-7", history.LastSuccessfulDeviceSerial)
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.GreaterOrEqual(t, directReads, 2)
				require.Empty(t, wire.mutations)
			})
		}
	}
}

func TestControllerPublishPinsAdmittedKubernetesNode(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"stable", "recreated", "newly-observed"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				node := newReadyNode("node-old", true)
				objects := []kruntime.Object{pv, pvc}
				if mode != "newly-observed" {
					objects = append(objects, node)
				}
				driver := newStickyTestDriver(t, objects...)
				nodes := driver.kubeRuntime.client.CoreV1().Nodes()
				var replace sync.Once
				provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if method != "one.vm.info" || mode == "stable" {
						return
					}
					replace.Do(func() {
						if mode == "recreated" {
							require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
						}
						replacement := node.DeepCopy()
						replacement.UID = "replacement-node"
						_, err := nodes.Create(ctx, replacement, metav1.CreateOptions{})
						require.NoError(t, err)
					})
				})
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
				history, historyErr := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, historyErr)
				if mode == "stable" {
					require.NoError(t, err)
					require.Equal(t, "vdb", reply.GetPublishContext()["volumeName"])
					require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
					require.Equal(t, string(node.UID), history.LastSuccessfulNodeUID)
					require.False(t, history.LastSuccessfulPublishTime.IsZero())
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.ErrorContains(t, err, "publish Kubernetes Node identity changed")
					require.Nil(t, reply)
					require.Empty(t, history.LastSuccessfulNodeUID)
					require.True(t, history.LastSuccessfulPublishTime.IsZero())
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Empty(t, wire.mutations)
			})
		}
	}
}

func TestControllerPublishChecksRetainedNodeBeforeRestoration(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, path := range []struct {
			name    string
			arrival int
		}{{"attached", 0}, {"precheck", 2}, {"sticky", 3}} {
			for _, replaced := range []bool{false, true} {
				t.Run(fmt.Sprintf("queued=%t/%s/replaced=%t", queued, path.name, replaced), func(t *testing.T) {
					ctx := context.Background()
					const volume = "volume-test"
					pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
					node := newReadyNode("node-old", true)
					owner := node.UID
					if replaced {
						node.UID = "replacement-uid"
					}
					driver := newStickyTestDriver(t, pv, pvc, node, activePodForPVC("consumer", pvc.Name, node.Name))
					_, err := driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
						record.Backend = "ceph"
						record.LastSuccessfulNodeName = node.Name
						record.LastSuccessfulNodeUID = string(owner)
						record.LastSuccessfulImageID = 7
						record.LastSuccessfulOpenNebulaVMID = 41
						record.LastSuccessfulPublishTime = time.Now().Add(-time.Hour).UTC()
					})
					require.NoError(t, err)
					if path.name == "sticky" {
						require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
							VolumeID: volume, NodeID: node.Name, Backend: "local", StartedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute), GraceSeconds: 60,
						}))
					}
					var protocol *nodeAttachmentProtocol
					var directReads int
					provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
						if method != "one.vm.info" {
							return
						}
						protocol.mu.Lock()
						defer protocol.mu.Unlock()
						directReads++
						if directReads == path.arrival {
							protocol.disks[41] = 128
						}
					})
					protocol = wire
					wire.mu.Lock()
					wire.ceph, wire.marker = true, 256*1024*1024
					if path.arrival == 0 {
						wire.disks[41] = 128
					} else {
						delete(wire.disks, 41)
					}
					wire.mu.Unlock()
					server := NewControllerServer(driver, provider, nil)
					if queued {
						driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
						driver.hotplugQueue.Configure(true, 0, 0, 0, nil)
					}
					before, err := server.readAttachmentHistory(ctx, volume)
					require.NoError(t, err)
					reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
					after, historyErr := server.readAttachmentHistory(ctx, volume)
					require.NoError(t, historyErr)
					_, held := driver.stickyAttachments.Get(volume)
					wire.mu.Lock()
					defer wire.mu.Unlock()
					if replaced {
						require.Equal(t, codes.Unavailable, status.Code(err))
						require.ErrorContains(t, err, "does not match the retained attachment")
						require.Nil(t, reply)
						require.Equal(t, before, after)
						require.Equal(t, path.name == "sticky", held)
						require.Equal(t, max(1, path.arrival), directReads)
						require.Equal(t, 128, wire.disks[41])
						require.Empty(t, wire.mutations)
					} else {
						require.NoError(t, err)
						require.Equal(t, "vdb", reply.GetPublishContext()["volumeName"])
						require.True(t, after.LastSuccessfulPublishTime.After(before.LastSuccessfulPublishTime))
						require.False(t, held)
						require.Equal(t, 256, wire.disks[41])
						require.Equal(t, []string{"resize:41:256", "marker:268435456"}, wire.mutations)
					}
				})
			}
		}
	}
}

func TestPublishAndStagePreserveRetainedNodeUIDUntilFreshAttach(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%t", queued), func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
			pvc.Spec.AccessModes = pv.Spec.AccessModes
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node)
			driver.nodeID = node.Name
			driver.featureGates = defaultFeatureGates()
			driver.PluginConfig.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
			driver.PluginConfig.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.ceph, wire.disks[41], wire.marker, wire.version = true, 128, 128*1024*1024, "7.4.0"
			wire.mu.Unlock()
			server := NewControllerServer(driver, provider, nil)
			if queued {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			client := reviewControllerClient(t, server)
			publish := newPublishReq(volume, node.Name)
			publish.VolumeCapability.GetMount().FsType = "ext4"
			published, err := client.ControllerPublishVolume(ctx, publish)
			require.NoError(t, err)
			before, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, string(node.UID), before.LastSuccessfulNodeUID)

			nodes := driver.kubeRuntime.client.CoreV1().Nodes()
			require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
			replacement := node.DeepCopy()
			replacement.UID = "replacement-uid"
			replacement.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
			_, err = nodes.Create(ctx, replacement, metav1.CreateOptions{})
			require.NoError(t, err)

			reply, err := client.ControllerPublishVolume(ctx, publish)
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.ErrorContains(t, err, "does not match the retained attachment")
			require.Nil(t, reply)
			afterRetry, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, before, afterRetry)

			diskPath := withTestDiskPath(t)
			require.NoError(t, os.WriteFile(filepath.Join(diskPath, "vdb"), []byte("fixture"), 0o600))
			ns := NewNodeServer(driver, mount.NewSafeFormatAndMount(mount.NewFakeMounter(nil), reviewDeviceSerialFixture(t, func(string) string { return "onecsi-7" })))
			ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
			stage, err := ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
				VolumeId: volume, StagingTargetPath: t.TempDir(), VolumeCapability: publish.VolumeCapability, PublishContext: published.PublishContext,
			})
			require.NoError(t, err)
			require.NotNil(t, stage)
			afterStage, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, before, afterStage)

			wire.mu.Lock()
			unchangedMutations := append([]string(nil), wire.mutations...)
			delete(wire.disks, 41)
			wire.mu.Unlock()
			require.Empty(t, unchangedMutations)
			consumer := activePodForPVC("fresh-consumer", pvc.Name, node.Name)
			consumer.UID = "fresh-consumer"
			consumer.Status.Phase = corev1.PodPending
			consumer.CreationTimestamp = metav1.NewTime(replacement.CreationTimestamp.Add(-time.Minute))
			setReplacementHistoryVersion(t, driver)
			_, err = driver.kubeRuntime.client.CoreV1().Pods(consumer.Namespace).Create(ctx, consumer, metav1.CreateOptions{})
			require.NoError(t, err)
			fresh, err := client.ControllerPublishVolume(ctx, publish)
			require.NoError(t, err)
			require.Equal(t, "onecsi-7", fresh.GetPublishContext()[publishContextDeviceSerial])
			afterAttach, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, string(replacement.UID), afterAttach.LastSuccessfulNodeUID)
			require.Equal(t, new(2), afterAttach.LastSuccessfulDiskID)
			require.True(t, afterAttach.LastSuccessfulPublishTime.After(before.LastSuccessfulPublishTime))
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Equal(t, []string{"attach:41:false"}, wire.mutations)
		})
	}
}

func TestControllerPublishVerifiesDiskSlot(t *testing.T) {
	for _, tc := range []struct {
		name, slot     string
		poolConflict   bool
		directConflict bool
		grow           bool
		noCensus       bool
		wantSuccess    bool
	}{
		{name: "pool-duplicate", slot: "2", poolConflict: true},
		{name: "pool-duplicate-before-growth", slot: "2", poolConflict: true, grow: true},
		{name: "direct-duplicate-before-growth", slot: "2", directConflict: true, grow: true},
		{name: "missing", slot: "missing"},
		{name: "negative", slot: "-1"},
		{name: "zero", slot: "0", wantSuccess: true},
		{name: "grow-zero", slot: "0", grow: true, wantSuccess: true},
		{name: "fallback-zero", slot: "0", noCensus: true, wantSuccess: true},
		{name: "fallback-negative", slot: "-1", noCensus: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node)
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.diskSlot = tc.slot
			wire.marker = 0
			if tc.grow {
				wire.disks[41], wire.marker = 128, 256*1024*1024
			}
			const competingDisk = "<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdc</TARGET></DISK>"
			if tc.poolConflict {
				wire.poolExtraDisk = competingDisk
			}
			if tc.directConflict {
				wire.extraDiskByVM = map[int]string{41: competingDisk}
			}
			wire.mu.Unlock()
			probe := &moveHistoryProvider{PersistentDiskVolumeProvider: provider, wire: wire, destination: node.Name, vmID: 41}
			probe.failCensus.Store(tc.noCensus)
			server := NewControllerServer(driver, probe, nil)
			reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			history, historyErr := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, historyErr)
			if tc.wantSuccess {
				require.NoError(t, err)
				require.Equal(t, "vdb", reply.GetPublishContext()["volumeName"])
				require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
				require.False(t, history.LastSuccessfulPublishTime.IsZero())
				require.Equal(t, new(0), history.LastSuccessfulDiskID)
			} else {
				if tc.grow {
					require.ErrorContains(t, err, "disk slot is claimed by multiple records")
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
				}
				require.Nil(t, reply)
				require.True(t, history.LastSuccessfulPublishTime.IsZero())
			}
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if tc.grow && tc.wantSuccess {
				require.Equal(t, []string{"resize:41:256", "marker:268435456"}, wire.mutations)
			} else {
				require.Empty(t, wire.mutations)
			}
		})
	}
}

func TestControllerPublishFreshAttachDoesNotRestoreAmbiguousSlot(t *testing.T) {
	ctx := context.Background()
	const volume = "volume-test"
	pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	node := newReadyNode("node-old", true)
	driver := newStickyTestDriver(t, pv, pvc, node, activePodForPVC("consumer", pvc.Name, node.Name))
	var protocol *nodeAttachmentProtocol
	provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
		if method == "one.vm.attach" {
			protocol.mu.Lock()
			protocol.directDiskFields = "<DISK_ID>3</DISK_ID>"
			protocol.mu.Unlock()
		}
	})
	protocol = wire
	wire.mu.Lock()
	delete(wire.disks, 41)
	wire.marker = 256 * 1024 * 1024
	wire.mu.Unlock()
	server := NewControllerServer(driver, provider, nil)
	reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
	require.ErrorContains(t, err, "repeated VM disk field DISK_ID")
	require.Nil(t, reply)
	history, err := server.readAttachmentHistory(ctx, volume)
	require.NoError(t, err)
	require.True(t, history.LastSuccessfulPublishTime.IsZero())
	wire.mu.Lock()
	defer wire.mu.Unlock()
	require.Equal(t, []string{"attach:41:false"}, wire.mutations)
	require.Equal(t, 128, wire.disks[41])
}

func TestControllerPublishRestorationRequiresConsistentOwnership(t *testing.T) {
	for _, mode := range []string{"consistent", "before-resize", "before-marker"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var wire *nodeAttachmentProtocol
			provider, protocol := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if method != "one.image.info" || mode == "consistent" {
					return
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if mode == "before-resize" || len(wire.mutations) != 0 {
					wire.alterDirectImage = func(body string) string {
						return strings.NewReplacer("<STATE>8</STATE>", "<STATE>1</STATE>", "<RUNNING_VMS>1</RUNNING_VMS>",
							"<RUNNING_VMS>0</RUNNING_VMS>", "<VMS><ID>41</ID></VMS>", "<VMS></VMS>").Replace(body)
					}
				}
			})
			wire = protocol
			wire.mu.Lock()
			wire.ceph, wire.version, wire.disks[41] = true, "7.4.0", 128
			wire.mu.Unlock()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
			reply, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-old"))
			wire.mu.Lock()
			defer wire.mu.Unlock()
			switch mode {
			case "consistent":
				require.NoError(t, err)
				require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
				require.Equal(t, []string{"resize:41:256", "marker:268435456"}, wire.mutations)
			case "before-resize":
				require.Error(t, err)
				require.Empty(t, wire.mutations)
			case "before-marker":
				require.Error(t, err)
				require.Equal(t, []string{"resize:41:256"}, wire.mutations)
			}
		})
	}
}

func TestControllerPublishWaitsForAcceptedAttachmentOwnership(t *testing.T) {
	for _, converges := range []bool{true, false} {
		t.Run(fmt.Sprintf("converges=%t", converges), func(t *testing.T) {
			var wire *nodeAttachmentProtocol
			readsAfterAttach := 0
			provider, protocol := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if method != "one.image.info" {
					return
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if wire.attachAttempts == 0 {
					return
				}
				readsAfterAttach++
				if !converges || readsAfterAttach <= 4 {
					wire.alterDirectImage = func(body string) string {
						return strings.NewReplacer("<STATE>8</STATE>", "<STATE>1</STATE>", "<RUNNING_VMS>1</RUNNING_VMS>",
							"<RUNNING_VMS>0</RUNNING_VMS>", "<VMS><ID>41</ID></VMS>", "<VMS></VMS>").Replace(body)
					}
				} else {
					wire.alterDirectImage = nil
				}
			})
			wire = protocol
			wire.mu.Lock()
			wire.ceph, wire.version, wire.marker = true, "7.4.0", 128*1024*1024
			delete(wire.disks, 41)
			wire.mu.Unlock()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
			reply, err := client.ControllerPublishVolume(context.Background(), newPublishReq("volume-test", "node-old"))
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if converges {
				require.NoError(t, err)
				require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
			} else {
				require.ErrorContains(t, err, "timed out")
				require.Nil(t, reply)
			}
			require.Greater(t, readsAfterAttach, 4)
			require.Equal(t, 1, wire.attachAttempts)
			require.Equal(t, []string{"attach:41:false"}, wire.mutations)
		})
	}
}

func TestControllerExpandPreservesStandaloneAdmission(t *testing.T) {
	for _, mode := range []string{"single-marker", "duplicate-initial-marker", "duplicate-final-marker", "duplicate-final-template", "unrelated-fields"} {
		t.Run(mode, func(t *testing.T) {
			var protocol *nodeAttachmentProtocol
			provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if method != "one.image.update" {
					return
				}
				protocol.mu.Lock()
				defer protocol.mu.Unlock()
				switch mode {
				case "duplicate-initial-marker":
					protocol.imageTemplateFields = ""
				case "duplicate-final-marker":
					protocol.imageTemplateFields = "<CSI_REQUESTED_SIZE_BYTES>268435456</CSI_REQUESTED_SIZE_BYTES>"
				case "duplicate-final-template":
					protocol.alterDirectImage = func(body string) string {
						return strings.Replace(body, "</IMAGE>", "<TEMPLATE></TEMPLATE></IMAGE>", 1)
					}
				}
			})
			protocol = wire
			wire.mu.Lock()
			wire.ceph, wire.version = true, "7.4.0"
			wire.disks[41], wire.marker = 128, 128*1024*1024
			switch mode {
			case "duplicate-initial-marker":
				wire.imageTemplateFields = "<CSI_REQUESTED_SIZE_BYTES>134217728</CSI_REQUESTED_SIZE_BYTES>"
			case "unrelated-fields":
				wire.imageTemplateFields = "<FORCE_DELETE>NO</FORCE_DELETE><FORCE_DELETE>YES</FORCE_DELETE>"
				wire.directImageFields = "<RUNNING_VMS>0</RUNNING_VMS>"
			}
			wire.mu.Unlock()
			server := NewControllerServer(newTestDriver(), provider, nil)
			reply, err := reviewControllerClient(t, server).ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
				VolumeId: "volume-test", CapacityRange: &csi.CapacityRange{RequiredBytes: 256 * 1024 * 1024},
			})
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Equal(t, []string{"resize:41:256", "marker:268435456"}, wire.mutations)
			if mode == "duplicate-final-marker" {
				require.Error(t, err)
				require.Nil(t, reply)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 256*1024*1024, reply.GetCapacityBytes())
			}
		})
	}
}

func TestAttachmentInventorySharesDatastorePoolPerPass(t *testing.T) {
	ctx := context.Background()
	var datastoreReads atomic.Int32
	provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
		if method == "one.datastorepool.info" {
			datastoreReads.Add(1)
		}
	})
	wire.mu.Lock()
	wire.extraDiskByVM = map[int]string{42: "<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdc</TARGET><SERIAL>onecsi-8</SERIAL></DISK>"}
	wire.imageInfoByID = map[int]string{8: "<IMAGE><ID>8</ID><NAME>other-volume</NAME><DATASTORE_ID>100</DATASTORE_ID></IMAGE>"}
	wire.mu.Unlock()
	for pass := int32(1); pass <= 2; pass++ {
		attachments, _, err := provider.ListCurrentAttachments(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, []opennebula.ObservedAttachment{
			{VolumeHandle: "volume-test", ImageID: 7, NodeName: "node-old", NodeID: 41, DiskID: 2, Backend: "local"},
			{VolumeHandle: "other-volume", ImageID: 8, NodeName: "node-new", NodeID: 42, DiskID: 2, Backend: "local"},
		}, attachments)
		require.Equal(t, pass, datastoreReads.Load())
	}
	datastore, err := provider.InspectVolumeDatastore(ctx, "volume-test", 7)
	require.NoError(t, err)
	require.Equal(t, "local", datastore.Backend)
	require.EqualValues(t, 3, datastoreReads.Load())
	wire.mu.Lock()
	wire.extraDiskByVM[42] = strings.Replace(wire.extraDiskByVM[42], "</DISK>", "<RESIZE>NO</RESIZE><RESIZE>YES</RESIZE></DISK>", 1)
	wire.mu.Unlock()
	attachments, _, err := provider.ListCurrentAttachments(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []opennebula.ObservedAttachment{
		{VolumeHandle: "volume-test", ImageID: 7, NodeName: "node-old", NodeID: 41, DiskID: 2, Backend: "local"},
		{VolumeHandle: "other-volume", ImageID: 8, NodeName: "node-new", NodeID: 42, DiskID: -1, Backend: "local"},
	}, attachments)
	require.EqualValues(t, 4, datastoreReads.Load())
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

func TestControllerPublishWithImageLessDiskOnTarget(t *testing.T) {
	for _, tc := range []struct {
		name, facts string
		allowed     bool
	}{
		{"empty CDROM", "<TYPE>CDROM</TYPE><SOURCE/>", true},
		{"volatile filesystem", "<TYPE>FS</TYPE><FORMAT>raw</FORMAT>", true},
		{"ambiguous", "<TYPE>DISK</TYPE>", false},
		{"backed CDROM", "<TYPE>CDROM</TYPE><SOURCE>/fixture/image.iso</SOURCE>", false},
		{"CSI serial", "<TYPE>CDROM</TYPE><SERIAL>onecsi-7</SERIAL>", false},
		{"omitted image ID on legacy device", "", true},
		{"omitted image ID on backed disk", "<IMAGE>volume-test</IMAGE><SOURCE>/fixture/disk</SOURCE><PERSISTENT>YES</PERSISTENT>", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node)
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			delete(wire.disks, 41)
			wire.marker = 128 * 1024 * 1024
			imageRef := "<IMAGE_ID/>"
			if strings.HasPrefix(tc.name, "omitted ") {
				imageRef = ""
			}
			wire.extraDiskBefore = "<DISK><DISK_ID>4</DISK_ID>" + imageRef + tc.facts + "</DISK>"
			wire.mu.Unlock()

			reply, err := reviewControllerClient(t, NewControllerServer(driver, provider, nil)).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if tc.allowed {
				require.NoError(t, err)
				require.Equal(t, "vdb", reply.GetPublishContext()["volumeName"])
				require.Equal(t, []string{"attach:41:false"}, wire.mutations)
			} else {
				require.Error(t, err)
				require.Nil(t, reply)
				require.Empty(t, wire.mutations)
			}
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
	for _, tc := range []struct {
		name, fields, message string
		invalidInventory      bool
	}{
		{"missing-image-id", "<SERIAL>onecsi-7</SERIAL>", "malformed VM disk image identity", true},
		{"conflicting-image-id", "<IMAGE_ID>8</IMAGE_ID><SERIAL>onecsi-7</SERIAL>", "CSI disk serial contradicts VM disk image identity", false},
		{"aliased-serial", "<IMAGE_ID>8</IMAGE_ID><SERIAL>onecsi-8</SERIAL><SERIAL>onecsi-007</SERIAL>", "repeated VM disk field SERIAL", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			provider, protocol := nodeAttachmentProvider(t)
			protocol.mu.Lock()
			delete(protocol.disks, 41)
			protocol.extraDiskBefore = "<DISK><DISK_ID>4</DISK_ID><TARGET>vdb</TARGET>" + tc.fields + "</DISK>"
			protocol.mu.Unlock()

			client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
			reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
			require.Nil(t, reply)
			require.Equal(t, codes.Unavailable, status.Code(err))
			_, err = provider.GetVolumeInNode(ctx, 7, 41)
			require.ErrorContains(t, err, tc.message)
			attachments, _, err := provider.ListCurrentAttachments(ctx)
			if tc.invalidInventory {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
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
	for _, mode := range []string{"serial", "wrong-image-id", "blank-image-name", "missing-image", "duplicate-resize", "duplicate-vm-state"} {
		t.Run(mode, func(t *testing.T) {
			testCephOrphanCleanupIgnoresUnrelatedCSIImageConflict(t, mode)
		})
	}
}

func testCephOrphanCleanupIgnoresUnrelatedCSIImageConflict(t *testing.T, mode string) {
	t.Helper()
	ctx := context.Background()
	pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
	otherPV, otherPVC := newLocalPVAndPVC("other-volume", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
	otherVA := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "other-va", UID: "other-va-uid"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName, NodeName: "node-new",
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &otherPV.Name},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	driver := newStickyTestDriver(t, pv, pvc, otherPV, otherPVC, otherVA, newReadyNode("node-old", true), newReadyNode("node-new", true))
	provider, wire := nodeAttachmentProvider(t)
	wire.mu.Lock()
	wire.ceph = true
	wire.disks[41] = 128
	wire.marker = 128 * 1024 * 1024
	wire.extraDiskBefore = `<DISK><DISK_ID>4</DISK_ID><TYPE>CDROM</TYPE><IMAGE_ID/><SOURCE/></DISK>`
	wire.extraDiskByVM = map[int]string{42: `<DISK><IMAGE_ID>8</IMAGE_ID><DISK_ID>2</DISK_ID><SERIAL>onecsi-9</SERIAL><TARGET>vdc</TARGET></DISK>`}
	wire.extraImages = `<IMAGE><ID>8</ID><NAME>other-volume</NAME></IMAGE>`
	switch mode {
	case "wrong-image-id":
		wire.imageInfoByID = map[int]string{8: `<IMAGE><ID>9</ID><NAME>other-volume</NAME></IMAGE>`}
	case "blank-image-name":
		wire.imageInfoByID = map[int]string{8: `<IMAGE><ID>8</ID><NAME></NAME></IMAGE>`}
	case "missing-image":
		wire.missingImageID = 8
	case "duplicate-resize", "duplicate-vm-state":
		if mode == "duplicate-vm-state" {
			wire.poolVMFields = map[int]string{42: "<LCM_STATE>14</LCM_STATE>"}
		} else {
			wire.extraDiskByVM[42] = strings.Replace(wire.extraDiskByVM[42], "</DISK>", "<RESIZE>NO</RESIZE><RESIZE>YES</RESIZE></DISK>", 1)
		}
		wire.imageInfoByID = map[int]string{8: `<IMAGE><ID>8</ID><NAME>other-volume</NAME><STATE>8</STATE><SIZE>128</SIZE><DATASTORE_ID>100</DATASTORE_ID><RUNNING_VMS>1</RUNNING_VMS><CLONING_OPS>0</CLONING_OPS><VMS><ID>42</ID></VMS></IMAGE>`}
	}
	if mode != "serial" {
		wire.extraDiskByVM[42] = strings.Replace(wire.extraDiskByVM[42], "onecsi-9", "onecsi-8", 1)
	}
	wire.mu.Unlock()

	_, err := provider.GetVolumeInNode(ctx, 7, 42)
	if mode == "duplicate-vm-state" {
		require.ErrorIs(t, err, opennebula.ErrConflictingAttachmentEvidence)
	} else {
		require.ErrorIs(t, err, opennebula.ErrAttachmentNotFound)
	}
	server := NewControllerServer(driver, provider, nil)
	reconciler := NewAttachmentReconciler(server)
	reconciler.orphanGrace = time.Nanosecond
	reconciler.staleVAGrace = time.Nanosecond
	require.NoError(t, reconciler.ReconcileOnce(ctx))
	require.Len(t, reconciler.orphanSeen, 1)
	require.NoError(t, reconciler.ReconcileOnce(ctx))
	_, err = driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(ctx, otherVA.Name, metav1.GetOptions{})
	require.NoError(t, err)
	if mode == "duplicate-resize" || mode == "duplicate-vm-state" {
		require.Empty(t, reconciler.staleVASeen)
		reply, err := reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "other-volume", NodeId: "node-new"})
		require.Equal(t, codes.Unavailable, status.Code(err))
		require.Nil(t, reply)
	}
	wire.mu.Lock()
	defer wire.mu.Unlock()
	require.Equal(t, []string{"detach:41"}, wire.mutations)
	require.Equal(t, 1, wire.detachAttempts)
}

type moveHistoryProvider struct {
	*opennebula.PersistentDiskVolumeProvider
	wire          *nodeAttachmentProtocol
	destination   string
	vmID          int
	failCensus    atomic.Bool
	timeoutAttach atomic.Bool
	failedCensus  atomic.Int32
}

func (p *moveHistoryProvider) AttachVolume(ctx context.Context, volume, node string, readOnly bool, parameters map[string]string) error {
	err := p.PersistentDiskVolumeProvider.AttachVolume(ctx, volume, node, readOnly, parameters)
	if err == nil && p.timeoutAttach.Swap(false) {
		return context.DeadlineExceeded
	}
	return err
}

func (p *moveHistoryProvider) InspectVolumeAttachment(ctx context.Context, volume, node string) (*opennebula.VolumeAttachmentMetadata, error) {
	p.wire.mu.Lock()
	_, attached := p.wire.disks[p.vmID]
	if node != p.destination || !attached || !p.failCensus.Load() {
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
			probe := &moveHistoryProvider{PersistentDiskVolumeProvider: provider, wire: wire, destination: newNode.Name, vmID: 42}
			server := NewControllerServer(driver, probe, nil)
			client := reviewControllerClient(t, server)

			_, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", oldNode.Name))
			require.NoError(t, err)
			before, err := server.readAttachmentHistory(ctx, "volume-test")
			require.NoError(t, err)
			require.Equal(t, oldNode.Name, before.LastSuccessfulNodeName)
			require.Equal(t, 41, before.LastSuccessfulOpenNebulaVMID)
			require.Equal(t, new(2), before.LastSuccessfulDiskID)
			_, err = client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: oldNode.Name})
			require.NoError(t, err)

			if sameNode {
				wire.mu.Lock()
				wire.vmNames = map[int]string{41: "retired-vm", 42: newNode.Name}
				wire.mu.Unlock()
			}

			probe.failCensus.Store(true)
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
			require.Equal(t, new(2), moved.LastSuccessfulDiskID)

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

type publishRestoreProbe struct {
	*opennebula.PersistentDiskVolumeProvider
	restored atomic.Bool
}

func (p *publishRestoreProbe) RestoreVolumeCapacity(ctx context.Context, volume string, nodeID, imageID int) error {
	err := p.PersistentDiskVolumeProvider.RestoreVolumeCapacity(ctx, volume, nodeID, imageID)
	if err == nil {
		p.restored.Store(true)
	}
	return err
}

func TestCephStaticPublishStageAndRebindRetainThenReconcile(t *testing.T) {
	for _, tc := range []struct {
		queued, stage bool
		drift         string
	}{
		{false, true, ""}, {true, true, ""}, {false, false, ""}, {true, false, ""},
		{false, false, "duplicate-target"}, {true, false, "duplicate-target"},
		{false, false, "invalid-target"}, {true, false, "invalid-target"},
		{false, false, "omitted-cloning"}, {true, false, "omitted-cloning"},
		{false, false, "duplicate-image"}, {true, false, "duplicate-image"},
		{false, false, "system-datastore"}, {true, false, "system-datastore"},
		{false, false, "late-duplicate-id"}, {true, false, "late-duplicate-id"},
		{false, false, "late-duplicate-resize"}, {true, false, "late-duplicate-resize"},
		{false, false, "late-duplicate-vm-lcm"}, {true, false, "late-duplicate-vm-lcm"},
		{false, false, "late-changed-vm-name"}, {true, false, "late-changed-vm-name"},
		{false, false, "late-missing-vm-name"}, {true, false, "late-missing-vm-name"},
		{false, false, "repeated-datastore-transfer"}, {true, false, "repeated-datastore-transfer"},
		{false, false, "repeated-datastore-template-type"}, {true, false, "repeated-datastore-template-type"},
		{false, false, "ready-census-owner"}, {true, false, "ready-census-owner"},
		{false, false, "late-duplicate-persistent"}, {true, false, "late-duplicate-persistent"},
		{false, false, "late-duplicate-capacity"}, {true, false, "late-duplicate-capacity"},
		{false, true, "zero-slot"}, {true, true, "zero-slot"},
		{false, false, "zero-slot-changed"}, {true, false, "zero-slot-changed"},
		{false, false, "legacy-slot-omitted"}, {true, false, "legacy-slot-omitted"},
	} {
		t.Run(fmt.Sprintf("queued=%t/stage=%t/drift=%s", tc.queued, tc.stage, tc.drift), func(t *testing.T) {
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
			var protocol *nodeAttachmentProtocol
			var unpublishActive atomic.Bool
			var imageReads, directReads atomic.Int32
			duplicateImage := tc.drift == "late-duplicate-id" || tc.drift == "late-duplicate-persistent" || tc.drift == "late-duplicate-capacity"
			provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if tc.drift == "ready-census-owner" && unpublishActive.Load() && method == "one.image.info" {
					protocol.mu.Lock()
					protocol.ready = imageReads.Add(1) == 2
					protocol.mu.Unlock()
				}
				if duplicateImage && unpublishActive.Load() && method == "one.image.info" && imageReads.Add(1) == 4 {
					protocol.mu.Lock()
					switch tc.drift {
					case "late-duplicate-id":
						protocol.prependImageID = true
					case "late-duplicate-persistent":
						protocol.directImageFields = "<PERSISTENT>0</PERSISTENT>"
					case "late-duplicate-capacity":
						protocol.imageTemplateFields = "<CSI_REQUESTED_SIZE_BYTES>1</CSI_REQUESTED_SIZE_BYTES>"
					}
					protocol.mu.Unlock()
				}
				if (tc.drift == "late-duplicate-resize" || tc.drift == "late-duplicate-vm-lcm" || tc.drift == "late-changed-vm-name" || tc.drift == "late-missing-vm-name") && unpublishActive.Load() && method == "one.vm.info" && directReads.Add(1) == 2 {
					protocol.mu.Lock()
					switch tc.drift {
					case "late-duplicate-vm-lcm":
						protocol.directVMFields = "<LCM_STATE>14</LCM_STATE>"
					case "late-changed-vm-name":
						protocol.alterDirectVM = func(body string) string {
							return strings.Replace(body, "<NAME>node-old</NAME>", "<NAME>node-other</NAME>", 1)
						}
					case "late-missing-vm-name":
						protocol.alterDirectVM = func(body string) string {
							return strings.Replace(body, "<NAME>node-old</NAME>", "", 1)
						}
					default:
						protocol.directDiskFields = "<RESIZE>NO</RESIZE><RESIZE>YES</RESIZE>"
					}
					protocol.mu.Unlock()
				}
			})
			protocol = wire
			wire.mu.Lock()
			wire.ceph, wire.version = true, "7.4.0"
			wire.disks[41], wire.marker = 128, 128*1024*1024
			zeroSlot := strings.HasPrefix(tc.drift, "zero-slot") || tc.drift == "legacy-slot-omitted"
			if zeroSlot {
				wire.diskSlot = "0"
			}
			wire.mu.Unlock()
			probe := &publishRestoreProbe{PersistentDiskVolumeProvider: provider}
			server := NewControllerServer(driver, probe, nil)
			if tc.queued {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			var stageActive, publishActive atomic.Bool
			var failedPublishReads, stagePVReads atomic.Int32
			publishActive.Store(!tc.stage)
			kube.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, kruntime.Object, error) {
				if stageActive.Load() {
					stagePVReads.Add(1)
					return true, nil, errors.New("node role cannot list persistent volumes")
				}
				if publishActive.Load() && probe.restored.Load() {
					failedPublishReads.Add(1)
					return true, nil, errors.New("ancillary PV lookup unavailable")
				}
				return false, nil, nil
			})
			client := reviewControllerClient(t, server)
			require.Empty(t, driver.volumeHistory.Snapshot())
			publish := newPublishReq(volume, node.Name)
			publish.VolumeContext = map[string]string{
				"example.test/static-attribute": "unrelated",
				paramPVName:                     pv.Name, paramPVCName: pvc.Name, paramPVCNamespace: pvc.Namespace,
			}
			publish.VolumeCapability.GetMount().FsType = "ext4"
			published, err := client.ControllerPublishVolume(ctx, publish)
			require.NoError(t, err)
			publishActive.Store(false)
			if tc.stage {
				require.Equal(t, "ceph", published.PublishContext[annotationBackend])
			} else {
				require.Positive(t, failedPublishReads.Load())
			}
			publishedHistory, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, "ceph", publishedHistory.Backend)
			require.Equal(t, 7, publishedHistory.LastSuccessfulImageID)
			if zeroSlot {
				require.Equal(t, new(0), publishedHistory.LastSuccessfulDiskID)
			}

			history := publishedHistory
			if tc.stage {
				diskPath := withTestDiskPath(t)
				require.NoError(t, os.WriteFile(filepath.Join(diskPath, "vdb"), []byte("fixture"), 0o600))
				stageMount := mount.NewFakeMounter(nil)
				ns := NewNodeServer(driver, mount.NewSafeFormatAndMount(stageMount, reviewDeviceSerialFixture(t, func(string) string {
					return "onecsi-7"
				})))
				ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
				stageContext := cloneStringMap(published.PublishContext)
				if tc.queued {
					stageContext[publishContextOpenNebulaImageID] = "8"
				}
				stageActive.Store(true)
				staged, err := ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
					VolumeId: volume, StagingTargetPath: t.TempDir(), VolumeCapability: publish.VolumeCapability, PublishContext: stageContext,
				})
				require.NoError(t, err)
				require.NotNil(t, staged)
				stageActive.Store(false)
				require.Zero(t, stagePVReads.Load())
				history, err = server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, "ceph", history.Backend)
				require.Equal(t, 7, history.LastSuccessfulImageID)
				require.False(t, history.LastSuccessfulStageTime.IsZero())
				if zeroSlot {
					require.Equal(t, new(0), history.LastSuccessfulDiskID)
				}
			} else {
				require.True(t, history.LastSuccessfulStageTime.IsZero())
			}

			require.NoError(t, kube.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{}))
			require.NoError(t, kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
			pv, pvc = pv.DeepCopy(), pvc.DeepCopy()
			pv.Name, pv.UID = "rebound-static-pv", "rebound-static-pv-uid"
			pvc.Name, pvc.UID = "rebound-static-pvc", "rebound-static-pvc-uid"
			pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.UID = pvc.Name, pvc.UID
			pvc.Spec.VolumeName = pv.Name
			delete(pv.Annotations, annotationBackend)
			pv.Spec.CSI.VolumeAttributes = map[string]string{annotationBackend: "Ceph"}
			for key, value := range publish.VolumeContext {
				pv.Spec.CSI.VolumeAttributes[key] = value
			}
			_, err = kube.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
			require.NoError(t, err)
			_, err = kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
			require.NoError(t, err)

			pod := activePodForPVC("replacement", pvc.Name, node.Name)
			pod.UID = "static-consumer-uid"
			pod.Status.Phase = corev1.PodPending
			_, err = kube.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			require.NoError(t, err)
			publish.VolumeContext = pv.Spec.CSI.VolumeAttributes
			rebound, err := client.ControllerPublishVolume(ctx, publish)
			require.NoError(t, err)
			require.Equal(t, pv.Name, rebound.PublishContext[paramPVName])
			require.Equal(t, pvc.Name, rebound.PublishContext[paramPVCName])
			require.Equal(t, pvc.Namespace, rebound.PublishContext[paramPVCNamespace])
			history, err = server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			if tc.drift == "legacy-slot-omitted" {
				_, err = driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
					record.LastSuccessfulDiskID = nil
				})
				require.NoError(t, err)
				history, err = server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Nil(t, history.LastSuccessfulDiskID)
			}
			if tc.drift == "invalid-target" {
				history, err = driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
					record.LastSuccessfulTarget = ""
				})
				require.NoError(t, err)
				delete(rebound.PublishContext, "volumeName")
			}
			if zeroSlot {
				cm, err := driver.kubeRuntime.GetConfigMap(ctx, "default", volumeHistoryStateConfigMapName)
				require.NoError(t, err)
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal([]byte(cm.Data[volume]), &fields))
				if tc.drift == "legacy-slot-omitted" {
					require.NotContains(t, fields, "lastSuccessfulDiskID")
				} else {
					require.JSONEq(t, "0", string(fields["lastSuccessfulDiskID"]))
				}
			}
			deleting := metav1.Now()
			va := &storagev1.VolumeAttachment{
				ObjectMeta: metav1.ObjectMeta{Name: "rebound-static-va", UID: "rebound-static-va-uid", DeletionTimestamp: &deleting, Finalizers: []string{"external-attacher/" + DefaultDriverName}},
				Spec:       storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
				Status:     storagev1.VolumeAttachmentStatus{Attached: true, AttachmentMetadata: rebound.PublishContext},
			}
			_, err = kube.StorageV1().VolumeAttachments().Create(ctx, va, metav1.CreateOptions{})
			require.NoError(t, err)
			wire.mu.Lock()
			switch tc.drift {
			case "duplicate-target":
				wire.extraDiskByVM = map[int]string{41: "<DISK><IMAGE_ID>9</IMAGE_ID><DISK_ID>3</DISK_ID><TARGET>vdb</TARGET></DISK>"}
			case "invalid-target":
				wire.diskTarget = "../vdb"
			case "omitted-cloning":
				wire.alterDirectImage = func(body string) string {
					return strings.Replace(body, "<CLONING_OPS>0</CLONING_OPS>", "", 1)
				}
			case "duplicate-image":
				wire.duplicateImage = true
			case "system-datastore":
				wire.imageDatastoreID = 200
			case "repeated-datastore-transfer":
				wire.alterDatastorePool = func(body string) string {
					return strings.Replace(body, "<TM_MAD>ceph</TM_MAD>", "<TM_MAD>ssh</TM_MAD><TM_MAD>ceph</TM_MAD>", 1)
				}
			case "repeated-datastore-template-type":
				wire.alterDatastorePool = func(body string) string {
					return strings.Replace(body, "<TEMPLATE>", "<TEMPLATE><TYPE>SYSTEM_DS</TYPE><TYPE>IMAGE_DS</TYPE>", 1)
				}
			case "zero-slot-changed":
				wire.diskSlot = "2"
			}
			wire.mu.Unlock()
			unpublishActive.Store(true)
			response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
			if tc.drift != "" && tc.drift != "zero-slot" && tc.drift != "legacy-slot-omitted" {
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, response)
				switch tc.drift {
				case "late-duplicate-resize", "late-duplicate-vm-lcm", "late-changed-vm-name", "late-missing-vm-name":
					require.ErrorContains(t, err, "provider")
					require.EqualValues(t, 2, directReads.Load())
				case "late-duplicate-persistent", "late-duplicate-capacity":
					require.ErrorContains(t, err, "provider")
					require.EqualValues(t, 4, imageReads.Load())
				case "zero-slot-changed":
					require.ErrorContains(t, err, "device identity contradicts observed history")
					require.NoError(t, kube.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
					reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.ErrorContains(t, err, "detach disk slot differs from observed history")
					require.Nil(t, reply)
				}
				after, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, history, after)
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if tc.drift == "late-duplicate-id" {
					require.True(t, wire.prependImageID)
					require.EqualValues(t, 4, imageReads.Load())
				}
				require.Empty(t, wire.mutations)
				require.Equal(t, map[int]int{41: 128}, wire.disks)
				return
			}
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

func TestDetachRechecksVAAfterFinalProtectionRead(t *testing.T) {
	for _, tc := range []struct{ queued, replace bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("queued=%t/replace=%t", tc.queued, tc.replace), func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			node := newReadyNode("node-old", true)
			deleting := metav1.Now()
			va := &storagev1.VolumeAttachment{
				ObjectMeta: metav1.ObjectMeta{Name: "local-deleting-va", UID: "local-deleting-va-uid", DeletionTimestamp: &deleting},
				Spec:       storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
				Status:     storagev1.VolumeAttachmentStatus{Attached: true, AttachmentMetadata: map[string]string{publishContextOpenNebulaImageID: "7"}},
			}
			driver := newStickyTestDriver(t, pv, pvc, node, va)
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			provider, wire := nodeAttachmentProvider(t)
			probe := &detachCensusProbe{PersistentDiskVolumeProvider: provider}
			server := NewControllerServer(driver, probe, nil)
			if tc.queued {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			var protectionReads atomic.Int32
			kube.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
				if action.(ktesting.GetAction).GetName() == hostArtifactStateConfigMapName && probe.inDetach.Load() {
					if protectionReads.Add(1) == 2 && tc.replace {
						replaceDeletingVolumeAttachment(t, kube, va)
					}
				}
				return false, nil, nil
			})
			client := reviewControllerClient(t, server)
			reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
			require.GreaterOrEqual(t, protectionReads.Load(), int32(2))
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if tc.replace {
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "VolumeAttachment deletion authority changed")
				require.Nil(t, reply)
				require.Zero(t, wire.detachAttempts)
				require.Contains(t, wire.disks, 41)
				history, _ := driver.volumeHistory.Get(volume)
				require.True(t, history.LastSafeDetachTime.IsZero())
			} else {
				require.NoError(t, err)
				require.NotNil(t, reply)
				require.Equal(t, []string{"detach:41"}, wire.mutations)
			}
		})
	}
}

func TestControllerUnpublishCephROXWithRemoteReaders(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, local := range []string{"released", "pod-demand", "mounted", "manual"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, local), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}, map[string]string{
					annotationBackend: "ceph", annotationDatastoreID: "100",
				})
				pv.Status.Phase = corev1.VolumeBound
				pvc.Status.Phase = corev1.ClaimBound
				pvc.Spec.AccessModes = pv.Spec.AccessModes
				node, remote, third := newReadyNode("node-old", true), newReadyNode("node-new", true), newReadyNode("node-third", true)
				deleting := metav1.Now()
				va := &storagev1.VolumeAttachment{
					ObjectMeta: metav1.ObjectMeta{Name: "deleting-reader-va", UID: "deleting-reader-uid", DeletionTimestamp: &deleting},
					Spec:       storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
					Status:     storagev1.VolumeAttachmentStatus{Attached: true, AttachmentMetadata: map[string]string{publishContextOpenNebulaImageID: "7"}},
				}
				objects := []kruntime.Object{pv, pvc, node, remote, third, va,
					activePodForPVC("reader-b", pvc.Name, remote.Name), activePodForPVC("reader-c", pvc.Name, third.Name)}
				switch local {
				case "pod-demand":
					objects = append(objects, activePodForPVC("reader-a", pvc.Name, node.Name))
				case "mounted":
					node.Status.VolumesInUse = []corev1.UniqueVolumeName{"kubernetes.io/csi/" + DefaultDriverName + "^" + volume}
				case "manual":
					pvc.Annotations[annotationRecoveryMode] = recoveryModeManual
				}
				driver := newStickyTestDriver(t, objects...)
				provider, wire := nodeAttachmentProvider(t)
				wire.mu.Lock()
				wire.ceph, wire.version = true, "7.4.0"
				wire.vmNames = map[int]string{43: third.Name}
				wire.disks[42], wire.disks[43] = 256, 256
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				client := reviewControllerClient(t, server)
				resp, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
				if local == "released" || local == "manual" {
					require.NoError(t, err)
					require.NotNil(t, resp)
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, resp)
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if local == "released" {
					require.Equal(t, []string{"detach:41"}, wire.mutations)
					require.Equal(t, map[int]int{42: 256, 43: 256}, wire.disks)
				} else {
					require.Empty(t, wire.mutations)
					require.Equal(t, map[int]int{41: 256, 42: 256, 43: 256}, wire.disks)
				}
			})
		}
	}
}

func TestCephSameVMFreshAttachReplacesUnobservedDiskSlot(t *testing.T) {
	for _, tc := range []struct{ queued, retry bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("queued=%t/retry=%t", tc.queued, tc.retry), func(t *testing.T) {
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
			driver.PluginConfig.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, true)
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.ceph, wire.version = true, "7.4.0"
			wire.disks[41], wire.marker = 128, 128*1024*1024
			wire.mu.Unlock()
			probe := &moveHistoryProvider{PersistentDiskVolumeProvider: provider, wire: wire, destination: node.Name, vmID: 41}
			server := NewControllerServer(driver, probe, nil)
			if tc.queued {
				driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
				driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
			}
			client := reviewControllerClient(t, server)
			publish := newPublishReq(volume, node.Name)
			unpublish := &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name}
			_, err := client.ControllerPublishVolume(ctx, publish)
			require.NoError(t, err)
			before, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, new(2), before.LastSuccessfulDiskID)
			_, err = client.ControllerUnpublishVolume(ctx, unpublish)
			require.NoError(t, err)
			wire.mu.Lock()
			wire.diskSlot = "3"
			wire.mu.Unlock()
			kube := driver.kubeRuntime.client
			pod := activePodForPVC("replacement", pvc.Name, node.Name)
			pod.UID = "same-vm-replacement"
			_, err = kube.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			require.NoError(t, err)
			if tc.retry {
				probe.timeoutAttach.Store(true)
				reply, err := client.ControllerPublishVolume(ctx, publish)
				require.Equal(t, codes.DeadlineExceeded, status.Code(err))
				require.Nil(t, reply)
				history, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, before.LastSuccessfulPublishTime, history.LastSuccessfulPublishTime)
			}
			probe.failCensus.Store(true)
			reply, err := client.ControllerPublishVolume(ctx, publish)
			require.NoError(t, err)
			require.EqualValues(t, 1, probe.failedCensus.Load())
			probe.failCensus.Store(false)
			after, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, new(3), after.LastSuccessfulDiskID)
			require.Equal(t, before.LastSuccessfulOpenNebulaVMID, after.LastSuccessfulOpenNebulaVMID)
			require.Equal(t, before.LastSuccessfulImageID, after.LastSuccessfulImageID)

			deleting := metav1.Now()
			_, err = kube.StorageV1().VolumeAttachments().Create(ctx, &storagev1.VolumeAttachment{
				ObjectMeta: metav1.ObjectMeta{Name: "deleting-new-slot", UID: "new-slot-va", DeletionTimestamp: &deleting},
				Spec:       storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
				Status:     storagev1.VolumeAttachmentStatus{Attached: true, AttachmentMetadata: reply.PublishContext},
			}, metav1.CreateOptions{})
			require.NoError(t, err)
			retained, err := client.ControllerUnpublishVolume(ctx, unpublish)
			require.NoError(t, err)
			require.NotNil(t, retained)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Equal(t, []string{"detach:41", "attach:41:false"}, wire.mutations)
			require.Equal(t, map[int]int{41: 128}, wire.disks)
		})
	}
}

type canceledCapacityRestoreProvider struct {
	*opennebula.PersistentDiskVolumeProvider
	err error
}

func (p canceledCapacityRestoreProvider) RestoreVolumeCapacity(context.Context, string, int, int) error {
	return p.err
}

func TestControllerPublishRecordsCancellationOutcome(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, cause), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				provider, wire := nodeAttachmentProvider(t)
				server := NewControllerServer(driver, canceledCapacityRestoreProvider{provider, cause}, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				client := reviewControllerClient(t, server)
				resp, err := client.ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
				require.Equal(t, status.FromContextError(cause).Code(), status.Code(err))
				require.Nil(t, resp)
				families, err := driver.metrics.registry.Gather()
				require.NoError(t, err)
				counts := map[string]uint64{}
				for _, family := range families {
					switch family.GetName() {
					case "opennebula_csi_operation_total", "opennebula_csi_controller_publish_duration_seconds":
						for _, metric := range family.Metric {
							count := uint64(metric.GetCounter().GetValue()) + metric.GetHistogram().GetSampleCount()
							counts[family.GetName()] += count
						}
					}
				}
				require.Equal(t, map[string]uint64{
					"opennebula_csi_operation_total": 1, "opennebula_csi_controller_publish_duration_seconds": 1,
				}, counts)
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Empty(t, wire.mutations)
			})
		}
	}
}

func TestCreateVolumeRetryReturnsObservedBackend(t *testing.T) {
	for _, mode := range []string{"valid", "ambiguous-selected-name", "unrelated-ambiguous-transfer"} {
		t.Run(mode, func(t *testing.T) {
			testCreateVolumeRetryReturnsObservedBackend(t, mode)
		})
	}
}

func testCreateVolumeRetryReturnsObservedBackend(t *testing.T, mode string) {
	t.Helper()
	ctx := context.Background()
	driver := newStickyTestDriver(t)
	driver.PluginConfig.OverrideVal(config.AllowedDatastoreTypesVar, "ceph")
	driver.featureGates.TopologyAccessibility = true
	provider, wire := nodeAttachmentProvider(t)
	wire.mu.Lock()
	wire.ceph, wire.version, wire.ready = true, "7.4.0", true
	wire.disks = map[int]int{}
	wire.marker = 128 * 1024 * 1024
	switch mode {
	case "ambiguous-selected-name":
		wire.alterDatastorePool = func(body string) string {
			return strings.Replace(body, "<NAME>images</NAME>", "<NAME>other</NAME><NAME>images</NAME>", 1)
		}
	case "unrelated-ambiguous-transfer":
		wire.alterDatastorePool = func(body string) string {
			return strings.Replace(body, "</DATASTORE_POOL>", "<DATASTORE><ID>300</ID><NAME>unrelated</NAME><TM_MAD>ssh</TM_MAD><TM_MAD>ceph</TM_MAD></DATASTORE></DATASTORE_POOL>", 1)
		}
	}
	wire.mu.Unlock()
	client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
	reply, err := client.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "volume-test", CapacityRange: &csi.CapacityRange{RequiredBytes: 128 * 1024 * 1024},
		VolumeCapabilities: []*csi.VolumeCapability{newPublishReq("volume-test", "node-old").VolumeCapability},
		Parameters:         map[string]string{storageClassParamDatastoreIDs: "100", paramPVCUID: "test-claim-uid"},
	})
	wire.mu.Lock()
	defer wire.mu.Unlock()
	require.Empty(t, wire.mutations)
	if mode == "ambiguous-selected-name" {
		require.Equal(t, codes.Unavailable, status.Code(err))
		require.Nil(t, reply)
		return
	}
	require.NoError(t, err)
	require.Equal(t, "volume-test", reply.Volume.VolumeId)
	require.EqualValues(t, 128*1024*1024, reply.Volume.CapacityBytes)
	require.Equal(t, "ceph", reply.Volume.VolumeContext[annotationBackend])
	require.Equal(t, "100", reply.Volume.VolumeContext[annotationDatastoreID])
	require.Equal(t, "images", reply.Volume.VolumeContext[annotationDatastoreName])
	require.NotContains(t, reply.Volume.VolumeContext, annotationSelectionPolicy)
	require.Empty(t, reply.Volume.AccessibleTopology)
	require.Equal(t, "test-claim-uid", reply.Volume.VolumeContext[volumeContextUUID])
}

func TestLegacyDiskCleanupVerifiesUndeclaredBackend(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, tc := range []struct {
			action, evidence string
		}{
			{"foreground", "verified"}, {"orphan", "verified"}, {"stale", "verified"},
			{"foreground", "unavailable"}, {"foreground", "contradictory"},
			{"orphan", "unavailable"}, {"stale", "unavailable"},
			{"foreground", "system-datastore"}, {"foreground", "declared-system"}, {"orphan", "system-datastore"}, {"stale", "system-datastore"},
			{"stale", "duplicate-image"}, {"stale", "missing-image-owners"}, {"stale", "declared-system"}, {"stale", "declared-unavailable"},
			{"stale", "declared-conflicting-drivers"}, {"stale", "declared-verified"},
		} {
			t.Run(fmt.Sprintf("queued=%t/%s/%s", queued, tc.action, tc.evidence), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				pv.ResourceVersion = "1"
				pv.Status.Phase = corev1.VolumeBound
				delete(pv.Annotations, annotationBackend)
				if tc.evidence == "contradictory" {
					pv.Annotations[annotationBackend] = "ceph"
					pv.Spec.CSI.VolumeAttributes = map[string]string{annotationBackend: "local"}
				}
				if strings.HasPrefix(tc.evidence, "declared-") {
					pv.Annotations[annotationBackend] = "ceph"
				}
				node := newReadyNode("node-old", true)
				objects := []kruntime.Object{pv, pvc, node}
				const attachmentName = "stale-undeclared-attachment"
				if tc.action == "stale" {
					objects = append(objects, &storagev1.VolumeAttachment{
						ObjectMeta: metav1.ObjectMeta{Name: attachmentName, UID: "stale-undeclared-uid", ResourceVersion: "1"},
						Spec:       storagev1.VolumeAttachmentSpec{Attacher: DefaultDriverName, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
						Status:     storagev1.VolumeAttachmentStatus{Attached: true, AttachmentMetadata: map[string]string{publishContextOpenNebulaImageID: "7"}},
					})
				}
				driver := newStickyTestDriver(t, objects...)
				if tc.action == "stale" {
					_, err := driver.volumeHistory.Upsert(ctx, volume, func(history *VolumeHistoryRecord) {
						history.Backend = "ceph"
						history.LastSuccessfulImageID = 7
						history.LastSuccessfulOpenNebulaVMID = 41
						history.LastSuccessfulNodeName = node.Name
						history.LastSuccessfulNodeUID = string(node.UID)
						history.LastSuccessfulPublishTime = time.Now().Add(-time.Hour)
					})
					require.NoError(t, err)
				}
				provider, wire := nodeAttachmentProvider(t)
				wire.mu.Lock()
				wire.ceph, wire.version = true, "7.4.0"
				if tc.action == "stale" {
					delete(wire.disks, 41)
				}
				switch tc.evidence {
				case "unavailable", "declared-unavailable":
					wire.failMethod = "one.datastorepool.info"
				case "system-datastore", "declared-system":
					wire.imageDatastoreID = 200
				case "duplicate-image":
					wire.duplicateImage = true
				case "missing-image-owners":
					wire.alterDirectImage = func(body string) string {
						return strings.Replace(body, "<VMS></VMS>", "", 1)
					}
				case "declared-conflicting-drivers":
					wire.imageTransfer = "ssh"
				}
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}

				if tc.action == "foreground" {
					client := reviewControllerClient(t, server)
					reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
					switch tc.evidence {
					case "verified":
						require.NoError(t, err)
						require.NotNil(t, reply)
					case "declared-system":
						require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
						require.Nil(t, reply)
					default:
						require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
						require.Nil(t, reply)
					}
				} else {
					r := NewAttachmentReconciler(server)
					r.orphanGrace, r.staleVAGrace = time.Nanosecond, time.Nanosecond
					require.NoError(t, r.ReconcileOnce(ctx))
					require.NoError(t, r.ReconcileOnce(ctx))
				}

				if tc.action == "stale" {
					_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(ctx, attachmentName, metav1.GetOptions{})
					if tc.evidence == "verified" || tc.evidence == "declared-verified" {
						require.True(t, apierrors.IsNotFound(err))
					} else {
						require.NoError(t, err)
					}
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if tc.evidence == "verified" && tc.action != "stale" {
					require.Equal(t, []string{"detach:41"}, wire.mutations)
					require.Empty(t, wire.disks)
				} else {
					require.Empty(t, wire.mutations)
				}
			})
		}
	}
}

func TestControllerPublishRechecksNodeAtProviderMutations(t *testing.T) {
	for _, mutation := range []string{"attach", "retry-attach", "resize", "post-attach-resize", "marker", "image-drift", "vm-drift", "slot-relocation", "slot-replacement", "marker-slot-relocation", "resize-final-read", "marker-final-read"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			node := newReadyNode("node-old", true)
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			driver := newStickyTestDriver(t, pv, pvc, node)
			var wire *nodeAttachmentProtocol
			var replaced atomic.Bool
			var finalRead atomic.Bool
			provider, state := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				wire.mu.Lock()
				if (mutation == "resize-final-read" && method == "one.system.version") ||
					(mutation == "marker-final-read" && method == "one.image.info" && len(wire.mutations) == 1) {
					finalRead.Store(true)
				}
				replace := (finalRead.Load() && method == "one.vm.info") ||
					(mutation == "attach" && method == "one.datastorepool.info") ||
					(mutation == "retry-attach" && method == "one.vm.info" && wire.attachAttempts == 1) ||
					((mutation == "resize" || mutation == "post-attach-resize") && method == "one.system.version") ||
					(mutation == "marker" && method == "one.image.info" && len(wire.mutations) == 1)
				if method == "one.system.version" && (mutation == "image-drift" || mutation == "vm-drift") {
					if mutation == "image-drift" {
						wire.duplicateImage = true
					} else {
						wire.vmNames = map[int]string{41: "node-other"}
					}
					replaced.Store(true)
				}
				if method == "one.system.version" && (mutation == "slot-relocation" || mutation == "slot-replacement") {
					wire.diskSlot = "3"
					if mutation == "slot-replacement" {
						wire.extraDiskByVM = map[int]string{41: "<DISK><IMAGE_ID>9</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdc</TARGET></DISK>"}
					}
					replaced.Store(true)
				}
				if mutation == "marker-slot-relocation" && method == "one.image.info" && len(wire.mutations) == 1 {
					wire.diskSlot = "3"
					replaced.Store(true)
				}
				wire.mu.Unlock()
				if replace && replaced.CompareAndSwap(false, true) {
					nodes := driver.kubeRuntime.client.CoreV1().Nodes()
					require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
					replacement := node.DeepCopy()
					replacement.UID = "replacement"
					_, err := nodes.Create(ctx, replacement, metav1.CreateOptions{})
					require.NoError(t, err)
				}
			})
			wire = state
			wire.mu.Lock()
			wire.ceph, wire.version = true, "7.4.0"
			wire.disks[41] = 128
			if mutation == "attach" || mutation == "retry-attach" || mutation == "post-attach-resize" {
				delete(wire.disks, 41)
			}
			wire.rejectFirstAttach = mutation == "retry-attach"
			wire.mu.Unlock()

			server := NewControllerServer(driver, provider, nil)
			before, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			switch mutation {
			case "slot-replacement", "slot-relocation":
				require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
				require.ErrorContains(t, err, "restoration disk slot differs")
			case "marker-slot-relocation":
				require.ErrorContains(t, err, "restoration disk slot differs")

			case "image-drift":
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "publish mutation image identity is unverified")
			case "vm-drift":
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "publish mutation VM identity is unverified")
			default:
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "publish Kubernetes Node identity changed")
			}
			require.Nil(t, reply)
			require.True(t, replaced.Load())
			after, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, before.LastSuccessfulPublishTime, after.LastSuccessfulPublishTime)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			switch mutation {
			case "marker", "marker-slot-relocation", "marker-final-read":
				require.Equal(t, []string{"resize:41:256"}, wire.mutations)
			case "post-attach-resize":
				require.Equal(t, []string{"attach:41:false"}, wire.mutations)
			default:
				require.Empty(t, wire.mutations)
			}
			if mutation == "retry-attach" {
				require.Equal(t, 1, wire.attachAttempts)
			}
		})
	}
}

func setReplacementHistoryVersion(t *testing.T, driver *Driver) {
	t.Helper()
	ctx := context.Background()
	cms := driver.kubeRuntime.client.CoreV1().ConfigMaps(driver.volumeHistory.namespace)
	cm, err := cms.Get(ctx, volumeHistoryStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	cm.ResourceVersion = "101"
	_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func TestControllerPublishReplacementAttachRetry(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"lost-reply", "foreign-marker", "duplicate-marker", "ambiguous-target", "malformed-target", "external-disk", "node-changed", "pv-changed", "history-changed", "safe-detach-updated", "unknown", "final-refusal", "write-conflict", "lost-write-reply", "readback-failed", "readback-unproven", "prepared-outage", "armed-outage", "armed-lost-ack", "competing-arm", "prepared-abandoned", "prepared-abandoned-quarantine", "prepared-abandoned-conflict", "prepared-abandoned-orphan", "concurrent-prepared", "pre-dispatch", "terminating-old-consumer", "ambiguous-old-consumer", "distinct-vm", "distinct-vm-lost-reply", "distinct-vm-original-gone-at-admission", "distinct-vm-original-gone-before-retry",
			"distinct-vm-inflight", "same-vm-abandoned-replay", "distinct-vm-abandoned-replay", "distinct-vm-abandoned-replay-replaced-va", "distinct-vm-abandoned-replay-protection", "distinct-vm-abandoned-completion-lost-ack", "distinct-vm-abandoned", "distinct-vm-orphan", "distinct-vm-abandoned-released", "distinct-vm-abandoned-rebound", "distinct-vm-orphan-released", "distinct-vm-abandoned-wrong-marker", "distinct-vm-abandoned-consumer",
			"distinct-vm-abandoned-late-uid", "distinct-vm-abandoned-late-quarantine", "distinct-vm-abandoned-terminating", "distinct-vm-abandoned-readback", "distinct-vm-abandoned-no-disk-orphan", "distinct-vm-abandoned-no-disk-stale-va", "distinct-vm-abandoned-no-disk-original-owner", "distinct-vm-abandoned-detach-reply", "distinct-vm-abandoned-retire-conflict", "phase-less", "distinct-vm-still-owned", "distinct-vm-unreadable-original", "distinct-vm-original-changed", "distinct-vm-marker-mismatch", "distinct-vm-node-changed", "distinct-vm-destination-changed", "other-version"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
				pvc.Spec.AccessModes = pv.Spec.AccessModes
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				kube := driver.kubeRuntime.client.(*fake.Clientset)
				var pendingWritten, readbackFailed, apiOutage, outageInjected, abandoned atomic.Bool
				var detachProofs atomic.Int32
				abandonedPreparation := strings.HasPrefix(mode, "prepared-abandoned")
				armError := mode == "armed-outage" || mode == "armed-lost-ack" || mode == "competing-arm"
				interruptedAdmission := mode == "prepared-outage" || armError || abandonedPreparation || mode == "concurrent-prepared"
				orphan := strings.HasPrefix(mode, "distinct-vm-orphan")
				replay := strings.Contains(mode, "abandoned-replay") || mode == "distinct-vm-abandoned-completion-lost-ack"
				cleanup := strings.HasPrefix(mode, "distinct-vm-abandoned") || orphan || replay
				lostReadback := mode == "distinct-vm-abandoned-readback" || strings.HasPrefix(mode, "distinct-vm-abandoned-no-disk")
				interruptedCleanup := lostReadback || mode == "distinct-vm-abandoned-detach-reply" || mode == "distinct-vm-abandoned-retire-conflict"
				var lostWriteToken, preparedToken, proposedArmToken string
				var wire *nodeAttachmentProtocol
				var failBeforeDispatch atomic.Bool
				provider, protocol := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if abandoned.Load() && method == "one.system.version" && detachProofs.Add(1) == 2 {
						switch mode {
						case "distinct-vm-abandoned-late-uid":
							current, err := kube.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
							require.NoError(t, err)
							current.UID = "late-replacement"
							_, err = kube.CoreV1().Nodes().Update(ctx, current, metav1.UpdateOptions{})
							require.NoError(t, err)
						case "distinct-vm-abandoned-late-quarantine":
							_, active, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: volume, Reason: "metadata drift"}, 1, time.Hour)
							require.NoError(t, err)
							require.True(t, active)
						}
					}
					if abandoned.Load() && lostReadback && method == "one.image.info" {
						wire.mu.Lock()
						if _, attached := wire.disks[42]; !attached {
							wire.failMethod = method
						}
						wire.mu.Unlock()
					}
					if method == "one.datastorepool.info" && failBeforeDispatch.Load() && pendingWritten.Load() {
						wire.mu.Lock()
						wire.failMethod = method
						wire.mu.Unlock()
					}
				})
				wire = protocol
				wire.mu.Lock()
				wire.ceph, wire.version, wire.marker, wire.disks[41] = true, "7.4.0", 128*1024*1024, 128
				wire.mu.Unlock()
				makeClient := func(d *Driver) csi.ControllerClient {
					server := NewControllerServer(d, provider, nil)
					if queued {
						d.hotplugQueue = NewHotplugQueueManager(nil, "default", d.metrics, 5*time.Second, time.Second)
						d.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
					}
					return reviewControllerClient(t, server)
				}
				client := makeClient(driver)
				publish := newPublishReq(volume, node.Name)
				_, err := client.ControllerPublishVolume(ctx, publish)
				require.NoError(t, err)
				setReplacementHistoryVersion(t, driver)
				nodes := kube.CoreV1().Nodes()
				require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
				replacement := node.DeepCopy()
				replacement.UID = "replacement"
				replacement.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
				_, err = nodes.Create(ctx, replacement, metav1.CreateOptions{})
				require.NoError(t, err)
				consumer := activePodForPVC("new-pod", pvc.Name, node.Name)
				consumer.UID = "new-pod"
				consumer.Status.Phase = corev1.PodPending
				consumer.CreationTimestamp = metav1.NewTime(replacement.CreationTimestamp.Add(-time.Minute))
				_, err = kube.CoreV1().Pods(pvc.Namespace).Create(ctx, consumer, metav1.CreateOptions{})
				require.NoError(t, err)
				if mode == "terminating-old-consumer" || mode == "ambiguous-old-consumer" {
					old := activePodForPVC("old-pod", pvc.Name, node.Name)
					now := metav1.Now()
					old.UID = "old-pod"
					if mode == "terminating-old-consumer" {
						old.DeletionTimestamp = &now
					} else {
						oldStart := metav1.NewTime(replacement.CreationTimestamp.Add(-time.Minute))
						old.Status.StartTime = &oldStart
					}
					_, err := kube.CoreV1().Pods(pvc.Namespace).Create(ctx, old, metav1.CreateOptions{})
					require.NoError(t, err)
				}
				wire.mu.Lock()
				if mode != "external-disk" && mode != "distinct-vm-still-owned" {
					delete(wire.disks, 41)
				}
				wire.loseAttachReply = mode != "distinct-vm" && !interruptedAdmission && mode != "readback-unproven"
				wire.abortFirstAttach = mode == "unknown" || mode == "distinct-vm-inflight"
				wire.allowImageDelete = mode == "distinct-vm-inflight"
				wire.rejectFirstAttach = mode == "final-refusal"
				vmID := 41
				if strings.HasPrefix(mode, "distinct-vm") {
					vmID = 42
					wire.vmNames = map[int]string{41: "retired-vm", 42: node.Name}
					wire.omitOriginalVM = mode == "distinct-vm-unreadable-original" || mode == "distinct-vm-original-gone-at-admission"
					wire.missingOriginalVM = mode == "distinct-vm-original-gone-at-admission"
				}
				if mode == "other-version" {
					wire.version = "7.4.1"
				}
				wire.mu.Unlock()
				failBeforeDispatch.Store(mode == "pre-dispatch")
				if mode == "write-conflict" || mode == "lost-write-reply" || mode == "readback-failed" || mode == "readback-unproven" || mode == "pre-dispatch" || interruptedAdmission {
					kube.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
						cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
						if cm.Name != volumeHistoryStateConfigMapName {
							return false, nil, nil
						}
						var fields map[string]json.RawMessage
						require.NoError(t, json.Unmarshal([]byte(cm.Data[volume]), &fields))
						gvr := corev1.SchemeGroupVersion.WithResource("configmaps")
						if mode == "concurrent-prepared" {
							previous, err := kube.Tracker().Get(gvr, cm.Namespace, cm.Name)
							require.NoError(t, err)
							rv := previous.(*corev1.ConfigMap).ResourceVersion
							if cm.ResourceVersion != rv {
								return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("stale resource version"))
							}
							version, err := strconv.Atoi(rv)
							require.NoError(t, err)
							cm.ResourceVersion = strconv.Itoa(version + 1)
						}
						if interruptedAdmission && fields["pendingReplacementAttach"] != nil {
							var attempt replacementAttachAttempt
							require.NoError(t, json.Unmarshal(fields["pendingReplacementAttach"], &attempt))
							if attempt.prepared() {
								preparedToken = attempt.ID
							}
							if attempt.dispatchPossible() && proposedArmToken == "" {
								proposedArmToken = attempt.ID
							}
							if (attempt.prepared() == !armError) && outageInjected.CompareAndSwap(false, true) {
								if mode == "competing-arm" {
									attempt.ID = strings.Repeat("e", 64)
									var err error
									fields["pendingReplacementAttach"], err = json.Marshal(attempt)
									require.NoError(t, err)
									payload, err := json.Marshal(fields)
									require.NoError(t, err)
									cm.Data[volume] = string(payload)
									cm.ResourceVersion = "102"
								}
								require.NoError(t, kube.Tracker().Update(gvr, cm.DeepCopy(), cm.Namespace))
								apiOutage.Store(mode != "armed-lost-ack" && mode != "competing-arm")
								return true, nil, errors.New("committed phase update reply lost")
							}
						}
						if fields["pendingReplacementAttach"] == nil {
							return false, nil, nil
						}
						if mode == "write-conflict" {
							return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("competing update"))
						}
						if mode == "lost-write-reply" && pendingWritten.CompareAndSwap(false, true) {
							var attempt replacementAttachAttempt
							require.NoError(t, json.Unmarshal(fields["pendingReplacementAttach"], &attempt))
							lostWriteToken = attempt.ID
							require.NoError(t, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.DeepCopy(), cm.Namespace))
							return true, nil, errors.New("admission update reply lost")
						}
						pendingWritten.Store(true)
						return false, nil, nil
					})
					kube.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
						if action.(ktesting.GetAction).GetName() == volumeHistoryStateConfigMapName &&
							(mode == "readback-failed" && pendingWritten.Load() && readbackFailed.CompareAndSwap(false, true) || mode == "readback-unproven" && pendingWritten.Load() && !readbackFailed.Load() || apiOutage.Load()) {
							return true, nil, errors.New("independent history read unavailable")
						}
						return false, nil, nil
					})
				}
				reply, err := client.ControllerPublishVolume(ctx, publish)
				if mode == "distinct-vm" {
					require.NoError(t, err)
					require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
					current, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
					require.NoError(t, err)
					require.Equal(t, "replacement", current.LastSuccessfulNodeUID)
					require.Equal(t, 42, current.LastSuccessfulOpenNebulaVMID)
					require.Nil(t, current.PendingReplacementAttach)
					wire.mu.Lock()
					defer wire.mu.Unlock()
					require.Equal(t, []string{"attach:42:false"}, wire.mutations)
					require.Equal(t, 1, strings.Count(wire.attachDiskTemplate, "CSI_ATTACH_ATTEMPT="))
					require.Contains(t, wire.attachDiskTemplate, `SERIAL="onecsi-7"`)
					return
				}
				if mode == "pre-dispatch" {
					require.Error(t, err)
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
				}
				require.Nil(t, reply)
				if interruptedAdmission || mode == "readback-unproven" {
					wire.mu.Lock()
					require.Zero(t, wire.attachAttempts)
					require.Empty(t, wire.mutations)
					wire.mu.Unlock()
					apiOutage.Store(false)
					readbackFailed.Store(true)
					server := NewControllerServer(driver, provider, nil)
					stored, err := server.readAttachmentHistory(ctx, volume)
					require.NoError(t, err)
					if mode == "armed-lost-ack" {
						require.Nil(t, stored.PendingReplacementAttach)
					} else {
						require.NotNil(t, stored.PendingReplacementAttach)
						require.Equal(t, !armError, stored.PendingReplacementAttach.prepared())
					}
					if armError {
						require.NotEmpty(t, preparedToken)
						require.NotEmpty(t, proposedArmToken)
						require.NotEqual(t, preparedToken, proposedArmToken)
					}
					driver.volumeHistory = NewVolumeHistoryManager(driver.kubeRuntime, driver.volumeHistory.namespace)
					restarted := makeClient(driver)
					if abandonedPreparation {
						require.NoError(t, kube.CoreV1().Pods(pvc.Namespace).Delete(ctx, consumer.Name, metav1.DeleteOptions{}))
						if mode == "prepared-abandoned-quarantine" {
							_, active, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: volume, Reason: "metadata drift"}, 1, time.Hour)
							require.NoError(t, err)
							require.True(t, active)
						}
						if mode == "prepared-abandoned-conflict" {
							kube.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
								cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
								if cm.Name != volumeHistoryStateConfigMapName {
									return false, nil, nil
								}
								return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("competing phase update"))
							})
						}
						if mode == "prepared-abandoned-orphan" {
							r := NewAttachmentReconciler(NewControllerServer(driver, provider, nil))
							r.orphanGrace = 0
							require.NoError(t, r.ReconcileOnce(ctx))
							require.NoError(t, r.ReconcileOnce(ctx))
						} else {
							_, err = restarted.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
						}
						current, historyErr := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, historyErr)
						if mode == "prepared-abandoned" || mode == "prepared-abandoned-orphan" {
							require.NoError(t, err)
							require.Nil(t, current.PendingReplacementAttach)
						} else {
							require.Error(t, err)
							require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
						}
						require.Equal(t, stored.LastSuccessfulNodeUID, current.LastSuccessfulNodeUID)
						require.True(t, current.LastSafeDetachTime.IsZero())
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Zero(t, wire.attachAttempts)
						require.Empty(t, wire.mutations)
						return
					}
					if mode == "concurrent-prepared" {
						other := newStickyTestDriver(t)
						other.kubeRuntime = driver.kubeRuntime
						other.volumeHistory = NewVolumeHistoryManager(other.kubeRuntime, driver.volumeHistory.namespace)
						parallel := makeClient(other)
						start := make(chan struct{})
						done := make(chan error, 2)
						for _, contender := range []csi.ControllerClient{restarted, parallel} {
							go func() {
								<-start
								_, err := contender.ControllerPublishVolume(ctx, publish)
								done <- err
							}()
						}
						close(start)
						first, second := <-done, <-done
						require.True(t, first == nil || second == nil, "competing publications failed: %v and %v", first, second)
					}
					retried, err := restarted.ControllerPublishVolume(ctx, publish)
					wire.mu.Lock()
					defer wire.mu.Unlock()
					if mode == "armed-outage" || mode == "competing-arm" {
						require.Error(t, err)
						current, err := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
						if mode == "competing-arm" {
							require.Equal(t, strings.Repeat("e", 64), current.PendingReplacementAttach.ID)
							require.NotEqual(t, proposedArmToken, current.PendingReplacementAttach.ID)
						}
						require.Zero(t, wire.attachAttempts)
						require.Empty(t, wire.mutations)
					} else {
						require.NoError(t, err)
						require.Equal(t, "onecsi-7", retried.GetPublishContext()[publishContextDeviceSerial])
						require.Equal(t, 1, wire.attachAttempts)
						require.Equal(t, []string{"attach:41:false"}, wire.mutations)
						require.NotEqual(t, preparedToken, wire.attachAttempt)
						if mode == "armed-lost-ack" {
							require.NotEqual(t, proposedArmToken, wire.attachAttempt)
						} else if interruptedAdmission {
							require.Equal(t, proposedArmToken, wire.attachAttempt)
						}
					}
					return
				}
				if mode == "write-conflict" || mode == "external-disk" ||
					mode == "terminating-old-consumer" || mode == "ambiguous-old-consumer" || mode == "distinct-vm-still-owned" || mode == "distinct-vm-unreadable-original" || mode == "other-version" {
					wire.mu.Lock()
					defer wire.mu.Unlock()
					require.Zero(t, wire.attachAttempts)
					require.Empty(t, wire.mutations)
					return
				}
				stored, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, string(node.UID), stored.LastSuccessfulNodeUID)
				if mode == "readback-failed" || mode == "lost-write-reply" || mode == "final-refusal" || mode == "pre-dispatch" {
					require.Nil(t, stored.PendingReplacementAttach)
					failBeforeDispatch.Store(false)
					wire.mu.Lock()
					attempts := wire.attachAttempts
					require.Empty(t, wire.mutations)
					wire.loseAttachReply, wire.failMethod = false, ""
					wire.mu.Unlock()
					if mode == "final-refusal" {
						require.Equal(t, 1, attempts)
					} else {
						require.Zero(t, attempts)
					}
					retried, err := client.ControllerPublishVolume(ctx, publish)
					require.NoError(t, err)
					require.Equal(t, "onecsi-7", retried.GetPublishContext()[publishContextDeviceSerial])
					wire.mu.Lock()
					defer wire.mu.Unlock()
					require.Equal(t, attempts+1, wire.attachAttempts)
					require.Equal(t, []string{"attach:41:false"}, wire.mutations)
					if mode == "lost-write-reply" {
						require.NotEmpty(t, lostWriteToken)
						require.NotEqual(t, lostWriteToken, wire.attachAttempt)
					}
					return
				}
				require.NotNil(t, stored.PendingReplacementAttach)
				require.Equal(t, 41, stored.LastSuccessfulOpenNebulaVMID)
				require.Equal(t, vmID, stored.PendingReplacementAttach.VMID)
				if vmID == 42 {
					require.Equal(t, 41, stored.PendingReplacementAttach.OriginalVMID)
					if mode == "distinct-vm-original-gone-at-admission" {
						require.Empty(t, stored.PendingReplacementAttach.OriginalVMName)
					} else {
						require.Equal(t, "retired-vm", stored.PendingReplacementAttach.OriginalVMName)
					}
				}
				wire.mu.Lock()
				switch mode {
				case "foreign-marker", "distinct-vm-marker-mismatch", "distinct-vm-abandoned-wrong-marker":
					wire.attachAttempt = strings.Repeat("f", 64)
				case "distinct-vm-original-gone-before-retry":
					wire.omitOriginalVM, wire.missingOriginalVM = true, true
				case "distinct-vm-original-changed":
					wire.vmNames[41] = "unverified-vm"
				case "distinct-vm-destination-changed":
					wire.vmNames[42], wire.vmNames[43] = "unverified-destination", node.Name
				case "duplicate-marker":
					wire.directDiskFields = "<CSI_ATTACH_ATTEMPT>" + wire.attachAttempt + "</CSI_ATTACH_ATTEMPT>"
				case "ambiguous-target":
					wire.extraDiskByVM = map[int]string{41: "<DISK><IMAGE_ID>9</IMAGE_ID><DISK_ID>3</DISK_ID><TARGET>vdb</TARGET></DISK>"}
				case "malformed-target":
					wire.diskTarget = "../vdb"
				}
				wire.mu.Unlock()
				switch mode {
				case "node-changed", "distinct-vm-node-changed":
					require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
					replacement.UID = "third-node"
					_, err = nodes.Create(ctx, replacement, metav1.CreateOptions{})
				case "pv-changed":
					require.NoError(t, kube.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{}))
					pv.UID = "replacement-pv"
					_, err = kube.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
				case "history-changed":
					_, err = driver.volumeHistory.Upsert(ctx, volume, func(history *VolumeHistoryRecord) {
						history.LastSuccessfulDiskID = new(9)
					})
				case "safe-detach-updated":
					NewControllerServer(driver, provider, nil).recordSafeDetach(ctx, volume, "prior-node")
				case "phase-less":
					stored, err = driver.volumeHistory.Upsert(ctx, volume, func(history *VolumeHistoryRecord) {
						attempt := *history.PendingReplacementAttach
						attempt.Version, attempt.Phase = 1, ""
						history.PendingReplacementAttach = &attempt
					})
				}
				require.NoError(t, err)
				driver.volumeHistory = NewVolumeHistoryManager(driver.kubeRuntime, driver.volumeHistory.namespace)
				restarted := makeClient(driver)
				deletingVA := func() *storagev1.VolumeAttachment {
					deleting := metav1.Now()
					return &storagev1.VolumeAttachment{
						ObjectMeta: metav1.ObjectMeta{Name: "released-va", UID: "released-va", ResourceVersion: "101", DeletionTimestamp: &deleting, Finalizers: []string{"external-attacher/" + driver.name}},
						Spec:       storagev1.VolumeAttachmentSpec{Attacher: driver.name, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
						Status:     storagev1.VolumeAttachmentStatus{Attached: true},
					}
				}
				if mode == "distinct-vm-inflight" {
					require.NoError(t, kube.CoreV1().Pods(pvc.Namespace).Delete(ctx, consumer.Name, metav1.DeleteOptions{}))
					_, err = restarted.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
					require.Equal(t, codes.Unavailable, status.Code(err))
					_, err = restarted.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volume})
					require.Equal(t, codes.Unavailable, status.Code(err))
					r := NewAttachmentReconciler(NewControllerServer(driver, provider, nil))
					r.orphanGrace, r.staleVAGrace = 0, 0
					require.NoError(t, r.ReconcileOnce(ctx))
					require.NoError(t, r.ReconcileOnce(ctx))
					va := deletingVA()
					_, err := kube.StorageV1().VolumeAttachments().Create(ctx, va, metav1.CreateOptions{})
					require.NoError(t, err)
					require.NoError(t, r.ReconcileOnce(ctx))
					require.NoError(t, r.ReconcileOnce(ctx))
					retained, err := kube.StorageV1().VolumeAttachments().Get(ctx, va.Name, metav1.GetOptions{})
					require.NoError(t, err)
					require.Equal(t, va.UID, retained.UID)
					current, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
					require.NoError(t, err)
					require.Equal(t, stored, current)
					wire.mu.Lock()
					defer wire.mu.Unlock()
					require.Equal(t, 1, wire.attachAttempts)
					require.Empty(t, wire.mutations)
					return
				}

				if cleanup {
					switch mode {
					case "distinct-vm-abandoned-terminating":
						terminating, err := kube.CoreV1().Pods(pvc.Namespace).Get(ctx, consumer.Name, metav1.GetOptions{})
						require.NoError(t, err)
						now := metav1.Now()
						terminating.DeletionTimestamp = &now
						_, err = kube.CoreV1().Pods(pvc.Namespace).Update(ctx, terminating, metav1.UpdateOptions{})
						require.NoError(t, err)
					case "distinct-vm-abandoned-consumer":
					default:
						require.NoError(t, kube.CoreV1().Pods(pvc.Namespace).Delete(ctx, consumer.Name, metav1.DeleteOptions{}))
					}
					if strings.HasSuffix(mode, "-released") || strings.HasSuffix(mode, "-rebound") {
						require.NoError(t, kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
						pv.Status.Phase = corev1.VolumeReleased
						_, err := kube.CoreV1().PersistentVolumes().UpdateStatus(ctx, pv, metav1.UpdateOptions{})
						require.NoError(t, err)
						if strings.HasSuffix(mode, "-rebound") {
							newClaim := pvc.DeepCopy()
							newClaim.UID, newClaim.Spec.VolumeName = "new-claim", ""
							_, err := kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, newClaim, metav1.CreateOptions{})
							require.NoError(t, err)
						}
					}
					var replayVA *storagev1.VolumeAttachment
					if replay {
						replayVA = deletingVA()
						_, err := kube.StorageV1().VolumeAttachments().Create(ctx, replayVA, metav1.CreateOptions{})
						require.NoError(t, err)
					}
					var lostCompletionReply atomic.Bool
					if mode == "distinct-vm-abandoned-completion-lost-ack" {
						kube.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
							cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
							if cm.Name != volumeHistoryStateConfigMapName || lostCompletionReply.Load() {
								return false, nil, nil
							}
							var current VolumeHistoryRecord
							require.NoError(t, json.Unmarshal([]byte(cm.Data[volume]), &current))
							if current.PendingReplacementAttach == nil || !current.PendingReplacementAttach.detached() {
								return false, nil, nil
							}
							require.NoError(t, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.DeepCopy(), cm.Namespace))
							lostCompletionReply.Store(true)
							return true, nil, errors.New("completion update committed and reply lost")
						})
					}
					if mode == "distinct-vm-abandoned-detach-reply" {
						wire.mu.Lock()
						wire.loseDetachReply = true
						wire.mu.Unlock()
					}
					if mode == "distinct-vm-abandoned-retire-conflict" {
						rejected := false
						kube.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
							cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
							if cm.Name != volumeHistoryStateConfigMapName || rejected {
								return false, nil, nil
							}
							var current VolumeHistoryRecord
							require.NoError(t, json.Unmarshal([]byte(cm.Data[volume]), &current))
							if current.PendingReplacementAttach != nil {
								return false, nil, nil
							}
							rejected = true
							return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("retirement resource version changed"))
						})
					}
					abandoned.Store(true)
					reconcile := func() {
						r := NewAttachmentReconciler(NewControllerServer(driver, provider, nil))
						r.orphanGrace, r.staleVAGrace = 0, 0
						require.NoError(t, r.ReconcileOnce(ctx))
						require.NoError(t, r.ReconcileOnce(ctx))
					}
					unpublish := func(client csi.ControllerClient) error {
						_, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
						return err
					}
					if orphan {
						reconcile()
					} else {
						err = unpublish(restarted)
					}
					if replay {
						require.NoError(t, err)
						if mode == "distinct-vm-abandoned-completion-lost-ack" {
							require.True(t, lostCompletionReply.Load())
						}
						s := NewControllerServer(driver, provider, nil)
						confirmed, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored.LastSuccessfulNodeUID, confirmed.LastSuccessfulNodeUID)
						require.Equal(t, stored.LastSuccessfulOpenNebulaVMID, confirmed.LastSuccessfulOpenNebulaVMID)
						require.Equal(t, stored.LastSuccessfulPublishTime, confirmed.LastSuccessfulPublishTime)
						require.NotNil(t, confirmed.PendingReplacementAttach)
						require.True(t, confirmed.PendingReplacementAttach.detached())
						require.Equal(t, string(replayVA.UID), confirmed.PendingReplacementAttach.DetachCompletion.VAUID)
						reconcile()
						stillDeleting, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, confirmed.PendingReplacementAttach, stillDeleting.PendingReplacementAttach)
						fresh := newStickyTestDriver(t)
						fresh.kubeRuntime.client = kube
						retry := makeClient(fresh)
						require.NoError(t, unpublish(retry))
						if mode == "distinct-vm-abandoned-replay-protection" {
							_, active, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: volume, Reason: "metadata drift"}, 1, time.Hour)
							require.NoError(t, err)
							require.True(t, active)
							require.Error(t, unpublish(retry))
						}
						vas := kube.StorageV1().VolumeAttachments()
						require.NoError(t, vas.Delete(ctx, replayVA.Name, metav1.DeleteOptions{}))
						if mode == "distinct-vm-abandoned-replay-replaced-va" {
							newVA := replayVA.DeepCopy()
							newVA.UID = "replacement-va"
							_, err = vas.Create(ctx, newVA, metav1.CreateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(retry))
						}
						reconcile()
						current, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Nil(t, current.PendingReplacementAttach)
						if mode == "distinct-vm-abandoned-replay-replaced-va" {
							replacement, err := vas.Get(ctx, replayVA.Name, metav1.GetOptions{})
							require.NoError(t, err)
							require.Equal(t, "replacement-va", string(replacement.UID))
						}
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, 1, wire.attachAttempts)
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID)}, wire.mutations)
						require.Contains(t, wire.attachDiskTemplate, `SERIAL="onecsi-7"`)
						require.Contains(t, wire.attachDiskTemplate, `CSI_ATTACH_ATTEMPT="`+stored.PendingReplacementAttach.ID+`"`)
						return
					}
					if interruptedCleanup {
						require.Error(t, err)
						current, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
						require.True(t, current.LastSafeDetachTime.IsZero())
						abandoned.Store(false)
						wire.mu.Lock()
						wire.failMethod = ""
						if mode == "distinct-vm-abandoned-no-disk-original-owner" {
							wire.disks[41] = 128
						}
						wire.mu.Unlock()
						driver.volumeHistory = NewVolumeHistoryManager(driver.kubeRuntime, driver.volumeHistory.namespace)
					}
					switch mode {
					case "distinct-vm-abandoned-no-disk-orphan":
						reconcile()
					case "distinct-vm-abandoned-no-disk-stale-va":
						deleting := metav1.Now()
						va := &storagev1.VolumeAttachment{
							ObjectMeta: metav1.ObjectMeta{Name: "released-va", UID: "released-va", ResourceVersion: "101", DeletionTimestamp: &deleting, Finalizers: []string{"external-attacher/" + driver.name}},
							Spec:       storagev1.VolumeAttachmentSpec{Attacher: driver.name, NodeName: node.Name, Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
							Status:     storagev1.VolumeAttachmentStatus{Attached: true},
						}
						_, err := kube.StorageV1().VolumeAttachments().Create(ctx, va, metav1.CreateOptions{})
						require.NoError(t, err)
						reconcile()
						stillPending, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored.PendingReplacementAttach, stillPending.PendingReplacementAttach)
					}
					if interruptedCleanup && mode != "distinct-vm-abandoned-no-disk-orphan" {
						err = unpublish(makeClient(driver))
					}

					current, historyErr := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
					require.NoError(t, historyErr)
					require.Equal(t, stored.LastSuccessfulPublishTime, current.LastSuccessfulPublishTime)
					require.Equal(t, string(node.UID), current.LastSuccessfulNodeUID)
					require.Equal(t, 41, current.LastSuccessfulOpenNebulaVMID)
					wire.mu.Lock()
					defer wire.mu.Unlock()
					require.Equal(t, 1, wire.attachAttempts)
					switch mode {
					case "distinct-vm-abandoned", "distinct-vm-orphan", "distinct-vm-abandoned-released", "distinct-vm-orphan-released":
						require.NoError(t, err)
						require.Equal(t, []string{"attach:42:false", "detach:42"}, wire.mutations)
						require.Nil(t, current.PendingReplacementAttach)
					case "distinct-vm-abandoned-readback", "distinct-vm-abandoned-no-disk-orphan", "distinct-vm-abandoned-no-disk-stale-va",
						"distinct-vm-abandoned-no-disk-original-owner", "distinct-vm-abandoned-detach-reply", "distinct-vm-abandoned-retire-conflict":
						require.Error(t, err)
						require.Equal(t, []string{"attach:42:false", "detach:42"}, wire.mutations)
						require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
					default:
						require.Error(t, err)
						require.Equal(t, []string{"attach:42:false"}, wire.mutations)
						require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
					}
					return
				}
				reply, err = restarted.ControllerPublishVolume(ctx, publish)
				current, historyErr := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
				require.NoError(t, historyErr)
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Equal(t, 1, wire.attachAttempts)
				if mode == "unknown" {
					require.Empty(t, wire.mutations)
				} else {
					require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID)}, wire.mutations)
					require.Contains(t, wire.attachDiskTemplate, `SERIAL="onecsi-7"`)
					require.Equal(t, 1, strings.Count(wire.attachDiskTemplate, "CSI_ATTACH_ATTEMPT="))
				}
				if mode == "lost-reply" || mode == "distinct-vm-lost-reply" || mode == "safe-detach-updated" ||
					mode == "distinct-vm-original-gone-at-admission" || mode == "distinct-vm-original-gone-before-retry" {
					require.NoError(t, err)
					require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
					require.Equal(t, "replacement", current.LastSuccessfulNodeUID)
					require.Equal(t, vmID, current.LastSuccessfulOpenNebulaVMID)
					require.Nil(t, current.PendingReplacementAttach)
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
					require.Nil(t, reply)
					require.Equal(t, string(node.UID), current.LastSuccessfulNodeUID)
					require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
				}
			})
		}
	}
}

func TestControllerPublishRechecksQuarantineForActualAttach(t *testing.T) {
	for _, mode := range []string{"during-inspection", "during-retry", "retry-read-failed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			node := newReadyNode("node-old", true)
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, node)
			manager := driver.hostArtifactQuarantine
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			now := time.Now().UTC()
			persist := func(vmid, slot int) {
				state := hostArtifactStateFromConflict("other-volume", &opennebula.HostArtifactConflictError{
					Target:     opennebula.HostArtifactAttachmentTarget{VMID: vmid, DiskID: slot},
					RawMessage: "private provider details",
				})
				state.FailureCount, state.FirstObservedAt, state.LastObservedAt, state.ExpiresAt = 1, now, now, now.Add(time.Hour)
				payload, err := json.Marshal(state)
				require.NoError(t, err)
				require.NoError(t, driver.kubeRuntime.UpsertConfigMapData(ctx, manager.namespace, hostArtifactStateConfigMapName, map[string]string{state.Key: string(payload)}))
			}
			persist(98, 1)
			var later atomic.Bool
			var wire *nodeAttachmentProtocol
			provider, state := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				wire.mu.Lock()
				attempts := wire.attachAttempts
				wire.mu.Unlock()
				if method == "one.datastorepool.info" && mode == "during-inspection" && later.CompareAndSwap(false, true) {
					persist(41, 1)
				}
				if method == "one.vmpool.info" && attempts == 1 && mode != "during-inspection" && !later.Load() {
					persist(41, 1)
					later.Store(true)
				}
			})
			wire = state
			wire.mu.Lock()
			delete(wire.disks, 41)
			wire.marker = 128 * 1024 * 1024
			wire.rejectFirstAttach = mode != "during-inspection"
			wire.extraDiskByVM = map[int]string{41: "<DISK><DISK_ID>0</DISK_ID><TYPE>SWAP</TYPE></DISK>"}
			wire.alterDatastorePool = func(body string) string {
				return strings.Replace(body, "<TYPE>SYSTEM</TYPE><TM_MAD>ssh</TM_MAD>", "<TYPE>SYSTEM</TYPE><TM_MAD>fs_lvm_ssh</TM_MAD>", 1)
			}
			wire.mu.Unlock()
			if mode == "retry-read-failed" {
				kube.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
					if action.(ktesting.GetAction).GetName() == hostArtifactStateConfigMapName && later.Load() {
						return true, nil, errors.New("fresh quarantine observation unavailable")
					}
					return false, nil, nil
				})
			}

			reply, err := reviewControllerClient(t, NewControllerServer(driver, provider, nil)).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			require.Nil(t, reply)
			require.True(t, later.Load())
			if mode == "retry-read-failed" {
				require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
			} else {
				require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
				require.ErrorContains(t, err, hostArtifactSlotBlocked)
			}
			require.NotContains(t, err.Error(), "private")
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Empty(t, wire.mutations)
			if mode == "during-inspection" {
				require.Zero(t, wire.attachAttempts)
			} else {
				require.Equal(t, 1, wire.attachAttempts)
			}
		})
	}
}

func TestControllerPublishRequiresUnambiguousPlacement(t *testing.T) {
	for _, mode := range []string{"zero-system-datastore", "missing-datastore", "duplicate-datastore", "duplicate-history", "duplicate-latest-sequence", "duplicate-pool-name", "duplicate-secret"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			node := newReadyNode("node-old", true)
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			driver := newStickyTestDriver(t, pv, pvc, node)
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.ceph, wire.version, wire.marker = true, "7.4.0", 128*1024*1024
			delete(wire.disks, 41)
			wire.alterDirectVM = func(body string) string {
				switch mode {
				case "zero-system-datastore":
					return strings.Replace(body, "<DS_ID>200</DS_ID>", "<DS_ID>0</DS_ID>", 1)
				case "missing-datastore":
					return strings.Replace(body, "<DS_ID>200</DS_ID>", "", 1)
				case "duplicate-datastore":
					return strings.Replace(body, "<DS_ID>200</DS_ID>", "<DS_ID>200</DS_ID><DS_ID>0</DS_ID>", 1)
				case "duplicate-history":
					return strings.Replace(body, "</VM>", "<HISTORY_RECORDS><HISTORY><SEQ>1</SEQ><DS_ID>0</DS_ID></HISTORY></HISTORY_RECORDS></VM>", 1)
				case "duplicate-latest-sequence":
					return strings.Replace(body, "</HISTORY_RECORDS>", "<HISTORY><SEQ>0</SEQ><DS_ID>0</DS_ID></HISTORY></HISTORY_RECORDS>", 1)
				default:
					return body
				}
			}
			wire.alterDatastorePool = func(body string) string {
				switch mode {
				case "zero-system-datastore", "missing-datastore", "duplicate-datastore", "duplicate-history", "duplicate-latest-sequence":
					return strings.NewReplacer("<ID>200</ID>", "<ID>0</ID>", "<COMPATIBLE_SYS_DS>200", "<COMPATIBLE_SYS_DS>0").Replace(body)
				case "duplicate-pool-name":
					return strings.Replace(body, "<POOL_NAME>fixture</POOL_NAME>", "<POOL_NAME>other</POOL_NAME><POOL_NAME>fixture</POOL_NAME>", 1)
				default:
					return strings.Replace(body, "<CEPH_SECRET>fixture</CEPH_SECRET>", "<CEPH_SECRET>private-credential</CEPH_SECRET><CEPH_SECRET>fixture</CEPH_SECRET>", 1)
				}
			}
			wire.mu.Unlock()

			reply, err := reviewControllerClient(t, NewControllerServer(driver, provider, nil)).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if mode == "zero-system-datastore" {
				require.NoError(t, err)
				require.NotNil(t, reply)
				require.Equal(t, []string{"attach:41:false"}, wire.mutations)
			} else {
				require.Error(t, err)
				require.Nil(t, reply)
				require.NotContains(t, err.Error(), "private-credential")
				require.Zero(t, wire.attachAttempts)
				require.Empty(t, wire.mutations)
			}
		})
	}
}

func TestControllerRequiresExplicitLiveImageAbsenceAuthority(t *testing.T) {
	for _, operation := range []string{"unpublish", "delete"} {
		for _, image := range []struct{ name, original, replacement string }{
			{name: "explicit-empty"},
			{"missing-count", "<RUNNING_VMS>0</RUNNING_VMS>", ""},
			{"missing-owners", "<VMS></VMS>", ""},
			{"missing-cloning", "<CLONING_OPS>0</CLONING_OPS>", ""},
			{"empty-count", "<RUNNING_VMS>0</RUNNING_VMS>", "<RUNNING_VMS/>"},
			{"malformed-owner", "<VMS></VMS>", "<VMS><ID/></VMS>"},
			{"inconsistent-count", "<RUNNING_VMS>0</RUNNING_VMS>", "<RUNNING_VMS>1</RUNNING_VMS>"},
			{"ineligible-state", "<STATE>1</STATE>", "<STATE>6</STATE>"},
		} {
			t.Run(operation+"/"+image.name, func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				node := newReadyNode("node-old", true)
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
				pv.ResourceVersion = "1"
				driver := newStickyTestDriver(t, pv, pvc, node)
				provider, wire := nodeAttachmentProvider(t)
				wire.mu.Lock()
				delete(wire.disks, 41)
				wire.allowImageDelete = operation == "delete" && image.original == ""
				if image.original != "" {
					wire.alterDirectImage = func(body string) string {
						require.Contains(t, body, image.original)
						return strings.Replace(body, image.original, image.replacement, 1)
					}
				}
				wire.mu.Unlock()
				client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
				var err error
				if operation == "delete" {
					_, err = client.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volume})
				} else {
					_, err = client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if image.original == "" {
					require.NoError(t, err)
					if operation == "delete" {
						require.Equal(t, []string{"image-delete:7"}, wire.mutations)
					} else {
						require.Empty(t, wire.mutations)
					}
				} else {
					require.Error(t, err)
					require.Empty(t, wire.mutations)
				}
			})
		}
	}
}

func TestControllerPublishChecksNodeAfterActualSlotAuthorization(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			node := newReadyNode("node-old", true)
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, node)
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			var inspecting, replaced atomic.Bool
			provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
				if method == "one.datastorepool.info" {
					inspecting.Store(true)
				}
			})
			wire.mu.Lock()
			delete(wire.disks, 41)
			wire.marker = 128 * 1024 * 1024
			wire.rejectFirstAttach = retry
			wire.extraDiskByVM = map[int]string{41: "<DISK><DISK_ID>0</DISK_ID><TYPE>SWAP</TYPE></DISK>"}
			wire.alterDatastorePool = func(body string) string {
				return strings.Replace(body, "<TYPE>SYSTEM</TYPE><TM_MAD>ssh</TM_MAD>", "<TYPE>SYSTEM</TYPE><TM_MAD>fs_lvm_ssh</TM_MAD>", 1)
			}
			wire.mu.Unlock()
			kube.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
				wire.mu.Lock()
				attempts := wire.attachAttempts
				wire.mu.Unlock()
				if action.(ktesting.GetAction).GetName() == hostArtifactStateConfigMapName &&
					inspecting.Load() && (!retry || attempts == 1) && replaced.CompareAndSwap(false, true) {
					replacement := node.DeepCopy()
					replacement.UID = "replacement"
					require.NoError(t, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), replacement, ""))
				}
				return false, nil, nil
			})

			reply, err := reviewControllerClient(t, NewControllerServer(driver, provider, nil)).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
			require.ErrorContains(t, err, "publish Kubernetes Node identity changed")
			require.Nil(t, reply)
			require.True(t, replaced.Load())
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Empty(t, wire.mutations)
			if retry {
				require.Equal(t, 1, wire.attachAttempts)
			} else {
				require.Zero(t, wire.attachAttempts)
			}
		})
	}
}
