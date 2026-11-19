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
	"regexp"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	mount "k8s.io/mount-utils"
)

type nodeAttachmentProtocol struct {
	endpoint            string
	hideDiskInfo        bool
	rejectFirstDetach   bool
	rejectFirstAttach   bool
	attachRefusal       string
	abortFirstAttach    bool
	alterDirectImage    func(string) string
	loseAttachReply     bool
	loseDetachReply     bool
	loseDeleteReply     bool
	detachReply         string
	attachFields        map[string]string
	attachedDisks       map[int]map[string]string
	expectedAttachNonce func(context.Context) string
	readOnlyAttach      bool
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
	extendedPoolExtraVM string
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
	omitVMID            int
	missingVMID         int
	replacementDisk     bool
	imageDatastoreID    int
	imageSizeMiB        int
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

func decodeAttachDiskTemplate(t *testing.T, raw string) map[string]string {
	t.Helper()
	body, valid := strings.CutPrefix(strings.TrimSpace(raw), "DISK=[")
	require.True(t, valid, "XML-RPC attach must send one DISK vector")
	body, valid = strings.CutSuffix(body, "]")
	require.True(t, valid, "XML-RPC DISK vector is unterminated")
	lines := strings.Split(strings.TrimSpace(body), "\n")
	fields := make(map[string]string, len(lines))
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if i < len(lines)-1 {
			line, valid = strings.CutSuffix(line, ",")
			require.True(t, valid, "DISK assignments must be comma-separated")
		}
		name, quoted, valid := strings.Cut(line, "=")
		require.True(t, valid, "DISK contains an invalid assignment")
		name = strings.ToUpper(strings.TrimSpace(name))
		require.NotEmpty(t, name)
		require.NotContains(t, fields, name, "duplicate DISK field")
		require.True(t, strings.HasPrefix(strings.TrimSpace(quoted), `"`))
		value, err := strconv.Unquote(strings.TrimSpace(quoted))
		require.NoError(t, err)
		fields[name] = value
	}
	return fields
}

func requireAttachDiskIdentity(t *testing.T, fields map[string]string, image int, nonce string, readOnly bool) {
	t.Helper()
	require.Equal(t, strconv.Itoa(image), fields["IMAGE_ID"])
	require.Equal(t, fmt.Sprintf("onecsi-%d", image), fields["SERIAL"])
	if nonce == "" {
		require.NotContains(t, fields, "CSI_ATTACH_ATTEMPT")
	} else {
		require.Equal(t, nonce, fields["CSI_ATTACH_ATTEMPT"])
	}
	if readOnly {
		require.Equal(t, "YES", fields["READONLY"])
	} else {
		require.NotContains(t, fields, "READONLY")
	}
}

var shortVMDiskXML = regexp.MustCompile(`(?s)<DISK>.*?</DISK>`)
var shortVMDiskFields = regexp.MustCompile(`(?s)<(?:DISK_ID|DATASTORE|DATASTORE_ID|IMAGE|IMAGE_ID|SIZE|TYPE|CLONE|CLONE_TARGET|LN_TARGET|DISK_SNAPSHOT_TOTAL_SIZE)(?:/>|>[^<]*</[^>]+>)`)

// This fixture models durable shareable storage, not ephemeral cloned disks.
// Image.cc emits PERSISTENT=YES/CLONE=NO/SAVE=YES and SHAREABLE=YES for a
// persistent raw image with PERSISTENT_TYPE=SHAREABLE. The former PERSISTENT=0
// contradicted both that intent and its USED_PERS state.
func nodeAttachmentProvider(t *testing.T, observe ...func(string, context.Context)) (*opennebula.PersistentDiskVolumeProvider, *nodeAttachmentProtocol) {
	t.Helper()
	state := &nodeAttachmentProtocol{disks: map[int]int{41: 256}, attachedDisks: make(map[int]map[string]string), marker: 256 * 1024 * 1024}
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
		var attachNonce string
		state.mu.Lock()
		if call.Method == "one.vm.attach" && state.expectedAttachNonce != nil {
			expected := state.expectedAttachNonce
			state.mu.Unlock()
			attachNonce = expected(r.Context())
			state.mu.Lock()
		}
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
				diskImage, serial, target := strconv.Itoa(imageID), fmt.Sprintf("onecsi-%d", imageID), "vdb"
				if fields := state.attachedDisks[id]; fields != nil {
					diskImage, serial = fields["IMAGE_ID"], fields["SERIAL"]
					target = firstNonEmpty(fields["DEV_PREFIX"], "vd") + "b"
				}
				disk = fmt.Sprintf(`<DISK><PERSISTENT>YES</PERSISTENT><CLONE>NO</CLONE><SAVE>YES</SAVE><SHAREABLE>YES</SHAREABLE><LN_TARGET>SYSTEM</LN_TARGET><TM_MAD>ssh</TM_MAD><TM_MAD_SYSTEM>ssh</TM_MAD_SYSTEM><IMAGE_ID>%s</IMAGE_ID><DISK_ID>%s</DISK_ID><TARGET>%s</TARGET><SIZE>%d</SIZE><SERIAL>%s</SERIAL>%s</DISK>`, diskImage, slot, firstNonEmpty(state.diskTarget, target), size, serial, marker)
				if state.attachedDisks[id]["READONLY"] == "YES" {
					disk = strings.TrimSuffix(disk, "</DISK>") + "<READONLY>YES</READONLY></DISK>"
				}
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
			size := state.imageSizeMiB
			if size == 0 {
				size = 128
			}
			return fmt.Sprintf(`<IMAGE><ID>%d</ID><NAME>%s</NAME><STATE>%d</STATE><SIZE>%d</SIZE><DATASTORE_ID>%d</DATASTORE_ID><SOURCE>/fixture/disk</SOURCE><PERSISTENT>1</PERSISTENT><FORMAT>raw</FORMAT><RUNNING_VMS>%d</RUNNING_VMS><CLONING_OPS>0</CLONING_OPS><VMS>%s</VMS><TEMPLATE><PERSISTENT_TYPE>SHAREABLE</PERSISTENT_TYPE>%s</TEMPLATE></IMAGE>`, imageID, firstNonEmpty(state.imageName, "volume-test"), imageState, size, datastoreID, count, ids, tag)
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
			case "one.vmpool.info", "one.vmpool.infoextended":
				body = "<VM_POOL>"
				if !state.omitOriginalVM && state.omitVMID != 41 {
					body += vmXML(41, false)
				}
				if state.omitVMID != 42 {
					body += vmXML(42, false)
				}
				if _, present := state.disks[43]; present || state.vmNames[43] != "" {
					body += vmXML(43, false)
				}
				if call.Method == "one.vmpool.infoextended" {
					body += state.extendedPoolExtraVM
				}
				body += "</VM_POOL>"
				if call.Method == "one.vmpool.info" {
					body = shortVMDiskXML.ReplaceAllStringFunc(body, func(disk string) string {
						fields := shortVMDiskFields.FindAllString(disk, -1)
						return "<DISK>" + strings.Join(fields, "") + "</DISK>"
					})
				}
			case "one.vm.info":
				body = vmXML(number(1), true)
				if number(1) == state.missingVMID || (number(1) == 41 && state.missingOriginalVM) {
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
				require.Len(t, call.Params, 3)
				fields := decodeAttachDiskTemplate(t, call.Params[2].Value.String)
				requireAttachDiskIdentity(t, fields, imageID, attachNonce, state.readOnlyAttach)
				state.attachFields = fields
				state.attachAttempts++
				if state.abortFirstAttach && state.attachAttempts == 1 {
					panic(http.ErrAbortHandler)
				}
				if state.attachAttempts == 1 && state.attachRefusal != "" {
					switch state.attachRefusal {
					case "authorization-refusal":
						ok, body, code = 0, "User is not authorized to perform MANAGE on VM 41", 0x200
					case "authentication-refusal":
						ok, body, code = 0, "User couldn't be authenticated, aborting call.", 0x100
					case "authentication-text":
						ok, body, code = 0, "AUTHENTICATION: User couldn't be authenticated", 0x800
					case "authorization-text":
						ok, body, code = 0, "AUTHORIZATION: User is not authorized to perform MANAGE on VM 41", 0x800
					case "authorization-malformed":
						_, err := fmt.Fprint(w, `<methodResponse><params><param><value><array><data><value><boolean>0</boolean></value><value><string>Not authorized</string></value><value><string>512</string></value></data></array></value></param></params></methodResponse>`)
						require.NoError(t, err)
						return
					}
					break
				}
				if state.rejectFirstAttach && state.attachAttempts == 1 {
					ok, body, code = 0, "wrong state HOTPLUG", 0x800
					break
				}
				id := number(1)
				if fields["CSI_ATTACH_ATTEMPT"] != "" {
					state.attachAttempt, state.attachAttemptVM = fields["CSI_ATTACH_ATTEMPT"], id
				}
				state.attachedDisks[id] = fields
				state.disks[id] = 128
				state.mutations = append(state.mutations, fmt.Sprintf("attach:%d:%t", id, fields["READONLY"] == "YES"))
				if state.loseAttachReply {
					panic(http.ErrAbortHandler)
				}
				body = strconv.Itoa(id)
			case "one.vm.detach":
				id := number(1)
				require.Equal(t, firstNonEmpty(state.diskSlot, "2"), strconv.Itoa(number(2)))
				state.detachAttempts++
				if state.detachReply == "failed" {
					ok, body, code = 0, "detach refused", 0x800
					break
				}
				if state.rejectFirstDetach && state.detachAttempts == 1 {
					ok, body, code = 0, "wrong state HOTPLUG: provider-private-disk-detail", 0x800
					break
				}
				delete(state.disks, id)
				delete(state.attachedDisks, id)
				if id == state.attachAttemptVM {
					state.attachAttempt = ""
				}
				state.mutations = append(state.mutations, fmt.Sprintf("detach:%d", id))
				if state.loseDetachReply {
					panic(http.ErrAbortHandler)
				}
				body = strconv.Itoa(id)
				if state.detachReply == "wrong-vm" {
					body = strconv.Itoa(id + 1)
				}
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
				if state.loseDeleteReply {
					panic(http.ErrAbortHandler)
				}
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
		bodyTag := "string"
		if call.Method == "one.vm.detach" && ok == 1 && state.detachReply != "string" {
			bodyTag = "int"
		}
		_, err := fmt.Fprintf(w, `<methodResponse><params><param><value><array><data><value><boolean>%d</boolean></value><value><%s>%s</%s></value><value><int>%d</int></value></data></array></value></param></params></methodResponse>`, ok, bodyTag, escaped.String(), bodyTag, code)
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
				state.failMethod = "one.vmpool.infoextended"
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
			state.mu.Lock()
			state.readOnlyAttach = shared
			state.mu.Unlock()
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
					case "one.vmpool.info", "one.vmpool.infoextended":
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
				if method == "one.vmpool.info" || method == "one.vmpool.infoextended" {
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
				state.failMethod = "one.vmpool.infoextended"
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
				if method != "one.vmpool.infoextended" || !censusImageRead.Swap(false) {
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
						state.failMethod = "one.vmpool.infoextended"
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

func replacementPublishVA(pv *corev1.PersistentVolume, node *corev1.Node) *storagev1.VolumeAttachment {
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "publish-va", UID: "publish-va-uid", ResourceVersion: "101",
			CreationTimestamp: metav1.NewTime(node.CreationTimestamp.Add(time.Second))},
		Spec: storagev1.VolumeAttachmentSpec{Attacher: pv.Spec.CSI.Driver, NodeName: node.Name,
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv.Name}},
	}
}

func (wire *nodeAttachmentProtocol) requirePersistedAttachNonce(t *testing.T, driver *Driver, volume string) {
	t.Helper()
	wire.mu.Lock()
	defer wire.mu.Unlock()
	wire.expectedAttachNonce = func(ctx context.Context) string {
		history, err := (&ControllerServer{driver: driver}).readAttachmentHistory(ctx, volume)
		require.NoError(t, err)
		require.NotNil(t, history.PendingReplacementAttach)
		require.True(t, history.PendingReplacementAttach.dispatchPossible())
		return history.PendingReplacementAttach.ID
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
			wire.requirePersistedAttachNonce(t, driver, volume)
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
			consumer.CreationTimestamp = metav1.NewTime(replacement.CreationTimestamp.Add(time.Second))
			setReplacementHistoryVersion(t, driver)
			_, err = driver.kubeRuntime.client.CoreV1().Pods(consumer.Namespace).Create(ctx, consumer, metav1.CreateOptions{})
			require.NoError(t, err)
			_, err = driver.kubeRuntime.client.StorageV1().VolumeAttachments().Create(ctx, replacementPublishVA(pv, replacement), metav1.CreateOptions{})
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
	p.wire.failMethod = "one.vmpool.infoextended"
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
	restored      atomic.Bool
	beforeRestore func()
	beforeSize    func()
}

func (p *publishRestoreProbe) ResolveVolumeSizeBytes(ctx context.Context, volume string) (int64, error) {
	if p.beforeSize != nil {
		p.beforeSize()
	}
	return p.PersistentDiskVolumeProvider.ResolveVolumeSizeBytes(ctx, volume)
}

func (p *publishRestoreProbe) RestoreVolumeCapacity(ctx context.Context, volume string, nodeID, imageID int) error {
	if p.beforeRestore != nil {
		p.beforeRestore()
	}
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
		{false, false, "rebind-then-replace"}, {true, false, "rebind-then-replace"},
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
			var transientPVListFailure atomic.Bool
			publishActive.Store(!tc.stage)
			kube.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, kruntime.Object, error) {
				if transientPVListFailure.Swap(false) {
					return true, nil, errors.New("initial Publish PV list unavailable")
				}
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
				require.Zero(t, failedPublishReads.Load())
				require.Equal(t, pv.Name, published.GetPublishContext()[paramPVName])
				require.Equal(t, pvc.Name, published.GetPublishContext()[paramPVCName])
				require.Equal(t, pvc.Namespace, published.GetPublishContext()[paramPVCNamespace])
			}
			publishedHistory, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, pv.Name, publishedHistory.PVName)
			require.Equal(t, pvc.Name, publishedHistory.PVCName)
			require.Equal(t, pvc.Namespace, publishedHistory.PVCNamespace)
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
			if tc.drift == "rebind-then-replace" {
				pvc.Namespace = "rebound-namespace"
				pv.Spec.ClaimRef.Namespace = pvc.Namespace
			}
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
			pod.Namespace = pvc.Namespace
			pod.UID = "static-consumer-uid"
			pod.Status.Phase = corev1.PodPending
			_, err = kube.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			require.NoError(t, err)
			publish.VolumeContext = pv.Spec.CSI.VolumeAttributes
			if tc.drift == "rebind-then-replace" {
				for _, assertion := range []struct{ key, value string }{
					{publishContextOpenNebulaImageID, "8"},
					{publishContextDeviceSerial, "onecsi-8"},
					{publishContextDeviceSerial, ""},
				} {
					inconsistent := pv.DeepCopy()
					inconsistent.Spec.CSI.VolumeAttributes[assertion.key] = assertion.value
					_, err = kube.CoreV1().PersistentVolumes().Update(ctx, inconsistent, metav1.UpdateOptions{})
					require.NoError(t, err)
					transientPVListFailure.Store(true)
					denied, err := client.ControllerPublishVolume(ctx, publish)
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, denied)
					wire.mu.Lock()
					require.Empty(t, wire.mutations)
					wire.mu.Unlock()
					unverified, err := server.readAttachmentHistory(ctx, volume)
					require.NoError(t, err)
					require.Equal(t, history, unverified)
				}
				_, err = kube.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			rebound, err := client.ControllerPublishVolume(ctx, publish)
			require.NoError(t, err)
			require.Equal(t, pv.Name, rebound.PublishContext[paramPVName])
			require.Equal(t, pvc.Name, rebound.PublishContext[paramPVCName])
			require.Equal(t, pvc.Namespace, rebound.PublishContext[paramPVCNamespace])
			history, err = server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, pv.Name, history.PVName)
			require.Equal(t, pvc.Namespace, history.PVCNamespace)
			require.Equal(t, pvc.Name, history.PVCName)
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
			if tc.drift != "" && tc.drift != "zero-slot" && tc.drift != "legacy-slot-omitted" && tc.drift != "rebind-then-replace" {
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
			if tc.drift == "rebind-then-replace" {
				wire.requirePersistedAttachNonce(t, driver, volume)
				setReplacementHistoryVersion(t, driver)
				require.NoError(t, kube.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{}))
				newNode := node.DeepCopy()
				newNode.UID = "rebound-replacement-node"
				newNode.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
				_, err = kube.CoreV1().Nodes().Create(ctx, newNode, metav1.CreateOptions{})
				require.NoError(t, err)
				pod.UID = "rebound-replacement-pod"
				pod.CreationTimestamp = metav1.Now()
				setPremountKubeletFixtureStatus(pod, "ContainerCreating")
				_, err = kube.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
				require.NoError(t, err)
				_, err = kube.StorageV1().VolumeAttachments().Create(ctx, replacementPublishVA(pv, newNode), metav1.CreateOptions{})
				require.NoError(t, err)
				reply, err := client.ControllerPublishVolume(ctx, publish)
				require.NoError(t, err)
				require.Equal(t, "onecsi-7", reply.PublishContext[publishContextDeviceSerial])
				observed, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, pv.Name, observed.PVName)
				require.Equal(t, pvc.Name, observed.PVCName)
				require.Equal(t, string(newNode.UID), observed.LastSuccessfulNodeUID)
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Equal(t, []string{"detach:41", "attach:41:false"}, wire.mutations)
				return
			}
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Equal(t, []string{"detach:41"}, wire.mutations)
			require.Empty(t, wire.disks)
		})
	}
}

func TestDetachRechecksVAAfterFinalProtectionRead(t *testing.T) {
	for _, tc := range []struct{ queued, replace, observe, mount bool }{
		{false, false, false, false}, {true, false, false, false},
		{false, true, false, false}, {true, true, false, false},
		{false, false, true, false}, {true, false, true, false},
		{false, false, false, true}, {true, false, false, true},
	} {
		t.Run(fmt.Sprintf("queued=%t/replace=%t/observe=%t/mount=%t", tc.queued, tc.replace, tc.observe, tc.mount), func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			if tc.observe {
				pv.Annotations[annotationBackend] = "ceph"
			}
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
			wire.mu.Lock()
			wire.ceph = tc.observe
			wire.mu.Unlock()
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
					if protectionReads.Add(1) == 3 {
						if tc.replace {
							replaceDeletingVolumeAttachment(t, kube, va)
						}
						if tc.mount {
							mounted := node.DeepCopy()
							mounted.Status.VolumesInUse = []corev1.UniqueVolumeName{"kubernetes.io/csi/csi.opennebula.io^volume-test"}
							require.NoError(t, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), mounted, ""))
						}
					}
				}
				return false, nil, nil
			})
			var observeInstalled atomic.Bool
			if tc.observe {
				kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, kruntime.Object, error) {
					if probe.inDetach.Load() && protectionReads.Load() >= 2 && observeInstalled.CompareAndSwap(false, true) {
						observed := pvc.DeepCopy()
						observed.Annotations[annotationRecoveryMode] = recoveryModeObserve
						require.NoError(t, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), observed, pvc.Namespace))
					}
					return false, nil, nil
				})
			}
			client := reviewControllerClient(t, server)
			reply, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
			require.GreaterOrEqual(t, protectionReads.Load(), int32(2))
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if tc.replace || tc.observe || tc.mount {
				require.Equal(t, codes.Unavailable, status.Code(err))
				if tc.observe {
					require.True(t, observeInstalled.Load())
					require.GreaterOrEqual(t, protectionReads.Load(), int32(3))
					require.ErrorContains(t, err, "recovery controls changed")
				} else if tc.mount {
					require.ErrorContains(t, err, "Node identity or release changed")
				} else {
					require.ErrorContains(t, err, "VolumeAttachment deletion authority changed")
				}
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

func setPremountKubeletFixtureStatus(pod *corev1.Pod, reason string) {
	start := pod.CreationTimestamp
	pod.Status.StartTime = &start
	container := pod.Spec.Containers[0]
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: container.Name, Image: container.Image,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
}

func fixtureDetachImageOwner(body string, vmID int) string {
	return strings.NewReplacer(
		"<STATE>1</STATE>", "<STATE>8</STATE>",
		"<RUNNING_VMS>0</RUNNING_VMS>", "<RUNNING_VMS>1</RUNNING_VMS>",
		"<VMS></VMS>", fmt.Sprintf("<VMS><ID>%d</ID></VMS>", vmID),
	).Replace(body)
}

func TestControllerPublishReplacementAttachRetry(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"lost-reply", "foreign-marker", "duplicate-marker", "ambiguous-target", "malformed-target", "external-disk", "node-changed", "pv-changed", "history-changed", "safe-detach-updated", "unknown", "final-refusal", "authorization-refusal", "authorization-text", "authorization-malformed", "authentication-refusal", "authentication-text", "write-conflict", "lost-write-reply", "readback-failed", "readback-unproven", "prepared-outage", "armed-outage", "armed-lost-ack", "armed-lost-ack-canceled", "armed-lost-ack-consumer-gone", "armed-lost-ack-retire-conflict", "armed-lost-ack-one-conflict", "armed-lost-ack-conflict-competing", "competing-arm", "prepared-abandoned", "prepared-abandoned-quarantine", "prepared-abandoned-conflict", "prepared-abandoned-orphan", "prepared-abandoned-destination", "prepared-abandoned-waiting", "prepared-abandoned-waiting-distinct-vm", "prepared-abandoned-waiting-arm-race", "prepared-abandoned-waiting-remote-publish", "prepared-abandoned-waiting-remote-reconcile", "prepared-abandoned-waiting-deleting-successor", "prepared-abandoned-waiting-deleting-successor-distinct-vm",
			"prepared-abandoned-destination-successor", "prepared-abandoned-destination-successor-distinct-vm",
			"prepared-abandoned-paired-forward", "prepared-abandoned-paired-forward-distinct-vm",
			"prepared-abandoned-paired-reverse", "prepared-abandoned-paired-reverse-distinct-vm",
			"prepared-abandoned-retired-pinned", "prepared-abandoned-retired-pinned-distinct-vm",
			"distinct-vm-abandoned-replay-retired-completed", "distinct-vm-abandoned-replay-retired-accepted",
			"same-vm-abandoned-replay-wire-retired", "distinct-vm-abandoned-replay-wire-retired",
			"distinct-vm-orphan-wire-retired", "distinct-vm-orphan-retired-accepted", "distinct-vm-orphan-lost-wire-retired",
			"distinct-vm-orphan-remote-wire-retired", "distinct-vm-orphan-remote-retired-accepted", "distinct-vm-orphan-remote-late-wire-retired",
			"distinct-vm-orphan-remote-replaced-wire-retired", "distinct-vm-orphan-remote-terminating-wire-retired",
			"distinct-vm-orphan-static-retired-accepted", "distinct-vm-orphan-static-same-name-retired-accepted",
			"distinct-vm-orphan-static-waiting-retired-accepted", "distinct-vm-orphan-static-same-name-reused-pv-waiting-retired-accepted",
			"distinct-vm-orphan-static-same-node-waiting-retired-accepted", "distinct-vm-orphan-static-same-node-rescheduled-waiting-retired-accepted",
			"distinct-vm-orphan-static-released-waiting-retired-accepted",
			"distinct-vm-orphan-static-same-name-reused-pv-released-waiting-retired-accepted",
			"distinct-vm-orphan-static-released-waiting-old-name-reused-retired-accepted",
			"distinct-vm-orphan-rebound-wire-retired", "distinct-vm-orphan-rebound-lost-wire-retired",
			"distinct-vm-orphan-rebound-unbound-wire-retired",
			"prepared-abandoned-waiting-remote-retired-publish", "prepared-abandoned-waiting-remote-retired-reconcile-distinct-vm",
			"prepared-abandoned-retired-without-va", "concurrent-prepared", "pre-dispatch", "terminating-old-consumer", "ambiguous-old-consumer", "distinct-vm", "distinct-vm-lost-reply", "distinct-vm-original-gone-at-admission", "distinct-vm-original-gone-before-retry",
			"distinct-vm-inflight", "same-vm-abandoned-replay",
			"no-publish-va", "attached-publish-va", "deleting-publish-va", "foreign-publish-va", "removed-publish-va", "replaced-publish-va",
			"current-runtime-consumer", "current-terminated-container", "current-restarted-container", "current-sandbox-ready",
			"unknown-container-status", "old-consumer-start-time", "current-terminating-consumer", "unknown-consumer-lifetime",
			"same-vm-abandoned-retirement-lost-ack", "distinct-vm-abandoned-retirement-lost-ack", "distinct-vm-abandoned-replay",
			"same-vm-abandoned-replay-worker-pv-gone", "distinct-vm-abandoned-replay-worker-pv-replaced",
			"same-vm-abandoned-replay-publish-pv-replaced", "distinct-vm-abandoned-replay-publish-pv-replaced",
			"same-vm-abandoned-replay-delete-pv-gone", "distinct-vm-abandoned-replay-delete-pv-replaced",
			"distinct-vm-abandoned-replay-completion-conflict", "distinct-vm-abandoned-replay-completion-conflict-changed-va",
			"distinct-vm-abandoned-replay-completion-conflict-changed-generation", "distinct-vm-abandoned-replay-completion-conflict-changed-provider",
			"same-vm-abandoned-replay-post-detach-pod", "distinct-vm-abandoned-replay-post-detach-pod", "distinct-vm-abandoned-post-detach-pod",
			"same-vm-abandoned-replay-detach-lag", "distinct-vm-abandoned-replay-detach-lag-retry",
			"distinct-vm-abandoned-replay-detach-lag-reboot", "distinct-vm-abandoned-replay-detach-lag-persistent-reboot",
			"distinct-vm-abandoned-replay-detach-lag-original-conflict",
			"same-vm-abandoned-replay-detach-lag-deadline", "same-vm-abandoned-orphan-republish",
			"distinct-vm-orphan-republish", "distinct-vm-orphan-remote-orphan-republish", "distinct-vm-abandoned-replay-accepted-wire-observe", "distinct-vm-abandoned-replay-accepted-wire-node-change", "same-vm-abandoned-replay-accepted-wire-claim-gone", "distinct-vm-abandoned-replay-accepted-wire-claim-gone", "distinct-vm-abandoned-replay-accepted-wire-claim-changed", "distinct-vm-abandoned-replay-accepted-wire-claim-reuse", "distinct-vm-abandoned-replay-accepted-wire-claim-conflict", "distinct-vm-abandoned-replay-accepted-wire-claim-pending", "distinct-vm-abandoned-replay-accepted-wire-claim-pending-binding", "distinct-vm-abandoned-replay-accepted-wire-claim-pending-conflict", "distinct-vm-abandoned-replay-detach-acceptance-write-failed",
			"distinct-vm-abandoned-replay-detach-reply-wrong-vm", "distinct-vm-abandoned-replay-detach-reply-string", "distinct-vm-abandoned-replay-detach-reply-failed",
			"distinct-vm-abandoned-replay-missing-va-pins", "distinct-vm-abandoned-replay-missing-va-name", "distinct-vm-abandoned-replay-missing-va-uid",
			"distinct-vm-abandoned-replay-wrong-va-uid", "distinct-vm-abandoned-hidden-pinned-va",
			"distinct-vm-abandoned-replay-post-detach-protection", "distinct-vm-abandoned-post-detach-protection", "distinct-vm-abandoned-replay-replaced-va", "distinct-vm-abandoned-replay-protection", "distinct-vm-abandoned-completion-lost-ack", "distinct-vm-abandoned", "distinct-vm-orphan", "distinct-vm-abandoned-released", "distinct-vm-abandoned-rebound", "distinct-vm-orphan-released", "distinct-vm-abandoned-wrong-marker", "distinct-vm-abandoned-consumer",
			"distinct-vm-abandoned-late-uid", "distinct-vm-abandoned-late-quarantine", "distinct-vm-abandoned-terminating", "distinct-vm-abandoned-readback", "distinct-vm-abandoned-no-disk-orphan", "distinct-vm-abandoned-no-disk-stale-va", "distinct-vm-abandoned-no-disk-original-owner", "distinct-vm-abandoned-detach-reply", "distinct-vm-abandoned-retire-conflict", "phase-less", "distinct-vm-still-owned", "distinct-vm-unreadable-original", "distinct-vm-original-changed", "distinct-vm-marker-mismatch", "distinct-vm-node-changed", "distinct-vm-destination-changed", "other-version"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				const canonicalVA = "csi-e4d7495542eef331c88a720776eeb3967cbd4ea98eec490f4f49c37f7f9dbbda"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
				pvc.Spec.AccessModes = pv.Spec.AccessModes
				node := newReadyNode("node-old", true)
				if strings.Contains(mode, "-reused-pv-") {
					pv.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
				}
				driver := newStickyTestDriver(t, pv, pvc, node)
				kube := driver.kubeRuntime.client.(*fake.Clientset)
				prependReactor := func(verb, resource string, react ktesting.ReactionFunc) {
					kube.Lock()
					defer kube.Unlock()
					kube.PrependReactor(verb, resource, react)
				}
				var pendingWritten, readbackFailed, apiOutage, outageInjected, abandoned atomic.Bool
				var detachProofs atomic.Int32
				abandonedPreparation := strings.HasPrefix(mode, "prepared-abandoned")
				renewedPreparation := strings.HasPrefix(mode, "prepared-abandoned-waiting")
				canceledDestination := strings.HasPrefix(mode, "prepared-abandoned-destination")
				destinationHasSourceSuccessor := strings.HasPrefix(mode, "prepared-abandoned-destination-successor")
				retiredSource := strings.Contains(mode, "remote-retired")
				distinctPreparation := abandonedPreparation && strings.HasSuffix(mode, "-distinct-vm")
				paired := strings.HasPrefix(mode, "prepared-abandoned-paired")
				retiredPinned := strings.HasPrefix(mode, "prepared-abandoned-retired-pinned")
				lostArmAck := strings.HasPrefix(mode, "armed-lost-ack")
				armError := mode == "armed-outage" || lostArmAck || mode == "competing-arm"
				competingAfterConflict := mode == "armed-lost-ack-conflict-competing"
				armRetired := lostArmAck && mode != "armed-lost-ack-retire-conflict" && !competingAfterConflict
				var armConflictInjected atomic.Bool
				const otherHistory = `{"volumeID":"other-volume","version":1,"backend":"local"}`
				publishCtx, cancelPublish := context.WithCancel(ctx)
				defer cancelPublish()
				retirementWrite := make(chan struct{}, 1)
				interruptedAdmission := mode == "prepared-outage" || armError || abandonedPreparation || mode == "concurrent-prepared"
				orphan := strings.HasPrefix(mode, "distinct-vm-orphan")
				replay := strings.Contains(mode, "abandoned-replay") || mode == "distinct-vm-abandoned-completion-lost-ack"
				cleanup := strings.Contains(mode, "-abandoned-") && !abandonedPreparation || mode == "distinct-vm-abandoned" || orphan
				lostReadback := mode == "distinct-vm-abandoned-readback" || strings.HasPrefix(mode, "distinct-vm-abandoned-no-disk")
				interruptedCleanup := lostReadback || mode == "distinct-vm-abandoned-detach-reply"
				var lostWriteToken, preparedToken, proposedArmToken string
				var wire *nodeAttachmentProtocol
				var failBeforeDispatch, postDetachChange, latePublishVAChanged atomic.Bool
				invalidPublishVA := strings.HasSuffix(mode, "-publish-va")
				var publishVA *storagev1.VolumeAttachment
				vas := kube.StorageV1().VolumeAttachments()
				nodes := kube.CoreV1().Nodes()
				latePod := activePodForPVC("post-detach-consumer", pvc.Name, node.Name)
				latePod.UID = "post-detach-consumer"
				latePod.Status.Phase = corev1.PodPending
				latePod.CreationTimestamp = metav1.Now()
				setPremountKubeletFixtureStatus(latePod, "ContainerCreating")
				latePodArrival := strings.HasSuffix(mode, "-post-detach-pod")
				lateProtection := strings.HasSuffix(mode, "-post-detach-protection")
				setObserve := func(active bool) {
					current, err := kube.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
					require.NoError(t, err)
					if active {
						current.Annotations[annotationRecoveryMode] = recoveryModeObserve
					} else {
						delete(current.Annotations, annotationRecoveryMode)
					}
					_, err = kube.CoreV1().PersistentVolumes().Update(ctx, current, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				wireObserve := strings.HasSuffix(mode, "-wire-observe")
				wireNodeChange := strings.HasSuffix(mode, "-wire-node-change")
				wireRetired := strings.HasSuffix(mode, "-wire-retired")
				retiredAccepted := strings.HasSuffix(mode, "-retired-accepted")
				retiredCompleted := strings.HasSuffix(mode, "-retired-completed")
				orphanRetirement := orphan && (wireRetired || retiredAccepted)
				orphanLostReply := orphanRetirement && strings.HasSuffix(mode, "-lost-wire-retired")
				orphanRemoteRetirement := orphanRetirement && strings.Contains(mode, "-orphan-remote-")
				lateRemote := orphanRemoteRetirement && strings.Contains(mode, "-remote-late-")
				replacedRemote := orphanRemoteRetirement && strings.Contains(mode, "-remote-replaced-")
				terminatingRemote := orphanRemoteRetirement && strings.Contains(mode, "-remote-terminating-")
				wireStaticRebind := orphanRetirement && strings.Contains(mode, "-orphan-rebound-")
				orphanStaticRetirement := orphanRetirement && strings.Contains(mode, "-orphan-static-")
				unboundDuringReply := wireStaticRebind && strings.Contains(mode, "-rebound-unbound-")
				sameNodeRebound := orphanStaticRetirement && strings.Contains(mode, "-same-node-")
				wireClaimGone := strings.HasSuffix(mode, "-wire-claim-gone")
				wireClaimChanged := strings.HasSuffix(mode, "-wire-claim-changed")
				wireClaimRebound := strings.HasSuffix(mode, "-wire-claim-reuse")
				wirePendingBinding := strings.HasSuffix(mode, "-wire-claim-pending-binding")
				wireClaimPending := strings.HasSuffix(mode, "-wire-claim-pending") || wirePendingBinding
				wirePendingConflict := strings.HasSuffix(mode, "-wire-claim-pending-conflict")
				wireClaimConflict := strings.HasSuffix(mode, "-wire-claim-conflict") || wirePendingConflict
				claimTurnover := wireClaimGone || wireClaimRebound || wireClaimPending
				bindIndependentClaim := func(reused *corev1.PersistentVolumeClaim) {
					other := pv.DeepCopy()
					other.Name, other.UID, other.Spec.CSI.VolumeHandle = "independent-pv", "independent-pv-uid", "independent-volume"
					other.Spec.ClaimRef.UID = reused.UID
					reused.Spec.VolumeName = other.Name
					reused.Status.Phase = corev1.ClaimBound
					_, err := kube.CoreV1().PersistentVolumes().Create(ctx, other, metav1.CreateOptions{})
					require.NoError(t, err)
				}
				orphanRepublish := strings.HasSuffix(mode, "-orphan-republish")
				remoteOrphan := mode == "distinct-vm-orphan-remote-orphan-republish"
				remoteNode := newReadyNode("node-remote", true)
				remoteNode.UID = "independent-orphan-destination-node"
				remoteNode.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
				remotePod := latePod.DeepCopy()
				remotePod.Name, remotePod.UID, remotePod.Spec.NodeName = "remote-orphan-consumer", "remote-orphan-pod", remoteNode.Name
				remoteVA := replacementPublishVA(pv, remoteNode)
				remoteVA.Name, remoteVA.UID = "csi-d0f14d64d3d3a6b04c9005c10e373303290c14d77fcf8125d47bc2d3b8aae477", "orphan-remote-destination"
				var replacement *corev1.Node
				provider, protocol := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if method == "one.vm.detach" && (wireObserve || wireNodeChange || wireRetired || retiredAccepted || claimTurnover || wireClaimChanged || wireClaimConflict || orphanRepublish) && postDetachChange.CompareAndSwap(false, true) {
						if wireObserve || retiredAccepted || orphanRetirement {
							setObserve(true)
						}
						if wireRetired {
							require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
							wire.mu.Lock()
							wire.omitVMID, wire.missingVMID = wire.attachAttemptVM, wire.attachAttemptVM
							wire.mu.Unlock()
						}
						if lateRemote {
							_, err := vas.Create(ctx, remoteVA, metav1.CreateOptions{})
							require.NoError(t, err)
							_, err = kube.CoreV1().Pods(pvc.Namespace).Create(ctx, remotePod, metav1.CreateOptions{})
							require.NoError(t, err)
						}
						if replacedRemote {
							require.NoError(t, vas.Delete(ctx, remoteVA.Name, metav1.DeleteOptions{}))
							successor := remoteVA.DeepCopy()
							successor.UID = "changed-during-detach"
							_, err := vas.Create(ctx, successor, metav1.CreateOptions{})
							require.NoError(t, err)
						}
						if terminatingRemote {
							terminating := remotePod.DeepCopy()
							now := metav1.Now()
							terminating.DeletionTimestamp = &now
							_, err := kube.CoreV1().Pods(pvc.Namespace).Update(ctx, terminating, metav1.UpdateOptions{})
							require.NoError(t, err)
						}
						if wireStaticRebind {
							pvs, claims := kube.CoreV1().PersistentVolumes(), kube.CoreV1().PersistentVolumeClaims(pvc.Namespace)
							rebound, currentClaim := pv.DeepCopy(), pvc.DeepCopy()
							rebound.UID, currentClaim.UID = "rebound-during-detach", "claim-rebound-during-detach"
							rebound.Spec.ClaimRef.UID = currentClaim.UID
							if unboundDuringReply {
								_, active, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: volume, Reason: "fixture quarantine"}, 1, time.Hour)
								require.NoError(t, err)
								require.True(t, active)
								rebound.Spec.ClaimRef = nil
								rebound.Status.Phase = corev1.VolumeReleased
								currentClaim.Status.Phase = corev1.ClaimPending
							}
							rebound.Annotations[annotationRecoveryMode] = recoveryModeObserve
							require.NoError(t, claims.Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
							var err error
							if unboundDuringReply {
								rebound.UID = pv.UID
								rebound.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
								_, err = pvs.Update(ctx, rebound, metav1.UpdateOptions{})
							} else {
								require.NoError(t, pvs.Delete(ctx, pv.Name, metav1.DeleteOptions{}))
								_, err = pvs.Create(ctx, rebound, metav1.CreateOptions{})
							}
							require.NoError(t, err)
							_, err = claims.Create(ctx, currentClaim, metav1.CreateOptions{})
							require.NoError(t, err)
						}
						if wireNodeChange {
							current, err := nodes.Get(ctx, node.Name, metav1.GetOptions{})
							require.NoError(t, err)
							current.UID = "changed-during-detach"
							_, err = nodes.Update(ctx, current, metav1.UpdateOptions{})
							require.NoError(t, err)
						}
						if wireClaimChanged {
							current := pv.DeepCopy()
							current.Spec.ClaimRef.UID = "changed-claim-ref"
							_, err := kube.CoreV1().PersistentVolumes().Update(ctx, current, metav1.UpdateOptions{})
							require.NoError(t, err)
						}
						if claimTurnover || wireClaimChanged || wireClaimConflict {
							admitted, err := kube.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
							require.NoError(t, err)
							require.Equal(t, corev1.VolumeBound, admitted.Status.Phase)
							claims := kube.CoreV1().PersistentVolumeClaims(pvc.Namespace)
							require.NoError(t, claims.Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
							if wireClaimRebound || wireClaimPending || wireClaimConflict {
								reused := pvc.DeepCopy()
								reused.UID = "new-pvc-uid"
								if wireClaimRebound {
									bindIndependentClaim(reused)
								}
								if wireClaimPending || wirePendingConflict {
									reused.Status.Phase = corev1.ClaimPending
								}
								if wireClaimPending {
									reused.CreationTimestamp = metav1.Now()
									reused.Spec.VolumeName = ""
									if wirePendingBinding {
										other, _ := newLocalPVAndPVC("pending-independent-volume", pv.Spec.AccessModes, nil)
										other.Status.Phase = corev1.VolumePending
										other.Spec.ClaimRef = &corev1.ObjectReference{Namespace: reused.Namespace, Name: reused.Name, UID: reused.UID}
										reused.Spec.VolumeName = other.Name
										_, err := kube.CoreV1().PersistentVolumes().Create(ctx, other, metav1.CreateOptions{})
										require.NoError(t, err)
									}
								}
								_, err := claims.Create(ctx, reused, metav1.CreateOptions{})
								require.NoError(t, err)
							}
						}
						if orphanRepublish {
							wireSuccessor := replacementPublishVA(pv, replacement)
							wireSuccessor.Name, wireSuccessor.UID = canonicalVA, "orphan-successor"
							_, err := vas.Create(ctx, wireSuccessor, metav1.CreateOptions{})
							require.NoError(t, err)
							if remoteOrphan {
								_, err = nodes.Create(ctx, remoteNode, metav1.CreateOptions{})
								require.NoError(t, err)
								_, err = vas.Create(ctx, remoteVA, metav1.CreateOptions{})
								require.NoError(t, err)
								_, err = kube.CoreV1().Pods(pvc.Namespace).Create(ctx, remotePod, metav1.CreateOptions{})
								require.NoError(t, err)
							}
						}
					}
					if (mode == "removed-publish-va" || mode == "replaced-publish-va") && method == "one.system.version" && !latePublishVAChanged.Load() {
						history, err := (&ControllerServer{driver: driver}).readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						if attempt := history.PendingReplacementAttach; attempt != nil && attempt.dispatchPossible() && latePublishVAChanged.CompareAndSwap(false, true) {
							require.NoError(t, vas.Delete(ctx, publishVA.Name, metav1.DeleteOptions{}))
							if mode == "replaced-publish-va" {
								other := publishVA.DeepCopy()
								other.UID = "competing-va"
								_, err := vas.Create(ctx, other, metav1.CreateOptions{})
								require.NoError(t, err)
							}
						}
					}
					if (latePodArrival || lateProtection) && abandoned.Load() && method == "one.vm.info" {
						wire.mu.Lock()
						detached := wire.detachAttempts == 1
						wire.mu.Unlock()
						if detached && postDetachChange.CompareAndSwap(false, true) {
							if latePodArrival {
								_, err := kube.CoreV1().Pods(pvc.Namespace).Create(ctx, latePod, metav1.CreateOptions{})
								require.NoError(t, err)
								current, err := kube.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
								require.NoError(t, err)
								current.Status.VolumesInUse = []corev1.UniqueVolumeName{corev1.UniqueVolumeName("kubernetes.io/csi/" + driver.name + "^" + volume)}
								_, err = kube.CoreV1().Nodes().UpdateStatus(ctx, current, metav1.UpdateOptions{})
								require.NoError(t, err)
							} else {
								setObserve(true)
							}
						}
					}
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
				wire.requirePersistedAttachNonce(t, driver, volume)
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
				placementWatch, err := kube.CoreV1().PersistentVolumes().Watch(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				_, err = client.ControllerPublishVolume(ctx, publish)
				require.NoError(t, err)
				deadline := time.NewTimer(10 * time.Second)
			placementObserved:
				for {
					select {
					case event, open := <-placementWatch.ResultChan():
						require.True(t, open, "placement watch closed before the seed Publish updated the PV")
						annotated, ok := event.Object.(*corev1.PersistentVolume)
						if ok && annotated.UID == pv.UID && annotated.Annotations[annotationLastAttachedNode] == node.Name {
							break placementObserved
						}
					case <-deadline.C:
						t.Fatal("initial Publish placement update did not complete")
					}
				}
				deadline.Stop()
				placementWatch.Stop()
				setReplacementHistoryVersion(t, driver)
				require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
				replacement = node.DeepCopy()
				replacement.UID = "replacement"
				replacement.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
				replacement.Status.VolumesInUse = []corev1.UniqueVolumeName{corev1.UniqueVolumeName("kubernetes.io/csi/" + driver.name + "^" + volume)}
				_, err = nodes.Create(ctx, replacement, metav1.CreateOptions{})
				require.NoError(t, err)
				consumer := activePodForPVC("new-pod", pvc.Name, node.Name)
				consumer.UID = "new-pod"
				consumer.Status.Phase = corev1.PodPending
				consumer.CreationTimestamp = metav1.NewTime(replacement.CreationTimestamp.Add(time.Second))
				switch mode {
				case "lost-reply", "distinct-vm", "distinct-vm-lost-reply":
					consumer.Spec.InitContainers = []corev1.Container{{Name: "init", Image: "busybox"}}
					setPremountKubeletFixtureStatus(consumer, "PodInitializing")
					consumer.Status.InitContainerStatuses = []corev1.ContainerStatus{{
						Name: "init", Image: "busybox", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
					}}
				case "current-runtime-consumer":
					setPremountKubeletFixtureStatus(consumer, "ContainerCreating")
					consumer.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: consumer.CreationTimestamp}}
				case "current-terminated-container":
					setPremountKubeletFixtureStatus(consumer, "ContainerCreating")
					consumer.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}
				case "current-restarted-container":
					setPremountKubeletFixtureStatus(consumer, "ContainerCreating")
					consumer.Status.ContainerStatuses[0].RestartCount = 1
					consumer.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{}
				case "current-sandbox-ready":
					setPremountKubeletFixtureStatus(consumer, "ContainerCreating")
					consumer.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionTrue}}
				case "unknown-container-status":
					setPremountKubeletFixtureStatus(consumer, "ContainerCreating")
					consumer.Status.ContainerStatuses[0].Name = "undeclared"
				case "old-consumer-start-time":
					setPremountKubeletFixtureStatus(consumer, "ContainerCreating")
					consumer.Status.StartTime = &replacement.CreationTimestamp
				case "current-terminating-consumer":
					now := metav1.Now()
					consumer.DeletionTimestamp = &now
				case "unknown-consumer-lifetime":
					consumer.CreationTimestamp = metav1.Time{}
				}
				publishVA = replacementPublishVA(pv, replacement)
				if orphanRepublish || renewedPreparation || destinationHasSourceSuccessor || paired || retiredPinned {
					publishVA.Name = canonicalVA
				}
				switch mode {
				case "attached-publish-va":
					publishVA.Status.Attached = true
				case "deleting-publish-va":
					now := metav1.Now()
					publishVA.DeletionTimestamp = &now
				case "foreign-publish-va":
					publishVA.Spec.Attacher = "foreign.csi.example"
				}
				if mode != "no-publish-va" {
					_, err = vas.Create(ctx, publishVA, metav1.CreateOptions{})
					require.NoError(t, err)
				}
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
				if strings.HasPrefix(mode, "authorization-") || strings.HasPrefix(mode, "authentication-") {
					wire.attachRefusal = mode
				}
				vmID := 41
				if strings.HasPrefix(mode, "distinct-vm") || distinctPreparation {
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
					prependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
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
								apiOutage.Store(!lostArmAck && mode != "competing-arm")
								if mode == "armed-lost-ack-canceled" {
									cancelPublish()
								}
								if mode == "armed-lost-ack-consumer-gone" {
									require.NoError(t, kube.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), consumer.Namespace, consumer.Name))
								}
								return true, nil, errors.New("committed phase update reply lost")
							}
						}
						if fields["pendingReplacementAttach"] == nil {
							return false, nil, nil
						}
						var next replacementAttachAttempt
						require.NoError(t, json.Unmarshal(fields["pendingReplacementAttach"], &next))
						if next.prepared() && proposedArmToken != "" {
							if (mode == "armed-lost-ack-one-conflict" || competingAfterConflict) && armConflictInjected.CompareAndSwap(false, true) {
								previous, err := kube.Tracker().Get(gvr, cm.Namespace, cm.Name)
								require.NoError(t, err)
								concurrent := previous.(*corev1.ConfigMap).DeepCopy()
								concurrent.ResourceVersion = "102"
								concurrent.Data["other-volume"] = otherHistory
								if competingAfterConflict {
									var history VolumeHistoryRecord
									require.NoError(t, json.Unmarshal([]byte(concurrent.Data[volume]), &history))
									history.PendingReplacementAttach.ID = strings.Repeat("e", 64)
									payload, err := json.Marshal(history)
									require.NoError(t, err)
									concurrent.Data[volume] = string(payload)
								}
								require.NoError(t, kube.Tracker().Update(gvr, concurrent, cm.Namespace))
								return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("unrelated history row updated"))
							}
							if mode == "armed-lost-ack-retire-conflict" {
								return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("arming history changed before retirement"))
							}
							if mode == "armed-lost-ack-canceled" {
								select {
								case retirementWrite <- struct{}{}:
								default:
								}
							}
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
					prependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
						if action.(ktesting.GetAction).GetName() == volumeHistoryStateConfigMapName &&
							(mode == "readback-failed" && pendingWritten.Load() && readbackFailed.CompareAndSwap(false, true) || mode == "readback-unproven" && pendingWritten.Load() && !readbackFailed.Load() || apiOutage.Load()) {
							return true, nil, errors.New("independent history read unavailable")
						}
						return false, nil, nil
					})
				}
				reply, err := client.ControllerPublishVolume(publishCtx, publish)
				publishErr := err
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
					require.NotEmpty(t, wire.attachFields["CSI_ATTACH_ATTEMPT"])
					return
				}
				switch mode {
				case "armed-lost-ack-canceled":
					require.Equal(t, codes.Canceled, status.Code(err))
					select {
					case <-retirementWrite:
					case <-time.After(4 * time.Second):
						t.Fatal("canceled Publish did not retire its unsent dispatch ID")
					}
				case "pre-dispatch":
					require.Error(t, err)
				default:
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
					if mode == "armed-lost-ack-one-conflict" || competingAfterConflict {
						require.True(t, armConflictInjected.Load())
						historyCM, err := kube.CoreV1().ConfigMaps(driver.volumeHistory.namespace).Get(ctx, volumeHistoryStateConfigMapName, metav1.GetOptions{})
						require.NoError(t, err)
						require.Equal(t, otherHistory, historyCM.Data["other-volume"])
					}
					if armRetired {
						require.NotNil(t, stored.PendingReplacementAttach)
						require.True(t, stored.PendingReplacementAttach.prepared())
						require.Equal(t, preparedToken, stored.PendingReplacementAttach.ID)
					} else {
						require.NotNil(t, stored.PendingReplacementAttach)
						require.Equal(t, !armError, stored.PendingReplacementAttach.prepared())
					}
					if armError {
						require.NotEmpty(t, preparedToken)
						require.NotEmpty(t, proposedArmToken)
						require.NotEqual(t, preparedToken, proposedArmToken)
					}
					if mode == "armed-lost-ack-consumer-gone" {
						_, err := kube.CoreV1().Pods(pvc.Namespace).Get(ctx, consumer.Name, metav1.GetOptions{})
						require.True(t, apierrors.IsNotFound(err))
						return
					}
					fresh := newStickyTestDriver(t)
					fresh.kubeRuntime.client = kube
					restarted := makeClient(fresh)
					if renewedPreparation {
						require.NotNil(t, stored.PendingReplacementAttach)
						require.True(t, stored.PendingReplacementAttach.prepared())
						require.Equal(t, vmID, stored.PendingReplacementAttach.VMID)
						pods := kube.CoreV1().Pods(pvc.Namespace)
						require.NoError(t, pods.Delete(ctx, consumer.Name, metav1.DeleteOptions{}))
						waiting := latePod.DeepCopy()
						_, err = pods.Create(ctx, waiting, metav1.CreateOptions{})
						require.NoError(t, err)
						unpublish := func() error {
							_, err := restarted.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
							return err
						}
						require.Error(t, unpublish(), "the live pinned VA cannot be released by Unpublish")
						deleting := publishVA.DeepCopy()
						now := metav1.Now()
						deleting.DeletionTimestamp = &now
						deleting.Finalizers = []string{"external-attacher/" + driver.name}
						wrong := deleting.DeepCopy()
						wrong.UID = "unadmitted-attachment"
						wrong.CreationTimestamp = metav1.NewTime(replacement.CreationTimestamp.Add(-time.Second))
						_, err = vas.Update(ctx, wrong, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish())
						attached := deleting.DeepCopy()
						attached.Status.Attached = true
						_, err = vas.Update(ctx, attached, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish())
						_, err = vas.Update(ctx, deleting, metav1.UpdateOptions{})
						require.NoError(t, err)
						inUse := waiting.DeepCopy()
						inUse.Status.ContainerStatuses[0].ContainerID = "containerd://running"
						_, err = pods.UpdateStatus(ctx, inUse, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish())
						inUse = waiting.DeepCopy()
						inUse.Status.StartTime = nil
						_, err = pods.UpdateStatus(ctx, inUse, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish(), "volumesInUse needs a verified waiting Pod start")
						_, err = pods.UpdateStatus(ctx, waiting, metav1.UpdateOptions{})
						require.NoError(t, err)
						wire.mu.Lock()
						wire.disks[41] = 128
						wire.mu.Unlock()
						require.Error(t, unpublish(), "a prepared attempt cannot release a provider-owned disk")
						wire.mu.Lock()
						delete(wire.disks, 41)
						wire.mu.Unlock()
						preserved, err := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored, preserved)

						if mode == "prepared-abandoned-waiting-arm-race" {
							armed := *stored.PendingReplacementAttach
							armed.Phase, armed.ID = replacementAttachArmed, strings.Repeat("b", 64)
							competing := stored
							competing.PendingReplacementAttach = &armed
							payload, err := json.Marshal(competing)
							require.NoError(t, err)
							var attachmentReads atomic.Int32
							prependReactor("list", "volumeattachments", func(ktesting.Action) (bool, kruntime.Object, error) {
								if attachmentReads.Add(1) != 2 {
									return false, nil, nil
								}
								resource := corev1.SchemeGroupVersion.WithResource("configmaps")
								cm, err := kube.Tracker().Get(resource, fresh.volumeHistory.namespace, volumeHistoryStateConfigMapName)
								require.NoError(t, err)
								current := cm.(*corev1.ConfigMap).DeepCopy()
								current.ResourceVersion = "102"
								current.Data[volume] = string(payload)
								require.NoError(t, kube.Tracker().Update(resource, current, current.Namespace))
								return false, nil, nil
							})
							require.Error(t, unpublish())
							require.GreaterOrEqual(t, attachmentReads.Load(), int32(2))
							preserved, err := server.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, competing, preserved)
							wire.mu.Lock()
							defer wire.mu.Unlock()
							require.Zero(t, wire.attachAttempts)
							require.Zero(t, wire.detachAttempts)
							require.Empty(t, wire.mutations)
							return
						}

						_, err = restarted.ControllerPublishVolume(ctx, publish)
						require.Error(t, err, "Publish must wait for deletion of the pinned VA")
						var processed atomic.Bool
						lostReply := reviewControllerClient(t, NewControllerServer(fresh, provider, nil), grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
							reply, err := handler(ctx, req)
							if _, ok := req.(*csi.ControllerUnpublishVolumeRequest); ok && err == nil {
								processed.Store(true)
								return nil, status.Error(codes.Unavailable, "CSI response lost after Unpublish completed")
							}
							return reply, err
						}))
						req := &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name}
						_, err = lostReply.ControllerUnpublishVolume(ctx, req)
						require.Equal(t, codes.Unavailable, status.Code(err))
						require.True(t, processed.Load())
						fresh = newStickyTestDriver(t)
						fresh.kubeRuntime.client = kube
						restarted = makeClient(fresh)
						_, err = pods.UpdateStatus(ctx, inUse, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish(), "replay must recheck Pod use")
						_, err = pods.UpdateStatus(ctx, waiting, metav1.UpdateOptions{})
						require.NoError(t, err)
						_, err = vas.Update(ctx, wrong, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish(), "replay must match the exact deleting VA")
						_, err = vas.Update(ctx, deleting, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.NoError(t, unpublish())
						require.NoError(t, unpublish())
						preserved, err = server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored, preserved)
						currentVA, err := vas.Get(ctx, deleting.Name, metav1.GetOptions{})
						require.NoError(t, err)
						require.Equal(t, deleting, currentVA)
						_, err = restarted.ControllerPublishVolume(ctx, publish)
						require.Error(t, err, "the finalizer still owns VA retirement")
						wire.mu.Lock()
						require.Zero(t, wire.attachAttempts)
						require.Zero(t, wire.detachAttempts)
						require.Empty(t, wire.mutations)
						wire.mu.Unlock()
						require.NoError(t, vas.Delete(ctx, deleting.Name, metav1.DeleteOptions{}))
						next := publishVA.DeepCopy()
						next.UID = "later-publication"
						publishVM := vmID
						remote := strings.HasPrefix(mode, "prepared-abandoned-waiting-remote-")
						if remote {
							released := replacement.DeepCopy()
							released.Status.VolumesInUse = nil
							_, err = nodes.UpdateStatus(ctx, released, metav1.UpdateOptions{})
							require.NoError(t, err)
							destination := newReadyNode("remote-node", true)
							destination.UID = "prepared-destination"
							destination.CreationTimestamp = replacement.CreationTimestamp
							_, err = nodes.Create(ctx, destination, metav1.CreateOptions{})
							require.NoError(t, err)
							next = replacementPublishVA(pv, destination)
							next.Name, next.UID = remoteFixtureVAName, "prepared-remote-successor"
							waiting.Spec.NodeName = destination.Name
							_, err = pods.Update(ctx, waiting, metav1.UpdateOptions{})
							require.NoError(t, err)
							publish, publishVM = newPublishReq(volume, destination.Name), 43
							wire.mu.Lock()
							if wire.vmNames == nil {
								wire.vmNames = map[int]string{41: node.Name}
							}
							wire.vmNames[43] = destination.Name
							wire.expectedAttachNonce = nil
							wire.mu.Unlock()
							wrongDestination := next.DeepCopy()
							wrongDestination.Spec.Attacher = "foreign.example"
							_, err = vas.Create(ctx, wrongDestination, metav1.CreateOptions{})
							require.NoError(t, err)
							_, err = restarted.ControllerPublishVolume(ctx, publish)
							require.Error(t, err)
							require.NoError(t, vas.Delete(ctx, wrongDestination.Name, metav1.DeleteOptions{}))
						}
						if strings.HasPrefix(mode, "prepared-abandoned-waiting-deleting-successor") {
							canceled := next.DeepCopy()
							canceled.DeletionTimestamp = &now
							canceled.Finalizers = deleting.Finalizers
							_, err = vas.Create(ctx, canceled, metav1.CreateOptions{})
							require.NoError(t, err)
							_, err = restarted.ControllerPublishVolume(ctx, publish)
							require.Error(t, err, "a deleting successor cannot be published")
							active := waiting.DeepCopy()
							active.Status.ContainerStatuses[0].ContainerID = "containerd://running"
							_, err = pods.UpdateStatus(ctx, active, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish())
							_, err = pods.UpdateStatus(ctx, waiting, metav1.UpdateOptions{})
							require.NoError(t, err)
							var acknowledged atomic.Bool
							lost := reviewControllerClient(t, NewControllerServer(fresh, provider, nil), grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
								reply, err := handler(ctx, req)
								if err == nil {
									acknowledged.Store(true)
									return nil, status.Error(codes.Unavailable, "CSI response lost after successor Unpublish completed")
								}
								return reply, err
							}))
							_, err = lost.ControllerUnpublishVolume(ctx, req)
							require.Equal(t, codes.Unavailable, status.Code(err))
							require.True(t, acknowledged.Load())
							fresh = newStickyTestDriver(t)
							fresh.kubeRuntime.client = kube
							restarted = makeClient(fresh)
							reconciler := NewAttachmentReconciler(NewControllerServer(fresh, provider, nil))
							reconciler.orphanGrace = 0
							require.NoError(t, reconciler.ReconcileOnce(ctx))
							require.NoError(t, reconciler.ReconcileOnce(ctx))
							changed := canceled.DeepCopy()
							changed.Spec.Attacher = "foreign.example"
							_, err = vas.Update(ctx, changed, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish())
							_, err = vas.Update(ctx, canceled, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.NoError(t, unpublish(), "the deleting successor must replay after restart and reconciliation")
							require.NoError(t, unpublish())
							current, err := server.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, stored, current)
							unchanged, err := vas.Get(ctx, canceled.Name, metav1.GetOptions{})
							require.NoError(t, err)
							require.Equal(t, canceled, unchanged)
							require.NoError(t, vas.Delete(ctx, canceled.Name, metav1.DeleteOptions{}))
							next.UID = "publication-after-cancellation"
						}
						_, err = vas.Create(ctx, next, metav1.CreateOptions{})
						require.NoError(t, err)
						if remote {
							active := waiting.DeepCopy()
							active.Status.ContainerStatuses[0].ContainerID = "containerd://running"
							_, err = pods.UpdateStatus(ctx, active, metav1.UpdateOptions{})
							require.NoError(t, err)
							_, err = restarted.ControllerPublishVolume(ctx, publish)
							require.Error(t, err)
							preserved, err := server.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, stored, preserved)
							_, err = pods.UpdateStatus(ctx, waiting, metav1.UpdateOptions{})
							require.NoError(t, err)
							if retiredSource {
								require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
								_, err = restarted.ControllerPublishVolume(ctx, publish)
								require.Error(t, err, "Kubernetes Node absence cannot substitute for pinned VM absence")
								wire.mu.Lock()
								wire.omitVMID = vmID
								wire.mu.Unlock()
								_, err = restarted.ControllerPublishVolume(ctx, publish)
								require.Error(t, err, "a VM pool miss cannot substitute for direct pinned VM absence")
								wire.mu.Lock()
								wire.omitVMID, wire.missingVMID = 0, vmID
								wire.mu.Unlock()
								_, err = restarted.ControllerPublishVolume(ctx, publish)
								require.Error(t, err, "the VM census must agree with direct absence")
								wire.mu.Lock()
								wire.omitVMID = vmID
								wire.disks[43] = 128
								wire.mu.Unlock()
								_, err = restarted.ControllerPublishVolume(ctx, publish)
								require.Error(t, err, "a different VM must not own the image during preparation retirement")
								wire.mu.Lock()
								delete(wire.disks, 43)
								wire.extendedPoolExtraVM = fmt.Sprintf("<VM><ID>44</ID><NAME>%s</NAME><TEMPLATE></TEMPLATE></VM>", node.Name)
								wire.mu.Unlock()
								_, err = restarted.ControllerPublishVolume(ctx, publish)
								require.Error(t, err, "a similarly named VM is not the retired source")
								wire.mu.Lock()
								wire.extendedPoolExtraVM = ""
								wire.mu.Unlock()
								var failSourceRead atomic.Bool
								prependReactor("get", "nodes", func(action ktesting.Action) (bool, kruntime.Object, error) {
									if failSourceRead.Load() && action.(ktesting.GetAction).GetName() == node.Name {
										return true, nil, errors.New("source Node read unavailable")
									}
									return false, nil, nil
								})
								failSourceRead.Store(true)
								_, err = restarted.ControllerPublishVolume(ctx, publish)
								require.Error(t, err, "a failed Node read cannot prove retirement")
								failSourceRead.Store(false)
								recreated := replacement.DeepCopy()
								recreated.UID = "unrelated-new-source"
								_, err = nodes.Create(ctx, recreated, metav1.CreateOptions{})
								require.NoError(t, err)
								_, err = restarted.ControllerPublishVolume(ctx, publish)
								require.Error(t, err, "a same-name Kubernetes Node is not the retired source")
								require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
								preserved, err := server.readAttachmentHistory(ctx, volume)
								require.NoError(t, err)
								require.Equal(t, stored, preserved)
							}
						} else {
							require.Error(t, unpublish(), "a late Unpublish cannot acknowledge the nondeleting source successor")
						}
						if vmID != 41 || mode == "prepared-abandoned-waiting-remote-reconcile" {
							reconciler := NewAttachmentReconciler(NewControllerServer(fresh, provider, nil))
							reconciler.orphanGrace = 0
							require.NoError(t, reconciler.ReconcileOnce(ctx))
							require.NoError(t, reconciler.ReconcileOnce(ctx))
							retired, err := server.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Nil(t, retired.PendingReplacementAttach)
						}
						wire.mu.Lock()
						require.Empty(t, wire.mutations)
						wire.mu.Unlock()
						reply, err := restarted.ControllerPublishVolume(ctx, publish)
						require.NoError(t, err)
						require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", publishVM)}, wire.mutations)
						require.Zero(t, wire.detachAttempts)
						return
					}
					if abandonedPreparation {
						if !canceledDestination && !paired && !retiredPinned {
							require.NoError(t, vas.Delete(ctx, publishVA.Name, metav1.DeleteOptions{}))
						}
						released := replacement.DeepCopy()
						released.Status.VolumesInUse = nil
						_, err := nodes.UpdateStatus(ctx, released, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.NoError(t, kube.CoreV1().Pods(pvc.Namespace).Delete(ctx, consumer.Name, metav1.DeleteOptions{}))
						if paired || retiredPinned {
							reverse := strings.Contains(mode, "paired-reverse")
							now := metav1.Now()
							source := publishVA.DeepCopy()
							if reverse {
								require.NoError(t, vas.Delete(ctx, source.Name, metav1.DeleteOptions{}))
								source.UID = "waiting-source-successor"
								_, err = vas.Create(ctx, source, metav1.CreateOptions{})
							} else {
								source.DeletionTimestamp = &now
								source.Finalizers = []string{"external-attacher/" + driver.name}
								_, err = vas.Update(ctx, source, metav1.UpdateOptions{})
							}
							require.NoError(t, err)
							deleting, waitingNode, publishVM := source, source.Spec.NodeName, vmID
							var waitingVA *storagev1.VolumeAttachment
							if paired {
								remote := newReadyNode("remote-node", true)
								remote.UID, remote.CreationTimestamp = "paired-remote-node", replacement.CreationTimestamp
								_, err = nodes.Create(ctx, remote, metav1.CreateOptions{})
								require.NoError(t, err)
								destination := replacementPublishVA(pv, remote)
								destination.Name, destination.UID = remoteFixtureVAName, "paired-remote-attachment"
								if reverse {
									destination.DeletionTimestamp = &now
									destination.Finalizers = []string{"external-attacher/" + driver.name}
									deleting, waitingVA = destination, source
								} else {
									waitingVA, waitingNode, publishVM = destination, remote.Name, 43
								}
								_, err = vas.Create(ctx, destination, metav1.CreateOptions{})
								require.NoError(t, err)
								wire.mu.Lock()
								if wire.vmNames == nil {
									wire.vmNames = map[int]string{41: node.Name}
								}
								wire.vmNames[43] = remote.Name
								if !reverse {
									wire.expectedAttachNonce = nil
								}
								wire.mu.Unlock()
								waiter := latePod.DeepCopy()
								waiter.Spec.NodeName = waitingNode
								pods := kube.CoreV1().Pods(pvc.Namespace)
								_, err = pods.Create(ctx, waiter, metav1.CreateOptions{})
								require.NoError(t, err)
								_, err = restarted.ControllerPublishVolume(ctx, newPublishReq(volume, waitingNode))
								require.Error(t, err, "Publish must wait for finalization")
								active := waiter.DeepCopy()
								active.Status.ContainerStatuses[0].ContainerID = "containerd://running"
								_, err = pods.UpdateStatus(ctx, active, metav1.UpdateOptions{})
								require.NoError(t, err)
								_, err = restarted.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: deleting.Spec.NodeName})
								require.Error(t, err)
								_, err = pods.UpdateStatus(ctx, waiter, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							request := &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: deleting.Spec.NodeName}
							if retiredPinned {
								require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
								wire.mu.Lock()
								wire.omitVMID = vmID
								wire.mu.Unlock()
								_, err = restarted.ControllerUnpublishVolume(ctx, request)
								require.Error(t, err, "a name lookup miss cannot prove pinned VM absence")
								wire.mu.Lock()
								wire.missingVMID = vmID
								wire.mu.Unlock()
							}
							changed := deleting.DeepCopy()
							changed.Status.Attached = true
							_, err = vas.Update(ctx, changed, metav1.UpdateOptions{})
							require.NoError(t, err)
							_, err = restarted.ControllerUnpublishVolume(ctx, request)
							require.Error(t, err)
							_, err = vas.Update(ctx, deleting, metav1.UpdateOptions{})
							require.NoError(t, err)
							if retiredPinned {
								changed = deleting.DeepCopy()
								changed.UID = "unknown-deleting-uid"
								_, err = vas.Update(ctx, changed, metav1.UpdateOptions{})
								require.NoError(t, err)
								_, err = restarted.ControllerUnpublishVolume(ctx, request)
								require.Error(t, err)
								_, err = vas.Update(ctx, deleting, metav1.UpdateOptions{})
								require.NoError(t, err)
							} else {
								changed = waitingVA.DeepCopy()
								changed.Spec.Attacher = "foreign.example"
								_, err = vas.Update(ctx, changed, metav1.UpdateOptions{})
								require.NoError(t, err)
								_, err = restarted.ControllerUnpublishVolume(ctx, request)
								require.Error(t, err)
								_, err = vas.Update(ctx, waitingVA, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							var changeControl atomic.Bool
							changeControl.Store(true)
							prependReactor("get", "volumeattachments", func(action ktesting.Action) (bool, kruntime.Object, error) {
								if action.(ktesting.GetAction).GetName() == deleting.Name && changeControl.CompareAndSwap(true, false) {
									gvr := corev1.SchemeGroupVersion.WithResource("persistentvolumes")
									obj, err := kube.Tracker().Get(gvr, "", pv.Name)
									require.NoError(t, err)
									observed := obj.(*corev1.PersistentVolume).DeepCopy()
									observed.Annotations[annotationRecoveryMode] = recoveryModeObserve
									require.NoError(t, kube.Tracker().Update(gvr, observed, ""))
								}
								return false, nil, nil
							})
							_, err = restarted.ControllerUnpublishVolume(ctx, request)
							require.Error(t, err, "a late observe control must block acknowledgement")
							require.False(t, changeControl.Load())
							pvs := kube.CoreV1().PersistentVolumes()
							held, err := pvs.Get(ctx, pv.Name, metav1.GetOptions{})
							require.NoError(t, err)
							delete(held.Annotations, annotationRecoveryMode)
							_, err = pvs.Update(ctx, held, metav1.UpdateOptions{})
							require.NoError(t, err)
							lastProtectionRead := int32(4)
							if paired && !reverse {
								lastProtectionRead = 5
							}
							var protectionReads atomic.Int32
							prependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
								if action.(ktesting.GetAction).GetName() == hostArtifactStateConfigMapName && protectionReads.Add(1) == lastProtectionRead {
									if retiredPinned {
										reappeared := replacement.DeepCopy()
										reappeared.UID = "recreated-during-protection-refresh"
										require.NoError(t, kube.Tracker().Create(corev1.SchemeGroupVersion.WithResource("nodes"), reappeared, ""))
									} else {
										changed := deleting.DeepCopy()
										changed.UID = "changed-during-protection-refresh"
										require.NoError(t, kube.Tracker().Update(storagev1.SchemeGroupVersion.WithResource("volumeattachments"), changed, ""))
									}
								}
								return false, nil, nil
							})
							_, err = restarted.ControllerUnpublishVolume(ctx, request)
							require.Error(t, err, "release identity changed during the final protection read")
							require.GreaterOrEqual(t, protectionReads.Load(), lastProtectionRead)
							if retiredPinned {
								require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
							} else {
								_, err = vas.Update(ctx, deleting, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							var acknowledged atomic.Bool
							lost := reviewControllerClient(t, NewControllerServer(fresh, provider, nil), grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
								reply, err := handler(ctx, req)
								if err == nil {
									acknowledged.Store(true)
									return nil, status.Error(codes.Unavailable, "CSI response lost after Unpublish completed")
								}
								return reply, err
							}))
							kube.ClearActions()
							_, err = lost.ControllerUnpublishVolume(ctx, request)
							require.Equal(t, codes.Unavailable, status.Code(err))
							require.True(t, acknowledged.Load())
							fresh = newStickyTestDriver(t)
							fresh.kubeRuntime.client = kube
							restarted = makeClient(fresh)
							_, err = restarted.ControllerUnpublishVolume(ctx, request)
							require.NoError(t, err)
							for _, action := range kube.Actions() {
								require.Contains(t, []string{"get", "list"}, action.GetVerb())
							}
							current, err := server.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, stored, current)
							wire.mu.Lock()
							require.Empty(t, wire.mutations)
							wire.mu.Unlock()
							require.NoError(t, vas.Delete(ctx, deleting.Name, metav1.DeleteOptions{}))
							if paired {
								reply, err := restarted.ControllerPublishVolume(ctx, newPublishReq(volume, waitingNode))
								require.NoError(t, err)
								require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
								wire.mu.Lock()
								require.Equal(t, []string{fmt.Sprintf("attach:%d:false", publishVM)}, wire.mutations)
								wire.mu.Unlock()
							} else {
								reconciler := NewAttachmentReconciler(NewControllerServer(fresh, provider, nil))
								reconciler.orphanGrace = 0
								require.NoError(t, reconciler.ReconcileOnce(ctx))
								require.NoError(t, reconciler.ReconcileOnce(ctx))
								current, err = server.readAttachmentHistory(ctx, volume)
								require.NoError(t, err)
								require.Nil(t, current.PendingReplacementAttach)
								require.Equal(t, stored.LastSafeDetachTime, current.LastSafeDetachTime)
							}
							wire.mu.Lock()
							require.Zero(t, wire.detachAttempts)
							wire.mu.Unlock()
							return
						}
						if canceledDestination {
							now := metav1.Now()
							finalizers := []string{"external-attacher/" + driver.name}
							publishVA.DeletionTimestamp, publishVA.Finalizers = &now, finalizers
							if destinationHasSourceSuccessor {
								require.NoError(t, vas.Delete(ctx, publishVA.Name, metav1.DeleteOptions{}))
								publishVA.UID = "canceled-source-successor"
								_, err = vas.Create(ctx, publishVA, metav1.CreateOptions{})
							} else {
								_, err = vas.Update(ctx, publishVA, metav1.UpdateOptions{})
							}
							require.NoError(t, err)
							remote := newReadyNode("remote-node", true)
							remote.UID = "destination-node"
							remote.CreationTimestamp = metav1.NewTime(now.Add(-time.Minute))
							_, err = nodes.Create(ctx, remote, metav1.CreateOptions{})
							require.NoError(t, err)
							destination := replacementPublishVA(pv, remote)
							destination.Name, destination.UID = remoteFixtureVAName, "destination-attachment"
							destination.DeletionTimestamp, destination.Finalizers = &now, finalizers
							_, err = vas.Create(ctx, destination, metav1.CreateOptions{})
							require.NoError(t, err)
							wire.mu.Lock()
							if wire.vmNames == nil {
								wire.vmNames = map[int]string{41: node.Name}
							}
							wire.vmNames[43] = remote.Name
							wire.disks[vmID] = 128
							wire.mu.Unlock()

							request := &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: remote.Name}
							_, err = restarted.ControllerUnpublishVolume(ctx, request)
							require.Error(t, err, "preparation alone cannot authorize acknowledgement of an owned image")
							wire.mu.Lock()
							delete(wire.disks, vmID)
							wire.mu.Unlock()

							_, err = restarted.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
							require.Error(t, err)
							if destinationHasSourceSuccessor {
								foreign := publishVA.DeepCopy()
								foreign.Spec.Attacher = "foreign.example"
								_, err = vas.Update(ctx, foreign, metav1.UpdateOptions{})
								require.NoError(t, err)
								_, err = restarted.ControllerUnpublishVolume(ctx, request)
								require.Error(t, err)
								attached := publishVA.DeepCopy()
								attached.Status.Attached = true
								_, err = vas.Update(ctx, attached, metav1.UpdateOptions{})
								require.NoError(t, err)
								_, err = restarted.ControllerUnpublishVolume(ctx, request)
								require.Error(t, err)
								_, err = vas.Update(ctx, publishVA, metav1.UpdateOptions{})
								require.NoError(t, err)
								busy := released.DeepCopy()
								busy.Status.VolumesInUse = replacement.Status.VolumesInUse
								_, err = nodes.UpdateStatus(ctx, busy, metav1.UpdateOptions{})
								require.NoError(t, err)
								_, err = restarted.ControllerUnpublishVolume(ctx, request)
								require.Error(t, err)
								_, err = nodes.UpdateStatus(ctx, released, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							kube.ClearActions()
							_, err = restarted.ControllerUnpublishVolume(ctx, request)
							require.NoError(t, err)
							for _, action := range kube.Actions() {
								require.Contains(t, []string{"get", "list"}, action.GetVerb())
							}
							current, err := server.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, stored, current)
							fresh = newStickyTestDriver(t)
							fresh.kubeRuntime.client = kube
							restarted = makeClient(fresh)
							_, err = restarted.ControllerUnpublishVolume(ctx, request)
							require.NoError(t, err, "destination can replay while both finalizers are present")
							destination.Finalizers = nil
							_, err = vas.Update(ctx, destination, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.NoError(t, vas.Delete(ctx, destination.Name, metav1.DeleteOptions{}))
						}
						if mode == "prepared-abandoned-quarantine" {
							_, active, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: volume, Reason: "metadata drift"}, 1, time.Hour)
							require.NoError(t, err)
							require.True(t, active)
						}
						if mode == "prepared-abandoned-conflict" {
							prependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
								cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
								if cm.Name != volumeHistoryStateConfigMapName {
									return false, nil, nil
								}
								return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("competing phase update"))
							})
						}
						retiredWithoutVA := mode == "prepared-abandoned-retired-without-va"
						if retiredWithoutVA {
							require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
							wire.mu.Lock()
							wire.omitVMID, wire.missingVMID = vmID, vmID
							wire.mu.Unlock()
						}
						if mode == "prepared-abandoned-orphan" || retiredWithoutVA {
							r := NewAttachmentReconciler(NewControllerServer(fresh, provider, nil))
							r.orphanGrace = 0
							require.NoError(t, r.ReconcileOnce(ctx))
							require.NoError(t, r.ReconcileOnce(ctx))
						} else {
							_, err = restarted.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
						}
						current, historyErr := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, historyErr)
						if canceledDestination {
							require.NoError(t, err)
							require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
							_, err = restarted.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
							require.NoError(t, err, "source can replay after the destination retires")
							require.NoError(t, vas.Delete(ctx, publishVA.Name, metav1.DeleteOptions{}))
							_, err = restarted.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
							current, historyErr = server.readAttachmentHistory(ctx, volume)
							require.NoError(t, historyErr)
						}
						if mode == "prepared-abandoned" || mode == "prepared-abandoned-orphan" || retiredWithoutVA || canceledDestination {
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
						require.Zero(t, wire.detachAttempts)
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
					if mode == "armed-outage" || mode == "competing-arm" || mode == "armed-lost-ack-retire-conflict" || competingAfterConflict {
						require.Error(t, err)
						current, err := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
						if mode == "competing-arm" || competingAfterConflict {
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
						if armRetired {
							require.NotEqual(t, proposedArmToken, wire.attachAttempt)
						} else if interruptedAdmission {
							require.Equal(t, proposedArmToken, wire.attachAttempt)
						}
					}
					return
				}
				if invalidPublishVA || strings.HasPrefix(mode, "current-") || mode == "unknown-consumer-lifetime" ||
					mode == "unknown-container-status" || mode == "old-consumer-start-time" ||
					mode == "write-conflict" || mode == "external-disk" ||
					mode == "terminating-old-consumer" || mode == "ambiguous-old-consumer" || mode == "distinct-vm-still-owned" || mode == "distinct-vm-unreadable-original" || mode == "other-version" {
					if mode == "removed-publish-va" || mode == "replaced-publish-va" {
						require.True(t, latePublishVAChanged.Load())
					}
					wire.mu.Lock()
					defer wire.mu.Unlock()
					require.Zero(t, wire.attachAttempts)
					require.Empty(t, wire.mutations)
					return
				}
				stored, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, string(node.UID), stored.LastSuccessfulNodeUID)
				refusedBeforeDispatch := mode == "authorization-refusal" || mode == "authentication-refusal"
				if mode == "readback-failed" || mode == "lost-write-reply" || mode == "final-refusal" || refusedBeforeDispatch || mode == "pre-dispatch" {
					require.NotNil(t, stored.PendingReplacementAttach)
					require.True(t, stored.PendingReplacementAttach.prepared())
					require.Equal(t, string(publishVA.UID), stored.PendingReplacementAttach.VAUID)
					failBeforeDispatch.Store(false)
					wire.mu.Lock()
					attempts := wire.attachAttempts
					refusedToken := wire.attachFields["CSI_ATTACH_ATTEMPT"]
					require.Empty(t, wire.mutations)
					wire.loseAttachReply, wire.failMethod, wire.attachRefusal = false, "", ""
					wire.mu.Unlock()
					if mode == "final-refusal" || refusedBeforeDispatch {
						require.Equal(t, 1, attempts)
					} else {
						require.Zero(t, attempts)
					}
					retryClient := client
					if refusedBeforeDispatch {
						restarted := newStickyTestDriver(t)
						restarted.kubeRuntime.client = kube
						require.NoError(t, restarted.volumeHistory.LoadFromConfigMap(ctx))
						retryClient = makeClient(restarted)
					}
					retried, err := retryClient.ControllerPublishVolume(ctx, publish)
					require.NoError(t, err)
					require.Equal(t, "onecsi-7", retried.GetPublishContext()[publishContextDeviceSerial])
					wire.mu.Lock()
					defer wire.mu.Unlock()
					require.Equal(t, attempts+1, wire.attachAttempts)
					if refusedBeforeDispatch {
						require.NotEmpty(t, refusedToken)
						require.NotEqual(t, refusedToken, wire.attachAttempt)
					}
					require.Equal(t, []string{"attach:41:false"}, wire.mutations)
					if mode == "lost-write-reply" {
						require.NotEmpty(t, lostWriteToken)
						require.NotEqual(t, lostWriteToken, wire.attachAttempt)
					}
					return
				}
				require.NotNil(t, stored.PendingReplacementAttach, "replacement Publish returned %v", publishErr)
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
					require.NoError(t, vas.Delete(ctx, publishVA.Name, metav1.DeleteOptions{}))
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
					released := replacement.DeepCopy()
					released.Status.VolumesInUse = nil
					_, err = nodes.UpdateStatus(ctx, released, metav1.UpdateOptions{})
					require.NoError(t, err)
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
					invalidPins := strings.Contains(mode, "-missing-va-")
					wrongVA := strings.HasSuffix(mode, "-wrong-va-uid")
					hiddenPinnedVA := strings.HasSuffix(mode, "-hidden-pinned-va")
					if invalidPins {
						stored, err = driver.volumeHistory.Upsert(ctx, volume, func(history *VolumeHistoryRecord) {
							if !strings.HasSuffix(mode, "-uid") {
								history.PendingReplacementAttach.VAName = ""
							}
							if !strings.HasSuffix(mode, "-name") {
								history.PendingReplacementAttach.VAUID = ""
							}
						})
						require.NoError(t, err)
					}
					if replay {
						replayVA = publishVA.DeepCopy()
						now := metav1.Now()
						replayVA.DeletionTimestamp = &now
						replayVA.Status.Attached = true
						if invalidPins || wrongVA {
							require.NoError(t, vas.Delete(ctx, publishVA.Name, metav1.DeleteOptions{}))
							replayVA.UID = "different-deleting-va"
							if wrongVA {
								replayVA.CreationTimestamp = metav1.NewTime(replacement.CreationTimestamp.Add(-time.Second))
							}
							_, err := vas.Create(ctx, replayVA, metav1.CreateOptions{})
							require.NoError(t, err)
						} else {
							_, err := vas.Update(ctx, replayVA, metav1.UpdateOptions{})
							require.NoError(t, err)
						}
					} else if hiddenPinnedVA {
						hidden := publishVA.DeepCopy()
						otherPV := "different-pv"
						hidden.Spec.Source.PersistentVolumeName = &otherPV
						_, err := vas.Update(ctx, hidden, metav1.UpdateOptions{})
						require.NoError(t, err)
					} else {
						require.NoError(t, vas.Delete(ctx, publishVA.Name, metav1.DeleteOptions{}))
					}
					ownershipLag := strings.Contains(mode, "-detach-lag") || orphanRepublish || claimTurnover
					lagReplies, transitionalOriginalReads, originalReadsAfterImageLag := 0, 0, 0
					persistentReboot := strings.HasSuffix(mode, "-persistent-reboot")
					heldImageLag := strings.HasSuffix(mode, "-lag-deadline") || orphanRepublish || claimTurnover
					acceptanceWriteFailed := strings.HasSuffix(mode, "-acceptance-write-failed")
					_, invalidDetachReply, _ := strings.Cut(mode, "-replay-detach-reply-")
					wire.mu.Lock()
					wire.detachReply = invalidDetachReply
					wire.mu.Unlock()
					if acceptanceWriteFailed {
						prependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
							cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
							if cm.Name == volumeHistoryStateConfigMapName {
								var proposed VolumeHistoryRecord
								require.NoError(t, json.Unmarshal([]byte(cm.Data[volume]), &proposed))
								if proposed.PendingReplacementAttach != nil && proposed.PendingReplacementAttach.detachAccepted() {
									return true, nil, errors.New("acceptance write failed")
								}
							}
							return false, nil, nil
						})
					}
					originalConflict := strings.HasSuffix(mode, "-original-conflict")
					if ownershipLag {
						wire.mu.Lock()
						wire.rejectFirstDetach = strings.HasSuffix(mode, "-retry")
						wire.alterDirectImage = func(body string) string {
							if _, attached := wire.disks[vmID]; !attached && (heldImageLag || lagReplies < 9) {
								lagReplies++
								return strings.Replace(body, "<STATE>1</STATE>", "<STATE>8</STATE>", 1)
							}
							return body
						}
						if strings.HasSuffix(mode, "-reboot") || originalConflict {
							wire.alterDirectVM = func(body string) string {
								if _, attached := wire.disks[vmID]; attached {
									return body
								}
								if originalConflict {
									return strings.Replace(body, "<NAME>retired-vm</NAME>", "<NAME>unrelated-vm</NAME>", 1)
								}
								if persistentReboot || transitionalOriginalReads < 6 {
									transitionalOriginalReads++
									if lagReplies == 9 {
										originalReadsAfterImageLag++
									}
									return strings.Replace(body, "<LCM_STATE>3</LCM_STATE>", "<LCM_STATE>2</LCM_STATE>", 1)
								}
								return body
							}
						}
						wire.mu.Unlock()
					}
					var lostCompletionReply atomic.Bool
					lostRetirementAck := strings.HasSuffix(mode, "-retirement-lost-ack")
					if mode == "distinct-vm-abandoned-completion-lost-ack" || lostRetirementAck {
						prependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
							cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
							if cm.Name != volumeHistoryStateConfigMapName || lostCompletionReply.Load() {
								return false, nil, nil
							}
							var current VolumeHistoryRecord
							require.NoError(t, json.Unmarshal([]byte(cm.Data[volume]), &current))
							if current.PendingReplacementAttach != nil && current.PendingReplacementAttach.detachAccepted() {
								return false, nil, nil
							}
							if lostRetirementAck {
								require.Nil(t, current.PendingReplacementAttach)
							} else if current.PendingReplacementAttach == nil || !current.PendingReplacementAttach.detached() {
								return false, nil, nil
							}
							require.NoError(t, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.DeepCopy(), cm.Namespace))
							lostCompletionReply.Store(true)
							return true, nil, errors.New("completion update committed and reply lost")
						})
					}
					if mode == "distinct-vm-abandoned-detach-reply" || orphanLostReply {
						wire.mu.Lock()
						wire.loseDetachReply = true
						wire.mu.Unlock()
					}
					conflict := mode == "distinct-vm-abandoned-retire-conflict" || strings.Contains(mode, "replay-completion-conflict")
					vaChanged := strings.HasSuffix(mode, "completion-conflict-changed-va")
					generationChanged := strings.HasSuffix(mode, "completion-conflict-changed-generation")
					providerChanged := strings.HasSuffix(mode, "completion-conflict-changed-provider")
					var conflictInjected atomic.Bool
					if conflict {
						prependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
							cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
							if cm.Name != volumeHistoryStateConfigMapName {
								return false, nil, nil
							}
							gvr := corev1.SchemeGroupVersion.WithResource("configmaps")
							tracked, err := kube.Tracker().Get(gvr, cm.Namespace, cm.Name)
							require.NoError(t, err)
							current := tracked.(*corev1.ConfigMap).DeepCopy()
							var proposed VolumeHistoryRecord
							require.NoError(t, json.Unmarshal([]byte(cm.Data[volume]), &proposed))
							if proposed.PendingReplacementAttach != nil && proposed.PendingReplacementAttach.detachAccepted() {
								return false, nil, nil
							}
							if conflictInjected.CompareAndSwap(false, true) {
								if replay {
									require.NotNil(t, proposed.PendingReplacementAttach)
									require.True(t, proposed.PendingReplacementAttach.detached())
								} else {
									require.Nil(t, proposed.PendingReplacementAttach)
								}
								current.Data["other-volume"] = otherHistory
								current.ResourceVersion = "102"
								if generationChanged {
									var newHistory VolumeHistoryRecord
									require.NoError(t, json.Unmarshal([]byte(current.Data[volume]), &newHistory))
									newHistory.LastSuccessfulPublishTime = stored.LastSuccessfulPublishTime.Add(time.Second)
									payload, err := json.Marshal(newHistory)
									require.NoError(t, err)
									current.Data[volume] = string(payload)
								}
								require.NoError(t, kube.Tracker().Update(gvr, current, cm.Namespace))
								if providerChanged {
									wire.mu.Lock()
									wire.disks[41] = 128
									wire.mu.Unlock()
								}
								if vaChanged {
									newVA := replayVA.DeepCopy()
									newVA.UID = "replacement-during-conflict"
									require.NoError(t, kube.Tracker().Update(storagev1.SchemeGroupVersion.WithResource("volumeattachments"), newVA, ""))
								}
							}
							if cm.ResourceVersion != current.ResourceVersion {
								return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("stale resource version"))
							}
							return false, nil, nil
						})
					}
					abandoned.Store(true)
					reconcileDriver := func(owner *Driver) {
						r := NewAttachmentReconciler(NewControllerServer(owner, provider, nil))
						r.orphanGrace, r.staleVAGrace = 0, 0
						require.NoError(t, r.ReconcileOnce(ctx))
						require.NoError(t, r.ReconcileOnce(ctx))
					}
					reconcile := func() { reconcileDriver(driver) }
					unpublish := func(client csi.ControllerClient) error {
						_, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
						return err
					}
					if orphanRetirement {
						if orphanRemoteRetirement {
							_, err := nodes.Create(ctx, remoteNode, metav1.CreateOptions{})
							require.NoError(t, err)
							if !lateRemote {
								_, err = vas.Create(ctx, remoteVA, metav1.CreateOptions{})
								require.NoError(t, err)
								_, err = kube.CoreV1().Pods(pvc.Namespace).Create(ctx, remotePod, metav1.CreateOptions{})
								require.NoError(t, err)
							}
							wire.mu.Lock()
							wire.vmNames[43] = remoteNode.Name
							wire.mu.Unlock()
						}
						reconcile()
						s := NewControllerServer(driver, provider, nil)
						received, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.NotNil(t, received.PendingReplacementAttach)
						if orphanLostReply {
							require.Equal(t, stored, received)
						} else {
							require.True(t, received.PendingReplacementAttach.acceptedOrphan())
							require.Equal(t, "2", received.PendingReplacementAttach.DetachAcceptance.DiskID)
						}
						if !wireRetired && !sameNodeRebound {
							require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
							wire.mu.Lock()
							wire.omitVMID, wire.missingVMID = vmID, vmID
							wire.mu.Unlock()
						}
						fresh := newStickyTestDriver(t)
						fresh.kubeRuntime.client = kube
						retry := makeClient(fresh)
						require.Error(t, unpublish(retry), "the active observe control still holds retirement")
						setObserve(false)
						deletion := &csi.DeleteVolumeRequest{VolumeId: volume}
						_, err = retry.DeleteVolume(ctx, deletion)
						require.ErrorContains(t, err, "replacement attachment holds volume deletion")
						if terminatingRemote {
							reconcileDriver(fresh)
							current, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, received, current)
							pods := kube.CoreV1().Pods(pvc.Namespace)
							require.NoError(t, pods.Delete(ctx, remotePod.Name, metav1.DeleteOptions{}))
							waiter := remotePod.DeepCopy()
							waiter.UID = "waiting-after-terminating-pod"
							_, err = pods.Create(ctx, waiter, metav1.CreateOptions{})
							require.NoError(t, err)
						}
						if wireStaticRebind {
							if unboundDuringReply {
								reconcileDriver(fresh)
								held, err := s.readAttachmentHistory(ctx, volume)
								require.NoError(t, err)
								require.Equal(t, received, held)
								require.NoError(t, fresh.volumeQuarantine.Clear(ctx, volume))
								pvs, claims := kube.CoreV1().PersistentVolumes(), kube.CoreV1().PersistentVolumeClaims(pvc.Namespace)
								current, err := pvs.Get(ctx, pv.Name, metav1.GetOptions{})
								require.NoError(t, err)
								claim, err := claims.Get(ctx, pvc.Name, metav1.GetOptions{})
								require.NoError(t, err)
								current.Spec.ClaimRef = pv.Spec.ClaimRef.DeepCopy()
								current.Spec.ClaimRef.UID = claim.UID
								current.Status.Phase, claim.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
								_, err = pvs.Update(ctx, current, metav1.UpdateOptions{})
								require.NoError(t, err)
								_, err = claims.Update(ctx, claim, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							reconcileDriver(fresh)
							settled, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							if !orphanLostReply {
								received.PendingReplacementAttach = nil
							}
							require.Equal(t, received, settled)
							wire.mu.Lock()
							defer wire.mu.Unlock()
							require.Equal(t, []string{"attach:42:false", "detach:42"}, wire.mutations)
							return
						}
						if orphanStaticRetirement {
							pvs := kube.CoreV1().PersistentVolumes()
							claims := kube.CoreV1().PersistentVolumeClaims(pvc.Namespace)
							currentPV, currentPVC := pv.DeepCopy(), pvc.DeepCopy()
							currentPV.UID, currentPVC.UID = "rebound-pv-uid", "rebound-pvc-uid"
							reusedPV := strings.Contains(mode, "-reused-pv-")
							waitingRebind := strings.Contains(mode, "-waiting-")
							if reusedPV {
								currentPV.UID = pv.UID
							}
							if waitingRebind {
								currentPVC.CreationTimestamp = metav1.NewTime(time.Now().Add(-30 * time.Second))
								if !reusedPV {
									currentPV.CreationTimestamp = currentPVC.CreationTimestamp
								}
							}
							currentPV.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
							if !strings.Contains(mode, "same-name") {
								currentPV.Name, currentPVC.Name = "rebound-pv", "rebound-pvc"
							}
							currentPV.Spec.ClaimRef.Name, currentPV.Spec.ClaimRef.UID = currentPVC.Name, currentPVC.UID
							currentPVC.Spec.VolumeName = currentPV.Name
							if reusedPV {
								_, err = pvs.Update(ctx, currentPV, metav1.UpdateOptions{})
							} else {
								require.NoError(t, pvs.Delete(ctx, pv.Name, metav1.DeleteOptions{}))
								_, err = pvs.Create(ctx, currentPV, metav1.CreateOptions{})
							}
							require.NoError(t, err)
							stillHeld := func() {
								t.Helper()
								reconcileDriver(fresh)
								stored, err := s.readAttachmentHistory(ctx, volume)
								require.NoError(t, err)
								require.Equal(t, received, stored)
							}
							stillHeld()
							require.NoError(t, claims.Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
							_, err = claims.Create(ctx, currentPVC, metav1.CreateOptions{})
							require.NoError(t, err)
							staleVA := replacementPublishVA(pv, replacement)
							_, err = vas.Create(ctx, staleVA, metav1.CreateOptions{})
							require.NoError(t, err)
							stillHeld()
							require.NoError(t, vas.Delete(ctx, staleVA.Name, metav1.DeleteOptions{}))
							pods := kube.CoreV1().Pods(pvc.Namespace)
							waiter := latePod.DeepCopy()
							waiter.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = currentPVC.Name
							_, err = pods.Create(ctx, waiter, metav1.CreateOptions{})
							require.NoError(t, err)
							stillHeld()
							require.NoError(t, pods.Delete(ctx, waiter.Name, metav1.DeleteOptions{}))
							setCurrentObserve := func(active bool) {
								held := currentPV.DeepCopy()
								if active {
									held.Annotations[annotationRecoveryMode] = recoveryModeObserve
								}
								_, err := pvs.Update(ctx, held, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							setCurrentObserve(true)
							stillHeld()
							setCurrentObserve(false)
							wire.mu.Lock()
							wire.failMethod = "one.vm.info"
							wire.mu.Unlock()
							stillHeld()
							wire.mu.Lock()
							wire.failMethod = ""
							wire.mu.Unlock()
							var conflict atomic.Bool
							conflict.Store(true)
							prependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
								cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
								if cm.Name == volumeHistoryStateConfigMapName && conflict.Load() {
									return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("receipt compare-and-swap conflict"))
								}
								return false, nil, nil
							})
							stillHeld()
							conflict.Store(false)
							if waitingRebind {
								destination := remoteNode.DeepCopy()
								waitingVA := remoteVA.DeepCopy()
								waiter = remotePod.DeepCopy()
								if sameNodeRebound {
									destination = replacement.DeepCopy()
									waitingVA.Name = canonicalVA
									waitingVA.Spec.NodeName = destination.Name
									waiter.Spec.NodeName = destination.Name
								} else {
									_, err = nodes.Create(ctx, destination, metav1.CreateOptions{})
									require.NoError(t, err)
									wire.mu.Lock()
									wire.vmNames[43] = destination.Name
									wire.mu.Unlock()
								}
								waitingVA.Spec.Source.PersistentVolumeName = &currentPV.Name
								waitingVA.CreationTimestamp = latePod.CreationTimestamp
								waiter.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = currentPVC.Name
								now := metav1.Now()
								canceled := waitingVA.DeepCopy()
								canceled.UID = "canceled-rebound-attachment"
								canceled.DeletionTimestamp = &now
								_, err = vas.Create(ctx, canceled, metav1.CreateOptions{})
								require.NoError(t, err)
								stillHeld()
								cancelRequest := &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: destination.Name}
								active := waiter.DeepCopy()
								active.Status.ContainerStatuses[0].ContainerID = "containerd://running"
								_, err = pods.Create(ctx, active, metav1.CreateOptions{})
								require.NoError(t, err)
								_, err = retry.ControllerUnpublishVolume(ctx, cancelRequest)
								require.Error(t, err)
								require.NoError(t, pods.Delete(ctx, active.Name, metav1.DeleteOptions{}))
								setCurrentObserve(true)
								_, err = retry.ControllerUnpublishVolume(ctx, cancelRequest)
								require.Error(t, err)
								setCurrentObserve(false)
								rescheduled := strings.Contains(mode, "-rescheduled-")
								released := strings.Contains(mode, "-released-waiting-")
								if rescheduled {
									_, err = nodes.Create(ctx, remoteNode, metav1.CreateOptions{})
									require.NoError(t, err)
									wire.mu.Lock()
									wire.vmNames[43] = remoteNode.Name
									wire.mu.Unlock()
									nextVA := remoteVA.DeepCopy()
									nextVA.Spec.Source.PersistentVolumeName = &currentPV.Name
									nextVA.CreationTimestamp = latePod.CreationTimestamp
									_, err = vas.Create(ctx, nextVA, metav1.CreateOptions{})
									require.NoError(t, err)
									nextPod := remotePod.DeepCopy()
									nextPod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = currentPVC.Name
									_, err = pods.Create(ctx, nextPod, metav1.CreateOptions{})
									require.NoError(t, err)
									_, err = retry.ControllerUnpublishVolume(ctx, cancelRequest)
									require.NoError(t, err)
									stillHeld()
									require.NoError(t, vas.Delete(ctx, nextVA.Name, metav1.DeleteOptions{}))
									require.NoError(t, pods.Delete(ctx, nextPod.Name, metav1.DeleteOptions{}))
								}
								if released {
									require.NoError(t, claims.Delete(ctx, currentPVC.Name, metav1.DeleteOptions{}))
									releasedPV := currentPV.DeepCopy()
									releasedPV.Status.Phase = corev1.VolumeReleased
									releasedPV.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
									releasedPV.DeletionTimestamp = &now
									_, err = pvs.Update(ctx, releasedPV, metav1.UpdateOptions{})
									require.NoError(t, err)
									reused := currentPVC.DeepCopy()
									reused.UID, reused.Spec.VolumeName = "independent-rebound-claim", "independent-rebound-pv"
									_, err = claims.Create(ctx, reused, metav1.CreateOptions{})
									require.NoError(t, err)
									_, err = retry.ControllerUnpublishVolume(ctx, cancelRequest)
									require.Error(t, err)
									other := currentPV.DeepCopy()
									other.Name, other.UID, other.Spec.CSI.VolumeHandle = reused.Spec.VolumeName, "independent-rebound-pv", "independent-rebound-volume"
									other.Spec.ClaimRef.UID = reused.UID
									_, err = pvs.Create(ctx, other, metav1.CreateOptions{})
									require.NoError(t, err)
									if strings.Contains(mode, "-old-name-reused-") {
										reusedOld := pvc.DeepCopy()
										reusedOld.UID, reusedOld.Spec.VolumeName = "independent-old-claim", "independent-old-pv"
										reusedOld.CreationTimestamp = currentPVC.CreationTimestamp
										_, err = claims.Create(ctx, reusedOld, metav1.CreateOptions{})
										require.NoError(t, err)
										_, err = retry.ControllerUnpublishVolume(ctx, cancelRequest)
										require.Error(t, err)
										oldOther := other.DeepCopy()
										oldOther.Name, oldOther.UID, oldOther.Spec.CSI.VolumeHandle = reusedOld.Spec.VolumeName, "independent-old-pv", "independent-old-volume"
										oldOther.Spec.ClaimRef.Name, oldOther.Spec.ClaimRef.UID = reusedOld.Name, reusedOld.UID
										_, err = pvs.Create(ctx, oldOther, metav1.CreateOptions{})
										require.NoError(t, err)
									}
								}
								kube.ClearActions()
								_, err = retry.ControllerUnpublishVolume(ctx, cancelRequest)
								require.NoError(t, err)
								for _, action := range kube.Actions() {
									require.Contains(t, []string{"get", "list"}, action.GetVerb())
								}
								stillHeld()
								require.NoError(t, vas.Delete(ctx, canceled.Name, metav1.DeleteOptions{}))

								if !released {
									_, err = vas.Create(ctx, waitingVA, metav1.CreateOptions{})
									require.NoError(t, err)
									destination.Status.VolumesInUse = []corev1.UniqueVolumeName{corev1.UniqueVolumeName("kubernetes.io/csi/" + driver.name + "^" + volume)}
									_, err = nodes.UpdateStatus(ctx, destination, metav1.UpdateOptions{})
									require.NoError(t, err)
									waiter.Status.StartTime = nil
									_, err = pods.Create(ctx, waiter, metav1.CreateOptions{})
									require.NoError(t, err)
									stillHeld()
									started := waiter.CreationTimestamp
									waiter.Status.StartTime = &started
									_, err = pods.UpdateStatus(ctx, waiter, metav1.UpdateOptions{})
									require.NoError(t, err)
								}
							}
							kube.ClearActions()
							reconcileDriver(fresh)
							for _, action := range kube.Actions() {
								if action.GetVerb() == "get" || action.GetVerb() == "list" {
									continue
								}
								require.Equal(t, "update", action.GetVerb())
								require.Equal(t, volumeHistoryStateConfigMapName, action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap).Name)
							}
							expectedHistory := received
							expectedHistory.PendingReplacementAttach = nil
							cleaned, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, expectedHistory, cleaned)
							wire.mu.Lock()
							require.Equal(t, []string{"attach:42:false", "detach:42"}, wire.mutations)
							wire.expectedAttachNonce, wire.loseAttachReply = nil, false
							wire.mu.Unlock()
							if waitingRebind && !sameNodeRebound && !strings.Contains(mode, "-released-waiting-") {
								published, err := retry.ControllerPublishVolume(ctx, newPublishReq(volume, remoteNode.Name))
								require.NoError(t, err)
								require.Equal(t, "7", published.PublishContext[publishContextOpenNebulaImageID])
								wire.mu.Lock()
								defer wire.mu.Unlock()
								require.Equal(t, []string{"attach:42:false", "detach:42", "attach:43:false"}, wire.mutations)
							}
							return
						}
						if orphanLostReply {
							require.Error(t, unpublish(retry), "settled absence cannot supply a missing provider reply")
							reconcileDriver(fresh)
						} else {
							now := metav1.Now()
							otherVA := publishVA.DeepCopy()
							otherVA.UID, otherVA.DeletionTimestamp = "unrelated-deleting-attachment", &now
							_, err = vas.Create(ctx, otherVA, metav1.CreateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(retry))
							require.NoError(t, vas.Delete(ctx, otherVA.Name, metav1.DeleteOptions{}))
							pods := kube.CoreV1().Pods(pvc.Namespace)
							_, err = pods.Create(ctx, latePod, metav1.CreateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(retry), "an old-node Pod blocks orphan retirement")
							require.NoError(t, pods.Delete(ctx, latePod.Name, metav1.DeleteOptions{}))
							pvs := kube.CoreV1().PersistentVolumes()
							originalPV, err := pvs.Get(ctx, pv.Name, metav1.GetOptions{})
							require.NoError(t, err)
							changedPV := originalPV.DeepCopy()
							changedPV.Spec.ClaimRef.UID = "changed-claim"
							_, err = pvs.Update(ctx, changedPV, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(retry))
							_, err = pvs.Update(ctx, originalPV, metav1.UpdateOptions{})
							require.NoError(t, err)
							wire.mu.Lock()
							wire.failMethod = "one.vm.info"
							wire.mu.Unlock()
							require.Error(t, unpublish(retry), "a provider read failure is not retired VM proof")
							wire.mu.Lock()
							wire.failMethod, wire.missingVMID = "", 0
							wire.mu.Unlock()
							require.Error(t, unpublish(retry), "a VM pool miss is not a typed response for the pinned VM")
							wire.mu.Lock()
							wire.missingVMID = vmID
							wire.mu.Unlock()
							if orphanRemoteRetirement {
								remoteVA, err := vas.Get(ctx, remoteVA.Name, metav1.GetOptions{})
								require.NoError(t, err)
								remotePod, err := pods.Get(ctx, remotePod.Name, metav1.GetOptions{})
								require.NoError(t, err)
								conflicting := remoteVA.DeepCopy()
								conflicting.Spec.Attacher = "foreign.example"
								_, err = vas.Update(ctx, conflicting, metav1.UpdateOptions{})
								require.NoError(t, err)
								require.Error(t, unpublish(retry))
								conflicting.Spec.Attacher = remoteVA.Spec.Attacher
								conflicting.Status.Attached = true
								_, err = vas.Update(ctx, conflicting, metav1.UpdateOptions{})
								require.NoError(t, err)
								require.Error(t, unpublish(retry))
								_, err = vas.Update(ctx, remoteVA, metav1.UpdateOptions{})
								require.NoError(t, err)
								active := remotePod.DeepCopy()
								active.Status.ContainerStatuses[0].ContainerID = "containerd://running"
								_, err = pods.UpdateStatus(ctx, active, metav1.UpdateOptions{})
								require.NoError(t, err)
								require.Error(t, unpublish(retry))
								_, err = pods.UpdateStatus(ctx, remotePod, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							unsettled, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, received, unsettled)
							kube.ClearActions()
							if wireRetired {
								reconcileDriver(fresh)
							} else {
								require.NoError(t, unpublish(retry))
							}
							for _, action := range kube.Actions() {
								if action.GetVerb() == "get" || action.GetVerb() == "list" {
									continue
								}
								require.Equal(t, "update", action.GetVerb())
								require.Equal(t, volumeHistoryStateConfigMapName, action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap).Name)
							}
							received.PendingReplacementAttach = nil
						}
						settled, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, received, settled)
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, 1, wire.detachAttempts)
						require.Equal(t, []string{"attach:42:false", "detach:42"}, wire.mutations)
						return
					}
					if retiredAccepted || retiredCompleted || wireRetired {
						server := NewControllerServer(driver, provider, nil)
						first := restarted
						var replyLost atomic.Bool
						if retiredCompleted {
							first = reviewControllerClient(t, server, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
								reply, err := handler(ctx, req)
								if err == nil {
									replyLost.Store(true)
									return nil, status.Error(codes.Unavailable, "completed detach response lost")
								}
								return reply, err
							}))
						}
						require.Error(t, unpublish(first))
						require.Equal(t, retiredCompleted, replyLost.Load())
						receipt, err := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.NotNil(t, receipt.PendingReplacementAttach)
						require.Equal(t, retiredCompleted, receipt.PendingReplacementAttach.detached())
						require.True(t, receipt.PendingReplacementAttach.hasDetachReceipt())
						if retiredCompleted {
							require.False(t, receipt.LastSafeDetachTime.IsZero())
						}
						if retiredAccepted {
							setObserve(false)
						}
						if !wireRetired {
							require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
							wire.mu.Lock()
							wire.omitVMID, wire.missingVMID = vmID, vmID
							wire.mu.Unlock()
						}
						fresh := newStickyTestDriver(t)
						fresh.kubeRuntime.client = kube
						restarted = makeClient(fresh)
						wrong := replayVA.DeepCopy()
						wrong.UID = "different-receipt-uid"
						_, err = vas.Update(ctx, wrong, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish(restarted))
						_, err = vas.Update(ctx, replayVA, metav1.UpdateOptions{})
						require.NoError(t, err)
						recreated := replacement.DeepCopy()
						recreated.UID = "recreated-source-node"
						_, err = nodes.Create(ctx, recreated, metav1.CreateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish(restarted))
						require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
						wire.mu.Lock()
						wire.failMethod = "one.vm.info"
						wire.mu.Unlock()
						require.Error(t, unpublish(restarted))
						wire.mu.Lock()
						wire.failMethod = ""
						wire.disks[43] = 128
						wire.mu.Unlock()
						require.Error(t, unpublish(restarted))
						wire.mu.Lock()
						delete(wire.disks, 43)
						wire.mu.Unlock()
						current, err := server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, receipt, current)
						if retiredAccepted {
							reconcileDriver(fresh)
							observed, err := server.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.NotNil(t, observed.PendingReplacementAttach)
							require.True(t, observed.PendingReplacementAttach.detached())
						}
						require.NoError(t, unpublish(restarted))
						require.NoError(t, unpublish(restarted))
						current, err = server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.NotNil(t, current.PendingReplacementAttach)
						require.True(t, current.PendingReplacementAttach.detached())
						require.Equal(t, string(replayVA.UID), current.PendingReplacementAttach.DetachCompletion.VAUID)
						require.Equal(t, receipt.LastSafeDetachTime, current.LastSafeDetachTime)
						require.NoError(t, vas.Delete(ctx, replayVA.Name, metav1.DeleteOptions{}))
						reconcileDriver(fresh)
						current, err = server.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Nil(t, current.PendingReplacementAttach)
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, 1, wire.detachAttempts)
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID)}, wire.mutations)
						return
					}
					if orphan {
						reconcile()
					} else {
						err = unpublish(restarted)
					}
					if wireObserve || wireNodeChange || wireClaimChanged || wireClaimConflict {
						require.Error(t, err)
						require.True(t, postDetachChange.Load())
						s := NewControllerServer(driver, provider, nil)
						current, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.True(t, current.PendingReplacementAttach.detachAccepted())
						require.Equal(t, string(replayVA.UID), current.PendingReplacementAttach.DetachAcceptance.VAUID)
						require.Error(t, unpublish(restarted))
						held, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, current, held)
						switch {
						case wireNodeChange:
							_, err = nodes.Update(ctx, replacement, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(restarted), "the original node still reports volume use")
							released := replacement.DeepCopy()
							released.Status.VolumesInUse = nil
							_, err = nodes.UpdateStatus(ctx, released, metav1.UpdateOptions{})
							require.NoError(t, err)
						case wireClaimChanged || wireClaimConflict:
							if wireClaimConflict {
								require.NoError(t, kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
							}
							released := pv.DeepCopy()
							released.Status.Phase = corev1.VolumeReleased
							_, err = kube.CoreV1().PersistentVolumes().Update(ctx, released, metav1.UpdateOptions{})
							require.NoError(t, err)
						default:
							setObserve(false)
						}
						fresh := newStickyTestDriver(t)
						fresh.kubeRuntime.client = kube
						require.NoError(t, unpublish(makeClient(fresh)))
						completed, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.True(t, completed.PendingReplacementAttach.detached())
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, 1, wire.detachAttempts)
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID)}, wire.mutations)
						return
					}
					if orphanRepublish {
						if !orphan {
							require.Error(t, err)
						}
						s := NewControllerServer(driver, provider, nil)
						current, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.NotNil(t, current.PendingReplacementAttach)
						require.True(t, current.PendingReplacementAttach.acceptedOrphan())
						wire.mu.Lock()
						wire.alterDirectImage, wire.loseAttachReply = nil, false
						wire.mu.Unlock()
						require.True(t, postDetachChange.Load())
						next, err := vas.Get(ctx, canonicalVA, metav1.GetOptions{})
						require.NoError(t, err)
						require.Equal(t, types.UID("orphan-successor"), next.UID)
						require.Equal(t, canonicalVA, current.PendingReplacementAttach.VAName)
						waiting := latePod.DeepCopy()
						if remoteOrphan {
							require.Error(t, unpublish(restarted), "a nondeleting successor cannot be acknowledged")
							canceled := next.DeepCopy()
							now := metav1.Now()
							canceled.DeletionTimestamp = &now
							_, err = vas.Update(ctx, canceled, metav1.UpdateOptions{})
							require.NoError(t, err)

							foreign := remoteVA.DeepCopy()
							foreign.Spec.Attacher = "unrelated.example.com"
							_, err = vas.Update(ctx, foreign, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(restarted))
							_, err = vas.Update(ctx, remoteVA, metav1.UpdateOptions{})
							require.NoError(t, err)
							pods := kube.CoreV1().Pods(pvc.Namespace)
							used := remotePod.DeepCopy()
							used.Status.ContainerStatuses[0].ContainerID = "containerd://running"
							_, err = pods.UpdateStatus(ctx, used, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(restarted))
							_, err = pods.UpdateStatus(ctx, remotePod, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.NoError(t, unpublish(restarted))
							stillAccepted, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, current, stillAccepted)
							preserved, err := vas.Get(ctx, remoteVA.Name, metav1.GetOptions{})
							require.NoError(t, err)
							require.Equal(t, remoteVA, preserved)
							require.NoError(t, vas.Delete(ctx, canceled.Name, metav1.DeleteOptions{}))
							require.NoError(t, unpublish(restarted))
							retired, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Nil(t, retired.PendingReplacementAttach)

							require.NoError(t, pods.Delete(ctx, remotePod.Name, metav1.DeleteOptions{}))
							require.NoError(t, vas.Delete(ctx, remoteVA.Name, metav1.DeleteOptions{}))
							next.UID = "later-publication"
							_, err = vas.Create(ctx, next, metav1.CreateOptions{})
							require.NoError(t, err)
							_, err = pods.Create(ctx, waiting, metav1.CreateOptions{})
							require.NoError(t, err)
							reply, err := restarted.ControllerPublishVolume(ctx, publish)
							require.NoError(t, err)
							require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
							wire.mu.Lock()
							defer wire.mu.Unlock()
							require.Equal(t, 1, wire.detachAttempts)
							require.Equal(t, []string{"attach:42:false", "detach:42", "attach:42:false"}, wire.mutations)
							return
						}
						_, err = kube.CoreV1().Pods(pvc.Namespace).Create(ctx, waiting, metav1.CreateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish(restarted), "old Unpublish cannot acknowledge the live successor")
						fresh := newStickyTestDriver(t)
						fresh.kubeRuntime.client = kube
						retry := makeClient(fresh)
						rejectPublish := func() {
							t.Helper()
							_, err := retry.ControllerPublishVolume(ctx, publish)
							require.Error(t, err)
							observed, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, current, observed)
							wire.mu.Lock()
							defer wire.mu.Unlock()
							require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID)}, wire.mutations)
						}
						alternate, err := driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
							attempt := *record.PendingReplacementAttach
							attempt.VAName = "different-original-name"
							record.PendingReplacementAttach = &attempt
						})
						require.NoError(t, err)
						_, err = retry.ControllerPublishVolume(ctx, publish)
						require.Error(t, err, "a canonical successor cannot replace an alternate pinned name")
						observed, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, alternate, observed)
						_, err = driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
							record.PendingReplacementAttach = current.PendingReplacementAttach
						})
						require.NoError(t, err)
						foreign := next.DeepCopy()
						foreign.Spec.Attacher = "foreign.example.com"
						_, err = vas.Update(ctx, foreign, metav1.UpdateOptions{})
						require.NoError(t, err)
						rejectPublish()
						_, err = vas.Update(ctx, next, metav1.UpdateOptions{})
						require.NoError(t, err)
						changed := next.DeepCopy()
						changed.UID = "changed-orphan-successor"
						var changeSuccessor atomic.Bool
						changeSuccessor.Store(true)
						prependReactor("get", "volumeattachments", func(action ktesting.Action) (bool, kruntime.Object, error) {
							if action.(ktesting.GetAction).GetName() == current.PendingReplacementAttach.VAName &&
								changeSuccessor.CompareAndSwap(true, false) {
								if err := kube.Tracker().Update(storagev1.SchemeGroupVersion.WithResource("volumeattachments"), changed, ""); err != nil {
									return true, nil, err
								}
							}
							return false, nil, nil
						})
						rejectPublish()
						require.False(t, changeSuccessor.Load(), "successor changed during attachment verification")
						_, err = vas.Update(ctx, next, metav1.UpdateOptions{})
						require.NoError(t, err)
						wrongClaim := pvc.DeepCopy()
						wrongClaim.Spec.VolumeName = "wrong-binding"
						_, err = kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, wrongClaim, metav1.UpdateOptions{})
						require.NoError(t, err)
						rejectPublish()
						_, err = kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
						require.NoError(t, err)
						used := waiting.DeepCopy()
						used.Status.ContainerStatuses[0].ContainerID = "containerd://running"
						_, err = kube.CoreV1().Pods(pvc.Namespace).UpdateStatus(ctx, used, metav1.UpdateOptions{})
						require.NoError(t, err)
						rejectPublish()
						_, err = kube.CoreV1().Pods(pvc.Namespace).UpdateStatus(ctx, waiting, metav1.UpdateOptions{})
						require.NoError(t, err)
						wire.mu.Lock()
						wire.failMethod = "one.system.version"
						wire.mu.Unlock()
						rejectPublish()
						wire.mu.Lock()
						wire.failMethod = ""
						wire.mu.Unlock()
						if !orphan {
							canceled := next.DeepCopy()
							now := metav1.Now()
							canceled.DeletionTimestamp = &now
							_, err = vas.Update(ctx, canceled, metav1.UpdateOptions{})
							require.NoError(t, err)
							pods := kube.CoreV1().Pods(pvc.Namespace)
							_, err = pods.UpdateStatus(ctx, used, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(retry), "a consumer with actual use still references the canceled successor")
							_, err = pods.UpdateStatus(ctx, waiting, metav1.UpdateOptions{})
							require.NoError(t, err)
							var finalPodChange atomic.Pointer[corev1.Pod]
							var finalPodRead atomic.Bool
							prependReactor("get", "volumeattachments", func(action ktesting.Action) (bool, kruntime.Object, error) {
								if action.(ktesting.GetAction).GetName() == canceled.Name && finalPodChange.Load() != nil {
									finalPodRead.Store(true)
								}
								return false, nil, nil
							})
							prependReactor("list", "pods", func(ktesting.Action) (bool, kruntime.Object, error) {
								if finalPodRead.Swap(false) {
									if changed := finalPodChange.Swap(nil); changed != nil {
										return false, nil, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), changed, changed.Namespace)
									}
								}
								return false, nil, nil
							})
							changedWait := waiting.DeepCopy()
							changedWait.Status.ContainerStatuses[0].State.Waiting.Reason = "PodInitializing"
							finalPodChange.Store(changedWait)
							require.Error(t, unpublish(retry), "changed pre-mount evidence is rejected even with the same UID and resource version")
							require.Nil(t, finalPodChange.Load())
							_, err = pods.UpdateStatus(ctx, waiting, metav1.UpdateOptions{})
							require.NoError(t, err)
							relabeled := waiting.DeepCopy()
							relabeled.Labels = map[string]string{"review": "unrelated"}
							relabeled.ResourceVersion = "200"
							finalPodChange.Store(relabeled)
							require.NoError(t, unpublish(retry), "unrelated Pod metadata preserves the waiting proof")
							require.Nil(t, finalPodChange.Load())
							require.NoError(t, kube.CoreV1().Pods(pvc.Namespace).Delete(ctx, waiting.Name, metav1.DeleteOptions{}))

							mounted := replacement.DeepCopy()
							_, err = nodes.UpdateStatus(ctx, mounted, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(retry), "mount use blocks canceled successor acknowledgement")
							mounted.Status.VolumesInUse = nil
							_, err = nodes.UpdateStatus(ctx, mounted, metav1.UpdateOptions{})
							require.NoError(t, err)

							stale := canceled.DeepCopy()
							stale.UID = types.UID(current.PendingReplacementAttach.VAUID)
							_, err = vas.Update(ctx, stale, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(retry), "the original attachment UID cannot be a successor")
							require.NoError(t, vas.Delete(ctx, canceled.Name, metav1.DeleteOptions{}))
							wrongName := canceled.DeepCopy()
							wrongName.Name = "unrelated-deleting-attachment"
							_, err = vas.Create(ctx, wrongName, metav1.CreateOptions{})
							require.NoError(t, err)
							require.Error(t, unpublish(retry), "the successor must use the pinned canonical name")
							require.NoError(t, vas.Delete(ctx, wrongName.Name, metav1.DeleteOptions{}))
							_, err = vas.Create(ctx, canceled, metav1.CreateOptions{})
							require.NoError(t, err)
							require.NoError(t, unpublish(retry))
							stillAccepted, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, current, stillAccepted, "acknowledgement leaves the accepted orphan receipt intact")
							require.NoError(t, vas.Delete(ctx, canceled.Name, metav1.DeleteOptions{}))
							require.NoError(t, unpublish(retry))
							retired, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Nil(t, retired.PendingReplacementAttach)
							next.UID = "later-publication"
							_, err = vas.Create(ctx, next, metav1.CreateOptions{})
							require.NoError(t, err)
							_, err = kube.CoreV1().Pods(pvc.Namespace).Create(ctx, waiting, metav1.CreateOptions{})
							require.NoError(t, err)
						} else {
							worker := newStickyTestDriver(t)
							worker.kubeRuntime.client = kube
							worker.volumeHistory = NewVolumeHistoryManager(worker.kubeRuntime, driver.volumeHistory.namespace)
							worker.PluginConfig.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, true)
							reconcileDriver(worker)
							settled, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Nil(t, settled.PendingReplacementAttach)
						}
						reply, err := retry.ControllerPublishVolume(ctx, publish)
						require.NoError(t, err)
						require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
						require.Error(t, unpublish(retry))
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, 1, wire.detachAttempts)
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID), fmt.Sprintf("attach:%d:false", vmID)}, wire.mutations)
						return
					}
					if persistentReboot || originalConflict || heldImageLag {
						require.Error(t, err)
						s := NewControllerServer(driver, provider, nil)
						current, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						accepted := current.PendingReplacementAttach
						require.NotNil(t, accepted)
						require.True(t, accepted.detachAccepted())
						require.Equal(t, stored.PendingReplacementAttach.ID, accepted.ID)
						require.Equal(t, string(replayVA.UID), accepted.DetachAcceptance.VAUID)
						require.Error(t, unpublish(restarted))
						wire.mu.Lock()
						if originalConflict {
							require.Less(t, lagReplies, 9, "an original VM name conflict must stop interim polling")
							wire.mu.Unlock()
							return
						}
						if persistentReboot {
							require.Equal(t, 9, lagReplies)
							require.Positive(t, transitionalOriginalReads)
						}
						wire.alterDirectVM, wire.alterDirectImage = nil, nil
						wire.mu.Unlock()

						if claimTurnover {
							require.Error(t, unpublish(restarted), "acceptance alone cannot settle a Bound PV after claim turnover")
							released := pv.DeepCopy()
							released.Status.Phase = corev1.VolumeReleased
							_, err = kube.CoreV1().PersistentVolumes().Update(ctx, released, metav1.UpdateOptions{})
							require.NoError(t, err)

							if wireClaimPending {
								claims := kube.CoreV1().PersistentVolumeClaims(pvc.Namespace)
								pending, err := claims.Get(ctx, pvc.Name, metav1.GetOptions{})
								require.NoError(t, err)
								require.Equal(t, corev1.ClaimPending, pending.Status.Phase)
								conflict := pending.DeepCopy()
								conflict.Spec.VolumeName = pv.Name
								_, err = claims.Update(ctx, conflict, metav1.UpdateOptions{})
								require.NoError(t, err)
								require.Error(t, unpublish(restarted), "a Pending claim pointing at the original PV blocks settlement")
								_, err = claims.Update(ctx, pending, metav1.UpdateOptions{})
								require.NoError(t, err)
								pods := kube.CoreV1().Pods(pvc.Namespace)
								active := latePod.DeepCopy()
								active.Status.Phase = corev1.PodRunning
								active.CreationTimestamp = metav1.NewTime(pending.CreationTimestamp.Add(time.Second))
								_, err = pods.Create(ctx, active, metav1.CreateOptions{})
								require.NoError(t, err)
								require.Error(t, unpublish(restarted), "a newer Pod using the same-name Pending claim still blocks release")
								require.NoError(t, pods.Delete(ctx, active.Name, metav1.DeleteOptions{}))
								waiter := active.DeepCopy()
								waiter.Spec.NodeName = ""
								waiter.Status = corev1.PodStatus{Phase: corev1.PodPending}
								old := waiter.DeepCopy()
								old.CreationTimestamp = pending.CreationTimestamp
								_, err = pods.Create(ctx, old, metav1.CreateOptions{})
								require.NoError(t, err)
								require.Error(t, unpublish(restarted), "ambiguous claim lineage cannot release the old VA")
								require.NoError(t, pods.Delete(ctx, old.Name, metav1.DeleteOptions{}))
								_, err = pods.Create(ctx, waiter, metav1.CreateOptions{})
								require.NoError(t, err)
							}
						}

						fresh := newStickyTestDriver(t)
						fresh.kubeRuntime.client = kube
						retry := makeClient(fresh)
						_, err = driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
							record.PendingReplacementAttach.DetachAcceptance.VMName = ""
						})
						require.NoError(t, err)
						require.Error(t, unpublish(retry), "malformed acceptance cannot finish a detach")
						_, err = driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
							record.PendingReplacementAttach = accepted
						})
						require.NoError(t, err)
						changed := replayVA.DeepCopy()
						changed.UID = "different-accepted-attachment"
						_, err = vas.Update(ctx, changed, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.Error(t, unpublish(retry), "changed attachment cannot finish an accepted detach")
						_, err = vas.Update(ctx, replayVA, metav1.UpdateOptions{})
						require.NoError(t, err)
						if persistentReboot {
							worker := newStickyTestDriver(t)
							worker.kubeRuntime.client = kube
							worker.volumeHistory = NewVolumeHistoryManager(worker.kubeRuntime, driver.volumeHistory.namespace)
							worker.PluginConfig.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, true)
							reconcileDriver(worker)
							settled, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.True(t, settled.PendingReplacementAttach.detached(), "the reconciler settles an accepted detach after restart")
						}
						require.NoError(t, unpublish(retry))
						settled, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.True(t, settled.PendingReplacementAttach.detached())
						require.Equal(t, string(replayVA.UID), settled.PendingReplacementAttach.DetachCompletion.VAUID)
						if wireClaimPending {
							require.NoError(t, kube.CoreV1().Pods(pvc.Namespace).Delete(ctx, latePod.Name, metav1.DeleteOptions{}))
						}
						if wirePendingBinding {
							core := kube.CoreV1()
							claims := core.PersistentVolumeClaims(pvc.Namespace)
							pending, err := claims.Get(ctx, pvc.Name, metav1.GetOptions{})
							require.NoError(t, err)
							other, err := core.PersistentVolumes().Get(ctx, pending.Spec.VolumeName, metav1.GetOptions{})
							require.NoError(t, err)
							require.Equal(t, corev1.VolumePending, other.Status.Phase)
							other.Status.Phase = corev1.VolumeBound
							_, err = core.PersistentVolumes().UpdateStatus(ctx, other, metav1.UpdateOptions{})
							require.NoError(t, err)
							require.NoError(t, unpublish(retry))
							pending.Status.Phase = corev1.ClaimBound
							_, err = claims.UpdateStatus(ctx, pending, metav1.UpdateOptions{})
							require.NoError(t, err)
						}
						require.NoError(t, unpublish(retry))
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, 1, wire.detachAttempts)
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID)}, wire.mutations)
						return
					}
					if acceptanceWriteFailed || invalidDetachReply != "" {
						require.Error(t, err)
						current, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored, current)
						if invalidDetachReply != "failed" {
							require.Error(t, unpublish(restarted), "absence without durable acceptance must hold")
						}
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, 1, wire.detachAttempts)
						return
					}
					if invalidPins || wrongVA || hiddenPinnedVA {
						require.Equal(t, codes.Unavailable, status.Code(err))
						current, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						require.Equal(t, stored, current)
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Zero(t, wire.detachAttempts)
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID)}, wire.mutations)
						return
					}
					if conflict {
						require.True(t, conflictInjected.Load())
						cm, readErr := kube.CoreV1().ConfigMaps(driver.volumeHistory.namespace).Get(ctx, volumeHistoryStateConfigMapName, metav1.GetOptions{})
						require.NoError(t, readErr)
						require.Equal(t, otherHistory, cm.Data["other-volume"])
					}
					if lostRetirementAck {
						require.True(t, lostCompletionReply.Load())
						require.NoError(t, err)
						current, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						stored.PendingReplacementAttach = nil
						require.Equal(t, node.Name, current.LastSafeDetachNodeName)
						require.False(t, current.LastSafeDetachTime.IsZero())
						stored.LastSafeDetachNodeName, stored.LastSafeDetachTime = current.LastSafeDetachNodeName, current.LastSafeDetachTime
						require.Equal(t, stored, current)
						wire.mu.Lock()
						defer wire.mu.Unlock()
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID)}, wire.mutations)
						return
					}
					if vaChanged || generationChanged || providerChanged || latePodArrival || lateProtection {
						if replay && latePodArrival {
							require.NoError(t, err)
						} else {
							require.Error(t, err)
						}
						if latePodArrival || lateProtection {
							require.True(t, postDetachChange.Load())
						}
						s := NewControllerServer(driver, provider, nil)
						current, err := s.readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						expected := stored
						if replay && latePodArrival {
							require.Equal(t, node.Name, current.LastSafeDetachNodeName)
							require.False(t, current.LastSafeDetachTime.IsZero())
							expected.LastSafeDetachNodeName, expected.LastSafeDetachTime = current.LastSafeDetachNodeName, current.LastSafeDetachTime
						}
						if generationChanged {
							expected.LastSuccessfulPublishTime = expected.LastSuccessfulPublishTime.Add(time.Second)
						}
						require.NotNil(t, current.PendingReplacementAttach)
						require.Equal(t, stored.PendingReplacementAttach.ID, current.PendingReplacementAttach.ID)
						if !replay || !latePodArrival {
							require.True(t, current.PendingReplacementAttach.detachAccepted())
							require.Empty(t, current.PendingReplacementAttach.DetachCompletion.VAUID)
						}
						expected.PendingReplacementAttach = current.PendingReplacementAttach
						switch {
						case generationChanged || providerChanged:
						case replay:
							if latePodArrival {
								require.True(t, current.PendingReplacementAttach.detached())
								require.Equal(t, string(replayVA.UID), current.PendingReplacementAttach.DetachCompletion.VAUID)
							} else {
								require.Equal(t, string(replayVA.UID), current.PendingReplacementAttach.DetachAcceptance.VAUID)
							}
							fresh := newStickyTestDriver(t)
							fresh.kubeRuntime.client = kube
							retry := makeClient(fresh)
							if latePodArrival {
								currentPod, err := kube.CoreV1().Pods(pvc.Namespace).Get(ctx, latePod.Name, metav1.GetOptions{})
								require.NoError(t, err)
								currentPod.Status.ContainerStatuses[0].ContainerID = "containerd://running-writer"
								_, err = kube.CoreV1().Pods(pvc.Namespace).UpdateStatus(ctx, currentPod, metav1.UpdateOptions{})
								require.NoError(t, err)
								require.Error(t, unpublish(retry))
								currentPod.Status.ContainerStatuses[0].ContainerID = ""
								_, err = kube.CoreV1().Pods(pvc.Namespace).UpdateStatus(ctx, currentPod, metav1.UpdateOptions{})
								require.NoError(t, err)
							} else {
								require.Error(t, unpublish(retry))
							}
							if lateProtection {
								setObserve(false)
							}
							if vaChanged {
								_, err := vas.Update(ctx, replayVA, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							require.NoError(t, unpublish(retry))
							require.NoError(t, vas.Delete(ctx, replayVA.Name, metav1.DeleteOptions{}))
							r := NewAttachmentReconciler(NewControllerServer(fresh, provider, nil))
							require.NoError(t, r.ReconcileOnce(ctx))
							retired, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							original := expected
							original.PendingReplacementAttach = nil
							require.Equal(t, original, retired)
						default:
							require.Error(t, unpublish(restarted))
							if lateProtection {
								setObserve(false)
							}
							if latePodArrival {
								require.NoError(t, kube.CoreV1().Pods(pvc.Namespace).Delete(ctx, latePod.Name, metav1.DeleteOptions{}))
								currentNode, err := nodes.Get(ctx, node.Name, metav1.GetOptions{})
								require.NoError(t, err)
								currentNode.Status.VolumesInUse = nil
								_, err = nodes.UpdateStatus(ctx, currentNode, metav1.UpdateOptions{})
								require.NoError(t, err)
							}
							require.NoError(t, unpublish(restarted))
							retired, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Nil(t, retired.PendingReplacementAttach)
						}
						require.Equal(t, expected, current)
						wire.mu.Lock()
						require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID)}, wire.mutations)
						wire.loseAttachReply = false
						wire.mu.Unlock()
						if replay && latePodArrival {
							nextVA := replacementPublishVA(pv, replacement)
							nextVA.Name, nextVA.UID = "waiting-pod-va", "waiting-pod-va-uid"
							_, err := vas.Create(ctx, nextVA, metav1.CreateOptions{})
							require.NoError(t, err)
							reply, err := restarted.ControllerPublishVolume(ctx, publish)
							require.NoError(t, err)
							require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
							wire.mu.Lock()
							defer wire.mu.Unlock()
							require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID), fmt.Sprintf("attach:%d:false", vmID)}, wire.mutations)
						}
						return
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
						fresh.PluginConfig.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, false)
						retry := makeClient(fresh)
						require.NoError(t, unpublish(retry))

						workerRetires := strings.Contains(mode, "-worker-pv-")
						publishRetires := strings.Contains(mode, "-publish-pv-")
						deleteRetires := strings.Contains(mode, "-delete-pv-")
						if workerRetires || publishRetires || deleteRetires {
							pvs := kube.CoreV1().PersistentVolumes()
							require.NoError(t, pvs.Delete(ctx, pv.Name, metav1.DeleteOptions{}))
							if strings.HasSuffix(mode, "-pv-replaced") {
								replacementPV := pv.DeepCopy()
								replacementPV.UID = "replacement-pv"
								if deleteRetires {
									replacementPV.Status.Phase = corev1.VolumeReleased
									require.NoError(t, kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{}))
								}
								_, err = pvs.Create(ctx, replacementPV, metav1.CreateOptions{})
								require.NoError(t, err)
							}
							require.Error(t, unpublish(retry))
							if publishRetires {
								_, err = kube.CoreV1().Pods(pvc.Namespace).Create(ctx, consumer, metav1.CreateOptions{})
								require.NoError(t, err)
							}
							deleteVolume := &csi.DeleteVolumeRequest{VolumeId: volume}
							switch {
							case workerRetires:
								reconcile()
							case publishRetires:
								_, err = retry.ControllerPublishVolume(ctx, publish)
								require.Equal(t, codes.Unavailable, status.Code(err))
							case deleteRetires:
								_, err = retry.DeleteVolume(ctx, deleteVolume)
								require.Equal(t, codes.Unavailable, status.Code(err))
							}
							current, err := s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Equal(t, confirmed, current)
							vas := kube.StorageV1().VolumeAttachments()
							require.NoError(t, vas.Delete(ctx, replayVA.Name, metav1.DeleteOptions{}))
							if publishRetires {
								nextVA := replacementPublishVA(pv, replacement)
								nextVA.Name, nextVA.UID = "next-publish", "next-publish-uid"
								_, err = vas.Create(ctx, nextVA, metav1.CreateOptions{})
								require.NoError(t, err)
							}
							if publishRetires || deleteRetires {
								var unavailable atomic.Bool
								unavailable.Store(true)
								prependReactor("get", "volumeattachments", func(action ktesting.Action) (bool, kruntime.Object, error) {
									if unavailable.Load() && action.(ktesting.GetAction).GetName() == replayVA.Name {
										return true, nil, errors.New("VolumeAttachment read unavailable")
									}
									return false, nil, nil
								})
								if publishRetires {
									_, err = retry.ControllerPublishVolume(ctx, publish)
								} else {
									_, err = retry.DeleteVolume(ctx, deleteVolume)
								}
								require.Equal(t, codes.Unavailable, status.Code(err))
								current, err := s.readAttachmentHistory(ctx, volume)
								require.NoError(t, err)
								require.Equal(t, confirmed, current)
								unavailable.Store(false)
							}
							expected := []string{fmt.Sprintf("attach:%d:false", vmID), fmt.Sprintf("detach:%d", vmID)}
							wire.mu.Lock()
							require.Equal(t, expected, wire.mutations)
							requireAttachDiskIdentity(t, wire.attachFields, 7, stored.PendingReplacementAttach.ID, false)
							wire.loseAttachReply = false
							wire.allowImageDelete = deleteRetires
							wire.mu.Unlock()
							switch {
							case workerRetires:
								r := NewAttachmentReconciler(NewControllerServer(fresh, provider, nil))
								require.NoError(t, r.ReconcileOnce(ctx))
							case publishRetires:
								reply, err := retry.ControllerPublishVolume(ctx, publish)
								require.NoError(t, err)
								require.Equal(t, "onecsi-7", reply.GetPublishContext()[publishContextDeviceSerial])
								expected = append(expected, fmt.Sprintf("attach:%d:false", vmID))
							case deleteRetires:
								_, err := retry.DeleteVolume(ctx, deleteVolume)
								require.NoError(t, err)
								expected = append(expected, "image-delete:7")
							}
							current, err = s.readAttachmentHistory(ctx, volume)
							require.NoError(t, err)
							require.Nil(t, current.PendingReplacementAttach)
							if !publishRetires {
								confirmed.PendingReplacementAttach = nil
								require.Equal(t, confirmed, current)
							}
							wire.mu.Lock()
							defer wire.mu.Unlock()
							require.Equal(t, expected, wire.mutations)
							return
						}
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
						if ownershipLag {
							require.Equal(t, 9, lagReplies)
							if strings.HasSuffix(mode, "-reboot") {
								require.Greater(t, originalReadsAfterImageLag, 1, "original VM must still be booting after image convergence")
							}
							if wire.rejectFirstDetach {
								require.Equal(t, 2, wire.detachAttempts)
							} else {
								require.Equal(t, 1, wire.detachAttempts)
							}
						}
						requireAttachDiskIdentity(t, wire.attachFields, 7, stored.PendingReplacementAttach.ID, false)
						return
					}
					if interruptedCleanup {
						require.Error(t, err)
						current, err := NewControllerServer(driver, provider, nil).readAttachmentHistory(ctx, volume)
						require.NoError(t, err)
						if lostReadback {
							require.NotNil(t, current.PendingReplacementAttach)
							require.True(t, current.PendingReplacementAttach.detachAccepted())
							require.Equal(t, stored.PendingReplacementAttach.ID, current.PendingReplacementAttach.ID)
							stored.PendingReplacementAttach = current.PendingReplacementAttach
						} else {
							require.Equal(t, stored.PendingReplacementAttach, current.PendingReplacementAttach)
						}
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
					case "distinct-vm-abandoned", "distinct-vm-orphan", "distinct-vm-abandoned-released", "distinct-vm-orphan-released", "distinct-vm-abandoned-retire-conflict":
						require.NoError(t, err)
						require.Equal(t, []string{"attach:42:false", "detach:42"}, wire.mutations)
						require.Nil(t, current.PendingReplacementAttach)
					case "distinct-vm-abandoned-readback", "distinct-vm-abandoned-no-disk-orphan":
						if mode != "distinct-vm-abandoned-no-disk-orphan" {
							require.NoError(t, err)
						}
						require.Equal(t, []string{"attach:42:false", "detach:42"}, wire.mutations)
						require.Nil(t, current.PendingReplacementAttach)
					case "distinct-vm-abandoned-no-disk-stale-va", "distinct-vm-abandoned-no-disk-original-owner", "distinct-vm-abandoned-detach-reply":
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
				if mode == "unknown" || mode == "authorization-text" || mode == "authorization-malformed" || mode == "authentication-text" {
					require.Empty(t, wire.mutations)
				} else {
					require.Equal(t, []string{fmt.Sprintf("attach:%d:false", vmID)}, wire.mutations)
					requireAttachDiskIdentity(t, wire.attachFields, 7, stored.PendingReplacementAttach.ID, false)
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

type lateQuarantineAttachProvider struct {
	*opennebula.PersistentDiskVolumeProvider
	beforeAttach func()
}

func (p lateQuarantineAttachProvider) AttachVolume(ctx context.Context, volume, node string, readOnly bool, params map[string]string) error {
	p.beforeAttach()
	return p.PersistentDiskVolumeProvider.AttachVolume(ctx, volume, node, readOnly, params)
}

func TestControllerAttachHonorsLateVolumeWideHostQuarantine(t *testing.T) {
	for _, tc := range []struct {
		name, volume string
		failures     int
		before       bool
		malformed    bool
	}{
		{name: "late-observation", volume: "volume-test", failures: 1},
		{name: "late-hold", volume: "volume-test", failures: 2},
		{name: "late-missing-expiry", volume: "volume-test", malformed: true},
		{name: "early-observation", volume: "volume-test", failures: 1, before: true},
		{name: "early-hold", volume: "volume-test", failures: 2, before: true},
		{name: "unrelated-hold", volume: "unrelated-volume", failures: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			delete(wire.disks, 41)
			wire.marker = 128 * 1024 * 1024
			wire.mu.Unlock()

			record := func() {
				manager := driver.hostArtifactQuarantine
				state := hostArtifactStateFromConflict(tc.volume, &opennebula.HostArtifactConflictError{
					Target: opennebula.HostArtifactAttachmentTarget{VMID: 42, DiskID: 2}, RawMessage: "private fixture detail",
				})
				if tc.malformed {
					const record = `{"key":"vm-42.disk-2","volumeID":"volume-test","vmID":42,"diskID":2,"failureCount":1,"firstObservedAt":"2026-01-01T00:00:00Z","lastObservedAt":"2026-01-01T00:00:00Z"}`
					require.NoError(t, driver.kubeRuntime.UpsertConfigMapData(ctx, "default", hostArtifactStateConfigMapName, map[string]string{state.Key: record}))
					return
				}
				for failure := 1; failure <= tc.failures; failure++ {
					persisted, active, err := manager.MarkFailure(ctx, state, 2, time.Hour)
					require.NoError(t, err)
					require.Equal(t, failure, persisted.FailureCount)
					require.Equal(t, failure == 2, active)
					require.Equal(t, failure == 1, persisted.ExpiresAt.IsZero())
				}
			}
			if tc.before {
				record()
			}
			var called atomic.Bool
			probe := lateQuarantineAttachProvider{PersistentDiskVolumeProvider: provider, beforeAttach: func() {
				called.Store(true)
				if !tc.before {
					record()
				}
			}}
			reply, err := reviewControllerClient(t, NewControllerServer(driver, probe, nil)).
				ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-old"))
			wantCode := codes.OK
			if tc.malformed {
				wantCode = codes.Unavailable
			} else if tc.volume == "volume-test" && tc.failures == 2 {
				wantCode = codes.FailedPrecondition
			}
			require.Equal(t, !tc.before || wantCode == codes.OK, called.Load())
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if wantCode != codes.OK {
				require.Equal(t, wantCode, status.Code(err))
				require.Nil(t, reply)
				require.Zero(t, wire.attachAttempts)
				require.Empty(t, wire.mutations)
			} else {
				require.NoError(t, err)
				require.Equal(t, "vdb", reply.GetPublishContext()["volumeName"])
				require.Equal(t, []string{"attach:41:false"}, wire.mutations)
			}
		})
	}
}

func TestProviderMutationCensusPreservesKubernetesNodeName(t *testing.T) {
	for _, lateExact := range []bool{false, true} {
		t.Run(fmt.Sprintf("late_exact=%t", lateExact), func(t *testing.T) {
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.vmNames = map[int]string{41: "NODE-OLD"}
			wire.disks[41], wire.marker = 128, 256*1024*1024
			if lateExact {
				wire.extendedPoolExtraVM = "<VM><ID>43</ID><NAME>node-old</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE/></VM>"
			}
			wire.mu.Unlock()

			ctx := context.Background()
			err := provider.VerifyAttachmentBackend(ctx, "volume-test", "node-old", 7, 41, "local")
			if lateExact {
				require.ErrorIs(t, err, opennebula.ErrConflictingAttachmentEvidence)
			} else {
				require.NoError(t, err)
			}
			ctx = opennebula.WithPublishMutationAuthorization(ctx, "volume-test", "node-old", 41, 7, func(context.Context) error { return nil })
			err = provider.RestoreVolumeCapacity(ctx, "volume-test", 41, 7)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if lateExact {
				require.ErrorContains(t, err, "VM identity changed in the attachment census")
				require.Empty(t, wire.mutations)
				require.Equal(t, 128, wire.disks[41])
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"resize:41:256", "marker:268435456"}, wire.mutations)
				require.Equal(t, 256, wire.disks[41])
			}
		})
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
				if method == "one.vmpool.infoextended" && attempts == 1 && mode != "during-inspection" && !later.Load() {
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

func TestProviderAttachRetryHonorsHotplugDeadline(t *testing.T) {
	for _, mode := range []string{"authorization-expires", "ready-before-deadline", "lost-first-reply"} {
		t.Run(mode, func(t *testing.T) {
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			delete(wire.disks, 41)
			wire.marker = 128 * 1024 * 1024
			wire.rejectFirstAttach = mode != "lost-first-reply"
			wire.abortFirstAttach = mode == "lost-first-reply"
			wire.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			checks := 0
			ctx = opennebula.WithPublishMutationAuthorization(ctx, "volume-test", "node-old", 41, 7, func(ctx context.Context) error {
				checks++
				if checks == 2 && mode == "authorization-expires" {
					timer := time.NewTimer(1200 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-ctx.Done():
					case <-timer.C:
					}
				}
				return nil
			})
			err := provider.AttachVolume(ctx, "volume-test", "node-old", false, nil)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			switch mode {
			case "authorization-expires":
				var timeout *opennebula.HotplugTimeoutError
				require.ErrorAs(t, err, &timeout)
				require.True(t, timeout.LastObservedReady)
				require.Equal(t, 2, checks)
				require.Equal(t, 1, wire.attachAttempts)
				require.Empty(t, wire.mutations)
			case "ready-before-deadline":
				require.NoError(t, err)
				require.Equal(t, 2, checks)
				require.Equal(t, 2, wire.attachAttempts)
				require.Equal(t, []string{"attach:41:false"}, wire.mutations)
			case "lost-first-reply":
				require.Error(t, err)
				require.Equal(t, 1, checks)
				require.Equal(t, 1, wire.attachAttempts)
				require.Empty(t, wire.mutations)
			}
		})
	}
}

func TestRecreatedNodeConvergesOldVolumeAttachment(t *testing.T) {
	for _, distinctVM := range []bool{false, true} {
		for _, mode := range []string{"unpublish", "queued-unpublish", "reconcile", "running", "terminating", "unknown-use", "competing-owner", "unreadable-provider", "new-va", "replaced-va"} {
			t.Run(fmt.Sprintf("distinct=%t/%s", distinctVM, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
				pvc.Spec.AccessModes = pv.Spec.AccessModes
				node := newReadyNode("node-old", true)
				node.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
				pv.CreationTimestamp, pvc.CreationTimestamp = node.CreationTimestamp, node.CreationTimestamp
				driver := newStickyTestDriver(t, pv, pvc, node)
				kube := driver.kubeRuntime.client
				vas := kube.StorageV1().VolumeAttachments()
				va := replacementPublishVA(pv, node)
				va.Status.Attached = true
				va.Status.AttachmentMetadata = map[string]string{"volumeName": "vdb", publishContextDeviceSerial: "onecsi-7", publishContextOpenNebulaImageID: "7"}
				reconcile := mode == "reconcile" || mode == "replaced-va"
				if !reconcile {
					now := metav1.Now()
					va.DeletionTimestamp = &now
				}
				var afterReplacement, vaChanged atomic.Bool
				provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if mode == "replaced-va" && afterReplacement.Load() && method == "one.system.version" && vaChanged.CompareAndSwap(false, true) {
						require.NoError(t, vas.Delete(ctx, va.Name, metav1.DeleteOptions{}))
						next := va.DeepCopy()
						next.UID, next.ResourceVersion = "different-va", "102"
						next.CreationTimestamp = metav1.Now()
						_, err := vas.Create(ctx, next, metav1.CreateOptions{})
						require.NoError(t, err)
					}
				})
				wire.mu.Lock()
				wire.ceph, wire.version = true, "7.4.0"
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				client := reviewControllerClient(t, server)
				_, err := client.ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
				require.NoError(t, err)
				_, err = driver.volumeHistory.Upsert(ctx, volume, func(history *VolumeHistoryRecord) {
					history.LastSuccessfulPublishTime = time.Now().Add(-30 * time.Minute)
				})
				require.NoError(t, err)
				before, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, string(node.UID), before.LastSuccessfulNodeUID)
				require.Equal(t, 41, before.LastSuccessfulOpenNebulaVMID)

				replacement := node.DeepCopy()
				replacement.UID = "replacement"
				replacement.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
				replacement.Status.VolumesInUse = []corev1.UniqueVolumeName{corev1.UniqueVolumeName("kubernetes.io/csi/" + driver.name + "^" + volume)}
				nodes := kube.CoreV1().Nodes()
				require.NoError(t, nodes.Delete(ctx, node.Name, metav1.DeleteOptions{}))
				_, err = nodes.Create(ctx, replacement, metav1.CreateOptions{})
				require.NoError(t, err)

				pod := activePodForPVC("waiting", pvc.Name, node.Name)
				pod.UID, pod.Status.Phase, pod.CreationTimestamp = "waiting", corev1.PodPending, metav1.Now()
				setPremountKubeletFixtureStatus(pod, "ContainerCreating")
				switch mode {
				case "new-va":
					va.CreationTimestamp = metav1.Now()
				case "running":
					pod.Status.Phase = corev1.PodRunning
				case "terminating":
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				}
				if mode != "unknown-use" {
					_, err = kube.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
					require.NoError(t, err)
				}
				_, err = vas.Create(ctx, va, metav1.CreateOptions{})
				require.NoError(t, err)
				wire.mu.Lock()
				delete(wire.disks, 41)
				if distinctVM {
					wire.vmNames = map[int]string{41: "retired-vm", 42: node.Name}
				}
				switch mode {
				case "competing-owner":
					wire.disks[43] = 256
				case "unreadable-provider":
					wire.failMethod = "one.system.version"
				}
				wire.mu.Unlock()
				afterReplacement.Store(true)

				fresh := newStickyTestDriver(t)
				fresh.kubeRuntime.client = kube
				server = NewControllerServer(fresh, provider, nil)
				if mode == "queued-unpublish" {
					fresh.hotplugQueue = NewHotplugQueueManager(nil, "default", fresh.metrics, time.Second, time.Second)
					fresh.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				if reconcile {
					r := NewAttachmentReconciler(server)
					r.staleVAGrace = 0
					require.NoError(t, r.ReconcileOnce(ctx))
					require.NoError(t, r.ReconcileOnce(ctx))
				} else {
					_, err = reviewControllerClient(t, server).ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
					if mode == "unpublish" || mode == "queued-unpublish" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				}
				currentVA, err := vas.Get(ctx, va.Name, metav1.GetOptions{})
				switch mode {
				case "reconcile":
					require.True(t, apierrors.IsNotFound(err))
				case "replaced-va":
					require.True(t, vaChanged.Load())
					require.NoError(t, err)
					require.Equal(t, "different-va", string(currentVA.UID))
				default:
					require.NoError(t, err)
					require.Equal(t, va.UID, currentVA.UID)
				}
				after, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, before, after)
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Empty(t, wire.mutations)
			})
		}
	}
}

func TestProviderDetachWaitsForAcceptedOwnershipConvergence(t *testing.T) {
	for _, tc := range []struct {
		mode           string
		refuseFirst    bool
		postDetachRead string
	}{
		{mode: "initial-accepted", postDetachRead: "converges"},
		{mode: "retry-accepted", refuseFirst: true, postDetachRead: "converges"},
		{mode: "state-only-convergence", postDetachRead: "state-only"},
		{mode: "state-only-context-expiry", postDetachRead: "state-only-expiry"},
		{mode: "state-only-bad-count", postDetachRead: "state-only-bad-count"},
		{mode: "persistent-lag", postDetachRead: "lag"},
		{mode: "retry-persistent-lag", refuseFirst: true, postDetachRead: "lag"},
		{mode: "contradictory-owner", postDetachRead: "other-owner"},
		{mode: "malformed-owner-list", postDetachRead: "malformed"},
		{mode: "lost-detach-response", postDetachRead: "lost-reply"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			contextTimeout := 4 * time.Second
			if tc.postDetachRead == "state-only-expiry" {
				contextTimeout = 500 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
			defer cancel()
			provider, wire := nodeAttachmentProvider(t)
			readsAfterDetach := 0
			wire.mu.Lock()
			wire.ceph, wire.version = true, "7.4.0"
			wire.rejectFirstDetach = tc.refuseFirst
			wire.loseDetachReply = tc.postDetachRead == "lost-reply"
			wire.alterDirectImage = func(body string) string {
				if _, attached := wire.disks[41]; attached {
					return body
				}
				readsAfterDetach++
				switch tc.postDetachRead {
				case "converges":
					if readsAfterDetach > 9 {
						return body
					}
					return fixtureDetachImageOwner(body, 41)
				case "state-only", "state-only-expiry", "state-only-bad-count":
					if tc.postDetachRead == "state-only" && readsAfterDetach > 9 {
						return body
					}
					body = strings.Replace(body, "<STATE>1</STATE>", "<STATE>8</STATE>", 1)
					if tc.postDetachRead == "state-only-bad-count" {
						body = strings.Replace(body, "<RUNNING_VMS>0</RUNNING_VMS>", "<RUNNING_VMS>1</RUNNING_VMS>", 1)
					}
					return body
				case "lag":
					return fixtureDetachImageOwner(body, 41)
				case "other-owner":
					return fixtureDetachImageOwner(body, 42)
				case "malformed":
					return strings.Replace(body, "<VMS></VMS>", "<VMS><ID/></VMS>", 1)
				default:
					return body
				}
			}
			wire.mu.Unlock()

			accepted, err := provider.DetachVolumeWithAuthority(ctx, "volume-test", "node-old", 41, "ceph", nil, nil)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Equal(t, []string{"detach:41"}, wire.mutations)
			if tc.refuseFirst {
				require.Equal(t, 2, wire.detachAttempts)
			} else {
				require.Equal(t, 1, wire.detachAttempts)
			}
			var timeout *opennebula.HotplugTimeoutError
			switch tc.postDetachRead {
			case "converges", "state-only":
				require.NoError(t, err)
				require.True(t, accepted)
				require.Greater(t, readsAfterDetach, 9)
			case "state-only-expiry":
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Greater(t, readsAfterDetach, 1)
				require.False(t, accepted)
			case "lag":
				require.ErrorAs(t, err, &timeout)
				require.ErrorIs(t, timeout, opennebula.ErrConflictingAttachmentEvidence)
				require.Greater(t, readsAfterDetach, 1)
				require.False(t, accepted)
			default:
				require.Error(t, err)
				require.False(t, errors.As(err, &timeout))
				require.False(t, accepted)
			}
		})
	}
}

func TestControllerPublishLegacyCephImageAssertions(t *testing.T) {
	for _, mode := range []string{"absent", "matching", "wrong-image", "blank-image", "wrong-serial", "local", "false-local", "false-cephfs"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			delete(pv.Annotations, annotationBackend)
			if strings.HasPrefix(mode, "false-") {
				pv.Annotations[annotationBackend] = strings.TrimPrefix(mode, "false-")
			}
			pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
			switch mode {
			case "matching":
				pv.Spec.CSI.VolumeAttributes = map[string]string{publishContextOpenNebulaImageID: "7", publishContextDeviceSerial: "onecsi-7"}
			case "wrong-image", "local", "false-local", "false-cephfs":
				pv.Spec.CSI.VolumeAttributes = map[string]string{publishContextOpenNebulaImageID: "8"}
			case "blank-image":
				pv.Spec.CSI.VolumeAttributes = map[string]string{publishContextOpenNebulaImageID: ""}
			case "wrong-serial":
				pv.Spec.CSI.VolumeAttributes = map[string]string{publishContextDeviceSerial: "onecsi-8"}
			}
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node)
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.ceph, wire.version, wire.marker = mode != "local", "7.4.0", 128*1024*1024
			delete(wire.disks, 41)
			wire.mu.Unlock()
			server := NewControllerServer(driver, provider, nil)
			reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			history, readErr := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, readErr)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			switch mode {
			case "absent", "matching", "local":
				require.NoError(t, err)
				require.Equal(t, "7", reply.GetPublishContext()[publishContextOpenNebulaImageID])
				require.NotEmpty(t, wire.mutations)
				require.Equal(t, "attach:41:false", wire.mutations[0])
			default:
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, reply)
				require.Empty(t, wire.mutations)
				require.Zero(t, history)
			}
		})
	}
}

func TestControllerPublishPinsAdmittedClaim(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, retained := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%t/retained=%t", queued, retained), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				kube := driver.kubeRuntime.client.CoreV1()
				claim := pvc.DeepCopy()
				claim.Name, claim.UID = "rebound-pvc", "rebound-uid"
				var changed atomic.Bool
				provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if method != "one.vm.info" || !changed.CompareAndSwap(false, true) {
						return
					}
					_, err := kube.PersistentVolumeClaims(claim.Namespace).Create(ctx, claim, metav1.CreateOptions{})
					require.NoError(t, err)
					rebound := pv.DeepCopy()
					rebound.Spec.ClaimRef.Name, rebound.Spec.ClaimRef.UID = claim.Name, claim.UID
					_, err = kube.PersistentVolumes().Update(ctx, rebound, metav1.UpdateOptions{})
					require.NoError(t, err)
				})
				wire.mu.Lock()
				wire.ceph, wire.version = true, "7.4.0"
				if !retained {
					delete(wire.disks, 41)
				}
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				client := reviewControllerClient(t, server)
				request := newPublishReq(volume, node.Name)
				reply, err := client.ControllerPublishVolume(ctx, request)
				require.True(t, changed.Load())
				require.Error(t, err)
				require.Nil(t, reply)
				history, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Zero(t, history)
				wire.mu.Lock()
				require.Empty(t, wire.mutations)
				wire.mu.Unlock()

				consumer := activePodForPVC("rebound-consumer", claim.Name, node.Name)
				consumer.UID = "rebound-consumer"
				_, err = kube.Pods(claim.Namespace).Create(ctx, consumer, metav1.CreateOptions{})
				require.NoError(t, err)
				reply, err = client.ControllerPublishVolume(ctx, request)
				require.NoError(t, err)
				require.Equal(t, claim.Name, reply.GetPublishContext()[paramPVCName])
				history, err = server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, claim.Name, history.PVCName)
			})
		}
	}
}

func TestControllerPublishFreshCephReplacementReader(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"retired", "lost-reply", "separate-reader", "old-attachment", "in-use", "pending"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}, map[string]string{annotationBackend: "ceph"})
				pv.Status.Phase, pvc.Status.Phase = corev1.VolumeBound, corev1.ClaimBound
				pvc.Spec.AccessModes = pv.Spec.AccessModes
				node := newReadyNode("node-old", true)
				node.UID = "replacement-reader-node"
				node.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
				va := replacementPublishVA(pv, node)
				va.Name = csiVolumeAttachmentName(volume, pv.Spec.CSI.Driver, node.Name)
				if mode == "old-attachment" {
					va.CreationTimestamp = metav1.NewTime(node.CreationTimestamp.Add(-time.Minute))
				}
				if mode == "in-use" {
					node.Status.VolumesInUse = []corev1.UniqueVolumeName{corev1.UniqueVolumeName("kubernetes.io/csi/" + pv.Spec.CSI.Driver + "^" + volume)}
				}
				objects := []kruntime.Object{pv, pvc, node, va}
				if mode == "retired" || mode == "lost-reply" || mode == "separate-reader" {
					pod := activePodForPVC("reader-waiter", pvc.Name, node.Name)
					pod.UID = "reader-waiter"
					pod.CreationTimestamp = metav1.NewTime(node.CreationTimestamp.Add(2 * time.Second))
					pod.Status.Phase = corev1.PodPending
					setPremountKubeletFixtureStatus(pod, "ContainerCreating")
					objects = append(objects, pod)
				}
				if mode == "separate-reader" {
					other := newReadyNode("node-remote", true)
					other.CreationTimestamp = node.CreationTimestamp
					otherVA := replacementPublishVA(pv, other)
					otherVA.Name, otherVA.UID = csiVolumeAttachmentName(volume, pv.Spec.CSI.Driver, other.Name), "other-reader-va"
					otherVA.Status.Attached = true
					objects = append(objects, other, otherVA)
				}
				driver := newStickyTestDriver(t, objects...)
				_, err := driver.volumeHistory.Upsert(ctx, volume, func(state *VolumeHistoryRecord) {
					state.PVName, state.PVCNamespace, state.PVCName = pv.Name, pvc.Namespace, pvc.Name
					state.Backend = "ceph"
					state.LastSuccessfulNodeName, state.LastSuccessfulNodeUID = node.Name, "retired-node"
					state.LastSuccessfulImageID, state.LastSuccessfulOpenNebulaVMID = 7, 41
					state.LastSuccessfulPublishTime = node.CreationTimestamp.Add(-time.Minute)
					if mode == "lost-reply" {
						state.LastSuccessfulOpenNebulaVMID = 42
					}
					if mode == "pending" {
						state.PendingReplacementAttach = &replacementAttachAttempt{Version: 2, Phase: replacementAttachArmed}
					}
				})
				require.NoError(t, err)
				provider, wire := nodeAttachmentProvider(t)
				wire.mu.Lock()
				wire.ceph, wire.version, wire.readOnlyAttach, wire.marker = true, "7.4.0", true, 256*1024*1024
				delete(wire.disks, 41)
				wire.loseAttachReply = mode == "lost-reply"
				if mode == "separate-reader" {
					wire.disks[43] = 128
					wire.vmNames = map[int]string{43: "node-remote"}
					wire.attachedDisks[43] = map[string]string{"IMAGE_ID": "7", "SERIAL": "onecsi-7", "READONLY": "YES"}
				}
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				request := newPublishReq(volume, node.Name)
				request.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
				request.Readonly = true
				client := reviewControllerClient(t, server)
				reply, err := client.ControllerPublishVolume(ctx, request)
				if mode == "lost-reply" {
					require.Error(t, err)
					require.Nil(t, reply)
					wire.mu.Lock()
					require.Equal(t, []string{"attach:41:true"}, wire.mutations)
					wire.mu.Unlock()
					reply, err = client.ControllerPublishVolume(ctx, request)
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if mode == "retired" || mode == "lost-reply" || mode == "separate-reader" {
					require.NoError(t, err)
					require.Equal(t, "7", reply.GetPublishContext()[publishContextOpenNebulaImageID])
					require.Equal(t, []string{"attach:41:true", "resize:41:256", "marker:268435456"}, wire.mutations)
					require.NotContains(t, wire.attachFields, "CSI_ATTACH_ATTEMPT")
				} else {
					require.Error(t, err)
					require.Nil(t, reply)
					require.Empty(t, wire.mutations)
				}
			})
		}
	}
}

func TestControllerPublishRechecksAdmittedMetadata(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"manual", "datastore", "harmless"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				kube := driver.kubeRuntime.client.CoreV1()
				var changed atomic.Bool
				provider, wire := nodeAttachmentProvider(t, func(method string, _ context.Context) {
					if method != "one.system.version" || !changed.CompareAndSwap(false, true) {
						return
					}
					if mode == "manual" {
						current := pvc.DeepCopy()
						current.Annotations[annotationRecoveryMode] = recoveryModeManual
						_, err := kube.PersistentVolumeClaims(pvc.Namespace).Update(ctx, current, metav1.UpdateOptions{})
						require.NoError(t, err)
					} else {
						current := pv.DeepCopy()
						if mode == "datastore" {
							current.Annotations[annotationDatastoreName] = "updated-datastore"
						} else {
							current.Annotations["fixture.example/description"] = "harmless"
						}
						_, err := kube.PersistentVolumes().Update(ctx, current, metav1.UpdateOptions{})
						require.NoError(t, err)
					}
				})
				wire.mu.Lock()
				wire.ceph, wire.version, wire.disks[41] = true, "7.4.0", 128
				wire.mu.Unlock()
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				client := reviewControllerClient(t, server)
				req := newPublishReq(volume, node.Name)
				reply, err := client.ControllerPublishVolume(ctx, req)
				require.True(t, changed.Load())
				if mode == "harmless" {
					require.NoError(t, err)
					require.NotNil(t, reply)
					return
				}
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Nil(t, reply)
				history, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.True(t, history.LastSuccessfulPublishTime.IsZero())
				wire.mu.Lock()
				require.Empty(t, wire.mutations)
				wire.mu.Unlock()
				if mode == "manual" {
					_, err = kube.PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				reply, err = client.ControllerPublishVolume(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, reply)
			})
		}
	}
}

func TestControllerPublishTimeoutUsesEffectiveSize(t *testing.T) {
	ctx := context.Background()
	const volume = "volume-test"
	pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
	node := newReadyNode("node-old", true)
	driver := newStickyTestDriver(t, pv, pvc, node)
	_, wire := nodeAttachmentProvider(t)
	wire.mu.Lock()
	wire.ceph, wire.version = true, "7.4.0"
	wire.imageSizeMiB, wire.disks[41], wire.marker = 100*1024, 200*1024, 200*1024*1024*1024
	wire.mu.Unlock()
	provider, err := opennebula.NewPersistentDiskVolumeProvider(
		opennebula.NewClient(opennebula.OpenNebulaConfig{Endpoint: wire.endpoint, Credentials: "fixture:fixture"}),
		opennebula.HotplugTimeoutPolicy{BaseTimeout: 30 * time.Second, Per100GiB: 10 * time.Second, MaxTimeout: time.Minute},
		opennebula.CephBridgeIdentities{},
	)
	require.NoError(t, err)
	reply, err := reviewControllerClient(t, NewControllerServer(driver, provider, nil)).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
	require.NoError(t, err)
	require.Equal(t, "50", reply.GetPublishContext()[publishContextHotplugTimeoutSeconds])
	wire.mu.Lock()
	defer wire.mu.Unlock()
	require.Empty(t, wire.mutations)
}

func TestControllerPublishRefreshesProtectionAndTarget(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"repair-at-restore", "quarantine-at-restore", "expiry-at-restore", "node-at-size", "vm-at-size"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				node := newReadyNode("node-old", true)
				expiry := mode == "expiry-at-restore"
				var expires time.Time
				if expiry {
					expires = time.Now().Add(2 * time.Second)
					pv.Annotations[annotationBackend] = "local"
					pvc.Annotations[annotationBackend] = "local"
					pv.Annotations[annotationAllowCrossNodeUntil] = expires.Format(time.RFC3339Nano)
				}
				driver := newStickyTestDriver(t, pv, pvc, node)
				if expiry {
					_, err := driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
						record.Backend, record.LastSuccessfulNodeName = "local", "retired-node"
						record.LastSuccessfulPublishTime = time.Now().Add(-time.Minute)
					})
					require.NoError(t, err)
				}
				kube := driver.kubeRuntime.client.CoreV1()
				provider, wire := nodeAttachmentProvider(t)
				probe := &publishRestoreProbe{PersistentDiskVolumeProvider: provider}
				wire.mu.Lock()
				wire.ceph, wire.version, wire.disks[41], wire.marker = !expiry, "7.4.0", 128, 128*1024*1024
				if strings.HasSuffix(mode, "at-restore") {
					wire.marker = 256 * 1024 * 1024
				}
				if expiry {
					wire.alterDirectVM = func(vm string) string {
						return strings.Replace(vm, "<LN_TARGET>SYSTEM</LN_TARGET>", "<LN_TARGET>NONE</LN_TARGET>", 1)
					}
				}
				wire.mu.Unlock()
				var changed atomic.Bool
				if strings.HasSuffix(mode, "at-restore") {
					probe.beforeRestore = func() {
						changed.Store(true)
						switch mode {
						case "expiry-at-restore":
							expiration := time.NewTimer(time.Until(expires))
							defer expiration.Stop()
							<-expiration.C
						case "repair-at-restore":
							_, err := driver.volumeRepairState.Mark(ctx, VolumeRepairState{VolumeID: volume, Classification: repairClassificationWrongDeviceIdentity})
							require.NoError(t, err)
						case "quarantine-at-restore":
							_, active, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: volume, Reason: "late quarantine"}, 1, time.Hour)
							require.NoError(t, err)
							require.True(t, active)
						}
					}
				} else {
					probe.beforeSize = func() {
						require.True(t, probe.restored.Load())
						changed.Store(true)
						if mode == "node-at-size" {
							replacement := node.DeepCopy()
							replacement.UID = "new-node-uid"
							_, err := kube.Nodes().Update(ctx, replacement, metav1.UpdateOptions{})
							require.NoError(t, err)
						} else {
							wire.mu.Lock()
							wire.vmNames = map[int]string{41: "retired-vm", 42: node.Name}
							wire.mu.Unlock()
						}
					}
				}
				server := NewControllerServer(driver, probe, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				before, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				reply, err := reviewControllerClient(t, server).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
				require.True(t, changed.Load())
				require.Error(t, err)
				publishErr := err
				require.Nil(t, reply)
				history, err := server.readAttachmentHistory(ctx, volume)
				require.NoError(t, err)
				require.Equal(t, before.LastSuccessfulPublishTime, history.LastSuccessfulPublishTime)
				if expiry {
					require.ErrorContains(t, publishErr, "Publish placement grant expired")
				}
				if mode == "quarantine-at-restore" {
					_, active, err := server.activeVolumeQuarantine(ctx, volume)
					require.NoError(t, err)
					require.True(t, active)
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Empty(t, wire.mutations)
			})
		}
	}
}

func TestControllerPublishPlacementPatchRejectsChangedPV(t *testing.T) {
	for _, mode := range []string{"renewed-grant", "replaced-pv", "unchanged"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			pv.ResourceVersion = "1"
			pv.Annotations[annotationAllowCrossNodeUntil] = "2099-01-01T00:00:00Z"
			node := newReadyNode("node-old", true)
			driver := newStickyTestDriver(t, pv, pvc, node)
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			attempted := make(chan struct{})
			kube.PrependReactor("patch", "persistentvolumes", func(action ktesting.Action) (bool, kruntime.Object, error) {
				defer close(attempted)
				if mode != "unchanged" {
					changedPV := pv.DeepCopy()
					changedPV.ResourceVersion = "2"
					if mode == "renewed-grant" {
						changedPV.Annotations[annotationAllowCrossNodeUntil] = "2099-02-01T00:00:00Z"
					} else {
						changedPV.UID = "different-pv-uid"
					}
					require.NoError(t, kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), changedPV, ""))
				}
				return false, nil, nil
			})
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			wire.ceph, wire.version, wire.disks[41] = true, "7.4.0", 128
			wire.mu.Unlock()
			reply, err := reviewControllerClient(t, NewControllerServer(driver, provider, nil)).ControllerPublishVolume(ctx, newPublishReq(volume, node.Name))
			require.NoError(t, err)
			require.NotNil(t, reply)
			select {
			case <-attempted:
			case <-time.After(10 * time.Second):
				t.Fatal("Publish never attempted the placement patch")
			}
			current, err := kube.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			require.NoError(t, err)
			switch mode {
			case "renewed-grant":
				require.Equal(t, "2099-02-01T00:00:00Z", current.Annotations[annotationAllowCrossNodeUntil])
				require.Empty(t, current.Annotations[annotationLastAttachedNode])
			case "replaced-pv":
				require.Equal(t, types.UID("different-pv-uid"), current.UID)
				require.Empty(t, current.Annotations[annotationLastAttachedNode])
			case "unchanged":
				require.Equal(t, node.Name, current.Annotations[annotationLastAttachedNode])
				require.Equal(t, "2099-01-01T00:00:00Z", current.Annotations[annotationAllowCrossNodeUntil])
			}
		})
	}
}
