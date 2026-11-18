package driver

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type cephDeletionProtocolProvider struct {
	*cephRetentionTestProvider
	deleteProvider *opennebula.PersistentDiskVolumeProvider
}

func (p *cephDeletionProtocolProvider) DeleteVolume(ctx context.Context, volume string) error {
	return p.deleteProvider.DeleteVolume(ctx, volume)
}

func TestControllerDeleteCephRetainedOwnershipLifecycle(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			f := newCephRestartTestFixture(t, queued)
			f.pv.ResourceVersion = "1"
			f.syncObjects(t)
			_, err := f.server.driver.volumeHistory.Upsert(context.Background(), cephRestartVolume, func(record *VolumeHistoryRecord) {
				record.LastSuccessfulOpenNebulaVMID = 575
			})
			require.NoError(t, err)
			_, err = f.unpublish(t)
			require.NoError(t, err)
			f.requireNoProviderMutation(t)
			var mode, imageReads, deletes, otherMutations atomic.Int32
			var deleted atomic.Bool
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var call struct {
					Method string `xml:"methodName"`
					Params []struct {
						Value struct {
							Int int `xml:"int"`
							I4  int `xml:"i4"`
							I8  int `xml:"i8"`
						} `xml:"value"`
					} `xml:"params>param"`
				}
				require.NoError(t, xml.NewDecoder(r.Body).Decode(&call))
				state := 1
				owners := ""
				running := 0
				if mode.Load() == 0 {
					state, running = 8, 1
					owners = "<ID>575</ID>"
				}
				image := fmt.Sprintf(`<IMAGE><ID>3137</ID><NAME>%s</NAME><STATE>%d</STATE><SIZE>1024</SIZE><PERSISTENT>1</PERSISTENT><SOURCE>/ceph/retained</SOURCE><RUNNING_VMS>%d</RUNNING_VMS><VMS>%s</VMS></IMAGE>`, cephRestartVolume, state, running, owners)
				body, code, ok := "", 0, true
				switch call.Method {
				case "one.imagepool.info":
					body = "<IMAGE_POOL>" + image + "</IMAGE_POOL>"
					if deleted.Load() {
						body = "<IMAGE_POOL/>"
					}
				case "one.image.info":
					body = image
					if imageReads.Add(1) == 3 {
						switch mode.Load() {
						case 5:
							body = strings.Replace(body, "<ID>3137</ID>", "<ID>3138</ID>", 1)
						case 6:
							body = strings.Replace(body, "<ID>3137</ID>", "", 1)
						case 7:
							body = strings.Replace(body, "<NAME>"+cephRestartVolume+"</NAME>", "<NAME>different-volume</NAME>", 1)
						}
					}
					if deleted.Load() {
						ok, code, body = false, 0x400, "not present"
					}
				case "one.vmpool.info":
					vmState := 3
					if mode.Load() == 3 {
						vmState = 8
					}
					body = fmt.Sprintf(`<VM_POOL><VM><ID>575</ID><NAME>node-a</NAME><STATE>%d</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE><DISK><IMAGE_ID>3137</IMAGE_ID><DISK_ID>1</DISK_ID><TARGET>vdb</TARGET><SERIAL>onecsi-3137</SERIAL></DISK></TEMPLATE></VM></VM_POOL>`, vmState)
					if mode.Load() == 2 {
						ok, code, body = false, 0x200, "inventory unavailable"
					}
					if mode.Load() >= 4 {
						body = "<VM_POOL/>"
					}
				case "one.vm.info":
					require.GreaterOrEqual(t, len(call.Params), 2)
					id := call.Params[1].Value
					require.Equal(t, 575, id.Int+id.I4+id.I8)
					switch {
					case mode.Load() == 8 || (mode.Load() == 10 && deleted.Load()):
						body = `<VM><ID>575</ID><NAME>node-a</NAME><TEMPLATE><DISK><IMAGE_ID>3137</IMAGE_ID><DISK_ID>1</DISK_ID><TARGET>vdb</TARGET><SERIAL>onecsi-3137</SERIAL></DISK></TEMPLATE></VM>`
					case mode.Load() == 11 && deleted.Load():
						body = `<VM><ID>575</ID><NAME>node-a</NAME><TEMPLATE><DISK><DISK_ID>1</DISK_ID><IMAGE>` + cephRestartVolume + `</IMAGE><SOURCE>/ceph/retained</SOURCE><PERSISTENT>YES</PERSISTENT></DISK></TEMPLATE></VM>`
					case mode.Load() == 9:
						ok, code, body = false, 0x200, "pinned VM unavailable"
					default:
						body = `<VM><ID>575</ID><NAME>node-a</NAME><TEMPLATE/></VM>`
					}
				case "one.image.delete":
					deletes.Add(1)
					deleted.Store(true)
					body = "3137"
				default:
					otherMutations.Add(1)
					ok, code, body = false, 0x800, "unexpected operation"
				}
				var escaped strings.Builder
				require.NoError(t, xml.EscapeText(&escaped, []byte(body)))
				boolean := 0
				if ok {
					boolean = 1
				}
				w.Header().Set("Content-Type", "text/xml")
				_, err := fmt.Fprintf(w, `<?xml version="1.0"?><methodResponse><params><param><value><array><data><value><boolean>%d</boolean></value><value><string>%s</string></value><value><int>%d</int></value></data></array></value></param></params></methodResponse>`, boolean, escaped.String(), code)
				require.NoError(t, err)
			}))
			defer endpoint.Close()
			provider, err := opennebula.NewPersistentDiskVolumeProvider(opennebula.NewClient(opennebula.OpenNebulaConfig{Endpoint: endpoint.URL, Credentials: "fixture:fixture"}), opennebula.HotplugTimeoutPolicy{BaseTimeout: time.Second, MaxTimeout: time.Second, PollInterval: time.Millisecond}, opennebula.CephBridgeIdentities{})
			require.NoError(t, err)
			f.server.volumeProvider = &cephDeletionProtocolProvider{cephRetentionTestProvider: f.provider, deleteProvider: provider}
			client := reviewControllerClient(t, f.server)
			for _, i := range []int32{0, 1, 2, 3, 5, 6, 7, 8, 9} {
				mode.Store(i)
				imageReads.Store(0)
				response, err := client.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: cephRestartVolume})
				require.Error(t, err, "unresolved or unobservable VM ownership must retain the deletion finalizer")
				require.Nil(t, response)
				require.Zero(t, deletes.Load())
			}
			request := &csi.DeleteVolumeRequest{VolumeId: cephRestartVolume}
			for _, pending := range []int32{11, 10} {
				mode.Store(pending)
				response, err := client.DeleteVolume(context.Background(), request)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Nil(t, response)
				require.EqualValues(t, 1, deletes.Load())
			}
			mode.Store(4)
			response, err := client.DeleteVolume(context.Background(), request)
			require.NoError(t, err)
			require.NotNil(t, response)
			require.EqualValues(t, 1, deletes.Load())
			require.Zero(t, otherMutations.Load())
		})
	}
}

func TestControllerDeleteCephVerifiesMissingImageOwnership(t *testing.T) {
	for _, mode := range []string{"retained-disk", "backed-disk-without-id", "backed-disk-conflicting-serial", "backed-disk-blank-id-conflicting-serial", "serial-reference", "unknown-ownership", "replacement-image", "hidden-handle", "confirmed-absent", "unrelated-observed-conflict", "owner-omitted", "owner-omitted-released", "owner-omitted-gone"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
			pv.Finalizers = []string{"external-provisioner.volume.kubernetes.io/finalizer"}
			driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-old", true))
			provider, wire := nodeAttachmentProvider(t)
			server := NewControllerServer(driver, provider, nil)
			history, err := driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
				record.Backend = "ceph"
				record.LastSuccessfulImageID = 7
				record.LastSuccessfulOpenNebulaVMID = 41
				record.LastSuccessfulPublishTime = time.Now().UTC().Add(-time.Hour)
			})
			require.NoError(t, err)
			wire.mu.Lock()
			wire.ceph, wire.missingImage = true, true
			switch mode {
			case "backed-disk-without-id", "backed-disk-conflicting-serial", "backed-disk-blank-id-conflicting-serial":
				delete(wire.disks, 41)
				facts := "<DISK_ID>2</DISK_ID><IMAGE>volume-test</IMAGE><SOURCE>/fixture/disk</SOURCE><PERSISTENT>YES</PERSISTENT>"
				if mode != "backed-disk-without-id" {
					facts += "<SERIAL>onecsi-8</SERIAL>"
				}
				if mode == "backed-disk-blank-id-conflicting-serial" {
					facts += "<IMAGE_ID/>"
				}
				wire.extraDiskBefore = "<DISK>" + facts + "</DISK>"
			case "serial-reference":
				delete(wire.disks, 41)
				wire.extraDiskBefore = "<DISK><DISK_ID>2</DISK_ID><TARGET>vdb</TARGET><SERIAL>onecsi-7</SERIAL></DISK>"
			case "unknown-ownership":
				wire.failMethod = "one.vmpool.info"
			case "replacement-image":
				wire.duplicateImage, wire.replacementDisk = true, true
			case "hidden-handle":
				wire.hideImageName = true
				wire.missingImage = false
			case "confirmed-absent":
				delete(wire.disks, 41)
			case "unrelated-observed-conflict":
				delete(wire.disks, 41)
				wire.extraDiskByVM = map[int]string{42: "<DISK><DISK_ID>2</DISK_ID><IMAGE_ID>8</IMAGE_ID><SERIAL>onecsi-9</SERIAL></DISK>"}
			case "owner-omitted", "owner-omitted-released", "owner-omitted-gone":
				wire.omitOriginalVM = true
				if mode != "owner-omitted" {
					delete(wire.disks, 41)
					wire.missingOriginalVM = mode == "owner-omitted-gone"
				}
			}
			wire.mu.Unlock()

			client := reviewControllerClient(t, server)
			request := &csi.DeleteVolumeRequest{VolumeId: volume}
			response, err := client.DeleteVolume(ctx, request)
			switch mode {
			case "confirmed-absent", "unrelated-observed-conflict", "owner-omitted-released", "owner-omitted-gone":
				require.NoError(t, err)
				require.NotNil(t, response)
				response, err = client.DeleteVolume(ctx, request)
				require.NoError(t, err)
				require.NotNil(t, response)
			default:
				require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
				require.Nil(t, response)
			}
			currentPV, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, pv.Finalizers, currentPV.Finalizers)
			currentHistory, err := server.readAttachmentHistory(ctx, volume)
			require.NoError(t, err)
			require.Equal(t, history, currentHistory)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Empty(t, wire.mutations)
			require.Zero(t, wire.detachAttempts)
		})
	}
}

func TestUnpinnedDeleteRetryAfterLostReplyUsesPVIdentity(t *testing.T) {
	for _, imageID := range []int{7, 0} {
		t.Run(fmt.Sprint(imageID), func(t *testing.T) {
			testUnpinnedDeleteRetryAfterLostReplyUsesPVIdentity(t, imageID)
		})
	}
}

func testUnpinnedDeleteRetryAfterLostReplyUsesPVIdentity(t *testing.T, imageID int) {
	t.Helper()
	ctx := context.Background()
	const volume = "volume-test"
	driver := newStickyTestDriver(t)
	driver.PluginConfig.OverrideVal(config.AllowedDatastoreTypesVar, "ceph")
	provider, wire := nodeAttachmentProvider(t)
	wire.mu.Lock()
	wire.ceph, wire.ready, wire.allowImageDelete = true, true, true
	wire.zeroImage = imageID == 0
	delete(wire.disks, 41)
	wire.marker = 128 * 1024 * 1024
	wire.mu.Unlock()
	client := reviewControllerClient(t, NewControllerServer(driver, provider, nil))
	pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	provisioned, err := client.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               volume,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 128 * 1024 * 1024},
		VolumeCapabilities: []*csi.VolumeCapability{newPublishReq(volume, "node-old").VolumeCapability},
		Parameters:         map[string]string{storageClassParamDatastoreIDs: "100", paramPVCUID: string(pvc.UID)},
	})
	require.NoError(t, err)
	require.Equal(t, volume, provisioned.Volume.VolumeId)
	require.NotContains(t, provisioned.Volume.VolumeContext, publishContextOpenNebulaImageID)
	require.Equal(t, string(pvc.UID), provisioned.Volume.VolumeContext[volumeContextUUID])

	pv.Finalizers = []string{"external-provisioner.volume.kubernetes.io/finalizer"}
	pv.ResourceVersion = "1"
	pv.Spec.CSI.VolumeHandle = provisioned.Volume.VolumeId
	pv.Spec.CSI.VolumeAttributes = provisioned.Volume.VolumeContext
	kube := driver.kubeRuntime.client
	_, err = kube.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
	require.NoError(t, err)
	request := &csi.DeleteVolumeRequest{VolumeId: provisioned.Volume.VolumeId}
	_, err = client.DeleteVolume(ctx, request)
	require.NoError(t, err)

	restarted := newStickyTestDriver(t)
	restarted.kubeRuntime.client = kube
	server := NewControllerServer(restarted, provider, nil)
	response, err := reviewControllerClient(t, server).DeleteVolume(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response)
	current, err := kube.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, pv.Finalizers, current.Finalizers)
	require.JSONEq(t, fmt.Sprintf(`{"version":1,"pvUID":%q,"driver":%q,"handle":%q,"imageID":%d}`, pv.UID, DefaultDriverName, volume, imageID), current.Annotations[annotationDeleteVolumeIdentity])
	history, err := server.readAttachmentHistory(ctx, volume)
	require.NoError(t, err)
	require.Zero(t, history)
	wire.mu.Lock()
	defer wire.mu.Unlock()
	require.True(t, wire.missingImage)
	require.Equal(t, []string{fmt.Sprintf("image-delete:%d", imageID)}, wire.mutations)
}

func TestControllerDeleteVolumeRefreshesPinnedAdmission(t *testing.T) {
	for _, mode := range []string{"read-only-success", "before-rpc", "after-rpc", "already-missing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.ResourceVersion = "1"
			pv.Finalizers = []string{"external-provisioner.volume.kubernetes.io/finalizer"}
			driver := newStickyTestDriver(t, pv, pvc)
			_, err := driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
				record.LastSuccessfulImageID = 7
				record.LastSuccessfulOpenNebulaVMID = 41
			})
			require.NoError(t, err)
			kube := driver.kubeRuntime.client.(*fake.Clientset)
			var patches, censusReads atomic.Int32
			kube.PrependReactor("patch", "persistentvolumes", func(ktesting.Action) (bool, kruntime.Object, error) {
				patches.Add(1)
				return true, nil, fmt.Errorf("PV patch denied")
			})
			injectAt := map[string]int32{"before-rpc": 2, "after-rpc": 3, "already-missing": 1}[mode]
			provider, wire := nodeAttachmentProvider(t, func(method string, ctx context.Context) {
				if method == "one.vmpool.info" && censusReads.Add(1) == injectAt {
					_, active, err := driver.volumeQuarantine.MarkFailure(ctx, VolumeQuarantineState{VolumeID: volume, Reason: "ownership drift"}, 1, time.Hour)
					require.NoError(t, err)
					require.True(t, active)
				}
			})
			wire.mu.Lock()
			wire.allowImageDelete = true
			wire.missingImage = mode == "already-missing"
			delete(wire.disks, 41)
			wire.mu.Unlock()

			response, err := reviewControllerClient(t, NewControllerServer(driver, provider, nil)).DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volume})
			if mode == "read-only-success" {
				require.NoError(t, err)
				require.NotNil(t, response)
			} else {
				require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
				require.Nil(t, response)
			}
			require.Zero(t, patches.Load())
			current, err := kube.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, pv.Finalizers, current.Finalizers)
			require.NotContains(t, current.Annotations, annotationDeleteVolumeIdentity)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if mode == "read-only-success" || mode == "after-rpc" {
				require.Equal(t, []string{"image-delete:7"}, wire.mutations)
			} else {
				require.Empty(t, wire.mutations)
			}
		})
	}
}

func TestDeleteVolumeRechecksLiveImageWithExistingPVIdentity(t *testing.T) {
	ctx := context.Background()
	const volume = "volume-test"
	pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.ResourceVersion = "1"
	pv.Finalizers = []string{"external-provisioner.volume.kubernetes.io/finalizer"}
	raw := fmt.Sprintf(`{"version":1,"pvUID":%q,"driver":%q,"handle":%q,"imageID":7}`, pv.UID, DefaultDriverName, volume)
	pv.Annotations[annotationDeleteVolumeIdentity] = raw
	driver := newStickyTestDriver(t, pv, pvc)
	provider, wire := nodeAttachmentProvider(t)
	wire.mu.Lock()
	wire.allowImageDelete = true
	wire.mu.Unlock()
	server := NewControllerServer(driver, provider, nil)
	client := reviewControllerClient(t, server)
	request := &csi.DeleteVolumeRequest{VolumeId: volume}
	reply, err := client.DeleteVolume(ctx, request)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Nil(t, reply)
	wire.mu.Lock()
	require.Empty(t, wire.mutations)
	delete(wire.disks, 41)
	wire.mu.Unlock()

	reply, err = client.DeleteVolume(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, reply)
	current, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, raw, current.Annotations[annotationDeleteVolumeIdentity])
	require.Equal(t, pv.Finalizers, current.Finalizers)
	wire.mu.Lock()
	defer wire.mu.Unlock()
	require.Equal(t, []string{"image-delete:7"}, wire.mutations)
}

func TestControllerDeleteVolumeRejectsUnverifiedPVIdentity(t *testing.T) {
	for _, mode := range []string{"uid-drift", "resource-version-drift", "binding-drift", "patch-refused", "readback-refused",
		"wrong-readback", "provider-identity-drift", "provider-ownership-drift", "malformed-marker", "marker-other-pv", "marker-without-image-id", "marker-duplicate-image-id", "pool-without-image-id", "info-without-image-id",
		"marker-conflicts-with-history", "marker-conflicts-with-provider", "repair-required", "pool-conflicting-names", "duplicate-force-delete"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			const volume = "volume-test"
			pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.ResourceVersion = "1"
			pv.Finalizers = []string{"external-provisioner.volume.kubernetes.io/finalizer"}
			switch mode {
			case "malformed-marker":
				pv.Annotations[annotationDeleteVolumeIdentity] = "null"
			case "marker-other-pv":
				pv.Annotations[annotationDeleteVolumeIdentity] = fmt.Sprintf(`{"version":1,"pvUID":"different-uid","driver":%q,"handle":%q,"imageID":7}`, DefaultDriverName, volume)
			case "marker-without-image-id":
				pv.Annotations[annotationDeleteVolumeIdentity] = fmt.Sprintf(`{"version":1,"pvUID":%q,"driver":%q,"handle":%q}`, pv.UID, DefaultDriverName, volume)
			case "marker-duplicate-image-id":
				pv.Annotations[annotationDeleteVolumeIdentity] = fmt.Sprintf(`{"version":1,"pvUID":%q,"driver":%q,"handle":%q,"imageID":8,"ImageId":7}`, pv.UID, DefaultDriverName, volume)
			case "marker-conflicts-with-history", "marker-conflicts-with-provider":
				pv.Annotations[annotationDeleteVolumeIdentity] = fmt.Sprintf(`{"version":1,"pvUID":%q,"driver":%q,"handle":%q,"imageID":8}`, pv.UID, DefaultDriverName, volume)
			}
			driver := newStickyTestDriver(t, pv, pvc)
			if mode == "marker-conflicts-with-history" {
				_, err := driver.volumeHistory.Upsert(ctx, volume, func(record *VolumeHistoryRecord) {
					record.LastSuccessfulImageID = 7
				})
				require.NoError(t, err)
			}
			if mode == "repair-required" {
				_, err := driver.volumeRepairState.Mark(ctx, VolumeRepairState{VolumeID: volume, Classification: "wrong_device_identity", RequestedNode: "node-old"})
				require.NoError(t, err)
			}
			provider, wire := nodeAttachmentProvider(t)
			wire.mu.Lock()
			delete(wire.disks, 41)
			wire.allowImageDelete = mode == "marker-duplicate-image-id"
			if mode == "pool-without-image-id" || mode == "info-without-image-id" {
				wire.zeroImage = true
				if mode == "pool-without-image-id" {
					wire.omitPoolImageID = true
				} else {
					wire.imageInfoByID = map[int]string{0: `<IMAGE><NAME>volume-test</NAME><STATE>1</STATE><SIZE>128</SIZE><SOURCE>/fixture/disk</SOURCE></IMAGE>`}
				}
			}
			switch mode {
			case "pool-conflicting-names":
				wire.hideImageName = true
				wire.extraImages = `<IMAGE><ID>7</ID><NAME>volume-test</NAME><NAME>another-handle</NAME></IMAGE>`
			case "duplicate-force-delete":
				wire.imageTemplateFields = `<FORCE_DELETE>NO</FORCE_DELETE><FORCE_DELETE>YES</FORCE_DELETE>`
			}
			wire.mu.Unlock()

			kube := driver.kubeRuntime.client.(*fake.Clientset)
			patchAttempted := false
			kube.PrependReactor("patch", "persistentvolumes", func(action ktesting.Action) (bool, kruntime.Object, error) {
				patchAttempted = true
				switch mode {
				case "patch-refused":
					return true, nil, fmt.Errorf("patch refused")
				case "uid-drift", "resource-version-drift", "binding-drift":
					replacement := pv.DeepCopy()
					switch mode {
					case "uid-drift":
						replacement.UID = "replacement-uid"
					case "resource-version-drift":
						replacement.ResourceVersion = "2"
					case "binding-drift":
						replacement.Spec.CSI.VolumeHandle = "replacement-handle"
					}
					require.NoError(t, kube.Tracker().Update(action.GetResource(), replacement, ""))
				case "wrong-readback":
					return true, pv.DeepCopy(), nil
				case "provider-identity-drift", "provider-ownership-drift":
					wire.mu.Lock()
					defer wire.mu.Unlock()
					if mode == "provider-identity-drift" {
						wire.imageName = "replacement-image"
					} else {
						wire.disks[41] = 128
					}
				}
				return false, nil, nil
			})
			kube.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, kruntime.Object, error) {
				if mode == "readback-refused" && patchAttempted {
					return true, nil, fmt.Errorf("readback refused")
				}
				return false, nil, nil
			})

			response, err := reviewControllerClient(t, NewControllerServer(driver, provider, nil)).DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volume})
			require.Error(t, err)
			if mode == "marker-duplicate-image-id" {
				require.ErrorContains(t, err, "PV deletion identity is unverified")
			}
			require.Nil(t, response)
			current, err := kube.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, pv.Finalizers, current.Finalizers)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			require.Empty(t, wire.mutations)
		})
	}
}

func TestMissingImageRejectsMalformedOrUnpinnedOwnedHistory(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, mode := range []string{"empty", "null", "unknown", "malformed", "no-pin", "unowned"} {
			t.Run(fmt.Sprintf("queued=%t/%s", queued, mode), func(t *testing.T) {
				ctx := context.Background()
				const volume = "volume-test"
				pv, pvc := newLocalPVAndPVC(volume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{annotationBackend: "ceph"})
				pv.Finalizers = []string{"external-provisioner.volume.kubernetes.io/finalizer"}
				node := newReadyNode("node-old", true)
				driver := newStickyTestDriver(t, pv, pvc, node)
				provider, wire := nodeAttachmentProvider(t)
				server := NewControllerServer(driver, provider, nil)
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, server.validateHotplugQueueRequest)
				}
				raw, malformed := map[string]string{
					"empty": "", "null": "null", "unknown": `{"volumeID":"volume-test","version":1,"unknownImageID":7}`,
					"malformed": `{"volumeID":"volume-test","version":1,"lastSuccessfulImageID":"bad"}`,
				}[mode]
				if malformed {
					require.NoError(t, driver.kubeRuntime.UpsertConfigMapData(ctx, "default", volumeHistoryStateConfigMapName, map[string]string{volume: raw}))
				}
				wire.mu.Lock()
				wire.ceph, wire.missingImage = true, true
				if mode == "unowned" {
					delete(wire.disks, 41)
				}
				wire.mu.Unlock()
				if mode == "unowned" {
					require.NoError(t, driver.kubeRuntime.client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{}))
				}

				client := reviewControllerClient(t, server)
				unpublished, unpublishErr := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volume, NodeId: node.Name})
				deleted, deleteErr := client.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volume})
				if mode == "unowned" {
					require.NoError(t, unpublishErr)
					require.NotNil(t, unpublished)
					require.NoError(t, deleteErr)
					require.NotNil(t, deleted)
				} else {
					require.Equal(t, codes.Unavailable, status.Code(unpublishErr))
					require.Nil(t, unpublished)
					wantDelete := codes.FailedPrecondition
					if malformed {
						wantDelete = codes.Unavailable
					}
					require.Equal(t, wantDelete, status.Code(deleteErr))
					require.Nil(t, deleted)
					current, err := driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
					require.NoError(t, err)
					require.Equal(t, pv.Finalizers, current.Finalizers)
				}
				if malformed {
					cm, err := driver.kubeRuntime.GetConfigMap(ctx, "default", volumeHistoryStateConfigMapName)
					require.NoError(t, err)
					require.Contains(t, cm.Data, volume)
					require.Equal(t, raw, cm.Data[volume])
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				require.Empty(t, wire.mutations)
				require.Zero(t, wire.detachAttempts)
			})
		}
	}
}
