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

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			_, err := f.unpublish(t)
			require.NoError(t, err)
			f.requireNoProviderMutation(t)
			var mode, imageReads, deletes, otherMutations atomic.Int32
			var deleted atomic.Bool
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var call struct {
					Method string `xml:"methodName"`
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
			for _, i := range []int32{0, 1, 2, 3, 5, 6, 7} {
				mode.Store(i)
				imageReads.Store(0)
				response, err := client.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: cephRestartVolume})
				require.Error(t, err, "unresolved or unobservable VM ownership must retain the deletion finalizer")
				require.Nil(t, response)
				require.Zero(t, deletes.Load())
			}
			mode.Store(4)
			response, err := client.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: cephRestartVolume})
			require.NoError(t, err)
			require.NotNil(t, response)
			require.EqualValues(t, 1, deletes.Load())
			require.Zero(t, otherMutations.Load())
		})
	}
}

func TestControllerDeleteCephVerifiesMissingImageOwnership(t *testing.T) {
	for _, mode := range []string{"retained-disk", "serial-reference", "unknown-ownership", "replacement-image", "hidden-handle", "confirmed-absent", "owner-omitted", "owner-omitted-released", "owner-omitted-gone"} {
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
			case "owner-omitted", "owner-omitted-released", "owner-omitted-gone":
				wire.omitOriginalVM = true
				if mode != "owner-omitted" {
					delete(wire.disks, 41)
					wire.missingOriginalVM = mode == "owner-omitted-gone"
				}
			}
			wire.mu.Unlock()

			response, err := reviewControllerClient(t, server).DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volume})
			switch mode {
			case "confirmed-absent", "owner-omitted-released", "owner-omitted-gone":
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
