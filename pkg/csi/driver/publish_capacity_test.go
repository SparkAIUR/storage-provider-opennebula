package driver

import (
	"context"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type capacityProtocolProvider struct {
	*MockOpenNebulaVolumeProviderTestify
	capacity *opennebula.PersistentDiskVolumeProvider
}

func (p *capacityProtocolProvider) RestoreVolumeCapacity(ctx context.Context, volume string, vmID, imageID int) error {
	return p.capacity.RestoreVolumeCapacity(ctx, volume, vmID, imageID)
}

func TestControllerPublishCapacityRestorationRetries(t *testing.T) {
	for _, path := range []string{"already_attached", "queued_completion", "post_attach"} {
		t.Run(path, func(t *testing.T) {
			const desired = 256 * 1024 * 1024
			var size atomic.Int64
			size.Store(128)
			var reject atomic.Bool
			reject.Store(true)
			var resizes atomic.Int32
			image := fmt.Sprintf(`<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>8</STATE><SIZE>128</SIZE><TEMPLATE><CSI_REQUESTED_SIZE_BYTES>%d</CSI_REQUESTED_SIZE_BYTES></TEMPLATE></IMAGE>`, desired)
			vmXML := func() string {
				return fmt.Sprintf(`<VM><ID>11</ID><NAME>node-test</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE><DISK><IMAGE_ID>7</IMAGE_ID><DISK_ID>2</DISK_ID><TARGET>vdb</TARGET><SIZE>%d</SIZE></DISK></TEMPLATE></VM>`, size.Load())
			}
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
				ok, body, code := 1, "", 0
				switch call.Method {
				case "one.image.info":
					body = image
				case "one.imagepool.info":
					body = "<IMAGE_POOL>" + image + "</IMAGE_POOL>"
				case "one.vmpool.info":
					body = "<VM_POOL>" + vmXML() + "</VM_POOL>"
				case "one.vm.info":
					body = vmXML()
				case "one.vm.diskresize":
					require.Len(t, call.Params, 4)
					number := func(i int) int { v := call.Params[i].Value; return v.Int + v.I4 + v.I8 }
					require.Equal(t, 11, number(1))
					require.Equal(t, 2, number(2))
					require.Equal(t, "256", call.Params[3].Value.String)
					resizes.Add(1)
					if reject.Load() {
						ok, body, code = 0, "resize unavailable", 0x800
					} else {
						size.Store(256)
						body = "11"
					}
				case "one.image.update":
					body = "7"
				default:
					t.Errorf("unexpected XML-RPC method %s", call.Method)
					ok, body, code = 0, "unexpected", 0x800
				}
				var escaped strings.Builder
				require.NoError(t, xml.EscapeText(&escaped, []byte(body)))
				fmt.Fprintf(w, `<methodResponse><params><param><value><array><data><value><boolean>%d</boolean></value><value><string>%s</string></value><value><int>%d</int></value></data></array></value></param></params></methodResponse>`, ok, escaped.String(), code)
			}))
			t.Cleanup(endpoint.Close)
			capacity, err := opennebula.NewPersistentDiskVolumeProvider(opennebula.NewClient(opennebula.OpenNebulaConfig{Endpoint: endpoint.URL, Credentials: "fixture:fixture"}), opennebula.HotplugTimeoutPolicy{BaseTimeout: time.Second, MaxTimeout: time.Second, PollInterval: time.Millisecond})
			require.NoError(t, err)
			provider := &MockOpenNebulaVolumeProviderTestify{}
			provider.On("VolumeExists", mock.Anything, "volume-test").Return(7, desired, nil)
			provider.On("NodeExists", mock.Anything, "node-test").Return(11, nil)
			if path == "queued_completion" || path == "post_attach" {
				count := 1
				if path == "post_attach" {
					count = 2
					provider.On("AttachVolume", mock.Anything, "volume-test", "node-test", false, mock.Anything).Return(nil).Once()
				}
				provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("", opennebula.ErrAttachmentNotFound).Times(count)
			}
			provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil)
			controller := NewControllerServer(newTestDriver(), &capacityProtocolProvider{provider, capacity}, &MockSharedFilesystemProviderTestify{})
			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer()
			csi.RegisterControllerServer(server, controller)
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			connection, err := grpc.NewClient("passthrough:///capacity-fixture", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { connection.Close() })
			client := csi.NewControllerClient(connection)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := newPublishReq("volume-test", "node-test")
			response, err := client.ControllerPublishVolume(ctx, req)
			require.Error(t, err)
			require.Nil(t, response)
			require.EqualValues(t, 128, size.Load())
			require.EqualValues(t, 1, resizes.Load())
			reject.Store(false)
			response, err = client.ControllerPublishVolume(ctx, req)
			require.NoError(t, err)
			require.Equal(t, "vdb", response.PublishContext["volumeName"])
			require.EqualValues(t, 256, size.Load())
			require.EqualValues(t, 2, resizes.Load())
			_, err = client.ControllerPublishVolume(ctx, req)
			require.NoError(t, err)
			require.EqualValues(t, 2, resizes.Load())
			provider.AssertExpectations(t)
		})
	}
}

func TestStickyCapacityFailurePreservesReuseState(t *testing.T) {
	driver := newStickyTestDriver(t)
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{VolumeID: "volume-test", NodeID: "node-test", Backend: "local", StartedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute), GraceSeconds: 60}))
	provider := &MockOpenNebulaVolumeProviderTestify{}
	provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil)
	provider.On("RestoreVolumeCapacity", mock.Anything, "volume-test", 11, 7).Return(fmt.Errorf("resize unavailable")).Once()
	provider.On("RestoreVolumeCapacity", mock.Anything, "volume-test", 11, 7).Return(nil).Once()
	controller := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	req := newPublishReq("volume-test", "node-test")
	handled, response, err := controller.reuseStickyAttachment(context.Background(), req, 7, 11)
	require.True(t, handled)
	require.Error(t, err)
	require.Nil(t, response)
	_, exists := driver.stickyAttachments.Get(req.VolumeId)
	require.True(t, exists)
	handled, response, err = controller.reuseStickyAttachment(context.Background(), req, 7, 11)
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, "vdb", response.PublishContext["volumeName"])
	_, exists = driver.stickyAttachments.Get(req.VolumeId)
	require.False(t, exists)
	provider.AssertExpectations(t)
}
