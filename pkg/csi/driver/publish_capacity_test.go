package driver

import (
	"context"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type capacityProtocolProvider struct {
	*MockOpenNebulaVolumeProviderTestify
	capacity *opennebula.PersistentDiskVolumeProvider
}

func (p *capacityProtocolProvider) RestoreVolumeCapacity(ctx context.Context, volume string, vmID, imageID int) error {
	return p.capacity.RestoreVolumeCapacity(ctx, volume, vmID, imageID)
}

func (p *capacityProtocolProvider) ExpandVolume(ctx context.Context, volume string, size int64, allowDetached bool) (int64, error) {
	return p.capacity.ExpandVolume(ctx, volume, size, allowDetached)
}

func reviewControllerClient(t *testing.T, controller *ControllerServer) csi.ControllerClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	csi.RegisterControllerServer(server, controller)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve capacity fixture endpoint: %v", err)
		}
	}()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///capacity-fixture", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	return csi.NewControllerClient(connection)
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
				_, err := fmt.Fprintf(w, `<methodResponse><params><param><value><array><data><value><boolean>%d</boolean></value><value><string>%s</string></value><value><int>%d</int></value></data></array></value></param></params></methodResponse>`, ok, escaped.String(), code)
				require.NoError(t, err)
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
			client := reviewControllerClient(t, controller)
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

func TestControllerPublishAndExpandSerializeVerifiedCapacity(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, first := range []string{"publish", "expand"} {
			t.Run(fmt.Sprintf("queued=%v/first=%s", queued, first), func(t *testing.T) {
				const mib = int64(1024 * 1024)
				var size, marker atomic.Int64
				size.Store(128)
				marker.Store(256 * mib)
				writeEntered := make(chan struct{})
				resumeWrite := make(chan struct{})
				var resumeOnce sync.Once
				resume := func() { resumeOnce.Do(func() { close(resumeWrite) }) }
				defer resume()
				var paused atomic.Bool
				imageXML := func() string {
					return fmt.Sprintf(`<IMAGE><ID>7</ID><NAME>volume-test</NAME><STATE>8</STATE><SIZE>128</SIZE><TEMPLATE><CSI_REQUESTED_SIZE_BYTES>%d</CSI_REQUESTED_SIZE_BYTES></TEMPLATE></IMAGE>`, marker.Load())
				}
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
					number := func(i int) int { v := call.Params[i].Value; return v.Int + v.I4 + v.I8 }
					body := ""
					switch call.Method {
					case "one.image.info":
						body = imageXML()
					case "one.imagepool.info":
						body = "<IMAGE_POOL>" + imageXML() + "</IMAGE_POOL>"
					case "one.vmpool.info":
						body = "<VM_POOL>" + vmXML() + "</VM_POOL>"
					case "one.vm.info":
						body = vmXML()
					case "one.vm.diskresize":
						require.Len(t, call.Params, 4)
						require.Equal(t, 11, number(1))
						require.Equal(t, 2, number(2))
						requested, err := strconv.ParseInt(call.Params[3].Value.String, 10, 64)
						require.NoError(t, err)
						require.Contains(t, []int64{256, 512}, requested)
						size.Store(requested)
						body = "11"
					case "one.image.update":
						require.Equal(t, 7, number(1))
						require.Equal(t, 1, number(3))
						key, raw, found := strings.Cut(strings.TrimSpace(call.Params[2].Value.String), "=")
						require.True(t, found)
						require.Equal(t, "CSI_REQUESTED_SIZE_BYTES", strings.TrimSpace(key))
						value, err := strconv.Unquote(strings.TrimSpace(raw))
						require.NoError(t, err)
						verified, err := strconv.ParseInt(value, 10, 64)
						require.NoError(t, err)
						if paused.CompareAndSwap(false, true) {
							close(writeEntered)
							select {
							case <-resumeWrite:
							case <-r.Context().Done():
								return
							}
						}
						marker.Store(verified)
						body = "7"
					default:
						t.Errorf("unexpected XML-RPC method %s", call.Method)
					}
					var escaped strings.Builder
					require.NoError(t, xml.EscapeText(&escaped, []byte(body)))
					_, err := fmt.Fprintf(w, `<methodResponse><params><param><value><array><data><value><boolean>1</boolean></value><value><string>%s</string></value><value><int>0</int></value></data></array></value></param></params></methodResponse>`, escaped.String())
					require.NoError(t, err)
				}))
				t.Cleanup(endpoint.Close)
				capacity, err := opennebula.NewPersistentDiskVolumeProvider(opennebula.NewClient(opennebula.OpenNebulaConfig{Endpoint: endpoint.URL, Credentials: "fixture:fixture"}), opennebula.HotplugTimeoutPolicy{BaseTimeout: time.Second, MaxTimeout: time.Second, PollInterval: time.Millisecond})
				require.NoError(t, err)
				provider := &MockOpenNebulaVolumeProviderTestify{}
				provider.On("VolumeExists", mock.Anything, "volume-test").Return(7, int(256*mib), nil)
				provider.On("NodeExists", mock.Anything, "node-test").Return(11, nil)
				provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil)
				driver := newTestDriver()
				controller := NewControllerServer(driver, &capacityProtocolProvider{provider, capacity}, &MockSharedFilesystemProviderTestify{})
				if queued {
					driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
					driver.hotplugQueue.Configure(true, 0, 0, 0, controller.validateHotplugQueueRequest)
				}
				client := reviewControllerClient(t, controller)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				publishDone, expandDone := make(chan error, 1), make(chan error, 1)
				publish := func() {
					response, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-test"))
					if err == nil && response.GetPublishContext()["volumeName"] != "vdb" {
						err = fmt.Errorf("publish returned no verified attachment")
					}
					publishDone <- err
				}
				expand := func() {
					response, err := client.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{VolumeId: "volume-test", CapacityRange: &csi.CapacityRange{RequiredBytes: 512 * mib}})
					if err == nil && response.GetCapacityBytes() != 512*mib {
						err = fmt.Errorf("expansion returned capacity %d", response.GetCapacityBytes())
					}
					expandDone <- err
				}
				if first == "publish" {
					go publish()
				} else {
					go expand()
				}
				select {
				case <-writeEntered:
				case <-ctx.Done():
					t.Fatal("first request did not reach its capacity commit")
				}
				require.Empty(t, publishDone)
				require.Empty(t, expandDone)
				var duplicateDone chan error
				if queued && first == "publish" {
					duplicateDone = make(chan error, 1)
					go func() {
						response, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-test"))
						if response != nil {
							err = fmt.Errorf("coalesced publish returned an unverified response")
						}
						duplicateDone <- err
					}()
					require.Eventually(t, func() bool {
						active := driver.hotplugQueue.Snapshot()["node-test"].Active
						return active != nil && active.Coalesced == 1
					}, 5*time.Second, time.Millisecond)
					require.Empty(t, duplicateDone)
				}
				if first == "publish" {
					go expand()
				} else {
					go publish()
				}
				require.Eventually(t, func() bool {
					driver.operationLocks.mu.Lock()
					defer driver.operationLocks.mu.Unlock()
					ref := driver.operationLocks.locks[controllerVolumeLockKey("volume-test")]
					return (ref != nil && ref.refs >= 2) || len(publishDone) > 0 || len(expandDone) > 0
				}, 5*time.Second, time.Millisecond)
				require.Empty(t, publishDone)
				require.Empty(t, expandDone)
				resume()
				require.NoError(t, <-publishDone)
				require.NoError(t, <-expandDone)
				if duplicateDone != nil {
					require.Equal(t, codes.Aborted, status.Code(<-duplicateDone))
				}
				require.Equal(t, 512*mib, marker.Load())
				require.EqualValues(t, 512, size.Load())
				size.Store(128)
				response, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-test"))
				require.NoError(t, err)
				require.Equal(t, "vdb", response.GetPublishContext()["volumeName"])
				require.EqualValues(t, 512, size.Load())
				require.Equal(t, 512*mib, marker.Load())
				provider.AssertExpectations(t)
			})
		}
	}
}
