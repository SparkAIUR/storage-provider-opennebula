package driver

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

// This isolated upstream sanity subset exercises the serialized gRPC public
// identity/capability/validation contract. Mutation and physical-device sanity
// remain a distinct native gate; the original opt-in full suite is retained.
func TestFixtureCSISanity(t *testing.T) {
	if os.Getenv("RUN_CSI_SANITY_TESTS") == "1" {
		t.Skip("native suite owns the single Ginkgo RunSpecs invocation")
	}
	socket := filepath.Join(t.TempDir(), "driver.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	d := newTestDriver()
	d.version = "v0.6.0-rc.1"
	d.nodeID = "fixture-node"
	provider := &MockOpenNebulaVolumeProviderTestify{}
	server := grpc.NewServer()
	csi.RegisterIdentityServer(server, NewIdentityServer(d))
	csi.RegisterControllerServer(server, NewControllerServer(d, provider, &MockSharedFilesystemProviderTestify{}))
	csi.RegisterNodeServer(server, NewNodeServer(d, mount.NewSafeFormatAndMount(mount.NewFakeMounter(nil), utilexec.New())))
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve fixture CSI endpoint: %v", err)
		}
	}()
	defer server.Stop()
	cfg := sanity.NewTestConfig()
	cfg.Address = "unix://" + socket
	cfg.TargetPath = filepath.Join(t.TempDir(), "target")
	cfg.StagingPath = filepath.Join(t.TempDir(), "stage")
	sc := sanity.GinkgoTest(&cfg)
	defer sc.Finalize()
	gomega.RegisterFailHandler(ginkgo.Fail)
	suite, reporter := ginkgo.GinkgoConfiguration()
	suite.FocusStrings = []string{`GetPluginCapabilities|GetPluginInfo|Probe|ControllerGetCapabilities|CreateVolume should fail when no name is provided|CreateVolume should fail when no volume capabilities are provided|DeleteVolume should fail when no volume id is provided|ValidateVolumeCapabilities should fail when no volume id is provided`}
	ginkgo.RunSpecs(t, "Offline CSI public contract sanity", suite, reporter)
}
