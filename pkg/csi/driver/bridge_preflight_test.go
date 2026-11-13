package driver

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/stretchr/testify/require"
	testingexec "k8s.io/utils/exec/testing"
)

func TestBridgeIdentityPreflightUsesIndependentBinding(t *testing.T) {
	for _, bridges := range []string{"192.0.2.10", " \t192.0.2.10\n", "192.0.2.10\u00a0", "\u00a0192.0.2.10", "192.0.2.10\r"} {
		t.Run(fmt.Sprintf("%q", bridges), func(t *testing.T) {
			t.Setenv("KUBERNETES_SERVICE_HOST", "")
			t.Setenv("KUBERNETES_SERVICE_PORT", "")
			t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "absent"))
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var call struct {
					Method string `xml:"methodName"`
				}
				require.NoError(t, xml.NewDecoder(r.Body).Decode(&call))
				body := ""
				switch call.Method {
				case "one.system.version":
					body = "7.4.0"
				case "one.datastorepool.info":
					var bridgeXML strings.Builder
					require.NoError(t, xml.EscapeText(&bridgeXML, []byte(bridges)))
					body = `<DATASTORE_POOL><DATASTORE><ID>42</ID><NAME>fixture</NAME><TYPE>IMAGE</TYPE><STATE>0</STATE><DS_MAD>ceph</DS_MAD><TM_MAD>ceph</TM_MAD><TOTAL_MB>1024</TOTAL_MB><FREE_MB>512</FREE_MB><TEMPLATE><DISK_TYPE>RBD</DISK_TYPE><BRIDGE_LIST>192.0.2.10</BRIDGE_LIST><CEPH_HOST>monitor.example.invalid:6789</CEPH_HOST><POOL_NAME>volumes</POOL_NAME><CEPH_USER>csi</CEPH_USER><CEPH_SECRET>fixture-secret-reference</CEPH_SECRET></TEMPLATE></DATASTORE></DATASTORE_POOL>`
					body = strings.Replace(body, "<BRIDGE_LIST>192.0.2.10</BRIDGE_LIST>", "<BRIDGE_LIST>"+bridgeXML.String()+"</BRIDGE_LIST>", 1)
				case "one.hostpool.info":
					body = `<HOST_POOL><HOST><ID>7</ID><NAME>bridge-a.example.invalid</NAME><STATE>2</STATE></HOST></HOST_POOL>`
				default:
					t.Errorf("unexpected API %s", call.Method)
				}
				var escaped strings.Builder
				require.NoError(t, xml.EscapeText(&escaped, []byte(body)))
				_, err := fmt.Fprintf(w, `<methodResponse><params><param><value><array><data><value><boolean>1</boolean></value><value><string>%s</string></value><value><int>0</int></value></data></array></value></param></params></methodResponse>`, escaped.String())
				require.NoError(t, err)
			}))
			defer endpoint.Close()
			// Independent documented canonical bytes, not a hash obtained from the production helper.
			canonical := `{"version":1,"datastoreID":42,"category":"IMAGE","backend":"ceph-rbd","dsMad":"ceph","tmMad":"ceph","diskType":"RBD","bridges":["192.0.2.10"],"monitors":["monitor.example.invalid:6789"],"pool":"volumes","user":"csi","rbdFormat":"","ecPool":""}`
			hash := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(canonical)))
			binding := fmt.Sprintf(`{"version":1,"datastores":[{"datastoreID":42,"configFingerprint":%q,"endpoints":[{"endpoint":"192.0.2.10","hostID":7,"hostName":"bridge-a.example.invalid"}]}]}`, hash)
			cfg := config.LoadConfiguration()
			cfg.OverrideVal(config.OpenNebulaRPCEndpointVar, endpoint.URL)
			cfg.OverrideVal(config.OpenNebulaCredentialsVar, "fixture:fixture")
			cfg.OverrideVal(config.AllowedDatastoreTypesVar, "ceph")
			cfg.OverrideVal("ONE_CSI_CEPH_BRIDGE_IDENTITIES", binding)
			report := NewPreflightRunner(cfg, &testingexec.FakeExec{LookPathFunc: func(path string) (string, error) { return path, nil }}, PreflightOptions{Datastores: []string{"42"}}).Run(context.Background())
			wantOutcome := "fail"
			if bridges == "192.0.2.10" || bridges == " \t192.0.2.10\n" {
				wantOutcome = "pass"
			}
			checked := 0
			for _, result := range report.Results {
				if result.Check == "datastore_42_feasibility" || result.Check == "datastore_42_bridge_fingerprint" {
					require.Equal(t, wantOutcome, result.Outcome, result.Message)
					if wantOutcome == "fail" {
						require.Contains(t, result.Message, "unsupported Ceph bridge endpoint")
					}
					checked++
				}
			}
			require.Equal(t, 2, checked, "missing fingerprint or feasibility result: %+v", report.Results)
		})
	}
}

func TestBridgeIdentityMalformedPreflightConfigFailsBeforeTransport(t *testing.T) {
	calls := 0
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		t.Error("malformed policy must not dispatch provider requests")
	}))
	defer endpoint.Close()
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.OpenNebulaRPCEndpointVar, endpoint.URL)
	cfg.OverrideVal(config.OpenNebulaCredentialsVar, "fixture:fixture")
	cfg.OverrideVal(config.CephBridgeIdentitiesVar, `{"version":99,"datastores":[]}`)
	report := NewPreflightRunner(cfg, nil, PreflightOptions{}).Run(context.Background())
	require.False(t, report.Passed)
	require.Len(t, report.Results, 1)
	require.Equal(t, "ceph_bridge_identities", report.Results[0].Check)
	require.Zero(t, calls)
}
