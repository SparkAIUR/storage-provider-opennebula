package controller

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDatastoreSyncDistinguishesDisabledAndUnknownPrerequisites(t *testing.T) {
	for _, scenario := range []string{"provider-disabled", "spec-disabled", "observation-failed", "ready", "bound-IP", "unbound-IP", "bound-disabled-IP", "trailing-non-IFS", "leading-non-IFS", "trailing-CR"} {
		t.Run(scenario, func(t *testing.T) {
			var hostReads atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var call struct {
					Method string `xml:"methodName"`
				}
				require.NoError(t, xml.NewDecoder(r.Body).Decode(&call))
				ok, body, code := 1, "", 0
				switch call.Method {
				case "one.datastorepool.info":
					state := 0
					if scenario == "provider-disabled" {
						state = 1
					}
					bridge, attributes := "bridge", ""
					if strings.Contains(scenario, "IP") {
						bridge = "192.0.2.10"
						attributes = `<DISK_TYPE>RBD</DISK_TYPE><CEPH_HOST>monitor.example.invalid:6789</CEPH_HOST><POOL_NAME>volumes</POOL_NAME><CEPH_USER>csi</CEPH_USER>`
					}
					switch scenario {
					case "trailing-non-IFS":
						bridge += "\u00a0"
					case "leading-non-IFS":
						bridge = "\u00a0" + bridge
					case "trailing-CR":
						bridge += "\r"
					}
					var bridgeXML strings.Builder
					require.NoError(t, xml.EscapeText(&bridgeXML, []byte(bridge)))
					body = fmt.Sprintf(`<DATASTORE_POOL><DATASTORE><ID>100</ID><NAME>ceph</NAME><TYPE>IMAGE</TYPE><DS_MAD>ceph</DS_MAD><TM_MAD>ceph</TM_MAD><STATE>%d</STATE><TOTAL_MB>20000</TOTAL_MB><FREE_MB>10000</FREE_MB><TEMPLATE><BRIDGE_LIST>%s</BRIDGE_LIST>%s</TEMPLATE></DATASTORE></DATASTORE_POOL>`, state, bridgeXML.String(), attributes)
				case "one.hostpool.info":
					hostReads.Add(1)
					if scenario == "observation-failed" {
						ok, body, code = 0, "host observation unavailable", 0x800
					} else {
						body = `<HOST_POOL><HOST><ID>1</ID><NAME>bridge</NAME><STATE>2</STATE></HOST></HOST_POOL>`
						if strings.Contains(scenario, "IP") {
							state := 2
							if scenario == "bound-disabled-IP" {
								state = 4
							}
							body = fmt.Sprintf(`<HOST_POOL><HOST><ID>7</ID><NAME>bridge-a.example.invalid</NAME><STATE>%d</STATE></HOST></HOST_POOL>`, state)
						}
					}
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
			scheme := runtime.NewScheme()
			require.NoError(t, inventoryv1alpha1.AddToScheme(scheme))
			item := &inventoryv1alpha1.OpenNebulaDatastore{
				ObjectMeta: metav1.ObjectMeta{Name: datastoreObjectName(100), Generation: 1},
				Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Enabled: scenario != "spec-disabled", Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 100, ExpectedBackend: "ceph-rbd", Allowed: true}},
			}
			client := ctrlfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(item).WithObjects(item).Build()
			identities, parseErr := opennebula.ParseCephBridgeIdentities("")
			require.NoError(t, parseErr)
			if scenario == "bound-IP" || scenario == "bound-disabled-IP" {
				canonical := `{"version":1,"datastoreID":100,"category":"IMAGE","backend":"ceph-rbd","dsMad":"ceph","tmMad":"ceph","diskType":"RBD","bridges":["192.0.2.10"],"monitors":["monitor.example.invalid:6789"],"pool":"volumes","user":"csi","rbdFormat":"","ecPool":""}`
				fingerprint := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(canonical)))
				identities, parseErr = opennebula.ParseCephBridgeIdentities(fmt.Sprintf(`{"version":1,"datastores":[{"datastoreID":100,"configFingerprint":%q,"endpoints":[{"endpoint":"192.0.2.10","hostID":7,"hostName":"bridge-a.example.invalid"}]}]}`, fingerprint))
				require.NoError(t, parseErr)
			}
			syncer := &Syncer{bridgeIdentities: identities, client: client, apiReader: client, kube: fake.NewSimpleClientset(), ctrl: goca.NewController(opennebula.NewClient(opennebula.OpenNebulaConfig{Endpoint: endpoint.URL, Credentials: "fixture:fixture"}).Client)}
			require.NoError(t, syncer.syncDatastores(context.Background()))
			current := &inventoryv1alpha1.OpenNebulaDatastore{}
			require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: item.Name}, current))
			conditions := map[string]metav1.Condition{}
			for _, condition := range current.Status.Conditions {
				conditions[condition.Type] = condition
			}
			switch scenario {
			case "provider-disabled":
				require.Equal(t, inventoryv1alpha1.DatastoreHealthInvalid, current.Status.Health)
				require.Equal(t, "ProviderDisabled", conditions["ProvisioningEnabled"].Reason)
				require.Equal(t, "Datastore is disabled in OpenNebula", conditions["ProvisioningEnabled"].Message)
				require.Equal(t, metav1.ConditionFalse, conditions["ProvisioningEnabled"].Status)
				require.Equal(t, "ProvisioningBlocked", conditions["ProvisioningFeasible"].Reason)
				require.Zero(t, hostReads.Load())
			case "spec-disabled":
				require.Equal(t, inventoryv1alpha1.DatastoreHealthHealthy, current.Status.Health)
				require.Equal(t, inventoryv1alpha1.DatastorePhaseDisabled, current.Status.Phase)
				require.Equal(t, "ProvisioningDisabled", conditions["ProvisioningEnabled"].Reason)
				require.Equal(t, "Datastore provisioning is disabled", conditions["ProvisioningEnabled"].Message)
				require.Equal(t, metav1.ConditionFalse, conditions["ProvisioningEnabled"].Status)
				require.Equal(t, "ProvisioningBlocked", conditions["ProvisioningFeasible"].Reason)
				require.Zero(t, hostReads.Load())
			case "observation-failed":
				require.Equal(t, inventoryv1alpha1.DatastoreHealthLookupFailed, current.Status.Health)
				require.Equal(t, inventoryv1alpha1.DatastorePhaseUnavailable, current.Status.Phase)
				require.Equal(t, "ProviderPrerequisiteUnknown", conditions["ProvisioningFeasible"].Reason)
				require.Equal(t, metav1.ConditionFalse, conditions["ProvisioningFeasible"].Status)
				require.Contains(t, conditions["ProvisioningFeasible"].Message, "host observation unavailable")
				require.EqualValues(t, 1, hostReads.Load())
			case "unbound-IP", "bound-disabled-IP", "trailing-non-IFS", "leading-non-IFS", "trailing-CR":
				require.Equal(t, inventoryv1alpha1.DatastoreHealthLookupFailed, current.Status.Health)
				require.Equal(t, inventoryv1alpha1.DatastorePhaseUnavailable, current.Status.Phase)
				require.Equal(t, metav1.ConditionFalse, conditions["ProvisioningFeasible"].Status)
				require.EqualValues(t, 1, hostReads.Load())
				if strings.Contains(scenario, "non-IFS") || scenario == "trailing-CR" {
					require.Contains(t, conditions["ProvisioningFeasible"].Message, "unsupported Ceph bridge endpoint")
				}

			case "ready", "bound-IP":
				require.Equal(t, inventoryv1alpha1.DatastoreHealthHealthy, current.Status.Health)
				require.Equal(t, metav1.ConditionTrue, conditions["ProvisioningEnabled"].Status)
				require.Equal(t, "ProvisioningEnabled", conditions["ProvisioningEnabled"].Reason)
				require.Equal(t, metav1.ConditionTrue, conditions["ProvisioningFeasible"].Status)
				require.EqualValues(t, 1, hostReads.Load())
			}
		})
	}
}
