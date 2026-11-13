package opennebula

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	host "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/host"
	"github.com/stretchr/testify/require"
)

func bridgeFixtureDatastore() Datastore {
	return Datastore{ID: 42, Category: "IMAGE", Type: "ceph", Backend: "ceph", DSMad: "ceph", TMMad: "ceph", DiskType: "RBD", Enabled: true, FreeBytes: 1024 * sizeConversion, Ceph: &CephDatastoreAttributes{BridgeList: "192.0.2.10", CephHost: "monitor.example.invalid:6789", PoolName: "volumes", CephUser: "csi"}}
}
func bridgeFixtureBinding(fingerprint string, id int, name string) string {
	return fmt.Sprintf(`{"version":1,"datastores":[{"datastoreID":42,"configFingerprint":%q,"endpoints":[{"endpoint":"192.0.2.10","hostID":%d,"hostName":%q}]}]}`, fingerprint, id, name)
}
func TestCephBridgeConfigFingerprintContract(t *testing.T) {
	ds := bridgeFixtureDatastore()
	canonical := `{"version":1,"datastoreID":42,"category":"IMAGE","backend":"ceph-rbd","dsMad":"ceph","tmMad":"ceph","diskType":"RBD","bridges":["192.0.2.10"],"monitors":["monitor.example.invalid:6789"],"pool":"volumes","user":"csi","rbdFormat":"","ecPool":""}`
	want := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(canonical)))
	actual, err := CephBridgeConfigFingerprint(ds)
	require.NoError(t, err)
	require.Equal(t, want, actual)
	ds.Ceph.CephKey = "must-never-enter-fingerprint"
	ds.Ceph.CephSecret = "private-reference"
	ds.Ceph.CephConf = "private-config-path"
	ds.FreeBytes = 1
	ds.Enabled = false
	actual, err = CephBridgeConfigFingerprint(ds)
	require.NoError(t, err)
	require.Equal(t, want, actual)
	ds.Ceph.BridgeList = " 192.0.2.10\t"
	ds.Backend = "ceph-rbd"
	ds.Type = "IMAGE"
	ds.DSMad = " CEPH "
	ds.Ceph.CephHost = " monitor.example.invalid:6789; "
	actual, err = CephBridgeConfigFingerprint(ds)
	require.NoError(t, err)
	require.Equal(t, want, actual)
	ds.Ceph.BridgeList = "192.0.2.10 bridge-b.example.invalid"
	first, err := CephBridgeConfigFingerprint(ds)
	require.NoError(t, err)
	ds.Ceph.BridgeList = "bridge-b.example.invalid 192.0.2.10"
	second, err := CephBridgeConfigFingerprint(ds)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}
func TestParseCephBridgeIdentitiesRejectsMalformedAndDuplicateInputs(t *testing.T) {
	fp := "sha256:" + strings.Repeat("a", 64)
	valid := bridgeFixtureBinding(fp, 0, "bridge-a.example.invalid")
	entry := strings.TrimSuffix(strings.TrimPrefix(valid, `{"version":1,"datastores":[`), `]}`)
	for _, raw := range []string{"", `{"version":1,"datastores":[]}`, valid} {
		_, err := ParseCephBridgeIdentities(raw)
		require.NoError(t, err)
	}
	for name, raw := range map[string]string{
		"malformed": "{", "unknown version": `{"version":2,"datastores":[]}`, "missing version": `{"datastores":[]}`, "missing datastores": `{"version":1}`, "unknown field": `{"version":1,"datastores":[],"allowUnregistered":true}`,
		"noncanonical case alias": strings.Replace(valid, `"hostID"`, `"HostID"`, 1), "case-aliased duplicate": strings.Replace(valid, `"version":1`, `"version":1,"Version":1`, 1),
		"duplicate object key": `{"version":1,"version":1,"datastores":[]}`, "duplicate nested key": strings.Replace(valid, `"hostID":0`, `"hostID":0,"hostID":7`, 1),
		"missing hostID": strings.Replace(valid, `"hostID":0,`, "", 1), "missing datastoreID": strings.Replace(valid, `"datastoreID":42,`, "", 1), "invalid hash": strings.Replace(valid, fp, "unknown", 1), "negative hostID": strings.Replace(valid, `"hostID":0`, `"hostID":-1`, 1),
		"duplicate datastore":       `{"version":1,"datastores":[` + entry + `,` + entry + `]}`,
		"duplicate endpoint":        strings.Replace(valid, `"hostName":"bridge-a.example.invalid"}]`, `"hostName":"bridge-a.example.invalid"},{"endpoint":"192.0.2.10","hostID":0,"hostName":"bridge-a.example.invalid"}]`, 1),
		"conflicting host identity": strings.Replace(valid, `"hostName":"bridge-a.example.invalid"}]`, `"hostName":"bridge-a.example.invalid"},{"endpoint":"192.0.2.11","hostID":0,"hostName":"bridge-b.example.invalid"}]`, 1),
		"trailing value":            valid + ` {}`, "empty endpoint": strings.Replace(valid, "192.0.2.10", "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "duplicate datastore" || name == "duplicate endpoint" {
				require.True(t, json.Valid([]byte(raw)), "duplicate array regression must be valid JSON")
			}
			_, err := ParseCephBridgeIdentities(raw)
			require.Error(t, err)
		})
	}
}
func TestCephBridgeIdentityActualHostPoolObservations(t *testing.T) {
	for _, scenario := range []string{"literal", "IP binding", "host zero", "empty mapping", "wrong ID", "renamed", "reused ID", "missing host", "duplicate ID", "duplicate name", "hash drift", "bridge order drift", "healthy plus unknown", "mapped disabled plus healthy", "literal disabled plus healthy", "literal monitoring-disabled plus healthy", "literal monitoring-error plus healthy", "literal error plus healthy", "literal offline plus healthy", "no eligible", "monitoring update", "comma stays literal", "unicode space stays literal", "API error", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			ds := bridgeFixtureDatastore()
			fp, err := CephBridgeConfigFingerprint(ds)
			require.NoError(t, err)
			raw := bridgeFixtureBinding(fp, 7, "bridge-a.example.invalid")
			hosts := []host.Host{{ID: 7, Name: "bridge-a.example.invalid", StateRaw: int(host.Monitored)}}
			pass := false
			apiError := false
			switch scenario {
			case "literal":
				ds.Ceph.BridgeList = "bridge-a.example.invalid"
				raw = ""
				pass = true
			case "IP binding":
				pass = true
			case "host zero":
				raw = bridgeFixtureBinding(fp, 0, "bridge-a.example.invalid")
				hosts[0].ID = 0
				pass = true
			case "empty mapping":
				raw = ""
			case "wrong ID":
				raw = bridgeFixtureBinding(fp, 99, "bridge-a.example.invalid")
			case "renamed":
				hosts[0].Name = "renamed.example.invalid"
			case "reused ID":
				hosts[0].ID = 8
			case "missing host":
				hosts = nil
			case "duplicate ID":
				hosts = append(hosts, host.Host{ID: 7, Name: "other.example.invalid", StateRaw: 2})
			case "duplicate name":
				hosts = append(hosts, host.Host{ID: 8, Name: hosts[0].Name, StateRaw: 2})
			case "hash drift":
				ds.Ceph.PoolName = "changed"
			case "bridge order drift":
				ds.Ceph.BridgeList = "bridge-b.example.invalid 192.0.2.10"
			case "healthy plus unknown":
				ds.Ceph.BridgeList = "unknown.example.invalid bridge-a.example.invalid"
				raw = ""
			case "mapped disabled plus healthy":
				ds.Ceph.BridgeList += " bridge-b.example.invalid"
				fp, err = CephBridgeConfigFingerprint(ds)
				require.NoError(t, err)
				raw = bridgeFixtureBinding(fp, 7, hosts[0].Name)
				hosts[0].StateRaw = int(host.Disabled)
				hosts = append(hosts, host.Host{ID: 8, Name: "bridge-b.example.invalid", StateRaw: 2})
			case "literal disabled plus healthy", "literal monitoring-disabled plus healthy", "literal monitoring-error plus healthy", "literal error plus healthy", "literal offline plus healthy":
				ds.Ceph.BridgeList = "bridge-a.example.invalid bridge-b.example.invalid"
				raw = ""
				hosts[0].StateRaw = map[string]int{"literal disabled plus healthy": 4, "literal monitoring-disabled plus healthy": 7, "literal monitoring-error plus healthy": 5, "literal error plus healthy": 3, "literal offline plus healthy": 8}[scenario]
				hosts = append(hosts, host.Host{ID: 8, Name: "bridge-b.example.invalid", StateRaw: 2})
			case "no eligible":
				ds.Ceph.BridgeList = hosts[0].Name
				raw = ""
				hosts[0].StateRaw = int(host.Disabled)
			case "monitoring update":
				hosts[0].StateRaw = int(host.MonitoringMonitored)
			case "comma stays literal", "unicode space stays literal":
				delimiter := ","
				if scenario == "unicode space stays literal" {
					delimiter = "\u00a0"
				}
				ds.Ceph.BridgeList = "bridge-a.example.invalid" + delimiter + "bridge-b.example.invalid"
				raw = ""
				hosts = append(hosts, host.Host{ID: 8, Name: "bridge-b.example.invalid", StateRaw: 2})
			case "API error":
				apiError = true
			}
			identities, err := ParseCephBridgeIdentities(raw)
			require.NoError(t, err)
			var reads atomic.Int32
			p := lifecycleFixture(t, func(method string) (bool, string, int) {
				reads.Add(1)
				require.Equal(t, "one.hostpool.info", method)
				if apiError {
					return false, "host observation unavailable", 0x800
				}
				body := "<HOST_POOL>"
				for _, h := range hosts {
					body += fmt.Sprintf("<HOST><ID>%d</ID><NAME>%s</NAME><STATE>%d</STATE></HOST>", h.ID, h.Name, h.StateRaw)
				}
				return true, body + "</HOST_POOL>", 0
			})
			ctx := context.Background()
			if scenario == "canceled" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			err = ProvisioningFeasible(ctx, p.ctrl, ds, identities)
			if pass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if scenario == "canceled" {
				require.Zero(t, reads.Load())
			} else {
				require.EqualValues(t, 1, reads.Load())
			}
		})
	}
}
func TestCephBridgeAuthorizationDoesNotCacheProviderEligibility(t *testing.T) {
	ds := bridgeFixtureDatastore()
	fp, err := CephBridgeConfigFingerprint(ds)
	require.NoError(t, err)
	identities, err := ParseCephBridgeIdentities(bridgeFixtureBinding(fp, 7, "bridge-a.example.invalid"))
	require.NoError(t, err)
	var reads atomic.Int32
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		require.Equal(t, "one.hostpool.info", method)
		state := 2
		if reads.Add(1) > 1 {
			state = 4
		}
		return true, fmt.Sprintf(`<HOST_POOL><HOST><ID>7</ID><NAME>bridge-a.example.invalid</NAME><STATE>%d</STATE></HOST></HOST_POOL>`, state), 0
	})
	require.NoError(t, ProvisioningFeasible(context.Background(), p.ctrl, ds, identities))
	require.Error(t, ProvisioningFeasible(context.Background(), p.ctrl, ds, identities))
	require.EqualValues(t, 2, reads.Load())
}

func TestBridgeIdentityRejectsAllocationAndCloneBeforeMutation(t *testing.T) {
	for _, operation := range []string{"create", "clone"} {
		for _, scenario := range []string{"verified", "unmapped", "mapped-disabled", "hash-drift", "observation-failed", "canceled-observation", "literal-disabled-mixed", "literal-monitoring-disabled-mixed", "literal-monitoring-error-mixed"} {
			t.Run(operation+"/"+scenario, func(t *testing.T) {
				ds := bridgeFixtureDatastore()
				if strings.HasPrefix(scenario, "literal-") {
					ds.Ceph.BridgeList = "bridge-a.example.invalid bridge-b.example.invalid"
				}
				fingerprint, err := CephBridgeConfigFingerprint(ds)
				require.NoError(t, err)
				raw := bridgeFixtureBinding(fingerprint, 7, "bridge-a.example.invalid")
				if scenario == "unmapped" || strings.HasPrefix(scenario, "literal-") {
					raw = ""
				}
				if scenario == "hash-drift" {
					raw = bridgeFixtureBinding("sha256:"+strings.Repeat("a", 64), 7, "bridge-a.example.invalid")
				}
				identities, err := ParseCephBridgeIdentities(raw)
				require.NoError(t, err)
				var allocate, clone, otherMutation atomic.Int32
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				p := lifecycleFixture(t, func(method string) (bool, string, int) {
					switch method {
					case "one.datastorepool.info":
						return true, strings.Replace(`<DATASTORE_POOL><DATASTORE><ID>42</ID><NAME>fixture</NAME><TYPE>IMAGE</TYPE><STATE>0</STATE><DS_MAD>ceph</DS_MAD><TM_MAD>ceph</TM_MAD><FREE_MB>1024</FREE_MB><TOTAL_MB>2048</TOTAL_MB><TEMPLATE><DISK_TYPE>RBD</DISK_TYPE><BRIDGE_LIST>192.0.2.10</BRIDGE_LIST><CEPH_HOST>monitor.example.invalid:6789</CEPH_HOST><POOL_NAME>volumes</POOL_NAME><CEPH_USER>csi</CEPH_USER><CEPH_SECRET>fixture-secret-reference</CEPH_SECRET></TEMPLATE></DATASTORE></DATASTORE_POOL>`, "<BRIDGE_LIST>192.0.2.10</BRIDGE_LIST>", "<BRIDGE_LIST>"+ds.Ceph.BridgeList+"</BRIDGE_LIST>", 1), 0
					case "one.hostpool.info":
						if scenario == "canceled-observation" {
							cancel()
						}
						if scenario == "observation-failed" {
							return false, "host observation unavailable", 0x800
						}
						state := 2
						if scenario == "mapped-disabled" || scenario == "literal-disabled-mixed" {
							state = 4
						}
						if scenario == "literal-monitoring-disabled-mixed" {
							state = 7
						}
						if scenario == "literal-monitoring-error-mixed" {
							state = 5
						}
						body := fmt.Sprintf(`<HOST_POOL><HOST><ID>7</ID><NAME>bridge-a.example.invalid</NAME><STATE>%d</STATE></HOST>`, state)
						if strings.HasPrefix(scenario, "literal-") {
							body += `<HOST><ID>8</ID><NAME>bridge-b.example.invalid</NAME><STATE>2</STATE></HOST>`
						}
						return true, body + "</HOST_POOL>", 0
					case "one.imagepool.info":
						return true, `<IMAGE_POOL><IMAGE><ID>7</ID><NAME>source</NAME></IMAGE></IMAGE_POOL>`, 0
					case "one.image.info":
						return true, `<IMAGE><ID>7</ID><NAME>source</NAME><STATE>1</STATE><SIZE>128</SIZE><DATASTORE_ID>42</DATASTORE_ID><TEMPLATE><OWNER>csi.opennebula.io</OWNER><CSI_IMAGE_ID>7</CSI_IMAGE_ID></TEMPLATE></IMAGE>`, 0
					case "one.image.allocate":
						allocate.Add(1)
						return false, "fixture stops after verified prerequisite", 0x800
					case "one.image.clone":
						clone.Add(1)
						return false, "fixture stops after verified prerequisite", 0x800
					default:
						otherMutation.Add(1)
						return false, "unexpected mutation", 0x800
					}
				})
				client, ok := p.ctrl.Client.(*goca.Client)
				require.True(t, ok)
				provider, err := NewPersistentDiskVolumeProvider(&OpenNebulaClient{Client: client}, p.HotplugPolicy(), identities)
				require.NoError(t, err)
				selection := DatastoreSelectionConfig{Identifiers: []string{"42"}, AllowedTypes: []string{"ceph"}}
				if operation == "create" {
					_, err = provider.CreateVolume(ctx, "new-volume", 128*sizeConversion, "csi.opennebula.io", false, "ext4", nil, selection)
				} else {
					_, err = provider.CloneVolume(ctx, "new-clone", "source", selection)
				}
				require.Error(t, err)
				if strings.HasPrefix(scenario, "literal-") {
					require.ErrorContains(t, err, "not currently MONITORED")
				}
				expected := int32(0)
				if scenario == "verified" {
					expected = 1
					require.ErrorContains(t, err, "fixture stops after verified prerequisite")
				}
				require.Equal(t, expected, allocate.Load()+clone.Load())
				require.Zero(t, otherMutation.Load())
			})
		}
	}
}
