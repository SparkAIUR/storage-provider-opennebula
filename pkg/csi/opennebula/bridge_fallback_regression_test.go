package opennebula

import (
	"fmt"
	"strings"
	"testing"

	host "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/host"
	"github.com/stretchr/testify/require"
)

func TestBridgeFallbackRequiresEveryConfiguredEndpoint(t *testing.T) {
	for _, state := range []host.State{host.Disabled, host.Error, host.MonitoringDisabled, host.MonitoringError, host.Offline} {
		t.Run(state.String(), func(t *testing.T) {
			ds := Datastore{ID: 42, Type: "ceph", Ceph: &CephDatastoreAttributes{BridgeList: "ready.example.invalid excluded.example.invalid"}}
			hosts := []host.Host{{ID: 7, Name: "ready.example.invalid", StateRaw: int(host.Monitored)}, {ID: 8, Name: "excluded.example.invalid", StateRaw: int(state)}}
			require.Error(t, CephBridgeFeasible(ds, hosts, CephBridgeIdentities{}), "libfs may select the original list after onehost CLI failure")
		})
	}
}

func TestBridgeEndpointGrammarSharedByFingerprintAndAuthorization(t *testing.T) {
	for _, token := range []string{"ready.example.invalid,other.example.invalid", "ready;command", "ready|other", "ready&other", "$(command)", "`command`", "*", "ready?", "[2001:db8::1]", `"ready"`, "'ready'", "ready\\other", "ready>file", "ready<file", "ready/other", "user@ready", "-option", "ready\r", "ready\vother", "ready\u00a0other"} {
		t.Run(fmt.Sprintf("%q", token), func(t *testing.T) {
			ds := Datastore{ID: 42, Type: "ceph", Ceph: &CephDatastoreAttributes{BridgeList: token}}
			_, err := CephBridgeConfigFingerprint(ds)
			require.Error(t, err)
			require.Error(t, CephBridgeFeasible(ds, []host.Host{{ID: 7, Name: token, StateRaw: 2}}, CephBridgeIdentities{}))
			raw := fmt.Sprintf(`{"version":1,"datastores":[{"datastoreID":42,"configFingerprint":"sha256:%s","endpoints":[{"endpoint":%q,"hostID":7,"hostName":"ready.example.invalid"}]}]}`, strings.Repeat("a", 64), token)
			_, err = ParseCephBridgeIdentities(raw)
			require.Error(t, err)
		})
	}
	ds := Datastore{ID: 42, Type: "ceph", Ceph: &CephDatastoreAttributes{BridgeList: "ready.example.invalid ready.example.invalid"}}
	_, err := CephBridgeConfigFingerprint(ds)
	require.Error(t, err)
	require.Error(t, CephBridgeFeasible(ds, []host.Host{{ID: 7, Name: "ready.example.invalid", StateRaw: 2}}, CephBridgeIdentities{}))
	for _, endpoint := range []string{"192.0.2.10", "2001:db8::10", "ready.example.invalid", "ssh_alias-1"} {
		ds.Ceph.BridgeList = " \t" + endpoint + "\n"
		_, err := CephBridgeConfigFingerprint(ds)
		require.NoError(t, err)
		require.NoError(t, CephBridgeFeasible(ds, []host.Host{{ID: 7, Name: endpoint, StateRaw: 2}}, CephBridgeIdentities{}))
	}
}
