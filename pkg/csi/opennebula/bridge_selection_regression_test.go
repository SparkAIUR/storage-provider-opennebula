package opennebula

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBridgeSelectionRejectsHealthyPlusUnknown(t *testing.T) {
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		require.Equal(t, "one.hostpool.info", method)
		return true, `<HOST_POOL><HOST><ID>7</ID><NAME>ready.example.invalid</NAME><STATE>2</STATE></HOST></HOST_POOL>`, 0
	})
	ds := Datastore{ID: 42, Enabled: true, Type: "ceph", Backend: "ceph", Ceph: &CephDatastoreAttributes{BridgeList: "unknown.example.invalid ready.example.invalid"}}
	require.Error(t, ProvisioningFeasible(context.Background(), p.ctrl, ds, CephBridgeIdentities{}))
}
