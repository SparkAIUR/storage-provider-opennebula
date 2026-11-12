package opennebula

import (
	"context"
	"fmt"
	"strings"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	host "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/host"
)

// ProvisioningFeasible is observation-only; it never changes host or datastore state.
func ProvisioningFeasible(ctx context.Context, ctrl *goca.Controller, ds Datastore) error {
	if !ds.Enabled {
		return fmt.Errorf("datastore %d is disabled in OpenNebula", ds.ID)
	}
	if ds.Backend != "ceph-rbd" && ds.Type != "ceph" {
		return nil
	}
	if ds.Ceph == nil {
		return fmt.Errorf("datastore %d has no Ceph bridge configuration", ds.ID)
	}
	pool, err := ctrl.Hosts().InfoContext(ctx)
	if err != nil {
		return fmt.Errorf("Ceph bridge availability unknown: %w", err)
	}
	return CephBridgeFeasible(ds.Ceph.BridgeList, pool.Hosts)
}

func CephBridgeFeasible(bridges string, hosts []host.Host) error {
	wanted := strings.Fields(strings.ReplaceAll(bridges, ",", " "))
	if len(wanted) == 0 {
		return fmt.Errorf("Ceph BRIDGE_LIST is empty")
	}
	for _, name := range wanted {
		for _, h := range hosts {
			if h.Name != name {
				continue
			}
			state, err := h.State()
			if err == nil && state == host.Monitored {
				return nil
			}
		}
	}
	return fmt.Errorf("no configured Ceph bridge has verified monitored/enabled host state; unregistered bridges require external read-only verification")
}
