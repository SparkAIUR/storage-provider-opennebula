package opennebula

import (
	"context"
	"fmt"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
)

// ProvisioningFeasible is observation-only; it never changes host or datastore state.
func ProvisioningFeasible(ctx context.Context, ctrl *goca.Controller, ds Datastore, identities CephBridgeIdentities) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !ds.Enabled {
		return fmt.Errorf("datastore %d is disabled in OpenNebula", ds.ID)
	}
	if ds.Backend != "ceph-rbd" && ds.Type != "ceph" {
		return nil
	}
	if ds.Ceph == nil {
		return fmt.Errorf("datastore %d has no Ceph bridge configuration", ds.ID)
	}
	if ctrl == nil {
		return fmt.Errorf("ceph bridge provider observation unavailable")
	}
	pool, err := ctrl.Hosts().InfoContext(ctx)
	if err != nil {
		return fmt.Errorf("ceph bridge availability unknown: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return CephBridgeFeasible(ds, pool.Hosts, identities)
}
