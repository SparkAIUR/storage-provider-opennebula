package opennebula

import (
	"testing"

	datastoreSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveDatastoresSupportsDefaultAliasAndIDLookup(t *testing.T) {
	pool := []datastoreSchema.Datastore{
		{ID: 42, Name: "default", DSMad: "fs", TMMad: "local", StateRaw: 0, FreeMB: 512},
		{ID: 100, Name: "fast-local", DSMad: "fs", TMMad: "local", StateRaw: 0, FreeMB: 1024},
	}

	resolved, err := ResolveDatastores(pool, DatastoreSelectionConfig{
		Identifiers:  []string{"default", "100"},
		AllowedTypes: []string{"local"},
	})
	require.NoError(t, err)
	require.Len(t, resolved, 2)
	assert.Equal(t, 42, resolved[0].ID)
	assert.Equal(t, 100, resolved[1].ID)
	assert.Equal(t, "local", resolved[0].Type)
}

func TestResolveDatastoresRejectsUnknownIdentifier(t *testing.T) {
	_, err := ResolveDatastores([]datastoreSchema.Datastore{
		{ID: 100, Name: "fast-local", DSMad: "fs", TMMad: "local", StateRaw: 0},
	}, DatastoreSelectionConfig{
		Identifiers:  []string{"missing"},
		AllowedTypes: []string{"local"},
	})
	require.Error(t, err)
	assert.True(t, IsDatastoreConfigError(err))
}

func TestResolveDatastoresRejectsDisallowedType(t *testing.T) {
	_, err := ResolveDatastores([]datastoreSchema.Datastore{
		newTestDatastore(200, "ceph-a", "ceph", "ceph", map[string]string{
			"DISK_TYPE":   "RBD",
			"POOL_NAME":   "one",
			"CEPH_HOST":   "mon1 mon2",
			"CEPH_USER":   "libvirt",
			"CEPH_SECRET": "secret-id",
			"BRIDGE_LIST": "frontend",
		}),
	}, DatastoreSelectionConfig{
		Identifiers:  []string{"200"},
		AllowedTypes: []string{"local"},
	})
	require.Error(t, err)
	assert.True(t, IsDatastoreConfigError(err))
}

func TestResolveDatastoresAcceptsValidCephImageDatastore(t *testing.T) {
	resolved, err := ResolveDatastores([]datastoreSchema.Datastore{
		newTestDatastore(200, "ceph-a", "ceph", "ceph", map[string]string{
			"DISK_TYPE":   "RBD",
			"POOL_NAME":   "one",
			"CEPH_HOST":   "mon1 mon2",
			"CEPH_USER":   "libvirt",
			"CEPH_SECRET": "secret-id",
			"BRIDGE_LIST": "frontend",
			"RBD_FORMAT":  "2",
		}),
	}, DatastoreSelectionConfig{
		Identifiers:  []string{"200"},
		AllowedTypes: []string{"ceph"},
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	assert.Equal(t, "ceph", resolved[0].Type)
	require.NotNil(t, resolved[0].Ceph)
	assert.Equal(t, "one", resolved[0].Ceph.PoolName)
	assert.Equal(t, "RBD", resolved[0].DiskType)
}

func TestResolveDatastoresRejectsCephDatastoreMissingRequiredAttr(t *testing.T) {
	_, err := ResolveDatastores([]datastoreSchema.Datastore{
		newTestDatastore(200, "ceph-a", "ceph", "ceph", map[string]string{
			"DISK_TYPE":   "RBD",
			"POOL_NAME":   "one",
			"CEPH_USER":   "libvirt",
			"CEPH_SECRET": "secret-id",
			"BRIDGE_LIST": "frontend",
		}),
	}, DatastoreSelectionConfig{
		Identifiers:  []string{"200"},
		AllowedTypes: []string{"ceph"},
	})
	require.Error(t, err)
	assert.True(t, IsDatastoreConfigError(err))
	assert.Contains(t, err.Error(), "CEPH_HOST")
}

func TestResolveDatastoresRejectsCephDatastoreWithoutRBDDiskType(t *testing.T) {
	_, err := ResolveDatastores([]datastoreSchema.Datastore{
		newTestDatastore(200, "ceph-a", "ceph", "ceph", map[string]string{
			"DISK_TYPE":   "FILE",
			"POOL_NAME":   "one",
			"CEPH_HOST":   "mon1 mon2",
			"CEPH_USER":   "libvirt",
			"CEPH_SECRET": "secret-id",
			"BRIDGE_LIST": "frontend",
		}),
	}, DatastoreSelectionConfig{
		Identifiers:  []string{"200"},
		AllowedTypes: []string{"ceph"},
	})
	require.Error(t, err)
	assert.True(t, IsDatastoreConfigError(err))
	assert.Contains(t, err.Error(), "DISK_TYPE=RBD")
}

func TestResolveDatastoresRejectsCephDatastoreWithWrongMADs(t *testing.T) {
	_, err := ResolveDatastores([]datastoreSchema.Datastore{
		newTestDatastore(200, "ceph-a", "ceph", "ssh", map[string]string{
			"DISK_TYPE":   "RBD",
			"POOL_NAME":   "one",
			"CEPH_HOST":   "mon1 mon2",
			"CEPH_USER":   "libvirt",
			"CEPH_SECRET": "secret-id",
			"BRIDGE_LIST": "frontend",
		}),
	}, DatastoreSelectionConfig{
		Identifiers:  []string{"200"},
		AllowedTypes: []string{"ceph"},
	})
	require.Error(t, err)
	assert.True(t, IsDatastoreConfigError(err))
	assert.Contains(t, err.Error(), "TM_MAD=ceph")
}

func TestCompareCephConnectionIdentityRejectsMismatch(t *testing.T) {
	imageDS := newTestDatastore(200, "ceph-image", "ceph", "ceph", map[string]string{
		"DISK_TYPE":   "RBD",
		"POOL_NAME":   "one",
		"CEPH_HOST":   "mon1 mon2",
		"CEPH_USER":   "libvirt",
		"CEPH_SECRET": "secret-a",
		"BRIDGE_LIST": "frontend",
	})
	systemDS := newTestDatastore(201, "ceph-system", "", "ceph", map[string]string{
		"DISK_TYPE":   "RBD",
		"POOL_NAME":   "one",
		"CEPH_HOST":   "mon1 mon2",
		"CEPH_USER":   "libvirt",
		"CEPH_SECRET": "secret-b",
		"BRIDGE_LIST": "host-a",
	})

	err := compareCephConnectionIdentity(imageDS, systemDS)
	require.Error(t, err)
	assert.True(t, IsDatastoreConfigError(err))
	assert.Contains(t, err.Error(), "CEPH_SECRET")
}

func TestResolveDeploymentMode(t *testing.T) {
	assert.Equal(t, DeploymentModeCeph, resolveDeploymentMode(newTestDatastore(200, "ceph-system", "", "ceph", nil)))
	assert.Equal(t, DeploymentModeSSH, resolveDeploymentMode(newTestDatastore(201, "ssh-system", "", "ssh", nil)))
	assert.Equal(t, DeploymentModeUnknown, resolveDeploymentMode(newTestDatastore(202, "other", "", "shared", nil)))
}

func TestOrderDatastoresLeastUsedSortsByFreeCapacity(t *testing.T) {
	ordered, err := OrderDatastores([]Datastore{
		{ID: 100, FreeBytes: 10},
		{ID: 101, FreeBytes: 30},
		{ID: 102, FreeBytes: 20},
	}, DatastoreSelectionPolicyLeastUsed)
	require.NoError(t, err)
	assert.Equal(t, []int{101, 102, 100}, []int{ordered[0].ID, ordered[1].ID, ordered[2].ID})
}

func TestOrderDatastoresOrderedPreservesConfiguredOrder(t *testing.T) {
	ordered, err := OrderDatastores([]Datastore{
		{ID: 100, FreeBytes: 10},
		{ID: 101, FreeBytes: 30},
		{ID: 102, FreeBytes: 20},
	}, DatastoreSelectionPolicyOrdered)
	require.NoError(t, err)
	assert.Equal(t, []int{100, 101, 102}, []int{ordered[0].ID, ordered[1].ID, ordered[2].ID})
}

func TestSumDatastoreCapacityAggregatesConfiguredPool(t *testing.T) {
	total := SumDatastoreCapacity([]Datastore{
		{ID: 100, FreeBytes: 10},
		{ID: 101, FreeBytes: 20},
		{ID: 102, FreeBytes: 30},
	})

	assert.Equal(t, int64(60), total)
}

func newTestDatastore(id int, name, dsMad, tmMad string, attrs map[string]string) datastoreSchema.Datastore {
	tpl := datastoreSchema.NewTemplate()
	for key, value := range attrs {
		tpl.AddPair(key, value)
	}

	ds := datastoreSchema.Datastore{
		ID:       id,
		Name:     name,
		DSMad:    dsMad,
		TMMad:    tmMad,
		StateRaw: 0,
		Template: *tpl,
	}
	if value, ok := attrs["DISK_TYPE"]; ok {
		ds.DiskType = value
	}

	return ds
}
