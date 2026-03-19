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
		{ID: 200, Name: "ceph-a", DSMad: "ceph", TMMad: "ceph", StateRaw: 0},
	}, DatastoreSelectionConfig{
		Identifiers:  []string{"200"},
		AllowedTypes: []string{"local"},
	})
	require.Error(t, err)
	assert.True(t, IsDatastoreConfigError(err))
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
