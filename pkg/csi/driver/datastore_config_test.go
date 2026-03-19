package driver

import (
	"testing"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDatastoreSelectionConfigUsesStorageClassOverride(t *testing.T) {
	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.DefaultDatastoresVar, "100,101")
	pluginConfig.OverrideVal(config.DatastorePolicyVar, "ordered")
	pluginConfig.OverrideVal(config.AllowedDatastoreTypesVar, "local")

	driver := &Driver{PluginConfig: pluginConfig}

	selection, err := driver.GetDatastoreSelectionConfig(map[string]string{
		storageClassParamDatastoreIDs:             "200,201",
		storageClassParamDatastoreSelectionPolicy: "least-used",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"200", "201"}, selection.Identifiers)
	assert.Equal(t, opennebula.DatastoreSelectionPolicyLeastUsed, selection.Policy)
	assert.Equal(t, []string{"local"}, selection.AllowedTypes)
}

func TestGetDatastoreSelectionConfigFallsBackToDriverDefaults(t *testing.T) {
	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.DefaultDatastoresVar, "100,101")
	pluginConfig.OverrideVal(config.DatastorePolicyVar, "ordered")
	pluginConfig.OverrideVal(config.AllowedDatastoreTypesVar, "local,ceph")

	driver := &Driver{PluginConfig: pluginConfig}

	selection, err := driver.GetDatastoreSelectionConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"100", "101"}, selection.Identifiers)
	assert.Equal(t, opennebula.DatastoreSelectionPolicyOrdered, selection.Policy)
	assert.Equal(t, []string{"local", "ceph"}, selection.AllowedTypes)
}

func TestFilterProvisioningParamsRemovesReservedKeys(t *testing.T) {
	filtered := filterProvisioningParams(map[string]string{
		storageClassParamDatastoreIDs:             "100",
		storageClassParamDatastoreSelectionPolicy: "least-used",
		storageClassParamFSType:                   "xfs",
		"driver":                                  "raw",
		"cache":                                   "none",
	})

	assert.Equal(t, map[string]string{
		"driver": "raw",
		"cache":  "none",
	}, filtered)
}

func TestGetAllowedDatastoreTypesUsesCephEnabledDefault(t *testing.T) {
	pluginConfig := config.LoadConfiguration()

	driver := &Driver{PluginConfig: pluginConfig}

	assert.Equal(t, []string{"local", "ceph"}, driver.getAllowedDatastoreTypes())
}
