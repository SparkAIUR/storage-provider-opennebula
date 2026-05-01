package driver

import (
	"testing"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/stretchr/testify/assert"
)

func TestAdaptiveTimeoutSourceIDUsesValidConfigMapDataKeys(t *testing.T) {
	assert.Equal(t, "", (*Driver)(nil).adaptiveTimeoutSourceID())
	assert.Equal(t, "controller", (&Driver{}).adaptiveTimeoutSourceID())
	assert.Equal(t, "node.hplbravow02", (&Driver{nodeID: "hplbravow02"}).adaptiveTimeoutSourceID())
	assert.Equal(t, "node.node_a_1", (&Driver{nodeID: "node/a:1"}).adaptiveTimeoutSourceID())
	assert.Equal(t, "___", sanitizeConfigMapDataKey(":::"))
}

func TestNewDriverLoadsFeatureGatesFromConfig(t *testing.T) {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.FeatureGatesVar, "detachedDiskExpansion=false,cephfsExpansion=false,cephfsPersistentRecovery=true")

	d := NewDriver(&DriverOptions{
		NodeID:             "node-a",
		DriverName:         DefaultDriverName,
		GRPCServerEndpoint: DefaultGRPCServerEndpoint,
		PluginConfig:       cfg,
	})

	assert.Equal(t, "node-a", d.nodeID)
	assert.False(t, d.featureGates.DetachedDiskExpansion)
	assert.False(t, d.featureGates.CephFSExpansion)
	assert.True(t, d.featureGates.CephFSPersistentRecovery)
	assert.NotNil(t, d.operationLocks)
	assert.NotNil(t, d.hotplugGuard)
}

func TestLoadHotplugTimeoutPolicyUsesLegacyFallbackAndBounds(t *testing.T) {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.VMHotplugTimeoutBaseVar, 0)
	cfg.OverrideVal(config.VMHotplugTimeoutVar, 150)
	cfg.OverrideVal(config.VMHotplugTimeoutPer100GiVar, -1)
	cfg.OverrideVal(config.VMHotplugTimeoutMaxVar, 60)

	policy := loadHotplugTimeoutPolicy(cfg)

	assert.Equal(t, 150, int(policy.BaseTimeout.Seconds()))
	assert.Equal(t, 60, int(policy.Per100GiB.Seconds()))
	assert.Equal(t, 150, int(policy.MaxTimeout.Seconds()))
	assert.Equal(t, 1, int(policy.PollInterval.Seconds()))
}
