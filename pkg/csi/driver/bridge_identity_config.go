package driver

import (
	"fmt"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
)

func loadCephBridgeIdentities(cfg config.CSIPluginConfig) (opennebula.CephBridgeIdentities, error) {
	raw, _ := cfg.GetString(config.CephBridgeIdentitiesVar)
	identities, err := opennebula.ParseCephBridgeIdentities(raw)
	if err != nil {
		return identities, fmt.Errorf("%s: %w", config.CephBridgeIdentitiesVar, err)
	}
	return identities, nil
}
