package driver

import (
	"context"
	"strconv"
	"strings"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
)

func (s *ControllerServer) applyCreateVolumeTopology(ctx context.Context, selection opennebula.DatastoreSelectionConfig, params map[string]string, requirements *csi.TopologyRequirement) opennebula.DatastoreSelectionConfig {
	if s == nil || s.driver == nil {
		return selection
	}
	if systemDS := s.selectedNodeSystemDatastore(ctx, params); systemDS > 0 {
		selection.RequiredSystemDatastores = []int{systemDS}
		return selection
	}
	required := systemDatastoresFromTopologyRequirement(requirements)
	if len(required) > 0 {
		selection.RequiredSystemDatastores = required
	}
	return selection
}

func (s *ControllerServer) selectedNodeSystemDatastore(ctx context.Context, params map[string]string) int {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil {
		return 0
	}
	selectedNode, err := s.driver.kubeRuntime.SelectedNodeForPVC(ctx, params[paramPVCNamespace], params[paramPVCName])
	if err != nil || strings.TrimSpace(selectedNode) == "" {
		return 0
	}
	systemDS, err := s.driver.kubeRuntime.NodeSystemDatastore(ctx, selectedNode)
	if err != nil {
		return 0
	}
	id, err := strconv.Atoi(strings.TrimSpace(systemDS))
	if err != nil || id <= 0 {
		return 0
	}
	return id
}

func systemDatastoresFromTopologyRequirement(requirements *csi.TopologyRequirement) []int {
	if requirements == nil {
		return nil
	}
	if preferred := systemDatastoresFromTopologies(requirements.GetPreferred()); len(preferred) > 0 {
		return preferred
	}
	return systemDatastoresFromTopologies(requirements.GetRequisite())
}

func systemDatastoresFromTopologies(topologies []*csi.Topology) []int {
	if len(topologies) == 0 {
		return nil
	}
	seen := map[int]struct{}{}
	values := make([]int, 0, len(topologies))
	for _, topology := range topologies {
		if topology == nil {
			continue
		}
		raw := strings.TrimSpace(topology.GetSegments()[topologySystemDSLabel])
		if raw == "" {
			continue
		}
		id, err := strconv.Atoi(raw)
		if err != nil || id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		values = append(values, id)
	}
	return values
}

func (d *Driver) nodeAccessibleTopology(ctx context.Context) []*csi.Topology {
	if d == nil || !d.featureGates.TopologyAccessibility {
		return nil
	}

	if value, ok := d.PluginConfig.GetString(config.NodeTopologySystemDSVar); ok {
		if topology := topologyForSystemDatastore(strings.TrimSpace(value)); topology != nil {
			return []*csi.Topology{topology}
		}
	}

	if d.kubeRuntime == nil {
		return nil
	}

	value, err := d.kubeRuntime.GetNodeLabel(ctx, d.nodeID, topologySystemDSLabel)
	if err != nil {
		return nil
	}

	if topology := topologyForSystemDatastore(value); topology != nil {
		return []*csi.Topology{topology}
	}

	return nil
}

func accessibleTopologyForDatastore(datastore opennebula.Datastore) []*csi.Topology {
	if len(datastore.CompatibleSystemDatastores) == 0 {
		return nil
	}

	topologies := make([]*csi.Topology, 0, len(datastore.CompatibleSystemDatastores))
	for _, systemDSID := range datastore.CompatibleSystemDatastores {
		topology := topologyForSystemDatastore(strconv.Itoa(systemDSID))
		if topology == nil {
			continue
		}
		topologies = append(topologies, topology)
	}

	if len(topologies) == 0 {
		return nil
	}

	return topologies
}

func topologyForSystemDatastore(value string) *csi.Topology {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}

	return &csi.Topology{
		Segments: map[string]string{
			topologySystemDSLabel: trimmed,
		},
	}
}
