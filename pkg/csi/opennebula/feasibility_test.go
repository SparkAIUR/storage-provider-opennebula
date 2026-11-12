package opennebula

import (
	host "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/host"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCephBridgeFeasibility(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bridges string
		hosts   []host.Host
		pass    bool
	}{
		{"empty", "", nil, false}, {"unregistered", "bridge", nil, false},
		{"disabled", "bridge", []host.Host{{Name: "bridge", StateRaw: int(host.Disabled)}}, false},
		{"one ready", "bad bridge", []host.Host{{Name: "bridge", StateRaw: int(host.Monitored)}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CephBridgeFeasible(tc.bridges, tc.hosts)
			if tc.pass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
