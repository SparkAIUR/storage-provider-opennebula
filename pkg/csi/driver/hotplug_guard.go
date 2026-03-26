package driver

import (
	"sync"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
)

type HotplugGuard struct {
	mu       sync.Mutex
	cooldown time.Duration
	states   map[string]opennebula.HotplugCooldownState
}

func NewHotplugGuard(cooldown time.Duration) *HotplugGuard {
	return &HotplugGuard{
		cooldown: cooldown,
		states:   make(map[string]opennebula.HotplugCooldownState),
	}
}

func (g *HotplugGuard) MarkCooldown(node, operation, volume string, timeout time.Duration, attached, ready bool) opennebula.HotplugCooldownState {
	g.mu.Lock()
	defer g.mu.Unlock()

	state := opennebula.HotplugCooldownState{
		Node:                 node,
		Operation:            operation,
		Volume:               volume,
		ExpiresAt:            time.Now().Add(g.cooldown),
		Timeout:              timeout,
		LastObservedAttached: attached,
		LastObservedReady:    ready,
	}
	g.states[node] = state
	return state
}

func (g *HotplugGuard) Get(node string) (opennebula.HotplugCooldownState, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	state, ok := g.states[node]
	if !ok {
		return opennebula.HotplugCooldownState{}, false
	}
	if time.Now().After(state.ExpiresAt) {
		delete(g.states, node)
		return opennebula.HotplugCooldownState{}, false
	}
	return state, true
}

func (g *HotplugGuard) Clear(node string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.states, node)
}
