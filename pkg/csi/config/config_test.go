package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetStringSliceParsesCSVValues(t *testing.T) {
	cfg := LoadConfiguration()
	cfg.OverrideVal(DefaultDatastoresVar, "100, 101 ,default")

	values, ok := cfg.GetStringSlice(DefaultDatastoresVar)
	require.True(t, ok)
	assert.Equal(t, []string{"100", "101", "default"}, values)
}
