package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestGetStringSliceParsesCSVValues(t *testing.T) {
	cfg := LoadConfiguration()
	cfg.OverrideVal(DefaultDatastoresVar, "100, 101 ,default")

	values, ok := cfg.GetStringSlice(DefaultDatastoresVar)
	require.True(t, ok)
	assert.Equal(t, []string{"100", "101", "default"}, values)
}

func TestNodeExpandDefaults(t *testing.T) {
	cfg := LoadConfiguration()

	verifyTimeout, ok := cfg.GetInt(NodeExpandVerifyTimeoutSecondsVar)
	require.True(t, ok)
	assert.Equal(t, 120, verifyTimeout)

	retryInterval, ok := cfg.GetInt(NodeExpandRetryIntervalSecondsVar)
	require.True(t, ok)
	assert.Equal(t, 2, retryInterval)

	sizeToleranceBytes, ok := cfg.GetInt(NodeExpandSizeToleranceBytesVar)
	require.True(t, ok)
	assert.Equal(t, 134217728, sizeToleranceBytes)
}

func TestNodeExpandOverrides(t *testing.T) {
	cfg := LoadConfiguration()
	cfg.OverrideVal(NodeExpandVerifyTimeoutSecondsVar, 30)
	cfg.OverrideVal(NodeExpandRetryIntervalSecondsVar, 5)
	cfg.OverrideVal(NodeExpandSizeToleranceBytesVar, 67108864)

	verifyTimeout, ok := cfg.GetInt(NodeExpandVerifyTimeoutSecondsVar)
	require.True(t, ok)
	assert.Equal(t, 30, verifyTimeout)

	retryInterval, ok := cfg.GetInt(NodeExpandRetryIntervalSecondsVar)
	require.True(t, ok)
	assert.Equal(t, 5, retryInterval)

	sizeToleranceBytes, ok := cfg.GetInt(NodeExpandSizeToleranceBytesVar)
	require.True(t, ok)
	assert.Equal(t, 67108864, sizeToleranceBytes)
}

func TestFeatureGateDefaultsAlignWithDriverDefaults(t *testing.T) {
	cfg := LoadConfiguration()
	raw, ok := cfg.GetString(FeatureGatesVar)
	require.True(t, ok)

	for _, expected := range []string{
		"compatibilityAwareSelection=true",
		"detachedDiskExpansion=true",
		"cephfsExpansion=true",
		"cephfsSnapshots=false",
		"cephfsClones=false",
		"cephfsSelfHealing=false",
		"cephfsPersistentRecovery=true",
		"cephfsKernelMounts=false",
		"localRWOStaleMountRecovery=false",
		"topologyAccessibility=true",
	} {
		assert.Truef(t, strings.Contains(raw, expected), "expected feature gate default %q in %q", expected, raw)
	}
}

func TestApplyKubeAPIClientRateLimit(t *testing.T) {
	cfg := LoadConfiguration()
	cfg.OverrideVal(KubeAPIQPSVar, 50)
	cfg.OverrideVal(KubeAPIBurstVar, 100)
	restConfig := &rest.Config{}

	ApplyKubeAPIClientRateLimit(restConfig, cfg)

	assert.Equal(t, float32(50), restConfig.QPS)
	assert.Equal(t, 100, restConfig.Burst)
}
