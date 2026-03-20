package driver

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/stretchr/testify/assert"
)

func TestParseSecretRefsCSV(t *testing.T) {
	refs, err := ParseSecretRefsCSV("storage/cephfs-node,omni/cephfs-admin")
	assert.NoError(t, err)
	assert.Equal(t, []SecretRef{
		{Namespace: "storage", Name: "cephfs-node"},
		{Namespace: "omni", Name: "cephfs-admin"},
	}, refs)
}

func TestParseSecretRefsCSVRejectsInvalidValue(t *testing.T) {
	_, err := ParseSecretRefsCSV("missing-namespace")
	assert.Error(t, err)
}

func TestRunPreflightCommandOutputsJSONFailure(t *testing.T) {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.OpenNebulaRPCEndpointVar, "")
	cfg.OverrideVal(config.OpenNebulaCredentialsVar, "")

	var buf bytes.Buffer
	err := RunPreflightCommand(context.Background(), cfg, nil, PreflightOptions{}, "json", &buf)
	assert.Error(t, err)
	assert.Contains(t, buf.String(), "\"passed\": false")
	assert.Contains(t, buf.String(), "\"check\": \"opennebula_auth\"")
}

func TestRunPreflightCommandOutputsText(t *testing.T) {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.OpenNebulaRPCEndpointVar, "")
	cfg.OverrideVal(config.OpenNebulaCredentialsVar, "")

	var buf bytes.Buffer
	err := RunPreflightCommand(context.Background(), cfg, nil, PreflightOptions{}, "text", &buf)
	assert.Error(t, err)
	assert.True(t, strings.Contains(buf.String(), "[FAIL] opennebula_auth"))
}
