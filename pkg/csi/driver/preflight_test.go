package driver

import (
	"bytes"
	"context"
	"strings"
	"testing"

	datastoreSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/datastore"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/stretchr/testify/assert"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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

func TestCheckLocalImmediateBindingWarnsForLocalImmediate(t *testing.T) {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.PreflightLocalImmediateBindingPolicyVar, "warn")
	client := fake.NewSimpleClientset(&storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "local-immediate"},
		Provisioner: DefaultDriverName,
		Parameters: map[string]string{
			storageClassParamDatastoreIDs: "100",
		},
	})

	runner := NewPreflightRunner(cfg, nil, PreflightOptions{})
	outcome, message := runner.checkLocalImmediateBinding(context.Background(), client, []datastoreSchema.Datastore{
		{ID: 100, Name: "local-ds", DSMad: "fs", TMMad: "local", StateRaw: 0, Type: "IMAGE"},
	})

	assert.Equal(t, "warn", outcome)
	assert.Contains(t, message, "local-immediate")
}

func TestCheckLocalImmediateBindingFailsWhenPolicyRequiresIt(t *testing.T) {
	mode := storagev1.VolumeBindingImmediate
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.PreflightLocalImmediateBindingPolicyVar, "fail")
	client := fake.NewSimpleClientset(&storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "local-immediate"},
		Provisioner: DefaultDriverName,
		Parameters: map[string]string{
			storageClassParamDatastoreIDs: "100",
		},
		VolumeBindingMode: &mode,
	})

	runner := NewPreflightRunner(cfg, nil, PreflightOptions{})
	outcome, message := runner.checkLocalImmediateBinding(context.Background(), client, []datastoreSchema.Datastore{
		{ID: 100, Name: "local-ds", DSMad: "fs", TMMad: "local", StateRaw: 0, Type: "IMAGE"},
	})

	assert.Equal(t, "fail", outcome)
	assert.Contains(t, message, "Immediate")
}

func TestCheckLocalImmediateBindingPassesForWaitForFirstConsumer(t *testing.T) {
	mode := storagev1.VolumeBindingWaitForFirstConsumer
	cfg := config.LoadConfiguration()
	client := fake.NewSimpleClientset(&storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "local-wffc"},
		Provisioner: DefaultDriverName,
		Parameters: map[string]string{
			storageClassParamDatastoreIDs: "100",
		},
		VolumeBindingMode: &mode,
	})

	runner := NewPreflightRunner(cfg, nil, PreflightOptions{})
	outcome, message := runner.checkLocalImmediateBinding(context.Background(), client, []datastoreSchema.Datastore{
		{ID: 100, Name: "local-ds", DSMad: "fs", TMMad: "local", StateRaw: 0, Type: "IMAGE"},
	})

	assert.Equal(t, "pass", outcome)
	assert.Contains(t, message, "no local-backed StorageClasses")
}

func TestInspectStorageClassesIncludesWarnings(t *testing.T) {
	cfg := config.LoadConfiguration()
	immediate := storagev1.VolumeBindingImmediate
	client := fake.NewSimpleClientset(&storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "local-immediate"},
		Provisioner:          DefaultDriverName,
		Parameters:           map[string]string{storageClassParamDatastoreIDs: "100"},
		VolumeBindingMode:    &immediate,
		AllowVolumeExpansion: boolPtrPreflight(false),
	})

	runner := NewPreflightRunner(cfg, nil, PreflightOptions{})
	summaries := runner.inspectStorageClasses(context.Background(), client, []datastoreSchema.Datastore{
		{ID: 100, Name: "local-ds", DSMad: "fs", TMMad: "local", StateRaw: 0, Type: "IMAGE"},
	})

	assert.Len(t, summaries, 1)
	assert.Equal(t, "local", summaries[0].Backend)
	assert.NotEmpty(t, summaries[0].Warnings)
}

func boolPtrPreflight(value bool) *bool {
	return &value
}
