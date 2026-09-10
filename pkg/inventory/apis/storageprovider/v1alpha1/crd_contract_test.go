package v1alpha1

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

func TestDatastoreCRDIncludesDisplayColumnsAndTypedStatus(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve current file path")
	}
	crdPath := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../../../../helm/opennebula-csi/crds/opennebuladatastores.storageprovider.opennebula.sparkaiur.io.yaml"))
	payload, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("failed reading datastore CRD: %v", err)
	}
	text := string(payload)

	requiredSnippets := []string{
		"- name: Status",
		"- name: ID",
		"- name: Name",
		"- name: Capacity",
		"- name: Type",
		"- name: Backend",
		"- name: SCs",
		"- name: Metrics",
		"capacityDisplay:",
		"storageClassesDisplay:",
		"metricsDisplay:",
		"health:",
		"maintenanceMode:",
		"storageClassDetails:",
		"validationLastOutcome:",
	}
	for _, snippet := range requiredSnippets {
		if !strings.Contains(text, snippet) {
			t.Fatalf("expected datastore CRD to contain %q", snippet)
		}
	}
	if strings.Contains(text, "- name: Enabled") || strings.Contains(text, "jsonPath: .status.capacity.freeBytes") {
		t.Fatal("legacy datastore printer columns are still present")
	}
}

func TestBenchmarkRunCRDIncludesTypedStatus(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to resolve current file path")
	crdPath := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../../../../helm/opennebula-csi/crds/opennebuladatastorebenchmarkruns.storageprovider.opennebula.sparkaiur.io.yaml"))
	payload, err := os.ReadFile(crdPath)
	require.NoError(t, err)
	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.UnmarshalStrict(payload, &crd))
	require.Equal(t, "apiextensions.k8s.io/v1", crd.APIVersion)
	require.Equal(t, "CustomResourceDefinition", crd.Kind)
	require.Equal(t, "storageprovider.opennebula.sparkaiur.io", crd.Spec.Group)
	require.Equal(t, "OpenNebulaDatastoreBenchmarkRun", crd.Spec.Names.Kind)
	require.Equal(t, "opennebuladatastorebenchmarkruns", crd.Spec.Names.Plural)
	served := 0
	for _, version := range crd.Spec.Versions {
		if !version.Served {
			continue
		}
		served++
		t.Run(version.Name, func(t *testing.T) {
			require.NotNil(t, version.Schema)
			require.NotNil(t, version.Schema.OpenAPIV3Schema)
			root := version.Schema.OpenAPIV3Schema
			require.Equal(t, "object", root.Type)
			for section, fields := range map[string]map[string]string{
				"spec":   {"datastoreID": "integer", "accessModes": "array", "fioArgs": "array", "activeDeadlineSeconds": "integer"},
				"status": {"datastoreID": "integer", "datastoreName": "string", "selectedDatastoreID": "integer", "selectedDatastoreName": "string", "phase": "string", "summary": "string"},
			} {
				property, ok := root.Properties[section]
				require.True(t, ok, "missing %s schema", section)
				require.Equal(t, "object", property.Type)
				for name, fieldType := range fields {
					field, ok := property.Properties[name]
					require.True(t, ok, "missing %s.%s", section, name)
					require.Equal(t, fieldType, field.Type, "%s.%s", section, name)
					if fieldType == "array" {
						require.NotNil(t, field.Items)
						require.NotNil(t, field.Items.Schema)
						require.Equal(t, "string", field.Items.Schema.Type)
					}
				}
			}
			require.Contains(t, root.Properties["spec"].Required, "datastoreID")
			require.NotNil(t, version.Subresources)
			require.NotNil(t, version.Subresources.Status)
			columns := map[string]apiextensionsv1.CustomResourceColumnDefinition{}
			for _, column := range version.AdditionalPrinterColumns {
				columns[column.Name] = column
			}
			for name, path := range map[string]string{"Datastore": ".status.datastoreName", "Phase": ".status.phase"} {
				require.Equal(t, path, columns[name].JSONPath)
				require.Equal(t, "string", columns[name].Type)
			}
		})
	}
	require.Positive(t, served, "benchmark CRD must serve a typed version")
}

func TestNodeCRDIncludesHotplugDiagnosisStatus(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve current file path")
	}
	crdPath := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../../../../helm/opennebula-csi/crds/opennebulaNodes.storageprovider.opennebula.sparkaiur.io.yaml"))
	payload, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("failed reading node CRD: %v", err)
	}
	text := string(payload)
	requiredSnippets := []string{
		"- name: Hotplug",
		"jsonPath: .status.hotplug.diagnosis.classification",
		"diagnosis:",
		"classification:",
		"volumeHandle:",
		"stuckAfterSeconds:",
		"recommendedAction:",
	}
	for _, snippet := range requiredSnippets {
		if !strings.Contains(text, snippet) {
			t.Fatalf("expected node CRD to contain %q", snippet)
		}
	}
}
