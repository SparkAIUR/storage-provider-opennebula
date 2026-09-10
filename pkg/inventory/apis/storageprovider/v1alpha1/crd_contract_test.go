package v1alpha1

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

func readCRDContract(t *testing.T, filename, kind, plural string) apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to resolve current file path")
	crdPath := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../../../../helm/opennebula-csi/crds", filename))
	payload, err := os.ReadFile(crdPath)
	require.NoError(t, err)
	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.UnmarshalStrict(payload, &crd))
	require.Equal(t, "apiextensions.k8s.io/v1", crd.APIVersion)
	require.Equal(t, "CustomResourceDefinition", crd.Kind)
	require.Equal(t, "storageprovider.opennebula.sparkaiur.io", crd.Spec.Group)
	require.Equal(t, kind, crd.Spec.Names.Kind)
	require.Equal(t, plural, crd.Spec.Names.Plural)
	return crd
}

func crdProperty(t *testing.T, root apiextensionsv1.JSONSchemaProps, path ...string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	for _, name := range path {
		require.Equal(t, "object", root.Type)
		property, exists := root.Properties[name]
		require.True(t, exists, "missing schema property %s in %v", name, path)
		root = property
	}
	return root
}

func TestDatastoreCRDIncludesDisplayColumnsAndTypedStatus(t *testing.T) {
	crd := readCRDContract(t, "opennebuladatastores.storageprovider.opennebula.sparkaiur.io.yaml", "OpenNebulaDatastore", "opennebuladatastores")
	served := 0
	for _, version := range crd.Spec.Versions {
		if !version.Served {
			continue
		}
		served++
		t.Run(version.Name, func(t *testing.T) {
			require.NotNil(t, version.Schema)
			require.NotNil(t, version.Schema.OpenAPIV3Schema)
			root := *version.Schema.OpenAPIV3Schema
			require.Equal(t, "boolean", crdProperty(t, root, "spec", "maintenanceMode").Type)
			for name, fieldType := range map[string]string{
				"phase": "string", "id": "integer", "name": "string", "capacityDisplay": "string",
				"type": "string", "backend": "string", "storageClassesDisplay": "string", "metricsDisplay": "string",
				"health": "string", "validationLastOutcome": "string",
			} {
				require.Equal(t, fieldType, crdProperty(t, root, "status", name).Type, name)
			}
			classes := crdProperty(t, root, "status", "storageClassDetails")
			require.Equal(t, "array", classes.Type)
			require.NotNil(t, classes.Items)
			require.NotNil(t, classes.Items.Schema)
			for name, fieldType := range map[string]string{"name": "string", "volumeBindingMode": "string", "allowVolumeExpansion": "boolean", "backendCompatible": "boolean"} {
				require.Equal(t, fieldType, crdProperty(t, *classes.Items.Schema, name).Type, name)
			}
			columns := map[string]apiextensionsv1.CustomResourceColumnDefinition{}
			for _, column := range version.AdditionalPrinterColumns {
				require.NotEqual(t, "Enabled", column.Name)
				require.NotEqual(t, ".status.capacity.freeBytes", column.JSONPath)
				require.NotContains(t, columns, column.Name, "duplicate printer column")
				columns[column.Name] = column
			}
			for name, path := range map[string]string{
				"Status": ".status.phase", "ID": ".status.id", "Name": ".status.name", "Capacity": ".status.capacityDisplay",
				"Type": ".status.type", "Backend": ".status.backend", "SCs": ".status.storageClassesDisplay", "Metrics": ".status.metricsDisplay",
			} {
				require.Equal(t, path, columns[name].JSONPath, name)
				fieldType := "string"
				if name == "ID" {
					fieldType = "integer"
				}
				require.Equal(t, fieldType, columns[name].Type, name)
			}
			require.NotNil(t, version.Subresources)
			require.NotNil(t, version.Subresources.Status)
		})
	}
	require.Positive(t, served, "datastore CRD must serve a typed version")
}

func TestBenchmarkRunCRDIncludesTypedStatus(t *testing.T) {
	crd := readCRDContract(t, "opennebuladatastorebenchmarkruns.storageprovider.opennebula.sparkaiur.io.yaml", "OpenNebulaDatastoreBenchmarkRun", "opennebuladatastorebenchmarkruns")
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
	crd := readCRDContract(t, "opennebulaNodes.storageprovider.opennebula.sparkaiur.io.yaml", "OpenNebulaNode", "opennebulanodes")
	served := 0
	for _, version := range crd.Spec.Versions {
		if !version.Served {
			continue
		}
		served++
		t.Run(version.Name, func(t *testing.T) {
			require.NotNil(t, version.Schema)
			require.NotNil(t, version.Schema.OpenAPIV3Schema)
			diagnosis := crdProperty(t, *version.Schema.OpenAPIV3Schema, "status", "hotplug", "diagnosis")
			for name, fieldType := range map[string]string{"classification": "string", "volumeHandle": "string", "stuckAfterSeconds": "integer", "recommendedAction": "string"} {
				require.Equal(t, fieldType, crdProperty(t, diagnosis, name).Type, name)
			}
			require.Equal(t, "int64", crdProperty(t, diagnosis, "stuckAfterSeconds").Format)
			columns := map[string]apiextensionsv1.CustomResourceColumnDefinition{}
			for _, column := range version.AdditionalPrinterColumns {
				require.NotContains(t, columns, column.Name, "duplicate printer column")
				columns[column.Name] = column
			}
			require.Equal(t, ".status.hotplug.diagnosis.classification", columns["Hotplug"].JSONPath)
			require.Equal(t, "string", columns["Hotplug"].Type)
			require.NotNil(t, version.Subresources)
			require.NotNil(t, version.Subresources.Status)
		})
	}
	require.Positive(t, served, "node CRD must serve a typed version")
}
