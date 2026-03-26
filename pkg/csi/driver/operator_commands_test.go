package driver

import (
	"testing"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidationSummaryForCommandPrefersDisplaySummary(t *testing.T) {
	status := inventoryv1alpha1.OpenNebulaDatastoreStatus{
		LastValidationSummary: "R:12k W:8k p99:4ms",
		Validation: inventoryv1alpha1.OpenNebulaDatastoreValidationStatus{
			Phase:   inventoryv1alpha1.ValidationPhaseSucceeded,
			Message: "validation completed",
		},
	}
	if got := validationSummaryForCommand(status); got != "R:12k W:8k p99:4ms" {
		t.Fatalf("unexpected validation summary: %q", got)
	}
}

func TestFlattenStorageClassDetails(t *testing.T) {
	datastores := []inventoryv1alpha1.OpenNebulaDatastore{
		{
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				StorageClassDetails: []inventoryv1alpha1.StorageClassDetail{{
					Name:              "opennebula-local",
					VolumeBindingMode: string(storagev1.VolumeBindingImmediate),
					Warnings:          []string{"local datastore uses Immediate binding"},
				}},
			},
		},
	}
	classes := []storagev1.StorageClass{{ObjectMeta: metav1.ObjectMeta{Name: "opennebula-local"}}}
	flattened := flattenStorageClassDetails(datastores, classes)
	if len(flattened) != 1 {
		t.Fatalf("expected 1 flattened storage class detail, got %d", len(flattened))
	}
	if len(flattened[0].Warnings) != 1 {
		t.Fatalf("expected warnings to be preserved, got %#v", flattened[0].Warnings)
	}
}

func TestSupportBundleConfigIncludesInventorySettings(t *testing.T) {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.InventoryControllerEnabledVar, true)
	cfg.OverrideVal(config.InventoryDatastoreAuthorityModeVar, "strict")
	cfg.OverrideVal(config.PreflightLocalImmediateBindingPolicyVar, "warn")

	bundleCfg := supportBundleConfig(cfg)
	if enabled, ok := bundleCfg["inventoryControllerEnabled"].(bool); !ok || !enabled {
		t.Fatalf("expected inventory controller enabled in support bundle config, got %#v", bundleCfg["inventoryControllerEnabled"])
	}
	if bundleCfg["inventoryAuthorityMode"] != "strict" {
		t.Fatalf("unexpected authority mode: %#v", bundleCfg["inventoryAuthorityMode"])
	}
}
