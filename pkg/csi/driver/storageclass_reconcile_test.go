package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestStorageClassReconcileRecreatesUnmodifiedChartManagedClass(t *testing.T) {
	oldDesired := testDesiredStorageClass("opennebula-local", false)
	oldHash := storageClassDesiredSpecHash(oldDesired)
	live := desiredStorageClassObject(oldDesired, oldHash)

	newDesired := testDesiredStorageClass("opennebula-local", true)
	payload := storageClassReconcilePayload(t, []StorageClassReconcileDesired{newDesired})
	client := fake.NewSimpleClientset(
		live,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-reconcile", Namespace: "default"},
			Data:       map[string]string{storageClassReconcileDataKey: payload},
		},
	)

	var out bytes.Buffer
	err := RunStorageClassReconcile(context.Background(), client, StorageClassReconcileOptions{
		Namespace:     "default",
		ConfigMapName: "sc-reconcile",
	}, &out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "recreated")

	updated, err := client.StorageV1().StorageClasses().Get(context.Background(), "opennebula-local", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, updated.AllowVolumeExpansion)
	require.True(t, *updated.AllowVolumeExpansion)
	require.Equal(t, storageClassDesiredSpecHash(newDesired), updated.Annotations[storageClassAnnotationAppliedSpecHash])
}

func TestStorageClassReconcileTreatsMissingAllowedTopologiesAsEmpty(t *testing.T) {
	oldDesired := testDesiredStorageClass("opennebula-local", false)
	helmAppliedHash := storageClassSpecHash(storageClassSpecFingerprint{
		Provisioner:       DefaultDriverName,
		ReclaimPolicy:     string(corev1.PersistentVolumeReclaimDelete),
		AllowExpansion:    oldDesired.AllowExpansion,
		AllowedTopologies: []corev1.TopologySelectorTerm{},
		MountOptions:      []string{},
		VolumeBindingMode: string(storagev1.VolumeBindingWaitForFirstConsumer),
		Parameters:        map[string]string{"datastoreIDs": "101"},
	})
	live := desiredStorageClassObject(oldDesired, helmAppliedHash)
	require.Empty(t, live.AllowedTopologies)
	require.Equal(t, helmAppliedHash, storageClassLiveSpecHash(live))

	newDesired := oldDesired
	newDesired.AllowedTopologies = []corev1.TopologySelectorTerm{
		{
			MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{
				{
					Key:    "topology.opennebula.sparkaiur.io/system-ds",
					Values: []string{"100", "104"},
				},
			},
		},
	}
	payload := storageClassReconcilePayload(t, []StorageClassReconcileDesired{newDesired})
	client := fake.NewSimpleClientset(
		live,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-reconcile", Namespace: "default"},
			Data:       map[string]string{storageClassReconcileDataKey: payload},
		},
	)

	var out bytes.Buffer
	err := RunStorageClassReconcile(context.Background(), client, StorageClassReconcileOptions{
		Namespace:     "default",
		ConfigMapName: "sc-reconcile",
	}, &out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "recreated")
	updated, err := client.StorageV1().StorageClasses().Get(context.Background(), "opennebula-local", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newDesired.AllowedTopologies, updated.AllowedTopologies)
	require.Equal(t, storageClassDesiredSpecHash(newDesired), updated.Annotations[storageClassAnnotationAppliedSpecHash])
}

func TestStorageClassReconcileRejectsManualMutationByDefault(t *testing.T) {
	desired := testDesiredStorageClass("opennebula-local", false)
	appliedHash := storageClassDesiredSpecHash(desired)
	live := desiredStorageClassObject(desired, appliedHash)
	live.Parameters["datastoreIDs"] = "999"

	payload := storageClassReconcilePayload(t, []StorageClassReconcileDesired{testDesiredStorageClass("opennebula-local", true)})
	client := fake.NewSimpleClientset(
		live,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-reconcile", Namespace: "default"},
			Data:       map[string]string{storageClassReconcileDataKey: payload},
		},
	)

	err := RunStorageClassReconcile(context.Background(), client, StorageClassReconcileOptions{
		Namespace:     "default",
		ConfigMapName: "sc-reconcile",
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "manually modified")
}

func TestStorageClassReconcileAdoptsUnannotatedExactMatch(t *testing.T) {
	desired := testDesiredStorageClass("opennebula-local", true)
	live := desiredStorageClassObject(desired, storageClassDesiredSpecHash(desired))
	live.Annotations = nil

	payload := storageClassReconcilePayload(t, []StorageClassReconcileDesired{desired})
	client := fake.NewSimpleClientset(
		live,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-reconcile", Namespace: "default"},
			Data:       map[string]string{storageClassReconcileDataKey: payload},
		},
	)

	err := RunStorageClassReconcile(context.Background(), client, StorageClassReconcileOptions{
		Namespace:        "default",
		ConfigMapName:    "sc-reconcile",
		AdoptUnannotated: true,
	}, nil)
	require.NoError(t, err)
	updated, err := client.StorageV1().StorageClasses().Get(context.Background(), "opennebula-local", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "true", updated.Annotations[storageClassAnnotationChartManaged])
}

func testDesiredStorageClass(name string, allowExpansion bool) StorageClassReconcileDesired {
	return StorageClassReconcileDesired{
		Name:              name,
		ReleaseName:       "rel",
		ReleaseNamespace:  "default",
		Provisioner:       DefaultDriverName,
		ReclaimPolicy:     string(corev1.PersistentVolumeReclaimDelete),
		AllowExpansion:    allowExpansion,
		VolumeBindingMode: string(storagev1.VolumeBindingWaitForFirstConsumer),
		Parameters:        map[string]string{"datastoreIDs": "101"},
	}
}

func storageClassReconcilePayload(t *testing.T, desired []StorageClassReconcileDesired) string {
	t.Helper()
	for idx := range desired {
		desired[idx].SpecHash = storageClassDesiredSpecHash(desired[idx])
	}
	payload, err := json.Marshal(desired)
	require.NoError(t, err)
	return string(payload)
}
