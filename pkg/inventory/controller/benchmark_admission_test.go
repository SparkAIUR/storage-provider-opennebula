package controller

import (
	"context"
	"testing"

	datastoreSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/datastore"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBenchmarkAdmissionSurvivesItsOwnAttachmentAndRestart(t *testing.T) {
	for _, restartAt := range []string{"running", "before-status", "after-pvc"} {
		t.Run(restartAt, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, batchv1.AddToScheme(scheme))
			require.NoError(t, inventoryv1alpha1.AddToScheme(scheme))
			run := &inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRun{ObjectMeta: metav1.ObjectMeta{Name: "admission", Generation: 1}, Spec: inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRunSpec{DatastoreID: 111, StorageClassName: "local", NodeSelector: map[string]string{corev1.LabelHostname: "node-a"}}}
			node := &inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, DisplayState: "Ready", ActiveHotplugPressure: 7, Hotplug: inventoryv1alpha1.OpenNebulaNodeHotplugStatus{Ready: true}}}
			client := ctrlfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(run, node, &batchv1.Job{}).WithObjects(run, node).Build()
			kube := fake.NewSimpleClientset()
			s := &Syncer{client: client, apiReader: client, kube: kube, namespace: "default"}
			ds := datastoreSchema.Datastore{ID: 111, Name: "local"}
			discovered := map[int]datastoreSchema.Datastore{111: ds}
			require.NoError(t, s.reconcileBenchmarkRuns(ctx, discovered, nil))
			current := &inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRun{}
			require.NoError(t, client.Get(ctx, types.NamespacedName{Name: run.Name}, current))
			require.Equal(t, inventoryv1alpha1.ValidationPhaseRunning, current.Status.Phase)
			jobName, pvcName := current.Status.JobName, current.Status.PVCName
			if restartAt != "running" {
				current.Status = inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRunStatus{}
				require.NoError(t, client.Status().Update(ctx, current))
			}
			if restartAt == "after-pvc" {
				require.NoError(t, client.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"}}))
			}
			require.NoError(t, client.Get(ctx, types.NamespacedName{Name: node.Name}, node))
			node.Status.ActiveHotplugPressure = 8
			require.NoError(t, client.Status().Update(ctx, node))
			s = &Syncer{client: client, apiReader: client, kube: kube, namespace: "default"}
			require.NoError(t, s.reconcileBenchmarkRuns(ctx, discovered, nil))
			require.NoError(t, client.Get(ctx, types.NamespacedName{Name: run.Name}, current))
			require.Equal(t, inventoryv1alpha1.ValidationPhaseRunning, current.Status.Phase)
			require.NoError(t, client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: "default"}, &corev1.PersistentVolumeClaim{}))
			job := &batchv1.Job{}
			require.NoError(t, client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: "default"}, job))
			job.Status.Succeeded = 1
			require.NoError(t, client.Status().Update(ctx, job))
			require.NoError(t, s.reconcileBenchmarkRuns(ctx, discovered, nil))
			require.NoError(t, client.Get(ctx, types.NamespacedName{Name: run.Name}, current))
			require.Equal(t, inventoryv1alpha1.ValidationPhaseSucceeded, current.Status.Phase)
		})
	}
}

func TestBenchmarkAdmissionRejectsInitialPressureBeforeCreatingResources(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))
	require.NoError(t, inventoryv1alpha1.AddToScheme(scheme))
	run := &inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRun{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Generation: 1}, Spec: inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRunSpec{DatastoreID: 111, NodeSelector: map[string]string{corev1.LabelHostname: "node-a"}}}
	node := &inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, DisplayState: "Ready", ActiveHotplugPressure: 8, Hotplug: inventoryv1alpha1.OpenNebulaNodeHotplugStatus{Ready: true}}}
	client := ctrlfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(run).WithObjects(run, node).Build()
	s := &Syncer{client: client, apiReader: client, kube: fake.NewSimpleClientset(), namespace: "default"}
	require.NoError(t, s.reconcileBenchmarkRuns(ctx, map[int]datastoreSchema.Datastore{111: {ID: 111, Name: "local"}}, nil))
	current := &inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRun{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: run.Name}, current))
	require.Equal(t, inventoryv1alpha1.ValidationPhaseFailed, current.Status.Phase)
	var jobs batchv1.JobList
	var pvcs corev1.PersistentVolumeClaimList
	require.NoError(t, client.List(ctx, &jobs))
	require.NoError(t, client.List(ctx, &pvcs))
	require.Empty(t, jobs.Items)
	require.Empty(t, pvcs.Items)
}
