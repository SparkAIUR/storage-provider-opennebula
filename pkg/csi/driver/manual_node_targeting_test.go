package driver

import (
	"context"
	"testing"
	"time"

	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLocalRWOProtectionDecisionUsesExplicitRequiredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-manual", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationRequiredNode] = "node-b"
	nodeA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: hostLabel("a"), topologySystemDSLabel: "104"}}}
	nodeB := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: hostLabel("b"), topologySystemDSLabel: "104"}}}
	driver := newWebhookTestDriver(pv, pvc, nodeA, nodeB)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
	)

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-manual", "node-a")
	require.NoError(t, err)
	assert.True(t, decision.Protected)
	assert.Equal(t, "node-b", decision.RequiredNode)
	assert.Equal(t, protectionSourceExplicitRequiredNode, decision.Source)
	assert.Equal(t, placementSourcePVCRequiredNode, decision.PlacementSource)
	assert.Equal(t, placementDecisionRequired, decision.PlacementDecision)
	assert.Contains(t, decision.Message, "manual local RWO required-node is active")
}

func TestLocalRWOProtectionDecisionRejectsConflictingRequiredNodeWithoutOverride(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-conflict", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationRequiredNode] = "node-b"
	nodeA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: hostLabel("a"), topologySystemDSLabel: "104"}}}
	nodeB := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: hostLabel("b"), topologySystemDSLabel: "104"}}}
	driver := newWebhookTestDriver(pv, pvc, nodeA, nodeB)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.OpenNebulaNodeStatus{}.Phase, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
	)
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
		VolumeID:     "vol-conflict",
		NodeID:       "node-a",
		Backend:      "local",
		PVCNamespace: "default",
		PVCName:      pvc.Name,
		StartedAt:    time.Now().Add(-10 * time.Second),
		ExpiresAt:    time.Now().Add(90 * time.Second),
		GraceSeconds: 90,
		Reason:       "stateful_restart",
	}))

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-conflict", "node-b")
	require.NoError(t, err)
	assert.True(t, decision.Invalid)
	assert.Equal(t, placementDecisionConflicting, decision.PlacementDecision)
	assert.Equal(t, "node-a", decision.InferredRequiredNode)
	assert.Contains(t, decision.Message, "conflicts with protected node")
}

func TestLocalRWOProtectionDecisionAllowsConflictingRequiredNodeWithOverride(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-override", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationRequiredNode] = "node-b"
	pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	nodeA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: hostLabel("a"), topologySystemDSLabel: "104"}}}
	nodeB := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: hostLabel("b"), topologySystemDSLabel: "104"}}}
	driver := newWebhookTestDriver(pv, pvc, nodeA, nodeB)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 104}}},
	)
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
		VolumeID:     "vol-override",
		NodeID:       "node-a",
		Backend:      "local",
		PVCNamespace: "default",
		PVCName:      pvc.Name,
		StartedAt:    time.Now().Add(-10 * time.Second),
		ExpiresAt:    time.Now().Add(90 * time.Second),
		GraceSeconds: 90,
		Reason:       "stateful_restart",
	}))

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-override", "node-b")
	require.NoError(t, err)
	assert.False(t, decision.Invalid)
	assert.True(t, decision.Protected)
	assert.True(t, decision.OverrideUsed)
	assert.Equal(t, "node-b", decision.RequiredNode)
	assert.Equal(t, "node-a", decision.InferredRequiredNode)
}

func TestLocalRWOProtectionDecisionFallsBackAfterExpiredRequiredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-expired", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-a"
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationRequiredNode] = "node-b"
	pvc.Annotations[annotationRequiredNodeUntil] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	driver := newWebhookTestDriver(pv, pvc, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: hostLabel("a")}}})

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-expired", "")
	require.NoError(t, err)
	assert.True(t, decision.Protected)
	assert.True(t, decision.ExplicitRequiredNodeExpired)
	assert.Equal(t, "node-a", decision.RequiredNode)
	assert.Equal(t, protectionReasonHistory, decision.Reason)
	assert.Equal(t, protectionSourceHistoricalOwnership, decision.Source)
	assert.NotEmpty(t, decision.Warnings)
}

func TestLocalRWOProtectionDecisionSkipsMissingHistoricalLastAttachedNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-missing-last", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Annotations[annotationLastAttachedNode] = "node-missing"
	pvc.Spec.VolumeName = pv.Name
	driver := newWebhookTestDriver(pv, pvc)

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-missing-last", "")
	require.NoError(t, err)
	assert.True(t, decision.Protected)
	assert.Equal(t, "node-missing", decision.RequiredNode)
	assert.Equal(t, protectionReasonHistory, decision.Reason)
	assert.Equal(t, protectionSourceHistoricalOwnership, decision.Source)
	assert.False(t, decision.Invalid)
	assert.Empty(t, decision.Warnings)
}

func TestLocalRWOProtectionDecisionSkipsTopologyIncompatibleHistoricalLastAttachedNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-incompatible-last", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, map[string]string{
		annotationDatastoreID: "111",
	})
	pv.Annotations[annotationLastAttachedNode] = "node-a"
	pvc.Spec.VolumeName = pv.Name
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-a",
		Labels: map[string]string{
			corev1.LabelHostname:  hostLabel("a"),
			topologySystemDSLabel: "999",
		},
	}}
	driver := newWebhookTestDriver(pv, pvc, node)
	driver.kubeRuntime.inventoryClient = newInventoryFakeClient(t,
		&inventoryv1alpha1.OpenNebulaDatastore{
			ObjectMeta: metav1.ObjectMeta{Name: "ds-111"},
			Spec:       inventoryv1alpha1.OpenNebulaDatastoreSpec{Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{OpenNebulaDatastoreID: 111}},
			Status: inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ID:      111,
				Backend: "local",
				OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
					CompatibleSystemDatastores: []int{104},
				},
			},
		},
		&inventoryv1alpha1.OpenNebulaNode{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: inventoryv1alpha1.OpenNebulaNodeStatus{Phase: inventoryv1alpha1.NodePhaseReady, OpenNebula: inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{SystemDatastoreID: 999}}},
	)

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-incompatible-last", "")
	require.NoError(t, err)
	assert.True(t, decision.Protected)
	assert.Equal(t, "node-a", decision.RequiredNode)
	assert.Equal(t, protectionReasonHistory, decision.Reason)
	assert.Equal(t, protectionSourceHistoricalOwnership, decision.Source)
	assert.False(t, decision.Invalid)
	assert.Empty(t, decision.Warnings)
}

func TestLocalRWOProtectionDecisionWarnsOnInvalidExplicitPreferredNode(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-invalid-preferred", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	pvc.Annotations[annotationPreferredNode] = "node-missing"
	driver := newWebhookTestDriver(pv, pvc)

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-invalid-preferred", "")
	require.NoError(t, err)
	assert.False(t, decision.Invalid)
	assert.Empty(t, decision.PreferredNode)
	assert.NotEmpty(t, decision.Warnings)
	assert.Contains(t, decision.Warnings[0], "soft preferred-node was ignored")
}
