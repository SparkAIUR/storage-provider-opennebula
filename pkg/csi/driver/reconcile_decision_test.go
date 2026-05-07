package driver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestVolumeReconcileDecisionPausesLiveSameNodeDetach(t *testing.T) {
	record := VolumeHistoryRecord{
		Version:                   stateObjectVersion,
		VolumeID:                  "vol-decision-live",
		Backend:                   "local",
		LastSuccessfulNodeName:    "node-a",
		LastSuccessfulPublishTime: time.Now().UTC(),
	}
	payload, err := json.Marshal(record)
	require.NoError(t, err)

	pv, pvc := newLocalPVAndPVC("vol-decision-live", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	historyCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: volumeHistoryStateConfigMapName, Namespace: "default"},
		Data:       map[string]string{"vol-decision-live": string(payload)},
	}
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-a", true), activePodForPVC("pod-live", pvc.Name, "node-a"), historyCM)

	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-decision-live", "node-a").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  "vol-decision-live",
		ImageID:       12,
		RequestedNode: "node-a",
	}, nil).Once()
	provider.On("NodeExists", mock.Anything, "node-a").Return(101, nil).Once()
	provider.On("GetVolumeInNode", mock.Anything, 12, 101).Return("vdb", nil).Once()

	server := NewControllerServer(driver, provider, nil)
	decision := server.evaluateVolumeReconcile(context.Background(), "node-a", "detach", "vol-decision-live")
	require.Equal(t, VolumeReconcilePause, decision.Action)
	require.Equal(t, queueReasonSameNodeReuseRequired, decision.Reason)
	require.Equal(t, "node-a", decision.Evidence.DesiredState.NodeName)
	provider.AssertExpectations(t)
}

func TestVolumeReconcileDecisionBlocksQuarantinedAttachBeforeStaleAttachCheck(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-decision-quarantine", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	driver := newStickyTestDriver(t, pv, pvc, newReadyNode("node-a", true))
	state := VolumeQuarantineState{
		VolumeID:       "vol-decision-quarantine",
		Reason:         "metadata_drift",
		Classification: "metadata_drift",
		Message:        "metadata drift is active",
	}
	_, active, err := driver.volumeQuarantine.MarkFailure(context.Background(), state, 1, time.Hour)
	require.NoError(t, err)
	require.True(t, active)

	provider := new(MockOpenNebulaVolumeProviderTestify)
	provider.On("InspectVolumeLookup", mock.Anything, "vol-decision-quarantine", "node-a").Return(&opennebula.VolumeLookupResult{
		Status:        opennebula.VolumeLookupPresent,
		VolumeHandle:  "vol-decision-quarantine",
		ImageID:       14,
		RequestedNode: "node-a",
	}, nil).Once()
	provider.On("NodeExists", mock.Anything, "node-a").Return(101, nil).Once()
	provider.On("GetVolumeInNode", mock.Anything, 14, 101).Return("", assertNotAttachedErr{}).Once()

	server := NewControllerServer(driver, provider, nil)
	decision := server.evaluateVolumeReconcile(context.Background(), "node-a", "attach", "vol-decision-quarantine")
	require.Equal(t, VolumeReconcileReject, decision.Action)
	require.Equal(t, queueReasonVolumeQuarantined, decision.Reason)
	require.Contains(t, decision.Message, "metadata drift is active")
	provider.AssertExpectations(t)
}

type assertNotAttachedErr struct{}

func (assertNotAttachedErr) Error() string { return "not attached" }
