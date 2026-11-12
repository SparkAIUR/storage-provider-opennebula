package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func attachmentEvidenceFixture(t *testing.T, queued, desired bool) (*ControllerServer, *MockOpenNebulaVolumeProviderTestify, StickyAttachmentState) {
	t.Helper()
	pv, pvc := newLocalPVAndPVC("vol-evidence", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	objects := []runtime.Object{pv, pvc}
	if desired {
		objects = append(objects, activePodForPVC("consumer", pvc.Name, "node-a"))
	}
	driver := newStickyTestDriver(t, objects...)
	ctx := context.Background()
	_, err := driver.volumeHistory.Upsert(ctx, "vol-evidence", func(state *VolumeHistoryRecord) { state.Backend = "local" })
	require.NoError(t, err)
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
		VolumeID: "vol-evidence", NodeID: "node-a", Backend: "local",
		StartedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Minute), GraceSeconds: 60,
	}))
	state, exists := driver.stickyAttachments.Get("vol-evidence")
	require.True(t, exists)
	provider := &MockOpenNebulaVolumeProviderTestify{}
	provider.On("VolumeExists", mock.Anything, "vol-evidence").Return(7, 256*1024*1024, nil)
	provider.On("NodeExists", mock.Anything, "node-a").Return(11, nil)
	controller := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	if queued {
		driver.hotplugQueue = NewHotplugQueueManager(nil, "default", driver.metrics, 5*time.Second, time.Second)
		driver.hotplugQueue.Configure(true, 0, 0, 0, controller.validateHotplugQueueRequest)
	}
	return controller, provider, state
}

func requireAttachmentHoldPreserved(t *testing.T, controller *ControllerServer, before StickyAttachmentState) {
	t.Helper()
	current, exists := controller.driver.stickyAttachments.Get(before.VolumeID)
	require.True(t, exists)
	require.Equal(t, before, current)
	ctx := context.Background()
	restored := NewStickyAttachmentManager(controller.driver.kubeRuntime, "default")
	require.NoError(t, restored.LoadFromConfigMap(ctx))
	current, exists = restored.Get(before.VolumeID)
	require.True(t, exists)
	require.Equal(t, before, current)
	history := NewVolumeHistoryManager(controller.driver.kubeRuntime, "default")
	require.NoError(t, history.LoadFromConfigMap(ctx))
	record, exists := history.Get(before.VolumeID)
	require.True(t, exists)
	require.True(t, record.LastSafeDetachTime.IsZero())
	require.Empty(t, record.LastSafeDetachNodeName)
}

func TestControllerUnpublishRejectsUnknownAttachmentRecheck(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, absent := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%v/typed_absence=%v", queued, absent), func(t *testing.T) {
				controller, provider, before := attachmentEvidenceFixture(t, queued, false)
				provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil).Once()
				recheckErr := errors.New("provider transport unavailable")
				if absent {
					recheckErr = fmt.Errorf("confirmed absent: %w", opennebula.ErrAttachmentNotFound)
				}
				provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("", recheckErr).Once()
				client := reviewControllerClient(t, controller)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: before.VolumeID, NodeId: before.NodeID})
				if absent {
					require.NoError(t, err)
					require.NotNil(t, response)
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, response)
					requireAttachmentHoldPreserved(t, controller, before)
				}
				provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
				provider.AssertExpectations(t)
			})
		}
	}
}

func TestControllerPublishPreservesUnknownStickyAttachment(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, absent := range []bool{false, true} {
			t.Run(fmt.Sprintf("queued=%v/typed_absence=%v", queued, absent), func(t *testing.T) {
				controller, provider, before := attachmentEvidenceFixture(t, queued, true)
				checks := 2
				if queued {
					checks++
				}
				provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("", opennebula.ErrAttachmentNotFound).Times(checks)
				var recheckErr error = errors.New("provider lookup unavailable")
				if absent {
					recheckErr = fmt.Errorf("confirmed absent: %w", opennebula.ErrAttachmentNotFound)
					provider.On("AttachVolume", mock.Anything, before.VolumeID, before.NodeID, false, mock.Anything).Return(nil).Once()
				}
				provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("", recheckErr).Once()
				if absent {
					provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil).Once()
				}
				client := reviewControllerClient(t, controller)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				response, err := client.ControllerPublishVolume(ctx, newPublishReq(before.VolumeID, before.NodeID))
				if absent {
					require.NoError(t, err)
					require.Equal(t, "vdb", response.GetPublishContext()["volumeName"])
					_, exists := controller.driver.stickyAttachments.Get(before.VolumeID)
					require.False(t, exists)
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, response)
					requireAttachmentHoldPreserved(t, controller, before)
					provider.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				}
				provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
				provider.AssertExpectations(t)
			})
		}
	}
}

func TestExpireStickyDetachRetainsUnknownAttachment(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			controller, provider, before := attachmentEvidenceFixture(t, queued, false)
			before.StartedAt = time.Now().UTC().Add(-2 * time.Minute)
			before.ExpiresAt = time.Now().UTC().Add(-time.Minute)
			require.NoError(t, controller.driver.stickyAttachments.StartGrace(before))
			before, _ = controller.driver.stickyAttachments.Get(before.VolumeID)
			provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("", errors.New("provider unavailable")).Once()
			controller.expireStickyDetach(context.Background(), before)
			requireAttachmentHoldPreserved(t, controller, before)
			provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
			provider.AssertExpectations(t)
		})
	}
}

func TestControllerUnpublishRescueRetainsUnknownAttachment(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			controller, provider, before := attachmentEvidenceFixture(t, queued, true)
			provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil).Once()
			provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("", errors.New("provider transport unavailable")).Once()
			client := reviewControllerClient(t, controller)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: before.VolumeID, NodeId: before.NodeID})
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Nil(t, response)
			requireAttachmentHoldPreserved(t, controller, before)
			provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
			provider.AssertExpectations(t)
		})
	}
}

func TestVolumeReconcilePausesUnknownAttachment(t *testing.T) {
	for _, operation := range []string{"attach", "detach"} {
		t.Run(operation, func(t *testing.T) {
			controller, provider, before := attachmentEvidenceFixture(t, false, false)
			provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("", errors.New("provider unavailable")).Once()
			decision := controller.evaluateVolumeReconcile(context.Background(), before.NodeID, operation, before.VolumeID)
			require.Equal(t, VolumeReconcilePause, decision.Action)
			require.Equal(t, codes.Unavailable, status.Code(decision.Err()))
			require.Equal(t, HotplugQueueValidationPaused, decision.HotplugQueueValidation().Decision)
			requireAttachmentHoldPreserved(t, controller, before)
			provider.AssertExpectations(t)
		})
	}
}
