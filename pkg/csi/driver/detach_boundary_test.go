package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
)

func TestProviderDetachRequiresDurableCompletedCapacity(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			state.marker = 0
			state.pending = pending
			state.mu.Unlock()
			require.Error(t, provider.DetachVolume(context.Background(), "volume-test", "node-old"))
			state.mu.Lock()
			defer state.mu.Unlock()
			require.Empty(t, state.mutations)
			require.Equal(t, 256, state.disks[41])
			require.Zero(t, state.marker)
		})
	}
}

func TestProviderLegacyDetachAfterVerifiedExpansion(t *testing.T) {
	provider, state := nodeAttachmentProvider(t)
	state.mu.Lock()
	state.marker = 0
	state.ignoreMarkerWrite = true
	state.mu.Unlock()
	_, err := provider.ExpandVolume(context.Background(), "volume-test", 256*1024*1024, false)
	require.ErrorContains(t, err, "readback failed")
	require.Error(t, provider.DetachVolume(context.Background(), "volume-test", "node-old"))
	state.mu.Lock()
	require.NotContains(t, state.mutations, "detach:41")
	require.Zero(t, state.marker)
	state.ignoreMarkerWrite = false
	state.mu.Unlock()
	size, err := provider.ExpandVolume(context.Background(), "volume-test", 256*1024*1024, false)
	require.NoError(t, err)
	require.EqualValues(t, 256*1024*1024, size)
	require.NoError(t, provider.DetachVolume(context.Background(), "volume-test", "node-old"))
	state.mu.Lock()
	defer state.mu.Unlock()
	require.EqualValues(t, 256*1024*1024, state.marker)
	require.Contains(t, state.mutations, "detach:41")
	require.NotContains(t, state.mutations, "resize:41:256")
}

func TestControllerLegacyDetachBoundaries(t *testing.T) {
	for _, action := range []string{"unpublish", "transfer", "expiry"} {
		t.Run(action, func(t *testing.T) {
			provider, state := nodeAttachmentProvider(t)
			state.mu.Lock()
			state.marker = 0
			state.mu.Unlock()
			pv, pvc := newLocalPVAndPVC("volume-test", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
			pv.Annotations[annotationAllowCrossNodeUntil] = time.Now().UTC().Add(time.Minute).Format(time.RFC3339)
			driver := newStickyTestDriver(t, pv, pvc)
			driver.PluginConfig.OverrideVal(config.LocalRestartOptimizationEnabledVar, false)
			_, err := driver.volumeHistory.Upsert(context.Background(), "volume-test", func(record *VolumeHistoryRecord) { record.Backend = "local" })
			require.NoError(t, err)
			if action != "unpublish" {
				expires := time.Now().UTC().Add(time.Minute)
				if action == "expiry" {
					expires = time.Now().UTC().Add(-time.Minute)
				}
				require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{VolumeID: "volume-test", NodeID: "node-old", Backend: "local", StartedAt: time.Now().UTC().Add(-2 * time.Minute), ExpiresAt: expires, GraceSeconds: 60}))
			}
			controller := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
			client := reviewControllerClient(t, controller)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			switch action {
			case "unpublish":
				response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume-test", NodeId: "node-old"})
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Nil(t, response)
			case "transfer":
				response, err := client.ControllerPublishVolume(ctx, newPublishReq("volume-test", "node-new"))
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Nil(t, response)
			case "expiry":
				before, _ := driver.stickyAttachments.Get("volume-test")
				controller.expireStickyDetach(ctx, before)
			}
			if action != "unpublish" {
				_, exists := driver.stickyAttachments.Get("volume-test")
				require.True(t, exists)
			}
			record, exists := driver.volumeHistory.Get("volume-test")
			require.True(t, exists)
			require.True(t, record.LastSafeDetachTime.IsZero())
			_, quarantined := controller.activeVolumeQuarantine(ctx, "volume-test")
			require.False(t, quarantined)
			state.mu.Lock()
			defer state.mu.Unlock()
			require.Empty(t, state.mutations)
			require.Equal(t, 256, state.disks[41])
			require.Zero(t, state.marker)
		})
	}
}

func TestProviderDetachRejectsContradictoryNoDiskReread(t *testing.T) {
	provider, state := nodeAttachmentProvider(t)
	state.mu.Lock()
	state.hideDiskInfo = true
	state.mu.Unlock()
	require.Error(t, provider.DetachVolume(context.Background(), "volume-test", "node-old"))
	state.mu.Lock()
	defer state.mu.Unlock()
	require.Empty(t, state.mutations)
	require.Equal(t, 256, state.disks[41])
}

func TestControllerUnpublishPreservesLookupRejections(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, kind := range []string{"inconsistent", "image-missing", "unknown-state", "generic-not-found", "nil-result"} {
			t.Run(fmt.Sprintf("queued=%v/%s", queued, kind), func(t *testing.T) {
				controller, provider, before := attachmentEvidenceFixture(t, queued, false)
				provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil).Once()
				result := &opennebula.VolumeLookupResult{VolumeHandle: before.VolumeID, RequestedNode: before.NodeID}
				var lookupErr error
				code := codes.Unavailable
				switch kind {
				case "inconsistent":
					result.Status = opennebula.VolumeLookupProviderInconsistent
					code = codes.FailedPrecondition
				case "image-missing":
					result.Status = opennebula.VolumeLookupImageRecordMissing
					code = codes.FailedPrecondition
				case "generic-not-found":
					lookupErr = status.Error(codes.NotFound, "provider lookup failed")
				case "nil-result":
					result = nil
				}
				provider.On("InspectVolumeLookup", mock.Anything, before.VolumeID, before.NodeID).Return(result, lookupErr).Once()
				client := reviewControllerClient(t, controller)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				response, err := client.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: before.VolumeID, NodeId: before.NodeID})
				require.Equal(t, code, status.Code(err))
				require.Nil(t, response)
				requireAttachmentHoldPreserved(t, controller, before)
				provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
				provider.AssertExpectations(t)
			})
		}
	}
}

type nonInspectingVolumeProvider struct {
	opennebula.OpenNebulaVolumeProvider
}

func TestControllerUnpublishRejectsFallbackLookupFailure(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			controller, _, before := attachmentEvidenceFixture(t, queued, false)
			provider := &MockOpenNebulaVolumeProviderTestify{}
			provider.On("VolumeExists", mock.Anything, before.VolumeID).Return(7, 128*1024*1024, nil).Once()
			provider.On("VolumeExists", mock.Anything, before.VolumeID).Return(-1, -1, status.Error(codes.NotFound, "lookup unavailable")).Once()
			provider.On("NodeExists", mock.Anything, before.NodeID).Return(11, nil)
			provider.On("GetVolumeInNode", mock.Anything, 7, 11).Return("vdb", nil).Once()
			controller.volumeProvider = nonInspectingVolumeProvider{provider}
			client := reviewControllerClient(t, controller)
			response, err := client.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: before.VolumeID, NodeId: before.NodeID})
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Nil(t, response)
			requireAttachmentHoldPreserved(t, controller, before)
			provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
			provider.AssertExpectations(t)
		})
	}
}

func TestControllerUnpublishRejectsFailedNodeLookup(t *testing.T) {
	provider := &MockOpenNebulaVolumeProviderTestify{}
	provider.On("VolumeExists", mock.Anything, "volume").Return(7, 128*1024*1024, nil)
	provider.On("NodeExists", mock.Anything, "node").Return(-1, errors.New("VM lookup unavailable"))
	provider.On("VolumeReadyWithTimeout", mock.Anything, 7).Return(true, nil).Maybe()
	client := reviewControllerClient(t, getTestControllerServer(provider))
	response, err := client.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: "volume", NodeId: "node"})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Nil(t, response)
	provider.AssertNotCalled(t, "VolumeReadyWithTimeout", mock.Anything, mock.Anything)
	provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}
