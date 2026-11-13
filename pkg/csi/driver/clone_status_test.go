package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateVolumeCloneSentinelStatuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"capacity", opennebula.ErrCloneCapacity, codes.OutOfRange},
		{"wrapped_capacity", fmt.Errorf("source capacity 268435456 exceeds the 134217728 byte limit: %w", opennebula.ErrCloneCapacity), codes.OutOfRange},
		{"conflict", opennebula.ErrCloneConflict, codes.AlreadyExists},
		{"wrapped_conflict", fmt.Errorf("child has no durable clone origin for source 7: %w", opennebula.ErrCloneConflict), codes.AlreadyExists},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &MockOpenNebulaVolumeProviderTestify{}
			provider.On("CloneVolume", mock.Anything, "child", "source", mock.Anything).Return((*opennebula.VolumeCreateResult)(nil), tc.err).Once()
			client := reviewControllerClient(t, getTestControllerServer(provider))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response, err := client.CreateVolume(ctx, &csi.CreateVolumeRequest{
				Name:                "child",
				CapacityRange:       &csi.CapacityRange{RequiredBytes: 128 * 1024 * 1024, LimitBytes: 128 * 1024 * 1024},
				VolumeCapabilities:  []*csi.VolumeCapability{newPublishReq("child", "node").VolumeCapability},
				Parameters:          map[string]string{storageClassParamDatastoreIDs: "100"},
				VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"}}},
			})
			require.Nil(t, response)
			require.Equal(t, tc.code, status.Code(err))
			require.Equal(t, tc.err.Error(), status.Convert(err).Message())
			provider.AssertExpectations(t)
		})
	}
}
