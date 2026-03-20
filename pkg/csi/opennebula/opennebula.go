/*
Copyright 2025, OpenNebula Project, OpenNebula Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package opennebula

import (
	"context"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
)

type OpenNebulaConfig struct {
	Endpoint    string
	Credentials string
}

type OpenNebulaClient struct {
	*goca.Client
}

type OpenNebulaProvider struct {
	Client *OpenNebulaClient
}

type OpenNebulaVolumeProvider interface {
	CreateVolume(ctx context.Context, name string, size int64, owner string, immutable bool, fsType string, params map[string]string, selection DatastoreSelectionConfig) (*VolumeCreateResult, error)
	CloneVolume(ctx context.Context, name string, sourceVolume string, selection DatastoreSelectionConfig) (*VolumeCreateResult, error)
	DeleteVolume(ctx context.Context, volume string) error
	ExpandVolume(ctx context.Context, volume string, size int64, allowDetached bool) (int64, error)
	AttachVolume(ctx context.Context, volume string, node string, immutable bool, params map[string]string) error
	DetachVolume(ctx context.Context, volume string, node string) error
	ListVolumes(ctx context.Context, owner string, maxEntries int32, startingToken string) ([]string, error)
	GetCapacity(ctx context.Context, selection DatastoreSelectionConfig) (int64, error)
	VolumeExists(ctx context.Context, volume string) (int, int, error)
	CreateSnapshot(ctx context.Context, sourceVolume string, snapshotName string) (*VolumeSnapshot, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	ListSnapshots(ctx context.Context, snapshotID string, sourceVolumeID string, maxEntries int32, startingToken string) ([]VolumeSnapshot, string, error)
	NodeExists(ctx context.Context, node string) (int, error)
	GetVolumeInNode(ctx context.Context, volumeID int, nodeID int) (string, error)
	VolumeReadyWithTimeout(volumeID int) (bool, error)
}

type SharedFilesystemProvider interface {
	CreateSharedVolume(ctx context.Context, req SharedVolumeRequest) (*SharedVolumeCreateResult, error)
	CloneSharedVolume(ctx context.Context, req SharedVolumeCloneRequest) (*SharedVolumeCreateResult, error)
	DeleteSharedVolume(ctx context.Context, volumeID string, secrets map[string]string) error
	ExpandSharedVolume(ctx context.Context, volumeID string, sizeBytes int64, secrets map[string]string) (int64, error)
	CreateSharedSnapshot(ctx context.Context, sourceVolumeID string, snapshotName string, secrets map[string]string) (*VolumeSnapshot, error)
	DeleteSharedSnapshot(ctx context.Context, snapshotID string, secrets map[string]string) error
	ListSharedSnapshots(ctx context.Context, snapshotID string, sourceVolumeID string, maxEntries int32, startingToken string, secrets map[string]string) ([]VolumeSnapshot, string, error)
	PublishSharedVolume(ctx context.Context, volumeID string, readonly bool) (map[string]string, error)
	ValidateSharedVolume(ctx context.Context, volumeID string) (*SharedVolumeMetadata, error)
}

func NewClient(config OpenNebulaConfig) *OpenNebulaClient {
	return &OpenNebulaClient{
		Client: goca.NewDefaultClient(
			goca.OneConfig{
				Token:    config.Credentials,
				Endpoint: config.Endpoint,
			},
		),
	}
}

func (c *OpenNebulaClient) Probe(ctx context.Context) error {
	_, err := c.Client.CallContext(ctx, "one.system.version")
	if err != nil {
		return err
	}
	return nil
}
