package driver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestVolumeHistoryUpsertMergesIndependentWriters(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	runtime := &KubeRuntime{client: client, enabled: true}
	controller := NewVolumeHistoryManager(runtime, "default")
	node := NewVolumeHistoryManager(runtime, "default")
	publishTime := time.Now().UTC()
	_, err := controller.Upsert(ctx, "vol-1", func(state *VolumeHistoryRecord) {
		state.LastSuccessfulPublishTime = publishTime
		state.LastSuccessfulOpenNebulaVMID = 208
		state.LastSuccessfulDiskID = 3
	})
	require.NoError(t, err)
	stageTime := publishTime.Add(time.Minute)
	_, err = node.Upsert(ctx, "vol-1", func(state *VolumeHistoryRecord) {
		state.LastSuccessfulStageTime = stageTime
		state.LastHealthyIdentity = &LocalDiskIdentity{Version: stateObjectVersion, ObservedFromDevice: &LocalDiskObservedIdentity{Filesystem: &LocalDiskObservedFilesystemIdentity{FilesystemUUID: "fs-uuid", FilesystemType: "ext4"}}}
	})
	require.NoError(t, err)
	_, err = controller.Upsert(ctx, "vol-1", func(state *VolumeHistoryRecord) {
		state.LastSafeDetachNodeName = "node-a"
	})
	require.NoError(t, err)
	require.NoError(t, node.RefreshEntry(ctx, "vol-1"))
	state, exists := node.Get("vol-1")
	require.True(t, exists)
	require.Equal(t, publishTime, state.LastSuccessfulPublishTime)
	require.Equal(t, stageTime, state.LastSuccessfulStageTime)
	require.Equal(t, 208, state.LastSuccessfulOpenNebulaVMID)
	require.Equal(t, 3, state.LastSuccessfulDiskID)
	require.Equal(t, "node-a", state.LastSafeDetachNodeName)
	require.Equal(t, "fs-uuid", state.LastHealthyIdentity.ObservedFromDevice.Filesystem.FilesystemUUID)
}

func TestVolumeHistoryUpsertRetriesAgainstLatestRecord(t *testing.T) {
	for _, createRace := range []bool{false, true} {
		name := "update-conflict"
		if createRace {
			name = "create-conflict"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewSimpleClientset()
			runtimeClient := &KubeRuntime{client: client, enabled: true}
			manager := NewVolumeHistoryManager(runtimeClient, "default")
			if !createRace {
				_, err := manager.Upsert(ctx, "vol-1", func(state *VolumeHistoryRecord) { state.LastSuccessfulDiskID = 1 })
				require.NoError(t, err)
			}
			verb := "update"
			if createRace {
				verb = "create"
			}
			calls := 0
			client.PrependReactor(verb, "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				calls++
				if calls != 1 {
					return false, nil, nil
				}
				state := VolumeHistoryRecord{VolumeID: "vol-1", LastSuccessfulDiskID: 7, LastSuccessfulOpenNebulaVMID: 208}
				payload, err := json.Marshal(state)
				require.NoError(t, err)
				cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: volumeHistoryStateConfigMapName, Namespace: "default", ResourceVersion: "2"}, Data: map[string]string{"vol-1": string(payload), "vol-other": "preserved"}}
				resource := corev1.SchemeGroupVersion.WithResource("configmaps")
				if createRace {
					require.NoError(t, client.Tracker().Create(resource, cm, "default"))
					return true, nil, apierrors.NewAlreadyExists(corev1.Resource("configmaps"), cm.Name)
				}
				require.NoError(t, client.Tracker().Update(resource, cm, "default"))
				return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("concurrent writer"))
			})
			state, err := manager.Upsert(ctx, "vol-1", func(state *VolumeHistoryRecord) {
				state.LastSuccessfulDiskID++
				state.LastSafeDetachNodeName = "node-a"
			})
			require.NoError(t, err)
			require.Equal(t, 8, state.LastSuccessfulDiskID)
			require.Equal(t, 208, state.LastSuccessfulOpenNebulaVMID)
			cm, err := runtimeClient.GetConfigMap(ctx, "default", volumeHistoryStateConfigMapName)
			require.NoError(t, err)
			require.Equal(t, "preserved", cm.Data["vol-other"])
			var persisted VolumeHistoryRecord
			require.NoError(t, json.Unmarshal([]byte(cm.Data["vol-1"]), &persisted))
			require.Equal(t, state, persisted)
		})
	}
}

func TestVolumeHistoryUpsertDoesNotCacheFailedWrite(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	manager := NewVolumeHistoryManager(&KubeRuntime{client: client, enabled: true}, "default")
	_, err := manager.Upsert(ctx, "vol-1", func(state *VolumeHistoryRecord) { state.LastSuccessfulDiskID = 1 })
	require.NoError(t, err)
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API unavailable")
	})
	_, err = manager.Upsert(ctx, "vol-1", func(state *VolumeHistoryRecord) { state.LastSuccessfulDiskID = 2 })
	require.Error(t, err)
	cached, _ := manager.Get("vol-1")
	require.Equal(t, 1, cached.LastSuccessfulDiskID)
}
