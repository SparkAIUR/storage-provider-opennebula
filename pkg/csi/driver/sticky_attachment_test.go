package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestStickyAttachmentManagerPersistsAndLoadsState(t *testing.T) {
	runtime := &KubeRuntime{
		client:  fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: stickyAttachmentStateConfigMapName, Namespace: "default"}}),
		enabled: true,
	}
	manager := NewStickyAttachmentManager(runtime, "default")
	state := StickyAttachmentState{
		VolumeID:     "vol-1",
		NodeID:       "node-1",
		Backend:      "local",
		PVCNamespace: "default",
		PVCName:      "pvc-1",
		StartedAt:    time.Now().UTC(),
		ExpiresAt:    time.Now().Add(time.Minute).UTC(),
		GraceSeconds: 90,
		Reason:       "stateful_restart",
	}

	require.NoError(t, manager.StartGrace(state))

	restored := NewStickyAttachmentManager(runtime, "default")
	require.NoError(t, restored.LoadFromConfigMap(context.Background()))

	got, ok := restored.Get("vol-1")
	require.True(t, ok)
	assert.Equal(t, "node-1", got.NodeID)
	assert.Equal(t, 90, got.GraceSeconds)
	assert.Equal(t, "stateful_restart", got.Reason)
}
