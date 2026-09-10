package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Model the API's resourceVersion precondition while a request continues at the
// server after its client deadline. The late active write must lose to the clear.
func TestHotplugSnapshotTimeoutCannotOverwriteNewerClear(t *testing.T) {
	var mu sync.Mutex
	cm := corev1.ConfigMap{TypeMeta: metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"}, ObjectMeta: metav1.ObjectMeta{Name: "queue", Namespace: "default", ResourceVersion: "1"}, Data: map[string]string{}}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	patches := 0
	revision := 1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			mu.Lock()
			defer mu.Unlock()
			json.NewEncoder(w).Encode(cm)
			return
		}
		if r.Method != http.MethodPatch {
			http.Error(w, "unexpected method", 405)
			return
		}
		var patch struct {
			Metadata struct {
				ResourceVersion string            `json:"resourceVersion"`
				Annotations     map[string]string `json:"annotations"`
			} `json:"metadata"`
			Data map[string]*string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		mu.Lock()
		patches++
		number := patches
		mu.Unlock()
		if number == 1 {
			close(entered)
			<-release
			defer close(finished)
		}
		mu.Lock()
		defer mu.Unlock()
		if patch.Metadata.ResourceVersion != cm.ResourceVersion {
			w.WriteHeader(409)
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Reason: metav1.StatusReasonConflict, Code: 409})
			return
		}
		before := cm.DeepCopy()
		for key, value := range patch.Data {
			if value == nil {
				delete(cm.Data, key)
			} else {
				cm.Data[key] = *value
			}
		}
		if len(patch.Metadata.Annotations) != 0 && cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		for key, value := range patch.Metadata.Annotations {
			cm.Annotations[key] = value
		}
		if !reflect.DeepEqual(before.Data, cm.Data) || !reflect.DeepEqual(before.Annotations, cm.Annotations) {
			revision++
			cm.ResourceVersion = fmt.Sprint(revision)
		}
		json.NewEncoder(w).Encode(cm)
	}))
	defer func() { once.Do(func() { close(release) }); server.Close() }()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	runtime := &KubeRuntime{client: client, enabled: true}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.setConfigMapSnapshot(ctx, "default", "queue", "node", "old-active") }()
	<-entered
	require.Error(t, <-done)
	cmClient := client.CoreV1().ConfigMaps("default")
	noop, err := cmClient.Patch(context.Background(), "queue", types.MergePatchType, []byte(`{"metadata":{"resourceVersion":"1"},"data":{"node":null}}`), metav1.PatchOptions{})
	require.NoError(t, err)
	require.Equal(t, "1", noop.ResourceVersion)
	require.NoError(t, runtime.setConfigMapSnapshot(context.Background(), "default", "queue", "node", ""))
	cleared, err := cmClient.Get(context.Background(), "queue", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEqual(t, noop.ResourceVersion, cleared.ResourceVersion)
	require.Len(t, cleared.Annotations, 1)
	require.NoError(t, runtime.setConfigMapSnapshot(context.Background(), "default", "queue", "node", ""))
	repeated, err := cmClient.Get(context.Background(), "queue", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEqual(t, cleared.ResourceVersion, repeated.ResourceVersion)
	require.Len(t, repeated.Annotations, 1)
	require.NoError(t, runtime.setConfigMapSnapshot(context.Background(), "default", "queue", "other-node", "active"))
	once.Do(func() { close(release) })
	<-finished
	current, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), "queue", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, current.Data, "node")
	require.Equal(t, "active", current.Data["other-node"])
	require.Len(t, current.Annotations, 2)
	for key, value := range repeated.Annotations {
		require.Equal(t, value, current.Annotations[key])
	}
}
