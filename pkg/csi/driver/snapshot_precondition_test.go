package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
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
				ResourceVersion string `json:"resourceVersion"`
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
		for key, value := range patch.Data {
			if value == nil {
				delete(cm.Data, key)
			} else {
				cm.Data[key] = *value
			}
		}
		cm.ResourceVersion = fmt.Sprint(number + 1)
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
	require.NoError(t, runtime.setConfigMapSnapshot(context.Background(), "default", "queue", "node", ""))
	once.Do(func() { close(release) })
	<-finished
	current, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), "queue", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, current.Data, "node")
}
