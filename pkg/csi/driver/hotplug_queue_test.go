package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestHotplugQueueSequentializesSameNodeRequests(t *testing.T) {
	manager := NewHotplugQueueManager(nil, "", NewDriverMetrics("test", "test"), time.Second, 50*time.Millisecond)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var orderMu sync.Mutex
	order := []string{}

	errCh := make(chan error, 2)
	go func() {
		errCh <- manager.Run(context.Background(), "node-a", "attach", "vol-1", hotplugQueuePriorityNormal, func(context.Context) error {
			orderMu.Lock()
			order = append(order, "first")
			orderMu.Unlock()
			close(firstStarted)
			<-releaseFirst
			return nil
		})
	}()

	<-firstStarted
	go func() {
		errCh <- manager.Run(context.Background(), "node-a", "attach", "vol-2", hotplugQueuePriorityNormal, func(context.Context) error {
			orderMu.Lock()
			order = append(order, "second")
			orderMu.Unlock()
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	orderMu.Lock()
	assert.Equal(t, []string{"first"}, order)
	orderMu.Unlock()
	close(releaseFirst)

	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
	orderMu.Lock()
	assert.Equal(t, []string{"first", "second"}, order)
	orderMu.Unlock()
}

func TestHotplugQueuePrioritizesCriticalWork(t *testing.T) {
	manager := NewHotplugQueueManager(nil, "", NewDriverMetrics("test", "test"), time.Second, 0)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var orderMu sync.Mutex
	order := []string{}
	errCh := make(chan error, 3)

	go func() {
		errCh <- manager.Run(context.Background(), "node-a", "attach", "vol-1", hotplugQueuePriorityNormal, func(context.Context) error {
			orderMu.Lock()
			order = append(order, "first")
			orderMu.Unlock()
			close(firstStarted)
			<-releaseFirst
			return nil
		})
	}()
	<-firstStarted

	go func() {
		errCh <- manager.Run(context.Background(), "node-a", "detach", "vol-2", hotplugQueuePriorityBackground, func(context.Context) error {
			orderMu.Lock()
			order = append(order, "background")
			orderMu.Unlock()
			return nil
		})
	}()
	go func() {
		errCh <- manager.Run(context.Background(), "node-a", "attach", "vol-3", hotplugQueuePriorityCritical, func(context.Context) error {
			orderMu.Lock()
			order = append(order, "critical")
			orderMu.Unlock()
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	close(releaseFirst)
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)

	orderMu.Lock()
	assert.Equal(t, []string{"first", "critical", "background"}, order)
	orderMu.Unlock()
}

func TestHotplugQueueReturnsTimeout(t *testing.T) {
	manager := NewHotplugQueueManager(nil, "", NewDriverMetrics("test", "test"), 50*time.Millisecond, 0)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	go func() {
		_ = manager.Run(context.Background(), "node-a", "attach", "vol-1", hotplugQueuePriorityNormal, func(context.Context) error {
			close(firstStarted)
			<-releaseFirst
			return nil
		})
	}()
	<-firstStarted

	err := manager.Run(context.Background(), "node-a", "attach", "vol-2", hotplugQueuePriorityNormal, func(context.Context) error {
		return nil
	})
	close(releaseFirst)

	require.Error(t, err)
	var timeoutErr *HotplugQueueTimeoutError
	assert.True(t, errors.As(err, &timeoutErr))
	assert.Equal(t, "node-a", timeoutErr.Node)
}

func TestHotplugQueueCoalescesDuplicateRequests(t *testing.T) {
	manager := NewHotplugQueueManager(nil, "", NewDriverMetrics("test", "test"), time.Second, 0)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var runCount int32
	errCh := make(chan error, 2)
	run := func(ctx context.Context) error {
		atomic.AddInt32(&runCount, 1)
		close(firstStarted)
		<-releaseFirst
		return nil
	}

	go func() {
		errCh <- manager.Run(context.Background(), "node-a", "attach", "vol-1", hotplugQueuePriorityNormal, run)
	}()
	<-firstStarted
	go func() {
		errCh <- manager.Run(context.Background(), "node-a", "attach", "vol-1", hotplugQueuePriorityNormal, func(context.Context) error {
			atomic.AddInt32(&runCount, 1)
			return nil
		})
	}()

	time.Sleep(25 * time.Millisecond)
	close(releaseFirst)
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
	assert.Equal(t, int32(1), atomic.LoadInt32(&runCount))
}

func TestHotplugQueueReturnsActiveTimeout(t *testing.T) {
	manager := NewHotplugQueueManager(nil, "", NewDriverMetrics("test", "test"), time.Second, 0)
	manager.Configure(true, 0, 0, 25*time.Millisecond, nil)

	err := manager.Run(context.Background(), "node-a", "attach", "vol-1", hotplugQueuePriorityNormal, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	require.Error(t, err)
	var timeoutErr *HotplugQueueActiveTimeoutError
	assert.True(t, errors.As(err, &timeoutErr))
	assert.Equal(t, "node-a", timeoutErr.Node)
}

func TestHotplugQueueDropsStaleRequestBeforeDispatch(t *testing.T) {
	manager := NewHotplugQueueManager(nil, "", NewDriverMetrics("test", "test"), time.Second, 0)
	manager.Configure(true, 0, 0, 0, func(context.Context, string, string, string) HotplugQueueValidation {
		return HotplugQueueValidation{
			Decision: HotplugQueueValidationStale,
			Reason:   "persistent_volume_released",
		}
	})

	called := false
	err := manager.Run(context.Background(), "node-a", "attach", "vol-1", hotplugQueuePriorityNormal, func(context.Context) error {
		called = true
		return nil
	})

	require.Error(t, err)
	var staleErr *HotplugQueueStaleRequestError
	assert.True(t, errors.As(err, &staleErr))
	assert.False(t, called)
	assert.Equal(t, "persistent_volume_released", staleErr.Reason)
}

func TestHotplugQueueReturnsPausedRequestBeforeDispatch(t *testing.T) {
	manager := NewHotplugQueueManager(nil, "", NewDriverMetrics("test", "test"), time.Second, 0)
	manager.Configure(true, 0, 0, 0, func(context.Context, string, string, string) HotplugQueueValidation {
		return HotplugQueueValidation{
			Decision: HotplugQueueValidationPaused,
			Reason:   "kubernetes_node_not_ready",
			Message:  "node node-a hotplug operations are paused",
		}
	})

	called := false
	err := manager.Run(context.Background(), "node-a", "detach", "vol-1", hotplugQueuePriorityNormal, func(context.Context) error {
		called = true
		return nil
	})

	require.Error(t, err)
	var pausedErr *HotplugQueuePausedError
	assert.True(t, errors.As(err, &pausedErr))
	assert.False(t, called)
	assert.Equal(t, "kubernetes_node_not_ready", pausedErr.Reason)
}

func TestHotplugQueueSnapshotDebounce(t *testing.T) {
	runtime := &KubeRuntime{
		client: fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: hotplugQueueStateConfigMapName, Namespace: "default"},
			Data:       map[string]string{},
		}),
		enabled: true,
	}
	manager := NewHotplugQueueManager(runtime, "default", NewDriverMetrics("test", "test"), time.Second, 0)
	manager.SetSnapshotDebounce(50 * time.Millisecond)

	manager.persistSnapshot(HotplugQueueNodeSnapshot{
		Node:        "node-a",
		QueuedCount: 1,
		Queued: []HotplugQueueItemSnapshot{{
			ID:        1,
			Node:      "node-a",
			Operation: "attach",
			Volume:    "vol-a",
			Priority:  hotplugQueuePriorityNormal,
		}},
	})
	cm, err := runtime.client.CoreV1().ConfigMaps("default").Get(context.Background(), hotplugQueueStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, cm.Data, "node-a")

	require.Eventually(t, func() bool {
		cm, err := runtime.client.CoreV1().ConfigMaps("default").Get(context.Background(), hotplugQueueStateConfigMapName, metav1.GetOptions{})
		return err == nil && cm.Data["node-a"] != ""
	}, time.Second, 10*time.Millisecond)

	manager.persistSnapshot(HotplugQueueNodeSnapshot{Node: "node-a"})
	waitHotplugSnapshot(t, manager, runtime.client.(*fake.Clientset), "node-a", 0)
	cm, err = runtime.client.CoreV1().ConfigMaps("default").Get(context.Background(), hotplugQueueStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, cm.Data, "node-a")
}

func waitHotplugSnapshot(t *testing.T, manager *HotplugQueueManager, client *fake.Clientset, node string, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		cm, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), hotplugQueueStateConfigMapName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if count == 0 {
			return cm.Data[node] == ""
		}
		var snapshot HotplugQueueNodeSnapshot
		return json.Unmarshal([]byte(cm.Data[node]), &snapshot) == nil && snapshot.QueuedCount == count
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		manager.snapshotMu.Lock()
		defer manager.snapshotMu.Unlock()
		state := manager.snapshots[node]
		return state != nil && !state.running && state.timer == nil && state.persisted
	}, 2*time.Second, 10*time.Millisecond)
}

func TestHotplugQueueSkipsUnchangedSnapshotPersistence(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewHotplugQueueManager(&KubeRuntime{client: client, enabled: true}, "default", nil, time.Second, 0)
	var writes atomic.Int32
	client.PrependReactor("patch", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) { writes.Add(1); return false, nil, nil })
	snapshot := HotplugQueueNodeSnapshot{Node: "node-a", QueuedCount: 1}
	manager.persistSnapshot(snapshot)
	waitHotplugSnapshot(t, manager, client, snapshot.Node, 1)
	first := writes.Load()
	manager.persistSnapshot(snapshot)
	waitHotplugSnapshot(t, manager, client, snapshot.Node, 1)
	require.Equal(t, first, writes.Load())
}

func TestHotplugQueueRetriesFailedSnapshotPersistence(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(fmt.Sprint(clear), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			manager := NewHotplugQueueManager(&KubeRuntime{client: client, enabled: true}, "default", nil, time.Second, 0)
			snapshot := HotplugQueueNodeSnapshot{Node: "node-a", QueuedCount: 1}
			manager.persistSnapshot(snapshot)
			waitHotplugSnapshot(t, manager, client, snapshot.Node, 1)
			var calls atomic.Int32
			client.PrependReactor("patch", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if calls.Add(1) <= 2 {
					return true, nil, errors.New("transient API failure")
				}
				return false, nil, nil
			})
			snapshot.QueuedCount = 2
			if clear {
				snapshot.QueuedCount = 0
			}
			manager.persistSnapshot(snapshot)
			waitHotplugSnapshot(t, manager, client, snapshot.Node, snapshot.QueuedCount)
			require.EqualValues(t, 3, calls.Load())
		})
	}
}

func TestHotplugQueueBlockedSnapshotPreservesAdmissionAndCompletionOrder(t *testing.T) {
	client := fake.NewSimpleClientset()
	base := client.CoreV1().ConfigMaps("default")
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	manager := NewHotplugQueueManager(reviewRuntime(client, reviewConfigMaps{ConfigMapInterface: base, patch: func(ctx context.Context, name string, pt types.PatchType, payload []byte, opts metav1.PatchOptions, sub ...string) (*corev1.ConfigMap, error) {
		var patch struct {
			Data map[string]*string `json:"data"`
		}
		require.NoError(t, json.Unmarshal(payload, &patch))
		if patch.Data["node-a"] != nil && blocked.CompareAndSwap(false, true) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(hotplugSnapshotTimeout), deadline, time.Second)
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return base.Patch(ctx, name, pt, payload, opts, sub...)
	}}), "default", NewDriverMetrics("test", "test"), time.Second, 0)
	ctx := context.Background()
	activeDone := make(chan struct{})
	var activeOnce, releaseOnce sync.Once
	closeActive := func() { activeOnce.Do(func() { close(activeDone) }) }
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(closeActive)
	t.Cleanup(closeRelease)
	a := make(chan error, 1)
	go func() {
		a <- manager.Run(ctx, "node-a", "attach", "a", hotplugQueuePriorityNormal, func(context.Context) error { <-activeDone; return nil })
	}()
	<-entered
	b := make(chan error, 1)
	go func() {
		b <- manager.Run(ctx, "node-b", "attach", "b", hotplugQueuePriorityNormal, func(context.Context) error { return nil })
	}()
	select {
	case err := <-b:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("node B admission blocked by snapshot A")
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	canceled := make(chan error, 1)
	go func() {
		canceled <- manager.Run(cancelCtx, "node-a", "attach", "queued", hotplugQueuePriorityNormal, func(context.Context) error { t.Error("canceled item dispatched"); return nil })
	}()
	require.Eventually(t, func() bool { return manager.HasVolume("queued") }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-canceled:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("queue cancellation blocked by snapshot")
	}
	manager.mu.Lock()
	old := manager.snapshotLocked("node-a")
	manager.mu.Unlock()
	closeActive()
	require.NoError(t, <-a)
	require.Eventually(t, func() bool { return !manager.HasVolume("a") }, time.Second, time.Millisecond)
	closeRelease()
	waitHotplugSnapshot(t, manager, client, "node-a", 0)
	manager.persistSnapshot(old)
	waitHotplugSnapshot(t, manager, client, "node-a", 0)
}
