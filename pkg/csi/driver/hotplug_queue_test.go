package driver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
