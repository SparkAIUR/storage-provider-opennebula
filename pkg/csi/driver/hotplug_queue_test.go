package driver

import (
	"context"
	"errors"
	"sync"
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
