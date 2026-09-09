package driver

import (
	"context"
	"sync"
	"time"
)

const sharedFilesystemReapTimeout = time.Second

type sharedFilesystemFenceKey struct{}

// A cancelled mount-capable process must keep exclusive volume ownership until
// it is reaped. Waiting for that process must not consume a recovery worker.
type sharedFilesystemFence struct {
	mu       sync.Mutex
	waits    []<-chan struct{}
	unlock   func()
	onReaped func()
}

func fenceSharedFilesystemOperation(ctx context.Context, unlock, onReaped func()) (context.Context, func()) {
	fence := &sharedFilesystemFence{unlock: unlock, onReaped: onReaped}
	return context.WithValue(ctx, sharedFilesystemFenceKey{}, fence), fence.release
}

func (f *sharedFilesystemFence) hold(done <-chan struct{}) {
	f.mu.Lock()
	f.waits = append(f.waits, done)
	f.mu.Unlock()
}

func (f *sharedFilesystemFence) release() {
	f.mu.Lock()
	waits := append([]<-chan struct{}(nil), f.waits...)
	f.mu.Unlock()
	if len(waits) == 0 {
		f.unlock()
		return
	}
	go func() {
		for _, done := range waits {
			<-done
		}
		f.unlock()
		if f.onReaped != nil {
			f.onReaped()
		}
	}()
}

func awaitSharedFilesystemExit(ctx context.Context, done <-chan struct{}, stop func()) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}
	stop()
	reapBudget := sharedFilesystemReapTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline.Add(sharedFilesystemReapTimeout)); remaining < reapBudget {
			reapBudget = remaining
		}
	}
	timer := time.NewTimer(reapBudget)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		if fence, ok := ctx.Value(sharedFilesystemFenceKey{}).(*sharedFilesystemFence); ok {
			fence.hold(done)
		}
	}
	return ctx.Err()
}
