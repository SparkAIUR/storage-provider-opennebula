package driver

import (
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
)

// client-go 0.36 OnError can replace a non-conflict interruption with a nil
// last conflict. Never acknowledge a failed storage-state write as persisted.
func retryOnConflict(backoff wait.Backoff, operation func() error) error {
	var callbackErr error
	err := retry.RetryOnConflict(backoff, func() error { callbackErr = operation(); return callbackErr })
	if err == nil && callbackErr != nil {
		return callbackErr
	}
	return err
}
