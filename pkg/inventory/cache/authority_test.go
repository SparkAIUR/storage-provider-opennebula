package cache

import (
	"context"
	"sync/atomic"
	"testing"

	inventory "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type delayedAuthorityClient struct {
	client.Client
	calls                  atomic.Int32
	oldStarted, releaseOld chan struct{}
}

func (c *delayedAuthorityClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	n := c.calls.Add(1)
	out := list.(*inventory.OpenNebulaDatastoreList)
	if n == 1 {
		close(c.oldStarted)
		<-c.releaseOld
	}
	item := inventory.OpenNebulaDatastore{}
	item.Spec.Discovery.OpenNebulaDatastoreID = 100
	item.Spec.Discovery.Allowed = true
	item.Spec.Enabled = n == 1
	out.Items = []inventory.OpenNebulaDatastore{item}
	return nil
}
func TestFreshAuthorityCannotBeOverwrittenByDelayedOldObservation(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, inventory.AddToScheme(scheme))
	c := &delayedAuthorityClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), oldStarted: make(chan struct{}), releaseOld: make(chan struct{})}
	p := &Provider{client: c, enabled: true, authorityMode: AuthorityModeStrict}
	done := make(chan error, 1)
	go func() { done <- p.refresh(context.Background()) }()
	<-c.oldStarted
	require.Error(t, p.AuthorizeDatastore(context.Background(), 100))
	close(c.releaseOld)
	require.NoError(t, <-done)
	// Even after an older enabled response wins the observation cache, mutation re-reads authority.
	require.Error(t, p.AuthorizeDatastore(context.Background(), 100))
}
func TestFreshAuthorityRejectsMaintenanceAndMissing(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, inventory.AddToScheme(scheme))
	item := &inventory.OpenNebulaDatastore{}
	item.Name = "ds"
	item.Spec.Enabled = true
	item.Spec.MaintenanceMode = true
	item.Spec.Discovery.Allowed = true
	item.Spec.Discovery.OpenNebulaDatastoreID = 100
	p := &Provider{client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(item).Build(), enabled: true, authorityMode: AuthorityModeStrict}
	require.Error(t, p.AuthorizeDatastore(context.Background(), 100))
	require.Error(t, p.AuthorizeDatastore(context.Background(), 101))
}
