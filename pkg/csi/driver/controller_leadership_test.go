package driver

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestControllerLeaderInterceptorAllowsNonControllerMethods(t *testing.T) {
	leadership := &ControllerLeadership{enabled: true}
	interceptor := controllerLeaderInterceptor(leadership)

	called := false
	_, err := interceptor(context.Background(), "req", &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Node/NodeGetInfo",
	}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected non-controller method to reach handler")
	}
}

func TestControllerLeaderInterceptorRejectsFollowerControllerRPCs(t *testing.T) {
	leadership := &ControllerLeadership{
		enabled:        true,
		leaseName:      "lease-a",
		leaseNamespace: "kube-system",
	}
	interceptor := controllerLeaderInterceptor(leadership)

	called := false
	_, err := interceptor(context.Background(), "req", &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Controller/ControllerPublishVolume",
	}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
	if called {
		t.Fatal("expected follower controller RPC to be rejected before handler")
	}
}

func TestControllerLeaderInterceptorAllowsLeaderControllerRPCs(t *testing.T) {
	leadership := &ControllerLeadership{enabled: true}
	leadership.isLeader.Store(true)
	interceptor := controllerLeaderInterceptor(leadership)

	called := false
	_, err := interceptor(context.Background(), "req", &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Controller/ControllerPublishVolume",
	}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected leader controller RPC to reach handler")
	}
}
