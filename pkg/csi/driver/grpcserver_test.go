package driver

import (
	"context"
	"testing"

	"google.golang.org/grpc"
)

func TestParseGRPCServerEndpointUnix(t *testing.T) {
	protocol, address, err := parseGRPCServerEndpoint("unix:///tmp/opennebula-csi.sock")
	if err != nil {
		t.Fatalf("expected unix endpoint to parse, got err=%v", err)
	}
	if protocol != "unix" {
		t.Fatalf("expected unix protocol, got %q", protocol)
	}
	if address != "/tmp/opennebula-csi.sock" {
		t.Fatalf("expected unix address, got %q", address)
	}
}

func TestParseGRPCServerEndpointTCP(t *testing.T) {
	protocol, address, err := parseGRPCServerEndpoint("tcp://127.0.0.1:10000")
	if err != nil {
		t.Fatalf("expected tcp endpoint to parse, got err=%v", err)
	}
	if protocol != "tcp" {
		t.Fatalf("expected tcp protocol, got %q", protocol)
	}
	if address != "127.0.0.1:10000" {
		t.Fatalf("expected tcp address, got %q", address)
	}
}

func TestParseGRPCServerEndpointRejectsUnsupportedProtocol(t *testing.T) {
	_, _, err := parseGRPCServerEndpoint("http://127.0.0.1:10000")
	if err == nil {
		t.Fatal("expected unsupported protocol to fail")
	}
}

func TestLogInterceptorPassesRequestThrough(t *testing.T) {
	called := false
	resp, err := logInterceptor(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Node/NodeGetInfo"}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("expected interceptor to return handler response, got err=%v", err)
	}
	if !called {
		t.Fatal("expected handler to be invoked")
	}
	if got := resp.(string); got != "ok" {
		t.Fatalf("expected response 'ok', got %q", got)
	}
}
