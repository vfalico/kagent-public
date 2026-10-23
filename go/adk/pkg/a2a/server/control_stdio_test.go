package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/kagent-dev/kagent/go/pkg/controlstdio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
)

// Exercises the ordinary server Start path, including both actual listeners.
func TestOrdinaryA2AStdio(t *testing.T) {
	if os.Getenv("T807_A2A_STDIO_HELPER") == "1" {
		s, err := NewA2AServer(a2atype.AgentCard{}, substrateExecutor{}, slog.New(slog.DiscardHandler), ServerConfig{Host: "127.0.0.1", Port: "80", ShutdownTimeout: time.Second})
		if err != nil {
			panic(err)
		}
		if err = s.Start(); err != nil {
			panic(err)
		}
		if err = s.WaitForShutdown(); err != nil {
			os.Exit(1)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOrdinaryA2AStdio$")
	cmd.Env = append(os.Environ(), "T807_A2A_STDIO_HELPER=1", "RUNTIME_CONTROL_STDIO=1")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	mux := controlstdio.New(true, output, input, func() error { input.Close(); return output.Close() })
	defer func() { mux.Close(); cancel(); cmd.Wait() }()
	go mux.Serve()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return mux.Dial(ctx, 8081) }}
	defer transport.CloseIdleConnections()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://runtime-control/readyz", nil)
	reply, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reply.Body)
	reply.Body.Close()
	if err != nil || reply.StatusCode != 200 {
		t.Fatal(reply.StatusCode, string(data), err)
	}
	conn, err := grpc.NewClient("passthrough:///runtime-control", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return mux.Dial(ctx, 80) }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	status, err := health.NewHealthClient(conn).Check(ctx, &health.HealthCheckRequest{})
	if err != nil || status.Status != health.HealthCheckResponse_SERVING {
		t.Fatal(status, err)
	}
}
