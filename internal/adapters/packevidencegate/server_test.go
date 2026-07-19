package packevidencegate

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnixCoordinatorPersistsCooldownAcrossServerRestart(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	directory := shortTempDir(t)
	statePath := filepath.Join(directory, "gate.db")
	socketPath := filepath.Join(directory, "gate.sock")
	policy := testPolicy(now)

	server, cancel, result := startTestServer(t, ServerOptions{
		StatePath: statePath, SocketPath: socketPath, Policy: policy,
		AllowedUIDs: []uint32{uint32(os.Geteuid())}, Clock: clock,
	})
	client, err := NewClient(ClientOptions{SocketPath: socketPath, RunID: policy.RunID, ConfigSHA256: policy.ConfigSHA256})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Acquire(context.Background(), "Example.org.")
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := client.RecordResponse(first, 429, "120"); err != nil || applied != 2*time.Minute {
		t.Fatalf("record response = %v, %v", applied, err)
	}
	if err := client.Release(first); err != nil {
		t.Fatal(err)
	}
	client.Close()
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	_ = server

	server, cancel, result = startTestServer(t, ServerOptions{
		StatePath: statePath, SocketPath: socketPath, Policy: policy,
		AllowedUIDs: []uint32{uint32(os.Geteuid())}, Clock: clock,
	})
	defer func() {
		cancel()
		if err := <-result; err != nil {
			t.Error(err)
		}
	}()
	client, err = NewClient(ClientOptions{SocketPath: socketPath, RunID: policy.RunID, ConfigSHA256: policy.ConfigSHA256})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var waiting acquireResponse
	err = client.post(context.Background(), "/v1/acquire", acquireRequest{
		Binding: client.binding, AttemptID: "attempt-after-restart", Host: "example.org",
	}, &waiting)
	if err != nil || waiting.Granted || time.Duration(waiting.RetryAfterNano) != 2*time.Minute {
		t.Fatalf("restart acquire = %+v, %v", waiting, err)
	}
	_ = server
}

func TestUnixCoordinatorRejectsPolicyMismatchAndUnlistedUID(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	policy := testPolicy(now)
	directory := shortTempDir(t)
	server, cancel, result := startTestServer(t, ServerOptions{
		StatePath: filepath.Join(directory, "gate.db"), SocketPath: filepath.Join(directory, "gate.sock"), Policy: policy,
		AllowedUIDs: []uint32{uint32(os.Geteuid())}, Clock: func() time.Time { return now },
	})
	client, err := NewClient(ClientOptions{SocketPath: server.socketPath, RunID: policy.RunID, ConfigSHA256: strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Acquire(context.Background(), "example.org")
	if !errors.Is(err, ErrPolicyMismatch) {
		t.Fatalf("policy mismatch error = %v", err)
	}
	client.Close()
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	server, cancel, result = startTestServer(t, ServerOptions{
		StatePath: filepath.Join(directory, "unauthorized.db"), SocketPath: filepath.Join(directory, "unauthorized.sock"), Policy: policy,
		AllowedUIDs: []uint32{uint32(os.Geteuid()) + 1}, Clock: func() time.Time { return now },
	})
	defer func() {
		cancel()
		if err := <-result; err != nil {
			t.Error(err)
		}
	}()
	client, err = NewClient(ClientOptions{SocketPath: server.socketPath, RunID: policy.RunID, ConfigSHA256: policy.ConfigSHA256})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Acquire(context.Background(), "example.org")
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized error = %v", err)
	}
}

func TestClientRechecksReleasedLeaseBeforeConservativeCrashExpiry(t *testing.T) {
	for _, test := range []struct {
		name       string
		firstHost  string
		secondHost string
		concurrent uint64
	}{
		{name: "same host", firstHost: "one.example", secondHost: "one.example", concurrent: 2},
		{name: "aggregate concurrency", firstHost: "one.example", secondHost: "two.example", concurrent: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			start := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
			var clockNano atomic.Int64
			clockNano.Store(start.UnixNano())
			clock := func() time.Time { return time.Unix(0, clockNano.Load()).UTC() }
			directory := shortTempDir(t)
			policy := testPolicy(start)
			policy.GlobalConcurrency = test.concurrent
			server, cancelServer, serverResult := startTestServer(t, ServerOptions{
				StatePath: filepath.Join(directory, "gate.db"), SocketPath: filepath.Join(directory, "gate.sock"), Policy: policy,
				AllowedUIDs: []uint32{uint32(os.Geteuid())}, Clock: clock,
			})
			defer func() {
				cancelServer()
				if err := <-serverResult; err != nil {
					t.Error(err)
				}
			}()
			firstClient, err := NewClient(ClientOptions{SocketPath: server.socketPath, RunID: policy.RunID, ConfigSHA256: policy.ConfigSHA256})
			if err != nil {
				t.Fatal(err)
			}
			defer firstClient.Close()
			secondClient, err := NewClient(ClientOptions{SocketPath: server.socketPath, RunID: policy.RunID, ConfigSHA256: policy.ConfigSHA256})
			if err != nil {
				t.Fatal(err)
			}
			defer secondClient.Close()
			secondClient.maxAcquirePoll = 10 * time.Millisecond

			first, err := firstClient.Acquire(context.Background(), test.firstHost)
			if err != nil {
				t.Fatal(err)
			}
			waitContext, cancelWait := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancelWait()
			waitResult := make(chan error, 1)
			go func() {
				lease, acquireErr := secondClient.Acquire(waitContext, test.secondHost)
				if acquireErr == nil {
					acquireErr = secondClient.Release(lease)
				}
				waitResult <- acquireErr
			}()
			select {
			case err := <-waitResult:
				t.Fatalf("waiter returned before release: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			if err := firstClient.Release(first); err != nil {
				t.Fatal(err)
			}
			clockNano.Store(start.Add(2 * time.Second).UnixNano())
			select {
			case err := <-waitResult:
				if err != nil {
					t.Fatal(err)
				}
			case <-waitContext.Done():
				t.Fatal("waiter slept until the conservative crash expiry")
			}
		})
	}
}

func TestNewServerRefusesUnrelatedLiveSocketWithoutUnlinking(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	directory := shortTempDir(t)
	socketPath := filepath.Join(directory, "unrelated.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(socketPath, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerOptions{
		StatePath: filepath.Join(directory, "gate.db"), SocketPath: socketPath, Policy: testPolicy(now),
		AllowedUIDs: []uint32{uint32(os.Geteuid())}, Clock: func() time.Time { return now },
	})
	if server != nil {
		_ = server.Close()
	}
	if !errors.Is(err, ErrOwned) {
		t.Fatalf("NewServer error = %v, want ErrOwned", err)
	}
	after, statErr := os.Lstat(socketPath)
	if statErr != nil || !os.SameFile(before, after) {
		t.Fatalf("live socket changed: after=%v error=%v", after, statErr)
	}
}

func TestNewServerReclaimsVerifiedStaleSocket(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	directory := shortTempDir(t)
	socketPath := filepath.Join(directory, "stale.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(socketPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerOptions{
		StatePath: filepath.Join(directory, "gate.db"), SocketPath: socketPath, Policy: testPolicy(now),
		AllowedUIDs: []uint32{uint32(os.Geteuid())}, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}

func startTestServer(t *testing.T, options ServerOptions) (*Server, context.CancelFunc, <-chan error) {
	t.Helper()
	server, err := NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- server.Serve(ctx) }()
	return server, cancel, result
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/private/tmp", "fmg-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	return directory
}
