package packevidencegate

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

const (
	defaultControlTimeout = 5 * time.Second
	defaultAcquirePoll    = time.Second
)

var ErrUnavailable = errors.New("pack evidence gate: coordinator unavailable")

type ClientOptions struct {
	SocketPath     string
	RunID          string
	ConfigSHA256   string
	ControlTimeout time.Duration
}

type Client struct {
	httpClient     *http.Client
	transport      *http.Transport
	binding        binding
	now            func() time.Time
	maxAcquirePoll time.Duration
}

func NewClient(options ClientOptions) (*Client, error) {
	policy := Policy{RunID: options.RunID, ConfigSHA256: options.ConfigSHA256}
	if !runIDPattern.MatchString(policy.RunID) || len(policy.ConfigSHA256) != 64 {
		return nil, ErrInvalidOptions
	}
	if _, err := hex.DecodeString(policy.ConfigSHA256); err != nil || policy.ConfigSHA256 != string(bytes.ToLower([]byte(policy.ConfigSHA256))) {
		return nil, ErrInvalidOptions
	}
	socketPath, err := canonicalSocketPath(options.SocketPath)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(socketPath)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 || !ownedByEffectiveUser(info) {
		return nil, fmt.Errorf("%w: socket is not an owner-only Unix socket", ErrUnsafePath)
	}
	timeout := options.ControlTimeout
	if timeout == 0 {
		timeout = defaultControlTimeout
	}
	if timeout < time.Second || timeout > 30*time.Second {
		return nil, ErrInvalidOptions
	}
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		DisableCompression: true, MaxConnsPerHost: 4, MaxIdleConnsPerHost: 4,
		ResponseHeaderTimeout: timeout,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &Client{
		httpClient: &http.Client{Transport: transport, Timeout: timeout}, transport: transport,
		binding: binding{Version: protocolVersion, RunID: options.RunID, ConfigSHA256: options.ConfigSHA256, ProtocolRevision: "unix-http-json-v1"},
		now:     time.Now, maxAcquirePoll: defaultAcquirePoll,
	}, nil
}

func (client *Client) Acquire(ctx context.Context, rawHost string) (Lease, error) {
	if client == nil || ctx == nil {
		return Lease{}, ErrInvalidOptions
	}
	host, err := CanonicalHost(rawHost)
	if err != nil {
		return Lease{}, err
	}
	attemptID, err := newAttemptID()
	if err != nil {
		return Lease{}, fmt.Errorf("pack evidence gate: create attempt identity: %w", err)
	}
	for {
		var output acquireResponse
		if err := client.post(ctx, "/v1/acquire", acquireRequest{Binding: client.binding, AttemptID: attemptID, Host: host}, &output); err != nil {
			return Lease{}, err
		}
		if output.Granted {
			if err := validateLease(output.Lease); err != nil || output.Lease.Host != host || output.Lease.AttemptID != attemptID {
				return Lease{}, ErrProtocol
			}
			return output.Lease, nil
		}
		if output.RetryAfterNano <= 0 || time.Duration(output.RetryAfterNano) > 2*time.Hour {
			return Lease{}, ErrProtocol
		}
		wait := time.Duration(output.RetryAfterNano)
		if client.maxAcquirePoll > 0 && wait > client.maxAcquirePoll {
			wait = client.maxAcquirePoll
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Lease{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (client *Client) RecordResponse(lease Lease, status int, rawRetryAfter string) (time.Duration, error) {
	if client == nil {
		return 0, ErrInvalidOptions
	}
	ctx, cancel := context.WithTimeout(context.Background(), client.httpClient.Timeout)
	defer cancel()
	var output responseResponse
	if err := client.post(ctx, "/v1/response", responseRequest{Binding: client.binding, Lease: lease, Status: status, RetryAfter: rawRetryAfter}, &output); err != nil {
		return 0, err
	}
	if output.AppliedNano < 0 || time.Duration(output.AppliedNano) > time.Hour {
		return 0, ErrProtocol
	}
	return time.Duration(output.AppliedNano), nil
}

func (client *Client) Release(lease Lease) error {
	if client == nil {
		return ErrInvalidOptions
	}
	ctx, cancel := context.WithTimeout(context.Background(), client.httpClient.Timeout)
	defer cancel()
	var output emptyResponse
	if err := client.post(ctx, "/v1/release", releaseRequest{Binding: client.binding, Lease: lease}, &output); err != nil {
		return err
	}
	if !output.Released {
		return ErrProtocol
	}
	return nil
}

func (client *Client) Close() error {
	if client != nil && client.transport != nil {
		client.transport.CloseIdleConnections()
	}
	return nil
}

func (client *Client) post(ctx context.Context, path string, input, output any) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://fetchmark-gate"+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maximumProtocolBody+1)
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if response.StatusCode != http.StatusOK {
		var remote errorResponse
		if err := decoder.Decode(&remote); err != nil || remote.Code == "" {
			return ErrProtocol
		}
		switch remote.Code {
		case "unauthorized":
			return ErrUnauthorized
		case "policy_mismatch":
			return ErrPolicyMismatch
		case "run_expired":
			return ErrExpired
		case "lease_mismatch":
			return ErrLeaseMismatch
		case "invalid_request":
			return ErrInvalidOptions
		default:
			return ErrUnavailable
		}
	}
	if err := decoder.Decode(output); err != nil {
		return ErrProtocol
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrProtocol
	}
	return nil
}

func newAttemptID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}
