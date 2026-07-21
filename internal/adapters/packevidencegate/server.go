package packevidencegate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
)

const (
	maximumProtocolBody = 8 << 10
	maximumHeaderBytes  = 8 << 10
	serverIdleTimeout   = 15 * time.Second
)

var (
	ErrUnauthorized = errors.New("pack evidence gate: unauthorized peer")
	ErrProtocol     = errors.New("pack evidence gate: protocol error")
)

type ServerOptions struct {
	StatePath   string
	SocketPath  string
	Policy      Policy
	AllowedUIDs []uint32
	Clock       func() time.Time
}

type Server struct {
	store        *Store
	listener     *net.UnixListener
	httpServer   *http.Server
	socketPath   string
	socketInfo   os.FileInfo
	allowedUIDs  []uint32
	binding      binding
	now          func() time.Time
	closeOnce    sync.Once
	serveStarted bool
	mu           sync.Mutex
}

type peerContextKey struct{}

type peerIdentity struct {
	uid uint32
	err error
}

func NewServer(options ServerOptions) (*Server, error) {
	if len(options.AllowedUIDs) == 0 {
		return nil, ErrInvalidOptions
	}
	allowed := append([]uint32(nil), options.AllowedUIDs...)
	slices.Sort(allowed)
	allowed = slices.Compact(allowed)
	if options.Clock == nil {
		options.Clock = time.Now
	}
	store, err := Open(options.StatePath, options.Policy, options.Clock)
	if err != nil {
		return nil, err
	}
	fail := func(openErr error) (*Server, error) {
		_ = store.Close()
		return nil, openErr
	}
	socketPath, err := canonicalSocketPath(options.SocketPath)
	if err != nil {
		return fail(err)
	}
	if err := removeOwnedStaleSocket(socketPath); err != nil {
		return fail(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return fail(fmt.Errorf("pack evidence gate: listen: %w", err))
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return fail(fmt.Errorf("pack evidence gate: protect socket: %w", err))
	}
	info, err := os.Lstat(socketPath)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 || !ownedByEffectiveUser(info) {
		_ = listener.Close()
		return fail(fmt.Errorf("%w: created socket is not owner-only", ErrUnsafePath))
	}
	server := &Server{
		store: store, listener: listener, socketPath: socketPath, socketInfo: info,
		allowedUIDs: allowed, binding: protocolBinding(options.Policy), now: options.Clock,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/acquire", server.handleAcquire)
	mux.HandleFunc("POST /v1/response", server.handleResponse)
	mux.HandleFunc("POST /v1/release", server.handleRelease)
	server.httpServer = &http.Server{
		Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: serverIdleTimeout, MaxHeaderBytes: maximumHeaderBytes,
		ConnContext: func(ctx context.Context, connection net.Conn) context.Context {
			uid, peerErr := peerUID(connection)
			return context.WithValue(ctx, peerContextKey{}, peerIdentity{uid: uid, err: peerErr})
		},
	}
	return server, nil
}

func (server *Server) Serve(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	server.mu.Lock()
	if server.serveStarted {
		server.mu.Unlock()
		return errors.New("pack evidence gate: server already started")
	}
	server.serveStarted = true
	server.mu.Unlock()
	shutdownDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = server.httpServer.Shutdown(shutdownContext)
			cancel()
		case <-shutdownDone:
		}
	}()
	err := server.httpServer.Serve(server.listener)
	close(shutdownDone)
	closeErr := server.Close()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return errors.Join(err, closeErr)
}

func (server *Server) Close() error {
	if server == nil {
		return nil
	}
	var result error
	server.closeOnce.Do(func() {
		listenerErr := server.listener.Close()
		storeErr := server.store.Close()
		current, statErr := os.Lstat(server.socketPath)
		var removeErr error
		if statErr == nil && os.SameFile(server.socketInfo, current) {
			removeErr = os.Remove(server.socketPath)
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			removeErr = statErr
		}
		if errors.Is(listenerErr, net.ErrClosed) {
			listenerErr = nil
		}
		result = errors.Join(listenerErr, storeErr, removeErr)
	})
	return result
}

func canonicalSocketPath(raw string) (string, error) {
	if raw == "" || !filepath.IsAbs(raw) || filepath.Clean(raw) != raw {
		return "", ErrInvalidOptions
	}
	parent, err := secureconfigfile.ValidateDirectory(filepath.Dir(raw))
	if err != nil {
		return "", fmt.Errorf("%w: validate socket parent: %v", ErrUnsafePath, err)
	}
	return filepath.Join(parent, filepath.Base(raw)), nil
}

func removeOwnedStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: inspect socket: %v", ErrUnsafePath, err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 || !ownedByEffectiveUser(info) {
		return fmt.Errorf("%w: existing socket is not an owner-only Unix socket", ErrUnsafePath)
	}
	connection, dialErr := net.DialTimeout("unix", path, 250*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		return fmt.Errorf("%w: configured socket already has a live listener", ErrOwned)
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("%w: existing socket liveness is indeterminate: %v", ErrUnsafePath, dialErr)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, after) {
		return fmt.Errorf("%w: existing socket changed during liveness check", ErrUnsafePath)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("pack evidence gate: remove stale socket after exclusive state claim: %w", err)
	}
	return nil
}

func (server *Server) authorized(request *http.Request) bool {
	identity, ok := request.Context().Value(peerContextKey{}).(peerIdentity)
	return ok && identity.err == nil && slices.Contains(server.allowedUIDs, identity.uid)
}

func (server *Server) handleAcquire(writer http.ResponseWriter, request *http.Request) {
	if !server.authorized(request) {
		writeProtocolError(writer, http.StatusForbidden, "unauthorized")
		return
	}
	var input acquireRequest
	if err := decodeProtocolRequest(request, &input); err != nil {
		writeProtocolError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	if !validBinding(input.Binding, server.binding) {
		writeProtocolError(writer, http.StatusConflict, "policy_mismatch")
		return
	}
	decision, err := server.store.Acquire(input.AttemptID, input.Host)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	output := acquireResponse{Granted: decision.Granted, Lease: decision.Lease}
	if !decision.Granted {
		output.RetryAfterNano = int64(retryDelay(decision.RetryAt, server.now().UTC()))
	}
	writeProtocolJSON(writer, http.StatusOK, output)
}

func (server *Server) handleResponse(writer http.ResponseWriter, request *http.Request) {
	if !server.authorized(request) {
		writeProtocolError(writer, http.StatusForbidden, "unauthorized")
		return
	}
	var input responseRequest
	if err := decodeProtocolRequest(request, &input); err != nil {
		writeProtocolError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	if !validBinding(input.Binding, server.binding) {
		writeProtocolError(writer, http.StatusConflict, "policy_mismatch")
		return
	}
	applied, err := server.store.RecordResponse(input.Lease, input.Status, input.RetryAfter)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeProtocolJSON(writer, http.StatusOK, responseResponse{AppliedNano: int64(applied)})
}

func (server *Server) handleRelease(writer http.ResponseWriter, request *http.Request) {
	if !server.authorized(request) {
		writeProtocolError(writer, http.StatusForbidden, "unauthorized")
		return
	}
	var input releaseRequest
	if err := decodeProtocolRequest(request, &input); err != nil {
		writeProtocolError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	if !validBinding(input.Binding, server.binding) {
		writeProtocolError(writer, http.StatusConflict, "policy_mismatch")
		return
	}
	if err := server.store.Release(input.Lease); err != nil {
		writeStoreError(writer, err)
		return
	}
	writeProtocolJSON(writer, http.StatusOK, emptyResponse{Released: true})
}

func decodeProtocolRequest(request *http.Request, destination any) error {
	defer request.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(request.Body, maximumProtocolBody+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrProtocol
	}
	return nil
}

func writeStoreError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrPolicyMismatch):
		writeProtocolError(writer, http.StatusConflict, "policy_mismatch")
	case errors.Is(err, ErrLeaseMismatch), errors.Is(err, ErrAttemptComplete):
		writeProtocolError(writer, http.StatusConflict, "lease_mismatch")
	case errors.Is(err, ErrExpired):
		writeProtocolError(writer, http.StatusGone, "run_expired")
	case errors.Is(err, ErrInvalidHost), errors.Is(err, ErrInvalidOptions):
		writeProtocolError(writer, http.StatusBadRequest, "invalid_request")
	default:
		writeProtocolError(writer, http.StatusInternalServerError, "state_failure")
	}
}

func writeProtocolError(writer http.ResponseWriter, status int, code string) {
	writeProtocolJSON(writer, status, errorResponse{Code: code})
}

func writeProtocolJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
