package federationapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/federation"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
	"golang.org/x/time/rate"
)

const (
	DefaultPeerRate         = rate.Limit(2)
	DefaultPeerBurst        = 4
	DefaultPeerConcurrent   = 1
	DefaultMaxConcurrent    = 4
	DefaultMaxVerifications = 16
	DefaultSearchTimeout    = 2 * time.Second
	defaultErrorBody        = `{"error":"request rejected"}`
	internalErrorBody       = `{"error":"request failed"}`
)

// HandlerOptions contains only dependencies needed by the isolated private
// federation listener. The injected searcher is expected to represent the
// operator's explicitly curated local index.
type HandlerOptions struct {
	Identity      federation.Identity
	TrustRegistry federation.TrustRegistry
	Searcher      search.BatchSearcher
	ReplayCache   *ReplayCache

	Clock  func() time.Time
	Random io.Reader

	PeerRate          rate.Limit
	PeerBurst         int
	PeerMaxConcurrent int
	MaxConcurrent     int
	MaxVerifications  int
	SearchTimeout     time.Duration
	MaxClockSkew      time.Duration
}

type handler struct {
	identityID string
	privateKey ed25519.PrivateKey
	registry   federation.TrustRegistry
	searcher   search.BatchSearcher
	replay     *ReplayCache
	clock      func() time.Time
	random     io.Reader

	peerRate          rate.Limit
	peerBurst         int
	peerMaxConcurrent int
	verificationSlots chan struct{}
	searchSlots       chan struct{}
	searchTimeout     time.Duration
	clockSkew         time.Duration

	limitersMu sync.Mutex
	limiters   map[string]*rate.Limiter
	peerSlots  map[string]chan struct{}
	randomMu   sync.Mutex
}

// NewHandler constructs the complete private-federation router. It serves one
// exact POST route and deliberately does not mount the public Fetchmark API.
func NewHandler(options HandlerOptions) (http.Handler, error) {
	privateKey := options.Identity.PrivateKey()
	publicKey := options.Identity.PublicKey()
	if options.Identity.IdentityID() == "" || len(privateKey) != ed25519.PrivateKeySize ||
		len(publicKey) != ed25519.PublicKeySize || federation.KeyID(publicKey) != options.Identity.KeyID() {
		return nil, errors.New("federationapi: valid local identity is required")
	}
	if options.Searcher == nil {
		return nil, errors.New("federationapi: batch searcher is required")
	}
	if options.ReplayCache == nil {
		return nil, errors.New("federationapi: replay cache is required")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	if options.PeerRate == 0 {
		options.PeerRate = DefaultPeerRate
	}
	if options.PeerBurst == 0 {
		options.PeerBurst = DefaultPeerBurst
	}
	if options.PeerMaxConcurrent == 0 {
		options.PeerMaxConcurrent = DefaultPeerConcurrent
	}
	if options.MaxConcurrent == 0 {
		options.MaxConcurrent = DefaultMaxConcurrent
	}
	if options.MaxVerifications == 0 {
		options.MaxVerifications = DefaultMaxVerifications
	}
	if options.SearchTimeout == 0 {
		options.SearchTimeout = DefaultSearchTimeout
	}
	if options.MaxClockSkew == 0 {
		options.MaxClockSkew = federation.DefaultClockSkew
	}
	if options.PeerRate <= 0 || math.IsNaN(float64(options.PeerRate)) || math.IsInf(float64(options.PeerRate), 0) ||
		options.PeerBurst < 1 || options.PeerMaxConcurrent < 1 || options.MaxConcurrent < 1 ||
		options.MaxVerifications < 1 || options.SearchTimeout <= 0 ||
		options.MaxClockSkew < time.Second || options.MaxClockSkew > federation.MaxAllowedClockSkew {
		return nil, errors.New("federationapi: invalid resource limits")
	}

	return &handler{
		identityID:        options.Identity.IdentityID(),
		privateKey:        privateKey,
		registry:          options.TrustRegistry,
		searcher:          options.Searcher,
		replay:            options.ReplayCache,
		clock:             options.Clock,
		random:            options.Random,
		peerRate:          options.PeerRate,
		peerBurst:         options.PeerBurst,
		peerMaxConcurrent: options.PeerMaxConcurrent,
		verificationSlots: make(chan struct{}, options.MaxVerifications),
		searchSlots:       make(chan struct{}, options.MaxConcurrent),
		searchTimeout:     options.SearchTimeout,
		clockSkew:         options.MaxClockSkew,
		limiters:          make(map[string]*rate.Limiter),
		peerSlots:         make(map[string]chan struct{}),
	}, nil
}

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	metricPeer := "untrusted"
	metricOutcome := "rejected"
	defer func() {
		obs.FederationRequestTotal.WithLabelValues("inbound", metricPeer, metricOutcome).Inc()
		obs.FederationRequestDuration.WithLabelValues("inbound", metricPeer).Observe(time.Since(started).Seconds())
	}()
	setResponseHeaders(writer.Header())
	if request.URL == nil || request.URL.Path != federation.SearchPath ||
		request.URL.EscapedPath() != federation.SearchPath || request.URL.RawQuery != "" || request.URL.ForceQuery {
		writeGenericError(writer, http.StatusNotFound, defaultErrorBody)
		return
	}
	if request.Method != federation.SearchMethod {
		writer.Header().Set("Allow", federation.SearchMethod)
		writeGenericError(writer, http.StatusMethodNotAllowed, defaultErrorBody)
		return
	}
	contentTypes := request.Header.Values("Content-Type")
	if len(contentTypes) != 1 || !isJSONMediaType(contentTypes[0]) || len(request.Header.Values("Content-Encoding")) != 0 {
		writeGenericError(writer, http.StatusUnsupportedMediaType, defaultErrorBody)
		return
	}
	if request.Body == nil || request.ContentLength > federation.MaxRequestBytes {
		writeGenericError(writer, http.StatusBadRequest, defaultErrorBody)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, federation.MaxRequestBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > federation.MaxRequestBytes {
		writeGenericError(writer, http.StatusBadRequest, defaultErrorBody)
		return
	}
	select {
	case h.verificationSlots <- struct{}{}:
	default:
		metricOutcome = "verification_busy"
		writeGenericError(writer, http.StatusServiceUnavailable, internalErrorBody)
		return
	}
	envelope, _, err := federation.DecodeRequest(raw)
	if err != nil {
		<-h.verificationSlots
		writeGenericError(writer, http.StatusUnauthorized, defaultErrorBody)
		return
	}
	peer, trusted := h.registry.LookupInbound(envelope.Sender)
	if !trusted {
		<-h.verificationSlots
		metricOutcome = "auth"
		writeGenericError(writer, http.StatusUnauthorized, defaultErrorBody)
		return
	}
	now := h.clock().UTC()
	verified, err := federation.VerifyRequest(raw, peer.PublicKey(), federation.Verification{
		Sender: peer.IdentityID, Audience: h.identityID, Now: now, MaxClockSkew: h.clockSkew,
	})
	<-h.verificationSlots
	if err != nil {
		metricOutcome = "auth"
		writeGenericError(writer, http.StatusUnauthorized, defaultErrorBody)
		return
	}
	metricPeer = peer.PeerID
	if !h.allow(peer.PeerID, now) {
		metricOutcome = "rate_limited"
		writeGenericError(writer, http.StatusTooManyRequests, defaultErrorBody)
		return
	}
	releasePeer, acquired := h.acquirePeer(peer.PeerID)
	if !acquired {
		metricOutcome = "peer_busy"
		writeGenericError(writer, http.StatusServiceUnavailable, internalErrorBody)
		return
	}
	defer releasePeer()
	select {
	case h.searchSlots <- struct{}{}:
		defer func() { <-h.searchSlots }()
	default:
		metricOutcome = "busy"
		writeGenericError(writer, http.StatusServiceUnavailable, internalErrorBody)
		return
	}
	if err := h.replay.Reserve(peer.PeerID, verified.Envelope.Nonce, now); err != nil {
		status := http.StatusServiceUnavailable
		metricOutcome = "replay_capacity"
		if errors.Is(err, ErrReplay) {
			status = http.StatusConflict
			metricOutcome = "replay"
		}
		writeGenericError(writer, status, defaultErrorBody)
		return
	}

	searchContext, cancel := context.WithTimeout(request.Context(), h.searchTimeout)
	defer cancel()
	strictSafeSearch := 1
	batch, err := h.searcher.SearchBatch(searchContext, search.Query{
		Q: verified.Search.Query, MaxResults: verified.Search.Limit, SafeSearch: &strictSafeSearch,
	})
	if err != nil || searchContext.Err() != nil {
		status := http.StatusBadGateway
		metricOutcome = "search_error"
		if errors.Is(searchContext.Err(), context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
			metricOutcome = "timeout"
		}
		writeGenericError(writer, status, internalErrorBody)
		return
	}
	if batch.Status != search.BatchHealthy && batch.Status != search.BatchAuthoritativeEmpty {
		metricOutcome = "search_degraded"
		writeGenericError(writer, http.StatusBadGateway, internalErrorBody)
		return
	}

	response := federation.SearchResponse{Results: exportURLs(batch.Hits, verified.Search.Limit)}
	nonce := make([]byte, federation.NonceBytes)
	h.randomMu.Lock()
	_, randomErr := io.ReadFull(h.random, nonce)
	h.randomMu.Unlock()
	if randomErr != nil {
		metricOutcome = "internal"
		writeGenericError(writer, http.StatusInternalServerError, internalErrorBody)
		return
	}
	signedAt := h.clock().UTC().Truncate(time.Second)
	signed, err := federation.SignResponse(response, raw, signedAt, nonce, h.privateKey)
	if err != nil {
		metricOutcome = "internal"
		writeGenericError(writer, http.StatusInternalServerError, internalErrorBody)
		return
	}
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(signed)
	metricOutcome = "ok"
	if len(response.Results) == 0 {
		metricOutcome = "empty"
	}
}

func (h *handler) allow(peerID string, now time.Time) bool {
	h.limitersMu.Lock()
	limiter := h.limiters[peerID]
	if limiter == nil {
		limiter = rate.NewLimiter(h.peerRate, h.peerBurst)
		h.limiters[peerID] = limiter
	}
	h.limitersMu.Unlock()
	return limiter.AllowN(now, 1)
}

func (h *handler) acquirePeer(peerID string) (func(), bool) {
	h.limitersMu.Lock()
	slots := h.peerSlots[peerID]
	if slots == nil {
		slots = make(chan struct{}, h.peerMaxConcurrent)
		h.peerSlots[peerID] = slots
	}
	h.limitersMu.Unlock()
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	default:
		return func() {}, false
	}
}

func exportURLs(hits []search.Hit, limit int) []federation.SearchResult {
	results := make([]federation.SearchResult, 0, min(limit, len(hits)))
	seen := make(map[string]struct{}, min(limit, len(hits)))
	for _, hit := range hits {
		canonical, err := canonicalurl.V1(hit.URL)
		if err != nil || len(canonical) == 0 || len(canonical) > federation.MaxURLBytes {
			continue
		}
		parsed, err := url.Parse(canonical)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
			continue
		}
		if _, duplicate := seen[canonical]; duplicate {
			continue
		}
		seen[canonical] = struct{}{}
		results = append(results, federation.SearchResult{URL: canonical})
		if len(results) == limit {
			break
		}
	}
	return results
}

func isJSONMediaType(value string) bool {
	mediaType, parameters, err := mime.ParseMediaType(value)
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return false
	}
	for name, parameter := range parameters {
		if !strings.EqualFold(name, "charset") || !strings.EqualFold(parameter, "utf-8") {
			return false
		}
	}
	return true
}

func setResponseHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("Content-Type", "application/json")
	header.Set("X-Content-Type-Options", "nosniff")
}

func writeGenericError(writer http.ResponseWriter, status int, body string) {
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, body)
}

var _ http.Handler = (*handler)(nil)
