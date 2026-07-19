// Package packevidence collects bounded, replayable robots and noindex
// evidence for the separate open-index-pack publisher workflow. It is not used
// by Fetchmark's ordinary request path or by the network-free pack builder.
package packevidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/packevidencegate"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	"golang.org/x/sync/singleflight"
)

const (
	ObservationVersion     = 1
	MaxResponseHeaderBytes = 64 << 10
	MaxRobotsCacheEntries  = 1_024
	MaxRobotsCacheBytes    = 16 << 20
	MaxRetryAfter          = time.Hour
)

var ErrCoordination = errors.New("pack evidence observer: coordination failure")

// HostCoordinator is the narrow control-plane boundary used when multiple
// collector processes share one publisher host. A configured coordinator is
// fail-closed; the observer never falls back to its process-local gate.
type HostCoordinator interface {
	Acquire(context.Context, string) (packevidencegate.Lease, error)
	RecordResponse(packevidencegate.Lease, int, string) (time.Duration, error)
	Release(packevidencegate.Lease) error
}

// Observation is the body-free audit record for one normalized input row.
// PolicyBody is returned separately and stored content-addressed by the bundle
// writer; page representations never leave Observe.
type Observation struct {
	Version         int              `json:"version"`
	Row             uint64           `json:"row"`
	InputLineSHA256 string           `json:"input_line_sha256"`
	URL             string           `json:"url"`
	Outcome         string           `json:"outcome"`
	Reason          string           `json:"reason,omitempty"`
	Robots          RobotsEvidence   `json:"robots"`
	Indexing        ResponseEvidence `json:"indexing"`
}

type RobotsEvidence struct {
	Status            int    `json:"status"`
	RobotsURI         string `json:"robots_uri"`
	FinalURI          string `json:"final_uri"`
	CheckedAt         string `json:"checked_at"`
	ValidUntil        string `json:"valid_until"`
	Outcome           string `json:"outcome"`
	BodySHA256        string `json:"body_sha256"`
	BodyComplete      bool   `json:"body_complete"`
	FailureReason     string `json:"failure_reason,omitempty"`
	RetryAfterSeconds uint64 `json:"retry_after_seconds,omitempty"`
}

type ResponseEvidence struct {
	Status               int      `json:"status"`
	FinalURL             string   `json:"final_url"`
	CheckedAt            string   `json:"checked_at"`
	ValidUntil           string   `json:"valid_until"`
	Outcome              string   `json:"outcome"`
	FailureReason        string   `json:"failure_reason,omitempty"`
	RedirectLocation     string   `json:"redirect_location,omitempty"`
	ContentType          string   `json:"content_type,omitempty"`
	XRobotsTag           []string `json:"x_robots_tag,omitempty"`
	MetadataRobots       []string `json:"metadata_robots,omitempty"`
	HeadersSHA256        string   `json:"headers_sha256,omitempty"`
	RepresentationSHA256 string   `json:"representation_sha256,omitempty"`
	ParserVersion        string   `json:"parser_version,omitempty"`
	RetryAfterSeconds    uint64   `json:"retry_after_seconds,omitempty"`
}

type Result struct {
	Candidate          indexpackselection.Candidate
	Observation        Observation
	PolicyBody         []byte
	PolicyBodyComplete bool
}

// Observer performs live evidence collection. The exported constructor always
// installs Fetchmark's public-only, redirect-validating egress policy and never
// reads proxy configuration.
type Observer struct {
	client         *http.Client
	userAgent      string
	validity       time.Duration
	interval       time.Duration
	requestTimeout time.Duration
	now            func() time.Time
	validate       func(context.Context, string) error
	coordinator    HostCoordinator
	robotsSF       singleflight.Group

	mu         sync.Mutex
	hosts      map[string]*hostGate
	cache      map[string]robotsResult
	cacheOrder []string
	cacheBytes int
}

type hostGate struct {
	mu   sync.Mutex
	next time.Time
}

type robotsResult struct {
	observation indexpackselection.RobotsObservation
	evidence    RobotsEvidence
	body        []byte
	allowed     bool
}

// NewPublicObserver creates the production observer from the exact validated
// collector policy. Operational limits cannot be relaxed by call-site flags.
func NewPublicObserver(config indexpackadmission.Config, toolVersion string) (*Observer, error) {
	return newPublicObserver(config, toolVersion, nil)
}

// NewPublicObserverWithCoordinator creates an observer whose actual request
// hops use the durable shared host gate. Coordinator loss aborts collection.
func NewPublicObserverWithCoordinator(config indexpackadmission.Config, toolVersion string, coordinator HostCoordinator) (*Observer, error) {
	if coordinator == nil {
		return nil, errors.New("pack evidence observer: coordinator is required")
	}
	return newPublicObserver(config, toolVersion, coordinator)
}

func newPublicObserver(config indexpackadmission.Config, toolVersion string, coordinator HostCoordinator) (*Observer, error) {
	userAgent, err := config.UserAgent(toolVersion)
	if err != nil {
		return nil, err
	}
	timeout := time.Duration(config.RequestTimeoutSeconds) * time.Second
	interval := time.Duration(config.MinHostIntervalMillis) * time.Millisecond
	policy := egress.DefaultExternal()
	policy.ResponseHeaderTimeout = min(timeout, 10*time.Second)
	client := policy.HTTPClient(timeout)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		return nil, errors.New("pack evidence observer: public egress transport unavailable")
	}
	transport.DisableCompression = true
	transport.MaxResponseHeaderBytes = MaxResponseHeaderBytes
	observer, err := newObserver(client, userAgent, time.Duration(config.EvidenceValidityHours)*time.Hour, interval, time.Now)
	if err != nil {
		return nil, err
	}
	observer.validate = policy.Validate
	observer.coordinator = coordinator
	client.Transport = &pacedTransport{base: transport, observer: observer}
	return observer, nil
}

func newObserver(client *http.Client, userAgent string, validity, interval time.Duration, now func() time.Time) (*Observer, error) {
	if client == nil || strings.TrimSpace(userAgent) != userAgent || userAgent == "" || len(userAgent) > 256 ||
		validity <= 0 || validity > indexpackadmission.MaxValidityHours*time.Hour || interval < 0 || now == nil {
		return nil, errors.New("pack evidence observer: invalid dependencies or limits")
	}
	requestTimeout := client.Timeout
	// http.Client.Timeout starts before RoundTrip and therefore includes time
	// spent in the per-host pacing queue. The paced transport applies this same
	// bound only after a request is admitted to the network.
	client.Timeout = 0
	previousCheckRedirect := client.CheckRedirect
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		follow, _ := request.Context().Value(followRedirectsKey{}).(bool)
		if !follow {
			return http.ErrUseLastResponse
		}
		if previousCheckRedirect != nil {
			return previousCheckRedirect(request, via)
		}
		if len(via) >= 10 {
			return errors.New("pack evidence observer: stopped after 10 redirects")
		}
		return nil
	}
	return &Observer{client: client, userAgent: userAgent, validity: validity, interval: interval, requestTimeout: requestTimeout, now: now, hosts: make(map[string]*hostGate), cache: make(map[string]robotsResult)}, nil
}

// Observe evaluates one strict normalized row. Expected network, policy, and
// content failures are represented as deterministic negative evidence; only
// cancellation or an internal invariant error aborts the caller's run.
func (observer *Observer) Observe(ctx context.Context, row uint64, normalized ccindex.Candidate, rights indexpackselection.RightsDecision) (Result, error) {
	if ctx == nil || row == 0 {
		return Result{}, errors.New("pack evidence observer: context and row are required")
	}
	rawParsed, err := url.Parse(normalized.URL)
	if err != nil {
		return observer.negative(row, normalized, rights, "invalid_url"), nil
	}
	if rawParsed.User != nil {
		return observer.negative(row, normalized, rights, "non_public_url"), nil
	}
	canonical, err := canonicalurl.V1(normalized.URL)
	if err != nil {
		return observer.negative(row, normalized, rights, "invalid_url"), nil
	}
	if canonical != normalized.URL {
		return observer.negative(row, normalized, rights, "noncanonical_url"), nil
	}
	parsed, err := url.Parse(canonical)
	if err != nil || parsed.RawQuery != "" || parsed.ForceQuery {
		return observer.negative(row, normalized, rights, "query_not_allowed"), nil
	}
	preflight, reason, err := indexpackselection.PreflightCandidate(candidateWithAdmission(normalized, indexpackselection.Admission{}), observer.now().UTC())
	if err != nil {
		return observer.negative(row, normalized, rights, "invalid_capture_metadata"), nil
	}
	if reason != "" {
		return observer.negative(row, normalized, rights, reason), nil
	}
	if preflight.CanonicalURL != canonical {
		return observer.negative(row, normalized, rights, "noncanonical_url"), nil
	}
	robotsResult, err := observer.observeRobots(ctx, parsed)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		return Result{}, err
	}
	result := Result{
		Candidate:   candidateWithAdmission(normalized, indexpackselection.Admission{Version: indexpackselection.AdmissionVersion, Robots: robotsResult.observation, Rights: rights}),
		Observation: Observation{Version: ObservationVersion, Row: row, URL: canonical, Robots: robotsResult.evidence},
		PolicyBody:  append([]byte(nil), robotsResult.body...), PolicyBodyComplete: robotsResult.evidence.BodyComplete,
	}
	if !robotsResult.allowed {
		result.Observation.Outcome = "rejected"
		result.Observation.Reason = "robots_not_allowed"
		return result, nil
	}
	indexing, response, failure, err := observer.observePage(ctx, canonical)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		return Result{}, err
	}
	result.Candidate.Admission.Indexing = indexing
	result.Observation.Indexing = response
	if failure != "" {
		result.Observation.Outcome = "rejected"
		result.Observation.Reason = failure
	} else if indexing.FinalURL != canonical {
		result.Observation.Outcome = "rejected"
		result.Observation.Reason = "indexing_redirect_mismatch"
	} else if indexing.Outcome != "indexable" {
		result.Observation.Outcome = "rejected"
		result.Observation.Reason = "indexing_not_permitted"
	} else {
		result.Observation.Outcome = "admitted"
	}
	return result, nil
}

func (observer *Observer) observeRobots(ctx context.Context, page *url.URL) (robotsResult, error) {
	origin := page.Scheme + "://" + page.Host
	observer.mu.Lock()
	if cached, ok := observer.cache[origin]; ok {
		observer.mu.Unlock()
		return cloneRobotsResult(cached), nil
	}
	observer.mu.Unlock()
	value, err, _ := observer.robotsSF.Do(origin, func() (any, error) {
		observer.mu.Lock()
		if cached, ok := observer.cache[origin]; ok {
			observer.mu.Unlock()
			return cloneRobotsResult(cached), nil
		}
		observer.mu.Unlock()
		return observer.fetchRobots(ctx, origin, page)
	})
	if value == nil {
		return robotsResult{}, err
	}
	result, ok := value.(robotsResult)
	if !ok {
		return robotsResult{}, errors.New("pack evidence observer: invalid coalesced robots result")
	}
	return cloneRobotsResult(result), err
}

func (observer *Observer) fetchRobots(ctx context.Context, origin string, page *url.URL) (robotsResult, error) {
	robotsURI := origin + "/robots.txt"
	response, body, failure, err := observer.get(ctx, robotsURI, robots.MaxEvidencePolicyBytes, true, true)
	if err != nil {
		return robotsResult{}, err
	}
	checkedAt := response.checkedAt.UTC().Truncate(time.Second)
	evidence := RobotsEvidence{RobotsURI: robotsURI, FinalURI: robotsURI, CheckedAt: checkedAt.Format(time.RFC3339), ValidUntil: checkedAt.Add(observer.validity).Format(time.RFC3339)}
	evidence.Status = response.status
	evidence.FinalURI = response.effectiveURL
	evidence.FailureReason = failure
	bodyDigest := sha256.Sum256(body)
	evidence.BodySHA256 = hex.EncodeToString(bodyDigest[:])
	allowed := false
	switch {
	case failure != "":
		evidence.Outcome = "failed"
	case response.status == http.StatusTooManyRequests:
		evidence.BodyComplete = true
		evidence.Outcome = "failed"
		evidence.FailureReason = "robots_rate_limited"
		evidence.RetryAfterSeconds = observer.applyRetryAfter(response, checkedAt)
		body = nil
		emptyDigest := sha256.Sum256(nil)
		evidence.BodySHA256 = hex.EncodeToString(emptyDigest[:])
	case response.status >= 400 && response.status < 500:
		evidence.BodyComplete = true
		evidence.Outcome = "allowed"
		allowed = true
		body = nil
		emptyDigest := sha256.Sum256(nil)
		evidence.BodySHA256 = hex.EncodeToString(emptyDigest[:])
	case response.status >= 200 && response.status < 300:
		evidence.BodyComplete = true
		allowed, err = robots.EvaluatePolicy(body, observer.userAgent, page.String())
		if err != nil {
			evidence.Outcome = "failed"
			evidence.FailureReason = "robots_parse_failed"
		} else if allowed {
			evidence.Outcome = "allowed"
		} else {
			evidence.Outcome = "disallowed"
		}
	default:
		evidence.BodyComplete = true
		evidence.Outcome = "failed"
		evidence.FailureReason = "robots_status"
	}
	observation := indexpackselection.RobotsObservation{
		UserAgent: observer.userAgent, RobotsURI: robotsURI, CheckedAt: evidence.CheckedAt,
		ValidUntil: evidence.ValidUntil, Outcome: evidence.Outcome, BodySHA256: evidence.BodySHA256,
	}
	result := robotsResult{observation: observation, evidence: evidence, body: append([]byte(nil), body...), allowed: allowed}
	result = observer.cacheRobots(origin, result)
	return cloneRobotsResult(result), nil
}

func (observer *Observer) cacheRobots(origin string, result robotsResult) robotsResult {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if existing, ok := observer.cache[origin]; ok {
		return existing
	}
	entryBytes := len(result.body)
	if entryBytes > MaxRobotsCacheBytes {
		return result
	}
	for len(observer.cache) >= MaxRobotsCacheEntries || observer.cacheBytes+entryBytes > MaxRobotsCacheBytes {
		if len(observer.cacheOrder) == 0 {
			break
		}
		oldest := observer.cacheOrder[0]
		observer.cacheOrder = observer.cacheOrder[1:]
		if cached, ok := observer.cache[oldest]; ok {
			observer.cacheBytes -= len(cached.body)
			delete(observer.cache, oldest)
		}
	}
	observer.cache[origin] = result
	observer.cacheOrder = append(observer.cacheOrder, origin)
	observer.cacheBytes += entryBytes
	return result
}

func (observer *Observer) observePage(ctx context.Context, rawURL string) (indexpackselection.IndexingObservation, ResponseEvidence, string, error) {
	response, body, failure, err := observer.get(ctx, rawURL, indexpackadmission.MaxRepresentationBytes, false, false)
	if err != nil {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, "", err
	}
	checkedAt := response.checkedAt.UTC().Truncate(time.Second)
	evidence := ResponseEvidence{
		Status: response.status, FinalURL: response.finalURL, CheckedAt: checkedAt.Format(time.RFC3339),
		ValidUntil: checkedAt.Add(observer.validity).Format(time.RFC3339), Outcome: "failed", FailureReason: failure,
		ContentType: response.contentType, XRobotsTag: append([]string(nil), response.xRobotsTag...), RedirectLocation: response.location,
	}
	if response.status == http.StatusTooManyRequests {
		evidence.RetryAfterSeconds = observer.applyRetryAfter(response, checkedAt)
	}
	if failure != "" {
		return indexpackselection.IndexingObservation{CheckedAt: evidence.CheckedAt, ValidUntil: evidence.ValidUntil, FinalURL: response.finalURL, Outcome: "fetch_failed"}, evidence, "indexing_fetch_failed", nil
	}
	indexing, exact, err := indexpackadmission.EvaluateIndexing(indexpackadmission.IndexingInput{
		Status: response.status, FinalURL: response.finalURL, ContentType: response.contentType,
		XRobotsTag: response.xRobotsTag, Representation: body, Complete: true,
		UserAgent: observer.userAgent, ObservedAt: checkedAt, Validity: observer.validity,
	})
	if err != nil {
		evidence.FailureReason = classifyEvaluationError(err)
		return indexpackselection.IndexingObservation{CheckedAt: evidence.CheckedAt, ValidUntil: evidence.ValidUntil, FinalURL: response.finalURL, Outcome: "evaluation_failed"}, evidence, "indexing_evaluation_failed", nil
	}
	evidence.Outcome = indexing.Outcome
	evidence.FailureReason = ""
	evidence.MetadataRobots = append([]string(nil), exact.MetadataRobots...)
	evidence.HeadersSHA256 = indexing.HeadersSHA256
	evidence.RepresentationSHA256 = indexing.RepresentationSHA256
	evidence.ParserVersion = indexing.ParserVersion
	return indexing, evidence, "", nil
}

type httpEvidence struct {
	status       int
	finalURL     string
	contentType  string
	xRobotsTag   []string
	location     string
	retryAfter   string
	effectiveURL string
	checkedAt    time.Time
}

type followRedirectsKey struct{}

func (observer *Observer) get(ctx context.Context, rawURL string, maximum int, bodyless4xx, followRedirects bool) (httpEvidence, []byte, string, error) {
	if _, err := url.Parse(rawURL); err != nil {
		return httpEvidence{}, nil, "invalid_url", nil
	}
	if observer.validate != nil {
		if err := observer.validate(ctx, rawURL); err != nil {
			return httpEvidence{finalURL: rawURL, checkedAt: observer.now().UTC().Truncate(time.Second)}, nil, classifyHTTPError(err), nil
		}
	}
	requestContext := context.WithValue(ctx, followRedirectsKey{}, followRedirects)
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, rawURL, nil)
	if err != nil {
		return httpEvidence{}, nil, "invalid_url", nil
	}
	request.Header.Set("User-Agent", observer.userAgent)
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Accept", "text/html,application/xhtml+xml,text/plain;q=0.1,*/*;q=0.01")
	response, err := observer.client.Do(request)
	checkedAt := observer.now().UTC().Truncate(time.Second)
	if err != nil {
		if ctx.Err() != nil {
			return httpEvidence{}, nil, "", ctx.Err()
		}
		if errors.Is(err, ErrCoordination) {
			return httpEvidence{}, nil, "", err
		}
		return httpEvidence{finalURL: rawURL, checkedAt: checkedAt}, nil, classifyHTTPError(err), nil
	}
	effectiveURL := response.Request.URL.String()
	finalURL, canonicalErr := canonicalurl.V1(effectiveURL)
	if canonicalErr != nil {
		finalURL = effectiveURL
	}
	result := httpEvidence{
		status: response.StatusCode, finalURL: finalURL, contentType: response.Header.Get("Content-Type"),
		xRobotsTag: append([]string(nil), response.Header.Values("X-Robots-Tag")...),
		location:   response.Header.Get("Location"), retryAfter: response.Header.Get("Retry-After"),
		effectiveURL: effectiveURL, checkedAt: checkedAt,
	}
	if len(result.location) > 2<<10 || strings.ContainsAny(result.location, "\x00\r\n") {
		if err := closeEvidenceBody(response.Body); err != nil {
			return result, nil, "", err
		}
		return result, nil, "invalid_location_header", nil
	}
	if bodyless4xx && response.StatusCode >= 400 && response.StatusCode < 500 {
		if err := closeEvidenceBody(response.Body); err != nil {
			return result, nil, "", err
		}
		return result, nil, "", nil
	}
	if encoding := strings.TrimSpace(response.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		if err := closeEvidenceBody(response.Body); err != nil {
			return result, nil, "", err
		}
		return result, nil, "unsupported_content_encoding", nil
	}
	body, complete, readErr := readComplete(response.Body, response.ContentLength, maximum)
	closeErr := response.Body.Close()
	if errors.Is(readErr, ErrCoordination) || errors.Is(closeErr, ErrCoordination) {
		return result, nil, "", errors.Join(readErr, closeErr)
	}
	readErr = errors.Join(readErr, closeErr)
	if readErr != nil || !complete {
		return result, nil, classifyReadError(readErr), nil
	}
	return result, body, "", nil
}

func closeEvidenceBody(body io.ReadCloser) error {
	if body == nil {
		return nil
	}
	err := body.Close()
	if errors.Is(err, ErrCoordination) {
		return err
	}
	return nil
}

func readComplete(reader io.Reader, contentLength int64, maximum int) ([]byte, bool, error) {
	if reader == nil || maximum < 0 {
		return nil, false, errors.New("invalid bounded reader")
	}
	if contentLength > int64(maximum) {
		return nil, false, errors.New("content length exceeds limit")
	}
	body, err := io.ReadAll(io.LimitReader(reader, int64(maximum)+1))
	if err != nil {
		return nil, false, err
	}
	if len(body) > maximum {
		return nil, false, errors.New("body exceeds limit")
	}
	if contentLength >= 0 && int64(len(body)) != contentLength {
		return nil, false, io.ErrUnexpectedEOF
	}
	return body, true, nil
}

func (observer *Observer) hostGate(host string) *hostGate {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	gate := observer.hosts[host]
	if gate == nil {
		gate = &hostGate{}
		observer.hosts[host] = gate
	}
	return gate
}

func (observer *Observer) extendHostCooldown(host string, delay time.Duration) {
	if delay <= 0 {
		return
	}
	gate := observer.hostGate(strings.ToLower(host))
	gate.mu.Lock()
	defer gate.mu.Unlock()
	until := time.Now().Add(delay)
	if until.After(gate.next) {
		gate.next = until
	}
}

func (observer *Observer) applyRetryAfter(response httpEvidence, observedAt time.Time) uint64 {
	delay := retryafter.Parse(response.retryAfter, observedAt, MaxRetryAfter)
	return uint64(delay / time.Second)
}

type pacedTransport struct {
	base     http.RoundTripper
	observer *Observer
}

func (transport *pacedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport == nil || transport.base == nil || transport.observer == nil || request == nil || request.URL == nil {
		return nil, errors.New("pack evidence observer: invalid paced transport")
	}
	if transport.observer.coordinator != nil {
		return transport.coordinatedRoundTrip(request)
	}
	gate := transport.observer.hostGate(strings.ToLower(request.URL.Hostname()))
	gate.mu.Lock()
	if wait := time.Until(gate.next); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-request.Context().Done():
			timer.Stop()
			gate.mu.Unlock()
			return nil, request.Context().Err()
		case <-timer.C:
		}
	}
	gate.next = time.Now().Add(transport.observer.interval)
	networkRequest := request
	cancel := func() {}
	if transport.observer.requestTimeout > 0 {
		var networkContext context.Context
		networkContext, cancel = context.WithTimeout(request.Context(), transport.observer.requestTimeout)
		networkRequest = request.Clone(networkContext)
	}
	response, err := transport.base.RoundTrip(networkRequest)
	if err != nil {
		cancel()
		gate.mu.Unlock()
		return nil, err
	}
	if response.StatusCode == http.StatusTooManyRequests {
		delay := retryafter.Parse(response.Header.Get("Retry-After"), transport.observer.now().UTC(), MaxRetryAfter)
		until := time.Now().Add(delay)
		if delay > 0 && until.After(gate.next) {
			gate.next = until
		}
	}
	response.Body = &releasingBody{ReadCloser: response.Body, release: func() error {
		cancel()
		gate.mu.Unlock()
		return nil
	}}
	return response, nil
}

func (transport *pacedTransport) coordinatedRoundTrip(request *http.Request) (*http.Response, error) {
	lease, err := transport.observer.coordinator.Acquire(request.Context(), request.URL.Hostname())
	if err != nil {
		return nil, coordinationError("acquire host lease", err)
	}
	networkRequest := request
	cancel := func() {}
	if transport.observer.requestTimeout > 0 {
		var networkContext context.Context
		networkContext, cancel = context.WithTimeout(request.Context(), transport.observer.requestTimeout)
		networkRequest = request.Clone(networkContext)
	}
	response, networkErr := transport.base.RoundTrip(networkRequest)
	if networkErr != nil {
		cancel()
		releaseErr := transport.observer.coordinator.Release(lease)
		if releaseErr != nil {
			return nil, coordinationError("release failed network attempt", releaseErr)
		}
		return nil, networkErr
	}
	if _, recordErr := transport.observer.coordinator.RecordResponse(lease, response.StatusCode, response.Header.Get("Retry-After")); recordErr != nil {
		bodyErr := response.Body.Close()
		cancel()
		releaseErr := transport.observer.coordinator.Release(lease)
		return nil, errors.Join(coordinationError("record response headers", recordErr), bodyErr, releaseErr)
	}
	response.Body = &releasingBody{ReadCloser: response.Body, release: func() error {
		cancel()
		if err := transport.observer.coordinator.Release(lease); err != nil {
			return coordinationError("release host lease", err)
		}
		return nil
	}}
	return response, nil
}

func coordinationError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrCoordination, operation, err)
}

type releasingBody struct {
	io.ReadCloser
	once      sync.Once
	release   func() error
	finishErr error
}

func (body *releasingBody) finish() error {
	body.once.Do(func() { body.finishErr = body.release() })
	return body.finishErr
}

func (body *releasingBody) Read(buffer []byte) (int, error) {
	count, err := body.ReadCloser.Read(buffer)
	if err != nil {
		if finishErr := body.finish(); finishErr != nil {
			err = errors.Join(err, finishErr)
		}
	}
	return count, err
}

func (body *releasingBody) Close() error {
	err := body.ReadCloser.Close()
	if finishErr := body.finish(); finishErr != nil {
		return errors.Join(err, finishErr)
	}
	return err
}

func (observer *Observer) negative(row uint64, normalized ccindex.Candidate, rights indexpackselection.RightsDecision, reason string) Result {
	return Result{
		Candidate:   candidateWithAdmission(normalized, indexpackselection.Admission{Version: indexpackselection.AdmissionVersion, Rights: rights}),
		Observation: Observation{Version: ObservationVersion, Row: row, URL: normalized.URL, Outcome: "rejected", Reason: reason},
	}
}

func candidateWithAdmission(candidate ccindex.Candidate, admission indexpackselection.Admission) indexpackselection.Candidate {
	return indexpackselection.Candidate{
		URLKey: candidate.URLKey, Timestamp: candidate.Timestamp, URL: candidate.URL, MIME: candidate.MIME,
		MIMEDetected: candidate.MIMEDetected, Status: candidate.Status, Digest: candidate.Digest,
		Length: candidate.Length, Offset: candidate.Offset, Filename: candidate.Filename,
		Languages: candidate.Languages, Encoding: candidate.Encoding, Admission: admission,
	}
}

func cloneRobotsResult(result robotsResult) robotsResult {
	result.body = append([]byte(nil), result.body...)
	return result
}

func classifyHTTPError(err error) string {
	var egressError *egress.Error
	if errors.As(err, &egressError) {
		return "egress_" + egressError.Reason
	}
	var urlError *url.Error
	if errors.As(err, &urlError) && urlError.Timeout() {
		return "timeout"
	}
	return "network_error"
}

func classifyReadError(err error) string {
	if err == nil {
		return "incomplete_body"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "incomplete_body"
	}
	if strings.Contains(err.Error(), "limit") {
		return "body_too_large"
	}
	return "body_read_error"
}

func classifyEvaluationError(err error) string {
	if err == nil {
		return ""
	}
	return "invalid_response_evidence"
}
