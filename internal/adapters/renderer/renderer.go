// Package renderer exposes a small contract for turning a URL into
// post-JavaScript HTML by delegating to a headless browser service.
// The implementation intentionally stays thin: Fetchmark's own egress
// policy has already validated the URL before we get here, so the
// renderer's job is just to talk to the external service, enforce
// budgets, and hand back bytes the extractor can chew on.
package renderer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Renderer turns a URL into HTML after JavaScript has executed.
// Implementations must be safe for concurrent use.
type Renderer interface {
	// Render returns the rendered HTML for url. ctx governs the whole
	// call including network IO; callers pass a parent context so the
	// request budget is honoured.
	Render(ctx context.Context, url string) ([]byte, error)
}

// Options configures an HTTPRenderer. Zero values receive sane
// defaults so the common case is `renderer.NewHTTP(Options{Endpoint: ...})`.
type Options struct {
	// Endpoint is the full URL that accepts {"url": "..."} POST bodies
	// and responds with rendered HTML (or JSON with an "html" field —
	// both shapes are handled). Required.
	Endpoint string
	// Timeout caps the total render request. Default: 20s.
	Timeout time.Duration
	// MaxBody caps the accepted response size in bytes. Default: 10MiB.
	MaxBody int64
	// Client optionally overrides the HTTP client (for tests or shared
	// transports). When nil a dedicated client with the above timeout
	// is created.
	Client *http.Client
	// Token is an optional shared secret sent as
	// Authorization: Bearer <token>.
	Token string
}

// HTTPRenderer is the default Renderer implementation. It POSTs to a
// headless service (e.g. browserless/chromium, our own playwright box,
// etc.) and returns the response body as rendered HTML.
type HTTPRenderer struct {
	opts   Options
	client *http.Client
}

// NewHTTP constructs an HTTPRenderer. An empty Endpoint is rejected so
// callers cannot accidentally wire a no-op renderer into the pipeline.
func NewHTTP(opts Options) (*HTTPRenderer, error) {
	if opts.Endpoint == "" {
		return nil, errors.New("renderer: endpoint is required")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 20 * time.Second
	}
	if opts.MaxBody <= 0 {
		opts.MaxBody = 10 * 1024 * 1024
	}
	c := opts.Client
	if c == nil {
		c = &http.Client{Timeout: opts.Timeout}
	}
	return &HTTPRenderer{opts: opts, client: c}, nil
}

type renderRequest struct {
	URL string `json:"url"`
}

// Render posts {"url": target} and returns the response body. If the
// service replies with JSON carrying an "html" field we unwrap it;
// otherwise the raw body is treated as HTML. Anything beyond MaxBody
// is refused so a pathological renderer can't blow our memory budget.
func (h *HTTPRenderer) Render(ctx context.Context, url string) ([]byte, error) {
	if url == "" {
		return nil, errors.New("renderer: empty url")
	}
	body, err := json.Marshal(renderRequest{URL: url})
	if err != nil {
		return nil, err
	}
	reqCtx, cancel := context.WithTimeout(ctx, h.opts.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, h.opts.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/html, application/json")
	if h.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+h.opts.Token)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("renderer: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Drain a bit for log context but don't propagate large bodies.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("renderer: status %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}

	lim := io.LimitReader(resp.Body, h.opts.MaxBody+1)
	raw, err := io.ReadAll(lim)
	if err != nil {
		return nil, fmt.Errorf("renderer: read: %w", err)
	}
	if int64(len(raw)) > h.opts.MaxBody {
		return nil, fmt.Errorf("renderer: response exceeds max body %d", h.opts.MaxBody)
	}

	// Unwrap common JSON shape {"html": "..."} if present. We gate this
	// on Content-Type so a text/html page starting with "{" isn't
	// mis-parsed as JSON.
	ct := resp.Header.Get("Content-Type")
	if len(raw) > 0 && (containsMIME(ct, "application/json") || containsMIME(ct, "text/json")) {
		var wrap struct {
			HTML string `json:"html"`
			Body string `json:"body"`
		}
		if err := json.Unmarshal(raw, &wrap); err == nil {
			if wrap.HTML != "" {
				return []byte(wrap.HTML), nil
			}
			if wrap.Body != "" {
				return []byte(wrap.Body), nil
			}
		}
	}
	return raw, nil
}

// containsMIME does a cheap, case-insensitive check that strips the
// charset/params suffix commonly found in Content-Type headers.
func containsMIME(ct, want string) bool {
	if ct == "" {
		return false
	}
	for i := 0; i < len(ct); i++ {
		if ct[i] == ';' {
			ct = ct[:i]
			break
		}
	}
	return equalFold(trimSpace(ct), want)
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
