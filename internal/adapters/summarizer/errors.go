package summarizer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	anthropic "github.com/anthropics/anthropic-sdk-go"
	"github.com/openai/openai-go"
)

// ErrProviderNotFound is returned when a request names an unregistered
// provider.
var ErrProviderNotFound = errors.New("provider not found")

// ClassifiedError wraps upstream errors with a coarse category the
// HTTP handler uses to pick a status code.
type ClassifiedError struct {
	Kind     string // "auth", "rate_limit", "upstream", "network", "timeout"
	Provider string
	Err      error
}

// Error implements error.
func (e *ClassifiedError) Error() string {
	return fmt.Sprintf("%s: %s: %v", e.Provider, e.Kind, e.Err)
}

// Unwrap lets errors.Is / errors.As chain through.
func (e *ClassifiedError) Unwrap() error { return e.Err }

func classifyErr(providerTag string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &ClassifiedError{Kind: "timeout", Provider: providerTag, Err: err}
	}
	if errors.Is(err, context.Canceled) {
		return &ClassifiedError{Kind: "cancelled", Provider: providerTag, Err: err}
	}
	var code int
	var oaiErr *openai.Error
	var antErr *anthropic.Error
	switch {
	case errors.As(err, &oaiErr):
		code = oaiErr.StatusCode
	case errors.As(err, &antErr):
		code = antErr.StatusCode
	}
	switch {
	case code == http.StatusUnauthorized, code == http.StatusForbidden:
		return &ClassifiedError{Kind: "auth", Provider: providerTag, Err: err}
	case code == http.StatusTooManyRequests:
		return &ClassifiedError{Kind: "rate_limit", Provider: providerTag, Err: err}
	case code >= 500:
		return &ClassifiedError{Kind: "upstream", Provider: providerTag, Err: err}
	case code >= 400:
		return &ClassifiedError{Kind: "bad_request", Provider: providerTag, Err: err}
	}
	return &ClassifiedError{Kind: "network", Provider: providerTag, Err: err}
}

// DefaultTimeout returns a reasonable per-request deadline for a
// summarize call. Handler wraps its context.Deadline around this when
// none is already set.
func DefaultTimeout() time.Duration { return 60 * time.Second }
