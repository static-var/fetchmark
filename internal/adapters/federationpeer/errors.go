package federationpeer

import (
	"errors"
	"fmt"
	"time"
)

// Stable error classes let orchestration code react without parsing error
// strings or retaining untrusted response data.
var (
	ErrInvalidConfiguration = errors.New("federation peer: invalid configuration")
	ErrInvalidQuery         = errors.New("federation peer: invalid query")
	ErrUnsupportedControls  = errors.New("federation peer: unsupported controls")
	ErrCanceled             = errors.New("federation peer: canceled")
	ErrTimeout              = errors.New("federation peer: timeout")
	ErrEgressPolicy         = errors.New("federation peer: egress policy rejected destination")
	ErrTransport            = errors.New("federation peer: transport failure")
	ErrRedirect             = errors.New("federation peer: redirect rejected")
	ErrHTTPStatus           = errors.New("federation peer: unexpected HTTP status")
	ErrCompressedResponse   = errors.New("federation peer: compressed response rejected")
	ErrResponseTooLarge     = errors.New("federation peer: response too large")
	ErrMalformedResponse    = errors.New("federation peer: malformed response")
	ErrVerification         = errors.New("federation peer: response verification failed")
)

// Error contains only bounded local classification. It deliberately does not
// wrap transport or protocol errors, which may contain queries, URLs, signed
// envelopes, or response fragments.
type Error struct {
	Kind       error
	StatusCode int
	Retryable  bool
	RetryAfter time.Duration
}

func (err *Error) Error() string {
	if err == nil || err.Kind == nil {
		return "federation peer: failure"
	}
	if err.StatusCode != 0 {
		return fmt.Sprintf("%s (%d)", err.Kind.Error(), err.StatusCode)
	}
	return err.Kind.Error()
}

func (err *Error) Is(target error) bool {
	return err != nil && err.Kind == target
}
