package packevidencegate

import "time"

const protocolVersion = 1

type binding struct {
	Version          int    `json:"version"`
	RunID            string `json:"run_id"`
	ConfigSHA256     string `json:"config_sha256"`
	ProtocolRevision string `json:"protocol_revision"`
}

type acquireRequest struct {
	Binding   binding `json:"binding"`
	AttemptID string  `json:"attempt_id"`
	Host      string  `json:"host"`
}

type acquireResponse struct {
	Granted        bool  `json:"granted"`
	Lease          Lease `json:"lease,omitempty"`
	RetryAfterNano int64 `json:"retry_after_nanos,omitempty"`
}

type responseRequest struct {
	Binding    binding `json:"binding"`
	Lease      Lease   `json:"lease"`
	Status     int     `json:"status"`
	RetryAfter string  `json:"retry_after,omitempty"`
}

type responseResponse struct {
	AppliedNano int64 `json:"applied_retry_after_nanos"`
}

type releaseRequest struct {
	Binding binding `json:"binding"`
	Lease   Lease   `json:"lease"`
}

type emptyResponse struct {
	Released bool `json:"released"`
}

type errorResponse struct {
	Code string `json:"code"`
}

func protocolBinding(policy Policy) binding {
	return binding{Version: protocolVersion, RunID: policy.RunID, ConfigSHA256: policy.ConfigSHA256, ProtocolRevision: "unix-http-json-v1"}
}

func validBinding(got, expected binding) bool { return got == expected }

func retryDelay(retryAt, now time.Time) time.Duration {
	if !retryAt.After(now) {
		return time.Millisecond
	}
	return retryAt.Sub(now)
}
