package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/searxng"
)

func TestSearch_NonRetryableUpstreamStatusIsClientError(t *testing.T) {
	r, p := newTestRouter(nil)
	p.err = &searxng.StatusError{Code: http.StatusBadRequest}

	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader(`{"query":"go","engines":["missing"]}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "search_bad_request") {
		t.Fatalf("body missing client error: %s", rec.Body.String())
	}
}
