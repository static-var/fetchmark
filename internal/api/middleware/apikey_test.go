package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIKey(t *testing.T) {
	var seen Principal
	h := APIKey([]string{"userkey"}, []string{"adminkey"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = PrincipalFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	cases := []struct {
		name       string
		header     [2]string
		wantStatus int
		wantAdmin  bool
	}{
		{"no key", [2]string{"", ""}, http.StatusUnauthorized, false},
		{"bad key", [2]string{"X-API-Key", "nope"}, http.StatusUnauthorized, false},
		{"user key via header", [2]string{"X-API-Key", "userkey"}, http.StatusOK, false},
		{"admin via bearer", [2]string{"Authorization", "Bearer adminkey"}, http.StatusOK, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seen = Principal{}
			r := httptest.NewRequest("GET", "/", nil)
			if tc.header[0] != "" {
				r.Header.Set(tc.header[0], tc.header[1])
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d want %d", w.Code, tc.wantStatus)
			}
			if tc.wantStatus == http.StatusOK && seen.Admin != tc.wantAdmin {
				t.Fatalf("admin = %v want %v", seen.Admin, tc.wantAdmin)
			}
		})
	}
}
