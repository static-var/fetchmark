package middleware

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

type authCtxKey int

const (
	authKey authCtxKey = iota
)

// Principal identifies the authenticated caller.
type Principal struct {
	Key   string
	Admin bool
}

// APIKey enforces that every request carries a known API key, either via
// the Authorization: Bearer <key> header or the X-API-Key header. Admin
// keys are a strict superset and unlock gated request fields.
func APIKey(keys, adminKeys []string) func(http.Handler) http.Handler {
	kSet := toSet(keys)
	aSet := toSet(adminKeys)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := extractKey(r)
			if provided == "" {
				writeErr(w, http.StatusUnauthorized, "missing_api_key")
				return
			}
			admin := constantTimeHas(aSet, provided)
			if !(admin || constantTimeHas(kSet, provided)) {
				writeErr(w, http.StatusUnauthorized, "invalid_api_key")
				return
			}
			ctx := context.WithValue(r.Context(), authKey, Principal{Key: provided, Admin: admin})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// PrincipalFrom returns the authenticated principal, or a zero value if
// the handler was reached without passing through the APIKey middleware.
func PrincipalFrom(ctx context.Context) Principal {
	if p, ok := ctx.Value(authKey).(Principal); ok {
		return p
	}
	return Principal{}
}

func extractKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if rest, ok := strings.CutPrefix(h, "Bearer "); ok {
			return strings.TrimSpace(rest)
		}
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

func toSet(keys []string) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k != "" {
			out[k] = struct{}{}
		}
	}
	return out
}

// constantTimeHas compares the candidate against every entry in set using
// subtle.ConstantTimeCompare. We iterate even on match so timing does not
// leak the matched key's length or position.
func constantTimeHas(set map[string]struct{}, candidate string) bool {
	match := 0
	cb := []byte(candidate)
	for k := range set {
		kb := []byte(k)
		if len(kb) != len(cb) {
			continue
		}
		match |= subtle.ConstantTimeCompare(kb, cb)
	}
	return match == 1
}

func writeErr(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}
