// Package retryafter parses bounded HTTP Retry-After values without allowing
// untrusted delta-seconds to overflow time.Duration.
package retryafter

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Parse accepts RFC delay-seconds or an HTTP date and clamps the result to
// maximum. Invalid, non-positive, and already elapsed values return zero.
func Parse(value string, now time.Time, maximum time.Duration) time.Duration {
	if maximum <= 0 {
		return 0
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if decimalDigits(value) {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return maximum
		}
		maximumSeconds := uint64(maximum / time.Second)
		if seconds > maximumSeconds {
			return maximum
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	delay := when.Sub(now)
	if delay > maximum {
		return maximum
	}
	return delay
}

func decimalDigits(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return value != ""
}
