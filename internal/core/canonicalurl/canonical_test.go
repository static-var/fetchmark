package canonicalurl

import "testing"

func TestV1FrozenVectors(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"https://Example.com:443/a/b?utm_source=x&z=1&a=2#frag", "https://example.com/a/b?a=2&z=1"},
		{"http://Example.COM:80/?b=2&a=1&fbclid=xyz", "http://example.com/?a=1&b=2"},
		{"https://host/path?k=v", "https://host/path?k=v"},
		{"https://[2001:DB8::1]/page", "https://[2001:db8::1]/page"},
		{"https://[2001:DB8::1]:443/page", "https://[2001:db8::1]/page"},
		{"https://[2001:DB8::1]:8443/page", "https://[2001:db8::1]:8443/page"},
		{"HTTPS://EXAMPLE.COM/path?b=two+words&a=2&a=1&UtM_MeDiUm=x", "https://example.com/path?a=1&a=2&b=two+words"},
		{"https://example.com/a/../b?empty=", "https://example.com/a/../b?empty="},
	}
	for _, test := range tests {
		got, err := V1(test.raw)
		if err != nil {
			t.Fatalf("V1(%q): %v", test.raw, err)
		}
		if got != test.want {
			t.Errorf("V1(%q) = %q, want %q", test.raw, got, test.want)
		}
	}
}

func TestV1RejectsMissingAuthority(t *testing.T) {
	for _, raw := range []string{"relative/path", "mailto:operator@example.com"} {
		if _, err := V1(raw); err == nil {
			t.Fatalf("V1(%q) succeeded", raw)
		}
	}
}
