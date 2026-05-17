package pipeline

import (
	"net/url"
	"strings"

	"github.com/staticvar/fetchmark/internal/core/model"
)

type domainFilter struct {
	host string
	path string
}

func filterResultsByDomains(results []model.SearchResult, includeDomains, excludeDomains []string) []model.SearchResult {
	includes := parseDomainFilters(includeDomains)
	excludes := parseDomainFilters(excludeDomains)
	if len(includes) == 0 && len(excludes) == 0 {
		return results
	}
	out := results[:0]
	for _, r := range results {
		if len(includes) > 0 && !matchesAnyDomainFilter(r.URL, includes) {
			continue
		}
		if matchesAnyDomainFilter(r.URL, excludes) {
			continue
		}
		out = append(out, r)
	}
	return out
}

func parseDomainFilters(raw []string) []domainFilter {
	out := make([]domainFilter, 0, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(strings.ToLower(item))
		if item == "" {
			continue
		}
		if !strings.Contains(item, "://") {
			item = "https://" + item
		}
		u, err := url.Parse(item)
		if err != nil {
			continue
		}
		host := strings.TrimPrefix(u.Hostname(), "*.")
		host = strings.TrimPrefix(host, "www.")
		if host == "" {
			continue
		}
		path := strings.TrimRight(u.EscapedPath(), "/")
		out = append(out, domainFilter{host: host, path: path})
	}
	return out
}

func matchesAnyDomainFilter(rawURL string, filters []domainFilter) bool {
	if len(filters) == 0 {
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	path := strings.TrimRight(u.EscapedPath(), "/")
	for _, f := range filters {
		if host != f.host && !strings.HasSuffix(host, "."+f.host) {
			continue
		}
		if f.path != "" && !strings.HasPrefix(path, f.path) {
			continue
		}
		return true
	}
	return false
}
