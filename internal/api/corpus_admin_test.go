package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
)

type recordingCurator struct {
	admissions         []string
	focused            []pipeline.FocusedAdmission
	takedowns          []string
	safety             localcorpus.SafetyClassification
	authoritativeEmpty bool
}

func (curator *recordingCurator) AdmitCuratedFocused(_ context.Context, admission pipeline.FocusedAdmission) []pipeline.CuratedMutationResult {
	curator.focused = []pipeline.FocusedAdmission{admission}
	result := pipeline.CuratedMutationResult{URL: admission.URL, Status: pipeline.CuratedStatusAdmitted}
	if admission.MaxOutboundLinks > 0 {
		links := make([]string, 0)
		if !curator.authoritativeEmpty {
			links = append(links, "https://reference.example/one", "https://reference.example/two")
		}
		if len(links) > admission.MaxOutboundLinks {
			links = links[:admission.MaxOutboundLinks]
		}
		result.OutboundLinks = &links
	}
	return []pipeline.CuratedMutationResult{result}
}

func (curator *recordingCurator) AdmitCurated(_ context.Context, urls []string) []pipeline.CuratedMutationResult {
	return curator.AdmitCuratedClassified(context.Background(), urls, localcorpus.SafetyUnclassified)
}

func (curator *recordingCurator) AdmitCuratedClassified(_ context.Context, urls []string, classification localcorpus.SafetyClassification) []pipeline.CuratedMutationResult {
	curator.admissions = append([]string(nil), urls...)
	curator.safety = classification
	return []pipeline.CuratedMutationResult{{URL: urls[0], Status: pipeline.CuratedStatusAdmitted}}
}

func (curator *recordingCurator) TakedownCurated(_ context.Context, urls []string) []pipeline.CuratedMutationResult {
	curator.takedowns = append([]string(nil), urls...)
	return []pipeline.CuratedMutationResult{{URL: urls[0], Status: pipeline.CuratedStatusTakenDown}}
}

func curatedAdminRouter(mode string, adminKeys []string, curator CorpusCurator) http.Handler {
	return NewRouter(Deps{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config: config.Config{
			APIKeys: []string{"user-key"}, AdminAPIKeys: adminKeys, LocalCorpusMode: mode,
			ResultsCap: 2, MaxRequestOutputBytes: 1 << 20,
		},
		Pipeline: &fakePipeline{}, CorpusCurator: curator,
	})
}

func TestAdminCorpusRoutesRequireConfiguredAdminAndCuratedMode(t *testing.T) {
	t.Run("route absent without admin configuration", func(t *testing.T) {
		router := curatedAdminRouter("curated", nil, &recordingCurator{})
		request := httptest.NewRequest(http.MethodPost, "/admin/corpus/admissions", strings.NewReader(`{"urls":["https://example.com"]}`))
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("user key rejected", func(t *testing.T) {
		router := curatedAdminRouter("curated", []string{"admin-key"}, &recordingCurator{})
		request := httptest.NewRequest(http.MethodPost, "/admin/corpus/admissions", strings.NewReader(`{"urls":["https://example.com"]}`))
		request.Header.Set("X-API-Key", "user-key")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("wrong mode conflicts", func(t *testing.T) {
		curator := &recordingCurator{}
		router := curatedAdminRouter("personal", []string{"admin-key"}, curator)
		request := httptest.NewRequest(http.MethodPost, "/admin/corpus/admissions", strings.NewReader(`{"urls":["https://example.com"]}`))
		request.Header.Set("X-API-Key", "admin-key")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusConflict || len(curator.admissions) != 0 {
			t.Fatalf("status=%d admissions=%v body=%s", response.Code, curator.admissions, response.Body.String())
		}
	})
	t.Run("archive accepts takedowns but not admissions", func(t *testing.T) {
		curator := &recordingCurator{}
		router := curatedAdminRouter("archive", []string{"admin-key"}, curator)
		request := httptest.NewRequest(http.MethodPost, "/admin/corpus/takedowns", strings.NewReader(`{"urls":["https://example.com"]}`))
		request.Header.Set("X-API-Key", "admin-key")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusOK || len(curator.takedowns) != 1 {
			t.Fatalf("takedown status=%d takedowns=%v body=%s", response.Code, curator.takedowns, response.Body.String())
		}

		request = httptest.NewRequest(http.MethodPost, "/admin/corpus/admissions", strings.NewReader(`{"urls":["https://example.com"]}`))
		request.Header.Set("X-API-Key", "admin-key")
		response = httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusConflict || len(curator.admissions) != 0 {
			t.Fatalf("admission status=%d admissions=%v body=%s", response.Code, curator.admissions, response.Body.String())
		}
	})
}

func TestAdminCorpusValidatesStrictCanonicalURLBatches(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty", body: `{"urls":[]}`},
		{name: "unknown field", body: `{"urls":["https://example.com"],"raw_html":"secret"}`},
		{name: "trailing JSON", body: `{"urls":["https://example.com"]}{}`},
		{name: "non HTTP", body: `{"urls":["file:///etc/passwd"]}`},
		{name: "credentials", body: `{"urls":["https://user:secret@example.com/private"]}`},
		{name: "canonical duplicate", body: `{"urls":["https://EXAMPLE.com:443/a#one","https://example.com/a#two"]}`},
		{name: "over cap", body: `{"urls":["https://one.example","https://two.example","https://three.example"]}`},
		{name: "invalid safety classification", body: `{"urls":["https://example.com"],"safety_classification":"probably-safe"}`},
		{name: "null safety classification", body: `{"urls":["https://example.com"],"safety_classification":null}`},
		{name: "non-string safety classification", body: `{"urls":["https://example.com"],"safety_classification":true}`},
		{name: "empty safety classification", body: `{"urls":["https://example.com"],"safety_classification":""}`},
		{name: "blank safety classification", body: `{"urls":["https://example.com"],"safety_classification":"  "}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := curatedAdminRouter("curated", []string{"admin-key"}, &recordingCurator{})
			request := httptest.NewRequest(http.MethodPost, "/admin/corpus/admissions", strings.NewReader(test.body))
			request.Header.Set("X-API-Key", "admin-key")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestAdminCorpusAdmissionAcceptsExplicitSafetyClassificationOnly(t *testing.T) {
	curator := &recordingCurator{}
	router := curatedAdminRouter("curated", []string{"admin-key"}, curator)
	request := httptest.NewRequest(http.MethodPost, "/admin/corpus/admissions", strings.NewReader(`{"urls":["https://example.com"],"safety_classification":"safe"}`))
	request.Header.Set("X-API-Key", "admin-key")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || curator.safety != localcorpus.SafetySafe {
		t.Fatalf("status=%d safety=%q body=%s", response.Code, curator.safety, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/admin/corpus/takedowns", strings.NewReader(`{"urls":["https://example.com"],"safety_classification":"safe"}`))
	request.Header.Set("X-API-Key", "admin-key")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "safety_classification_not_allowed") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAdminFocusedAdmissionUsesCrawlerOnlyBoundary(t *testing.T) {
	curator := &recordingCurator{}
	router := curatedAdminRouter("curated", []string{"admin-key"}, curator)
	request := httptest.NewRequest(http.MethodPost, "/admin/corpus/focused-admissions", strings.NewReader(`{"url":"https://EXAMPLE.com:443/docs/page#fragment","allowed_path_prefixes":["/docs/"],"denied_path_prefixes":["/docs/private/"],"max_outbound_links":2}`))
	request.Header.Set("X-API-Key", "admin-key")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(curator.focused) != 1 || curator.focused[0].URL != "https://example.com/docs/page" || curator.focused[0].MaxOutboundLinks != 2 || len(curator.admissions) != 0 {
		t.Fatalf("status=%d focused=%v admissions=%v body=%s", response.Code, curator.focused, curator.admissions, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"outbound_links":["https://reference.example/one","https://reference.example/two"]`) ||
		strings.Contains(response.Body.String(), `"content"`) || strings.Contains(response.Body.String(), `"snippet"`) {
		t.Fatalf("focused response metadata contract violated: %s", response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/admin/corpus/focused-admissions", strings.NewReader(`{"url":"https://example.com/docs/page","allowed_path_prefixes":["/docs/"],"max_outbound_links":0}`))
	request.Header.Set("X-API-Key", "admin-key")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "outbound_links") {
		t.Fatalf("zero-cap focused response = status %d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/admin/corpus/focused-admissions", strings.NewReader(`{"url":"https://example.com/docs/page","allowed_path_prefixes":["/docs/"],"safety_classification":"safe"}`))
	request.Header.Set("X-API-Key", "admin-key")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(curator.focused) != 1 {
		t.Fatalf("status=%d focused=%v body=%s", response.Code, curator.focused, response.Body.String())
	}
}

func TestAdminFocusedAdmissionSerializesAuthoritativeEmptySnapshot(t *testing.T) {
	curator := &recordingCurator{authoritativeEmpty: true}
	router := curatedAdminRouter("curated", []string{"admin-key"}, curator)
	request := httptest.NewRequest(http.MethodPost, "/admin/corpus/focused-admissions", strings.NewReader(`{"url":"https://example.com/docs/page","allowed_path_prefixes":["/docs/"],"max_outbound_links":4}`))
	request.Header.Set("X-API-Key", "admin-key")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"outbound_links":[]`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAdminFocusedAdmissionRejectsInvalidOrEscapingPolicies(t *testing.T) {
	for _, body := range []string{
		`{"url":"https://example.com/private/page","allowed_path_prefixes":["/docs/"]}`,
		`{"url":"https://example.com/docs/page","allowed_path_prefixes":[]}`,
		`{"url":"https://example.com/docs/page","allowed_path_prefixes":["/docs/%2fprivate/"]}`,
		`{"url":"https://example.com/docs/page","allowed_path_prefixes":["/docs/"],"denied_path_prefixes":["/docs/"]}`,
		`{"url":"https://example.com/docs/page","allowed_path_prefixes":["/docs/"],"denied_urls":["https://example.com/docs/page"]}`,
		`{"url":"https://example.com/docs/page","allowed_path_prefixes":["/docs/"],"max_outbound_links":-1}`,
		`{"url":"https://example.com/docs/page","allowed_path_prefixes":["/docs/"],"max_outbound_links":65}`,
	} {
		curator := &recordingCurator{}
		router := curatedAdminRouter("curated", []string{"admin-key"}, curator)
		request := httptest.NewRequest(http.MethodPost, "/admin/corpus/focused-admissions", strings.NewReader(body))
		request.Header.Set("X-API-Key", "admin-key")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || len(curator.focused) != 0 {
			t.Fatalf("body=%s status=%d focused=%v response=%s", body, response.Code, curator.focused, response.Body.String())
		}
	}
}

func TestAdminCorpusAdmissionAndTakedownReturnBodylessOutcomes(t *testing.T) {
	curator := &recordingCurator{}
	router := curatedAdminRouter("curated", []string{"admin-key"}, curator)
	for _, test := range []struct {
		path       string
		wantStatus string
	}{
		{path: "/admin/corpus/admissions", wantStatus: "admitted"},
		{path: "/admin/corpus/takedowns", wantStatus: "taken_down"},
	} {
		request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(`{"urls":["https://EXAMPLE.com:443/page#fragment"]}`))
		request.Header.Set("X-API-Key", "admin-key")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"`+test.wantStatus+`"`) {
			t.Fatalf("path=%s status=%d body=%s", test.path, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "content") || strings.Contains(response.Body.String(), "outbound_links") {
			t.Fatalf("mutation response leaked content: %s", response.Body.String())
		}
	}
	if len(curator.admissions) != 1 || curator.admissions[0] != "https://example.com/page" || len(curator.takedowns) != 1 || curator.takedowns[0] != "https://example.com/page" {
		t.Fatalf("admissions=%v takedowns=%v", curator.admissions, curator.takedowns)
	}
}
