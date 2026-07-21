package tufchannel

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/openpackbuilder"
	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/tufrepository"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

func TestStagedTargetRetrievalIndependentlyVerifiesAndInstalls(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	created := now.Add(-2 * time.Minute)
	root := t.TempDir()

	publisherRaw, publicIdentity, err := openpackbuilder.GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{61}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := openpackbuilder.DecodeIdentity(publisherRaw)
	if err != nil {
		t.Fatal(err)
	}
	candidates := integrationCandidateStream(t, created, "https://docs.example.org/installable")
	bundle := filepath.Join(root, "publisher-bundle")
	buildResult, err := openpackbuilder.Build(ctx, openpackbuilder.Options{
		SpecRaw:       integrationBuilderSpec(t, created, candidates),
		ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), Candidates: bytes.NewReader(candidates),
		Identity: publisher, BuilderVersion: "integration-test", OutputDir: bundle,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	verifiedPublisherBundle, err := openpackbuilder.VerifyBundle(ctx, bundle, publicIdentity, publisher.PublicKey())
	if err != nil {
		t.Fatalf("VerifyBundle publisher output: %v", err)
	}
	if verifiedPublisherBundle.ManifestSHA256 != buildResult.ManifestSHA256 {
		t.Fatalf("publisher verification = %#v, build = %#v", verifiedPublisherBundle, buildResult)
	}

	channelRandom := make([]byte, 256)
	for index := range channelRandom {
		channelRandom[index] = byte(index)
	}
	channelRaw, _, err := tufrepository.GenerateIdentity("fetchmark-community", now.Add(365*24*time.Hour), bytes.NewReader(channelRandom))
	if err != nil {
		t.Fatal(err)
	}
	channelIdentity, err := tufrepository.DecodeIdentity(channelRaw)
	if err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(root, "repository-v1")
	targetPath := "packs/developer/manifest.json"
	stageResult, err := tufrepository.Stage(ctx, tufrepository.StageOptions{
		Identity: channelIdentity, BundleDir: bundle, OutputDir: repository, TargetPath: targetPath, Version: 1,
		Now: now, TimestampExpires: now.Add(24 * time.Hour), SnapshotExpires: now.Add(48 * time.Hour),
		TargetsExpires:         now.Add(10 * 24 * time.Hour),
		PublisherKeys:          map[string]ed25519.PublicKey{publicIdentity.KeyID: publisher.PublicKey()},
		ExpectedManifestSHA256: verifiedPublisherBundle.ManifestSHA256,
	})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if stageResult.ManifestSHA256 != verifiedPublisherBundle.ManifestSHA256 {
		t.Fatalf("stage = %#v", stageResult)
	}

	stateDir := filepath.Join(root, "consumer-state")
	installRoot := filepath.Join(root, "installed")
	for _, directory := range []string{stateDir, installRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	retrievedBundle := filepath.Join(root, "retrieved-bundle")
	const targetBaseURL = "https://mirror.example.invalid/generation-1/targets/"
	retrieved, err := retrieveWithFetcher(ctx, RetrieveOptions{
		TrustedRootPath: filepath.Join(repository, "root.json"), MetadataDir: filepath.Join(repository, "metadata"),
		StateDir: stateDir, TargetPath: targetPath, TargetBaseURL: targetBaseURL, OutputDir: retrievedBundle,
	}, localRepositoryFetcher(t, repository, targetBaseURL))
	if err != nil {
		t.Fatalf("retrieveWithFetcher: %v", err)
	}
	if retrieved.Status != StatusRetrieved || retrieved.Path == "" || retrieved.ManifestSHA256 != verifiedPublisherBundle.ManifestSHA256 {
		t.Fatalf("retrieval = %#v", retrieved)
	}

	manifestRaw, err := os.ReadFile(filepath.Join(retrieved.Path, openpackbuilder.ManifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := os.ReadFile(filepath.Join(retrieved.Path, openpackbuilder.SignatureFilename))
	if err != nil {
		t.Fatal(err)
	}
	verifiedManifest, err := indexpack.VerifyManifest(
		manifestRaw, signatureRaw, map[string]ed25519.PublicKey{publicIdentity.KeyID: publisher.PublicKey()},
	)
	if err != nil {
		t.Fatalf("VerifyManifest retrieved output: %v", err)
	}
	if verifiedManifest.Digest != verifiedPublisherBundle.ManifestSHA256 {
		t.Fatalf("retrieved manifest digest = %s, want %s", verifiedManifest.Digest, verifiedPublisherBundle.ManifestSHA256)
	}
	manifest := verifiedManifest.Manifest
	installed, err := openpackindex.Install(ctx, openpackindex.InstallOptions{
		BundleDir: retrieved.Path, Root: installRoot,
		TrustedKeys: map[string]ed25519.PublicKey{publicIdentity.KeyID: publisher.PublicKey()},
		Acceptance: indexpack.Acceptance{
			Now: now, ExpectedPackID: manifest.PackID, ExpectedManifestSHA256: retrieved.ManifestSHA256,
			ExpectedKeyID: manifest.SigningKeyID, MinimumRevision: manifest.Revision, ExpectedRevision: manifest.Revision,
			ExpectedRecordCount: manifest.RecordCount, ExpectedCreatedAt: manifest.CreatedAt, ExpectedExpiresAt: manifest.ExpiresAt,
		},
	})
	if err != nil {
		t.Fatalf("Install retrieved bundle: %v", err)
	}
	if installed.ManifestSHA256 != retrieved.ManifestSHA256 || installed.RecordCount != manifest.RecordCount || installed.Path == "" {
		t.Fatalf("installed = %#v", installed)
	}
}

func localRepositoryFetcher(t *testing.T, repository, rawBaseURL string) artifactFetcher {
	t.Helper()
	baseURL, err := url.Parse(rawBaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return func(ctx context.Context, rawURL string, maximum int64, output io.Writer) (int64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Scheme != baseURL.Scheme || parsed.Host != baseURL.Host || !strings.HasPrefix(parsed.Path, baseURL.Path) {
			return 0, fmt.Errorf("unexpected repository URL %q", rawURL)
		}
		relative := strings.TrimPrefix(parsed.Path, baseURL.Path)
		if relative == "" || filepath.ToSlash(filepath.Clean(filepath.FromSlash(relative))) != relative {
			return 0, fmt.Errorf("unsafe repository path %q", relative)
		}
		file, err := os.Open(filepath.Join(repository, "targets", filepath.FromSlash(relative)))
		if err != nil {
			return 0, err
		}
		defer file.Close()
		return io.Copy(output, io.LimitReader(file, maximum+1))
	}
}

func integrationCandidateStream(t *testing.T, created time.Time, rawURL string) []byte {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	checked := created.Add(-time.Hour)
	candidate := indexpackselection.Candidate{
		URLKey: "org,example)/", Timestamp: "20260701000000", URL: rawURL,
		MIME: "text/html", MIMEDetected: "text/html", Status: "200", Digest: strings.Repeat("A", 32),
		Length: "12000", Offset: "1000", Filename: "crawl-data/CC-MAIN-2026-26/segments/fixture/warc/fixture.warc.gz",
		Languages: "eng", Encoding: "UTF-8",
		Admission: indexpackselection.Admission{
			Version: 1,
			Robots: indexpackselection.RobotsObservation{
				UserAgent: "Fetchmark-PackBuilder/1", RobotsURI: parsed.Scheme + "://" + parsed.Host + "/robots.txt",
				CheckedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(24 * time.Hour).Format(time.RFC3339),
				Outcome: "allowed", BodySHA256: strings.Repeat("1", 64),
			},
			Indexing: indexpackselection.IndexingObservation{
				CheckedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(24 * time.Hour).Format(time.RFC3339),
				FinalURL: rawURL, Outcome: "indexable", HeadersSHA256: strings.Repeat("2", 64),
				RepresentationSHA256: strings.Repeat("3", 64), ParserVersion: "fetchmark-noindex-v1",
			},
			Rights: indexpackselection.RightsDecision{
				Outcome: "permitted", AllowedFields: []string{"url_metadata"}, Basis: "url_metadata_policy",
				EvidenceURI: "https://commoncrawl.org/terms-of-use", EvidenceSHA256: strings.Repeat("4", 64),
				ObservedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(24 * time.Hour).Format(time.RFC3339),
				RightsNotice: "URL metadata only; third-party rights remain applicable",
			},
		},
	}
	var output bytes.Buffer
	if err := json.NewEncoder(&output).Encode(candidate); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func integrationBuilderSpec(t *testing.T, created time.Time, input []byte) []byte {
	t.Helper()
	digest := sha256.Sum256(input)
	spec := indexpackselection.Spec{
		Version: indexpackselection.SpecVersion, Profile: indexpackselection.ProfileLightweight,
		PackID: "commoncrawl-url-eng", Revision: 1, CreatedAt: created.Format(time.RFC3339),
		ExpiresAt: created.Add(30 * 24 * time.Hour).Format(time.RFC3339),
		Publisher: indexpack.Publisher{
			Name: "Fetchmark test publisher", ContactURI: "mailto:publisher@example.org",
			TakedownURI: "https://publisher.example.org/takedown", RightsNotice: "URL metadata only; no page bodies",
		},
		Languages: []string{"eng"}, MaxRecords: 10_000, MaxRecordsPerHost: 10,
		MaxPermissionAgeHours: 24, RobotsUserAgent: "Fetchmark-PackBuilder/1", CandidateSHA256: hex.EncodeToString(digest[:]),
		Inputs: []indexpack.BuildInput{{
			Name: "CC-MAIN-2026-26 URL Index export", URI: "https://index.commoncrawl.org/CC-MAIN-2026-26-index",
			RetrievedAt: created.Add(-time.Hour).Format(time.RFC3339), SHA256: strings.Repeat("8", 64),
			RightsNotice: "Common Crawl terms; URL metadata only",
		}},
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
