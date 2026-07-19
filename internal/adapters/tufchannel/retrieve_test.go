package tufchannel

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackchannel"
)

const retrievalBaseURL = "https://packs.example.invalid/targets/"

func TestRetrieveDownloadsExactChannelBundleAndActivatesWithoutOverwrite(t *testing.T) {
	fixture := writeRetrievalFixture(t)
	fetcher, requested := fixture.fetcher(t)

	retrieved, err := retrieveWithFetcher(context.Background(), fixture.options, fetcher)
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	canonicalOutput, err := filepath.EvalSymlinks(fixture.options.OutputDir)
	if err != nil {
		t.Fatalf("canonical output: %v", err)
	}
	if retrieved.Status != StatusRetrieved || retrieved.Path != canonicalOutput ||
		retrieved.PackID != fixture.manifest.PackID || retrieved.Revision != fixture.manifest.Revision ||
		retrieved.ManifestSHA256 != indexpack.ManifestDigest(fixture.manifestRaw) ||
		retrieved.ShardCount != 1 || retrieved.FileCount != 3 || retrieved.DownloadBytes == 0 {
		t.Fatalf("retrieval = %#v", retrieved)
	}
	for relative, expected := range map[string][]byte{
		"manifest.json":                 fixture.manifestRaw,
		"manifest.ed25519":              fixture.signatureRaw,
		fixture.manifest.Shards[0].Path: fixture.shardRaw,
	} {
		actual, err := os.ReadFile(filepath.Join(fixture.options.OutputDir, filepath.FromSlash(relative)))
		if err != nil || !bytes.Equal(actual, expected) {
			t.Fatalf("retrieved %s = %x, %v", relative, actual, err)
		}
	}
	if len(*requested) != 3 {
		t.Fatalf("requests = %v", *requested)
	}
	installRoot := t.TempDir()
	if err := os.Chmod(installRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	installed, err := openpackindex.Install(context.Background(), openpackindex.InstallOptions{
		BundleDir: retrieved.Path, Root: installRoot,
		TrustedKeys: map[string]ed25519.PublicKey{fixture.manifest.SigningKeyID: fixture.publicKey},
		Acceptance: indexpack.Acceptance{
			Now: fixture.now, ExpectedPackID: fixture.manifest.PackID,
			ExpectedManifestSHA256: indexpack.ManifestDigest(fixture.manifestRaw),
			ExpectedKeyID:          fixture.manifest.SigningKeyID, MinimumRevision: fixture.manifest.Revision,
			ExpectedRevision: fixture.manifest.Revision, ExpectedRecordCount: fixture.manifest.RecordCount,
			ExpectedCreatedAt: fixture.manifest.CreatedAt, ExpectedExpiresAt: fixture.manifest.ExpiresAt,
		},
		MaxProjectionBytes: openpackindex.MaxProjectionBytes,
	})
	if err != nil || installed.RecordCount != fixture.manifest.RecordCount {
		t.Fatalf("install retrieved bundle = %#v, %v", installed, err)
	}

	if _, err := retrieveWithFetcher(context.Background(), fixture.options, fetcher); !errors.Is(err, ErrRetrievalExists) {
		t.Fatalf("second Retrieve = %v, want ErrRetrievalExists", err)
	}
}

func TestRetrieveRejectsShardTamperAndCleansStaging(t *testing.T) {
	fixture := writeRetrievalFixture(t)
	shardURL := retrievalBaseURL + "packs/developer/" + fixture.manifest.Shards[0].Path
	fixture.objects[shardURL] = bytes.Repeat([]byte("x"), len(fixture.shardRaw))
	fetcher, _ := fixture.fetcher(t)

	if _, err := retrieveWithFetcher(context.Background(), fixture.options, fetcher); err == nil || !strings.Contains(err.Error(), "shard SHA-256") {
		t.Fatalf("Retrieve tampered shard = %v", err)
	}
	if _, err := os.Lstat(fixture.options.OutputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tampered retrieval activated output: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(fixture.options.OutputDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fetchmark-pack-fetch-") {
			t.Fatalf("tampered retrieval left staging path %q", entry.Name())
		}
	}
}

func TestRetrieveRejectsOutputParentReplacementBeforeActivation(t *testing.T) {
	fixture := writeRetrievalFixture(t)
	baseFetcher, _ := fixture.fetcher(t)
	parent := filepath.Dir(fixture.options.OutputDir)
	moved := parent + "-moved"
	replaced := false
	fetcher := func(ctx context.Context, rawURL string, maximum int64, destination io.Writer) (int64, error) {
		if strings.Contains(rawURL, "/shards/") && !replaced {
			replaced = true
			if err := os.Rename(parent, moved); err != nil {
				return 0, err
			}
			if err := os.Mkdir(parent, 0o700); err != nil {
				return 0, err
			}
		}
		return baseFetcher(ctx, rawURL, maximum, destination)
	}
	if _, err := retrieveWithFetcher(context.Background(), fixture.options, fetcher); err == nil || !strings.Contains(err.Error(), "output parent path changed") {
		t.Fatalf("Retrieve replaced output parent = %v", err)
	}
	for _, candidate := range []string{fixture.options.OutputDir, filepath.Join(moved, filepath.Base(fixture.options.OutputDir))} {
		if _, err := os.Lstat(candidate); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("replaced parent retrieval activated %s: %v", candidate, err)
		}
	}
	entries, err := os.ReadDir(moved)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), retrievalStagingPrefix) {
			t.Fatalf("replaced parent left staging path %q", entry.Name())
		}
	}
}

func TestRetrieveCancellationImmediatelyBeforeActivationDoesNotCommit(t *testing.T) {
	fixture := writeRetrievalFixture(t)
	fetcher, _ := fixture.fetcher(t)
	ctx, cancel := context.WithCancel(context.Background())
	dependencies := defaultRetrievalDependencies(fetcher)
	dependencies.beforeActivate = cancel

	if _, err := retrieveWithDependencies(ctx, fixture.options, dependencies); !errors.Is(err, context.Canceled) {
		t.Fatalf("Retrieve cancelled before activation = %v, want context.Canceled", err)
	}
	assertNoRetrievedOutputOrStaging(t, fixture.options.OutputDir)
}

func TestRetrieveRejectsManifestThatExpiresDuringDownload(t *testing.T) {
	fixture := writeRetrievalFixture(t)
	baseFetcher, _ := fixture.fetcher(t)
	activationTime := fixture.now
	expiresAt, err := time.Parse(time.RFC3339, fixture.manifest.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	fetcher := func(ctx context.Context, rawURL string, maximum int64, destination io.Writer) (int64, error) {
		count, err := baseFetcher(ctx, rawURL, maximum, destination)
		if strings.Contains(rawURL, "/shards/") {
			activationTime = expiresAt
		}
		return count, err
	}
	dependencies := defaultRetrievalDependencies(fetcher)
	dependencies.now = func() time.Time { return activationTime }

	if _, err := retrieveWithDependencies(context.Background(), fixture.options, dependencies); err == nil || !strings.Contains(err.Error(), "manifest is expired") {
		t.Fatalf("Retrieve manifest expiring during download = %v", err)
	}
	assertNoRetrievedOutputOrStaging(t, fixture.options.OutputDir)
}

func TestRetrieveReportsCommittedPathAfterActivationDirectoryCloseFailure(t *testing.T) {
	fixture := writeRetrievalFixture(t)
	fetcher, _ := fixture.fetcher(t)
	dependencies := defaultRetrievalDependencies(fetcher)
	injected := errors.New("injected activation directory close failure")
	dependencies.closeActivationDirectory = func(file *os.File) error {
		return errors.Join(file.Close(), injected)
	}

	result, err := retrieveWithDependencies(context.Background(), fixture.options, dependencies)
	committedPath, pathErr := filepath.EvalSymlinks(fixture.options.OutputDir)
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	if !errors.Is(err, injected) || result.Status != StatusRetrieved || result.Path != committedPath ||
		!strings.Contains(err.Error(), "bundle committed at "+committedPath) {
		t.Fatalf("Retrieve activation close fault = %#v, %v", result, err)
	}
	if _, err := os.Stat(fixture.options.OutputDir); err != nil {
		t.Fatalf("committed output missing: %v", err)
	}
}

func TestRetrieveReportsCommittedPathAfterParentRootCloseFailure(t *testing.T) {
	fixture := writeRetrievalFixture(t)
	fetcher, _ := fixture.fetcher(t)
	dependencies := defaultRetrievalDependencies(fetcher)
	injected := errors.New("injected parent root close failure")
	dependencies.closeParentRoot = func(root *os.Root) error {
		return errors.Join(root.Close(), injected)
	}

	result, err := retrieveWithDependencies(context.Background(), fixture.options, dependencies)
	committedPath, pathErr := filepath.EvalSymlinks(fixture.options.OutputDir)
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	if !errors.Is(err, injected) || result.Status != StatusRetrieved || result.Path != committedPath ||
		!strings.Contains(err.Error(), "bundle committed at "+committedPath) {
		t.Fatalf("Retrieve parent root close fault = %#v, %v", result, err)
	}
	if _, err := os.Stat(fixture.options.OutputDir); err != nil {
		t.Fatalf("committed output missing: %v", err)
	}
}

func TestRetrieveEnforcesAggregateBudgetBeforeFetchingShard(t *testing.T) {
	fixture := writeRetrievalFixture(t)
	fixture.options.MaxDownloadBytes = uint64(len(fixture.manifestRaw) + len(fixture.signatureRaw) + len(fixture.shardRaw) - 1)
	fetcher, requested := fixture.fetcher(t)

	if _, err := retrieveWithFetcher(context.Background(), fixture.options, fetcher); !errors.Is(err, ErrRetrievalLimit) {
		t.Fatalf("Retrieve over aggregate budget = %v, want ErrRetrievalLimit", err)
	}
	if len(*requested) != 2 {
		t.Fatalf("over-budget retrieval made requests %v, want manifest and signature only", *requested)
	}
	if _, err := os.Lstat(fixture.options.OutputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("over-budget retrieval activated output: %v", err)
	}
}

func TestRetrieveRejectsUnsafeOutputAndBaseBeforeStateOrNetwork(t *testing.T) {
	t.Run("nil context", func(t *testing.T) {
		if _, err := Retrieve(nil, RetrieveOptions{}); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("Retrieve nil context = %v, want ErrInvalidOptions", err)
		}
	})

	t.Run("output inside state", func(t *testing.T) {
		fixture := writeRetrievalFixture(t)
		fixture.options.OutputDir = filepath.Join(fixture.options.StateDir, "bundle")
		called := false
		_, err := retrieveWithFetcher(context.Background(), fixture.options, func(context.Context, string, int64, io.Writer) (int64, error) {
			called = true
			return 0, nil
		})
		if err == nil || called {
			t.Fatalf("Retrieve unsafe output = %v, network called=%v", err, called)
		}
		entries, readErr := os.ReadDir(fixture.options.StateDir)
		if readErr != nil || len(entries) != 0 {
			t.Fatalf("unsafe output mutated state: %v, %v", entries, readErr)
		}
	})

	t.Run("non HTTPS target base", func(t *testing.T) {
		fixture := writeRetrievalFixture(t)
		fixture.options.TargetBaseURL = "http://127.0.0.1/targets/"
		called := false
		_, err := retrieveWithFetcher(context.Background(), fixture.options, func(context.Context, string, int64, io.Writer) (int64, error) {
			called = true
			return 0, nil
		})
		if err == nil || called {
			t.Fatalf("Retrieve unsafe base = %v, network called=%v", err, called)
		}
		entries, readErr := os.ReadDir(fixture.options.StateDir)
		if readErr != nil || len(entries) != 0 {
			t.Fatalf("unsafe base mutated state: %v, %v", entries, readErr)
		}
	})

	t.Run("private HTTPS target base", func(t *testing.T) {
		fixture := writeRetrievalFixture(t)
		fixture.options.TargetBaseURL = "https://127.0.0.1/targets/"
		if _, err := Retrieve(context.Background(), fixture.options); err == nil || !strings.Contains(err.Error(), "private_ip_blocked") {
			t.Fatalf("Retrieve private target base = %v", err)
		}
		entries, readErr := os.ReadDir(fixture.options.StateDir)
		if readErr != nil || len(entries) != 0 {
			t.Fatalf("private target base mutated state: %v, %v", entries, readErr)
		}
	})
}

func TestRetrieveReturnsSignedAbsenceWithoutArtifactRequestsOrOutput(t *testing.T) {
	fixture := writeRetrievalFixtureWithChannelOptions(t, fixtureOptions{omitTarget: true})
	called := false
	retrieved, err := retrieveWithFetcher(context.Background(), fixture.options, func(context.Context, string, int64, io.Writer) (int64, error) {
		called = true
		return 0, errors.New("unexpected artifact request")
	})
	if err != nil {
		t.Fatalf("Retrieve signed absence: %v", err)
	}
	if retrieved.Status != StatusAbsent || called || retrieved.Path != "" || retrieved.DownloadBytes != 0 {
		t.Fatalf("signed absence = %#v, network called=%v", retrieved, called)
	}
	if _, err := os.Lstat(fixture.options.OutputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("signed absence created output: %v", err)
	}
}

func TestRemoteManifestPathUsesSelectedSHA256ForConsistentSnapshot(t *testing.T) {
	digest := strings.Repeat("a", 64)
	target := verifiedManifestTarget{
		Path: fixtureTargetPath, ConsistentSnapshot: true,
		Custom: openpackchannel.Target{ManifestSHA256: digest},
	}
	if got, want := remoteManifestPath(target), "packs/developer/"+digest+".manifest.json"; got != want {
		t.Fatalf("remoteManifestPath = %q, want %q", got, want)
	}
	target.ConsistentSnapshot = false
	if got := remoteManifestPath(target); got != fixtureTargetPath {
		t.Fatalf("non-consistent remoteManifestPath = %q", got)
	}
	if got, want := remoteSignaturePath(target), "packs/developer/"+digest+".manifest.ed25519"; got != want {
		t.Fatalf("remoteSignaturePath = %q, want %q", got, want)
	}
}

func TestHTTPArtifactFetcherRejectsRedirectEncodingAndOversize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("Accept-Encoding = %q", request.Header.Get("Accept-Encoding"))
		}
		if !request.Close {
			t.Error("artifact request allowed connection reuse")
		}
		switch request.URL.Path {
		case "/ok":
			_, _ = writer.Write([]byte("exact"))
		case "/redirect":
			http.Redirect(writer, request, "/ok", http.StatusFound)
		case "/encoding":
			writer.Header().Set("Content-Encoding", "gzip")
			_, _ = writer.Write([]byte("transformed"))
		case "/oversize":
			writer.Header().Set("Content-Length", "11")
			_, _ = writer.Write([]byte("12345678901"))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	policy := egress.DefaultInternal()
	policy.MaxRedirects = 0
	fetch := httpArtifactFetcher(policy, retrievalHTTPClient(policy, time.Second))

	var output bytes.Buffer
	count, err := fetch(context.Background(), server.URL+"/ok", 5, &output)
	if err != nil || count != 5 || output.String() != "exact" {
		t.Fatalf("fetch exact = %d, %q, %v", count, output.String(), err)
	}
	if _, err := fetch(context.Background(), server.URL+"/redirect", 16, io.Discard); err == nil || !strings.Contains(err.Error(), "too_many_redirects") {
		t.Fatalf("fetch redirect = %v", err)
	}
	if _, err := fetch(context.Background(), server.URL+"/encoding", 16, io.Discard); err == nil || !strings.Contains(err.Error(), "content encoding") {
		t.Fatalf("fetch encoded response = %v", err)
	}
	if _, err := fetch(context.Background(), server.URL+"/oversize", 10, io.Discard); !errors.Is(err, ErrRetrievalLimit) {
		t.Fatalf("fetch oversized response = %v, want ErrRetrievalLimit", err)
	}
}

func assertNoRetrievedOutputOrStaging(t *testing.T, output string) {
	t.Helper()
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retrieval activated output: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(output))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), retrievalStagingPrefix) {
			t.Fatalf("retrieval left staging path %q", entry.Name())
		}
	}
}

type retrievalFixture struct {
	options      RetrieveOptions
	manifest     indexpack.Manifest
	manifestRaw  []byte
	signatureRaw []byte
	shardRaw     []byte
	objects      map[string][]byte
	publicKey    ed25519.PublicKey
	now          time.Time
}

func writeRetrievalFixture(t *testing.T) retrievalFixture {
	return writeRetrievalFixtureWithChannelOptions(t, fixtureOptions{})
}

func writeRetrievalFixtureWithChannelOptions(t *testing.T, channelOptions fixtureOptions) retrievalFixture {
	t.Helper()
	repository := newFixtureRepository(t)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	fetchedAt := repository.now.Add(-2 * time.Hour).Format(time.RFC3339)
	recordRaw, err := json.Marshal(indexpack.Record{
		Operation: indexpack.OperationUpsert, URL: "https://example.com/retrieved", Title: "Retrieved pack record",
		Language: "en", FetchedAt: fetchedAt,
		Provenance: []indexpack.Provenance{{
			Source: "retrieval-test", SourceURI: "https://example.com/source", RetrievedAt: fetchedAt,
			RightsNotice: "CC0-1.0",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	uncompressed := append(recordRaw, '\n')
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	shardRaw := encoder.EncodeAll(uncompressed, nil)
	encoder.Close()
	shardDigest := indexpack.ShardDigest(shardRaw)
	uncompressedDigest := indexpack.Digest(uncompressed)
	manifest := channelManifest(repository.now)
	manifest.SigningKeyID = indexpack.KeyID(publicKey)
	manifest.Shards[0] = indexpack.Shard{
		Path: indexpack.ShardPath(shardDigest), Compression: "zstd", SHA256: shardDigest,
		CompressedSizeBytes: uint64(len(shardRaw)), UncompressedSHA256: uncompressedDigest,
		UncompressedSizeBytes: uint64(len(uncompressed)), RecordCount: 1,
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, privateKey)
	if err != nil {
		t.Fatalf("SignManifest: %v", err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		t.Fatalf("EncodeSignature: %v", err)
	}
	repository.manifestRaw = manifestRaw
	root := t.TempDir()
	metadataDir := repository.writeMetadataDir(t, filepath.Join(root, "metadata"), channelOptions)
	stateDir := mkdirFixture(t, filepath.Join(root, "state"))
	outputParent := mkdirFixture(t, filepath.Join(root, "output"))
	objects := map[string][]byte{
		retrievalBaseURL + fixtureTargetPath: manifestRaw,
		retrievalBaseURL + "packs/developer/" + indexpack.ManifestDigest(manifestRaw) + ".manifest.ed25519": signatureRaw,
		retrievalBaseURL + "packs/developer/" + manifest.Shards[0].Path:                                     shardRaw,
	}
	return retrievalFixture{
		options: RetrieveOptions{
			TrustedRootPath: repository.trustedRoot, MetadataDir: metadataDir, StateDir: stateDir,
			TargetPath: fixtureTargetPath, TargetBaseURL: retrievalBaseURL,
			OutputDir: filepath.Join(outputParent, "developer-en-r1"), MaxDownloadBytes: 2 << 20, MaxShards: 1,
		},
		manifest: manifest, manifestRaw: manifestRaw, signatureRaw: signatureRaw,
		shardRaw: shardRaw, objects: objects, publicKey: publicKey, now: repository.now,
	}
}

func (fixture retrievalFixture) fetcher(t *testing.T) (artifactFetcher, *[]string) {
	t.Helper()
	requested := make([]string, 0, len(fixture.objects))
	return func(ctx context.Context, rawURL string, maximum int64, destination io.Writer) (int64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		requested = append(requested, rawURL)
		raw, found := fixture.objects[rawURL]
		if !found {
			return 0, errors.New("unexpected artifact URL")
		}
		if int64(len(raw)) > maximum {
			return 0, ErrRetrievalLimit
		}
		return io.Copy(destination, bytes.NewReader(raw))
	}, &requested
}
