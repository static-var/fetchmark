package tufchannel

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sigstore/sigstore/pkg/signature"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/theupdateframework/go-tuf/v2/metadata"
	bolt "go.etcd.io/bbolt"
)

const fixtureTargetPath = "packs/developer/manifest.json"

func TestSelectVerifiesPinnedTUFMetadataAndExactBundleManifest(t *testing.T) {
	fixture := writeChannelFixture(t, fixtureOptions{})

	selected, err := Select(context.Background(), Options{
		TrustedRootPath: fixture.trustedRoot,
		MetadataDir:     fixture.metadataDir,
		StateDir:        fixture.stateDir,
		BundleDir:       fixture.bundleDir,
		TargetPath:      fixtureTargetPath,
	})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if selected.Status != StatusSelected {
		t.Fatalf("status = %q, want %q", selected.Status, StatusSelected)
	}
	if selected.TargetPath != fixtureTargetPath || selected.PackID != "developer-en" || selected.Revision != 1 {
		t.Fatalf("selection identity = %#v", selected)
	}
	if selected.ManifestSHA256 != indexpack.ManifestDigest(fixture.manifestRaw) {
		t.Fatalf("manifest digest = %q", selected.ManifestSHA256)
	}
	if selected.RootVersion != 1 || selected.TimestampVersion != 1 || selected.SnapshotVersion != 1 || selected.TargetsVersion != 1 {
		t.Fatalf("metadata versions = %#v", selected)
	}
	if selected.TargetLength != int64(len(fixture.manifestRaw)) {
		t.Fatalf("target length = %d", selected.TargetLength)
	}
	manifest, err := indexpack.DecodeManifest(fixture.manifestRaw)
	if err != nil {
		t.Fatal(err)
	}
	if selected.SigningKeyID != manifest.SigningKeyID || selected.ManifestRecordCount != manifest.RecordCount ||
		selected.CreatedAt != manifest.CreatedAt || selected.ExpiresAt != manifest.ExpiresAt {
		t.Fatalf("selected manifest policy = %#v", selected)
	}
}

func TestSelectReturnsSignedAbsenceWithoutRequiringBundle(t *testing.T) {
	fixture := writeChannelFixture(t, fixtureOptions{omitTarget: true})
	options := fixture.selectOptions()
	options.BundleDir = ""

	selected, err := Select(context.Background(), options)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if selected.Status != StatusAbsent || selected.TargetPath != fixtureTargetPath || selected.ManifestSHA256 != "" {
		t.Fatalf("selection = %#v", selected)
	}
}

func TestSelectRejectsDelegatedProfileAndInvalidBootstrapWithoutPinning(t *testing.T) {
	t.Run("delegations", func(t *testing.T) {
		fixture := writeChannelFixture(t, fixtureOptions{delegatedTargets: true})
		if _, err := Select(context.Background(), fixture.selectOptions()); err == nil || !strings.Contains(err.Error(), "delegated targets") {
			t.Fatalf("Select delegated profile = %v", err)
		}
	})

	t.Run("invalid bootstrap", func(t *testing.T) {
		fixture := writeChannelFixture(t, fixtureOptions{})
		badRoot := filepath.Join(t.TempDir(), "bad-root.json")
		if err := os.WriteFile(badRoot, []byte(`{"not":"tuf"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		options := fixture.selectOptions()
		options.TrustedRootPath = badRoot
		if _, err := Select(context.Background(), options); err == nil {
			t.Fatal("Select accepted invalid bootstrap")
		}
		if _, err := os.Lstat(filepath.Join(fixture.stateDir, stateDatabaseName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid bootstrap pinned state: %v", err)
		}
	})
}

func TestSelectRejectsTamperedManifestAndAcceptsExactRetry(t *testing.T) {
	fixture := writeChannelFixture(t, fixtureOptions{})
	tampered := append([]byte(nil), fixture.manifestRaw...)
	tampered[len(tampered)-2] ^= 1
	if err := os.WriteFile(filepath.Join(fixture.bundleDir, "manifest.json"), tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Select(context.Background(), fixture.selectOptions()); err == nil {
		t.Fatal("Select accepted tampered manifest")
	}
	if err := os.WriteFile(filepath.Join(fixture.bundleDir, "manifest.json"), fixture.manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Select(context.Background(), fixture.selectOptions()); err != nil {
		t.Fatalf("Select exact retry: %v", err)
	}
}

func TestSelectPinsBootstrapRootAcrossRuns(t *testing.T) {
	fixture := writeChannelFixture(t, fixtureOptions{})
	if _, err := Select(context.Background(), fixture.selectOptions()); err != nil {
		t.Fatalf("initial Select: %v", err)
	}
	other := writeChannelFixture(t, fixtureOptions{})
	options := fixture.selectOptions()
	options.TrustedRootPath = other.trustedRoot
	if _, err := Select(context.Background(), options); !errors.Is(err, ErrBootstrapChanged) {
		t.Fatalf("Select changed bootstrap = %v, want ErrBootstrapChanged", err)
	}
}

func TestSelectContinuesFromAcceptedRotatedRoot(t *testing.T) {
	repository := newFixtureRepository(t).rotateRoot(t)
	root := t.TempDir()
	metadataDir := repository.writeMetadataDir(t, filepath.Join(root, "metadata"), fixtureOptions{})
	stateDir := mkdirFixture(t, filepath.Join(root, "state"))
	bundleDir := mkdirFixture(t, filepath.Join(root, "bundle"))
	if err := os.WriteFile(filepath.Join(bundleDir, "manifest.json"), repository.manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		TrustedRootPath: repository.trustedRoot, MetadataDir: metadataDir,
		StateDir: stateDir, BundleDir: bundleDir, TargetPath: fixtureTargetPath,
	}
	selected, err := Select(context.Background(), options)
	if err != nil || selected.RootVersion != 2 {
		t.Fatalf("Select rotated root = %#v, %v", selected, err)
	}
	if err := os.Remove(filepath.Join(metadataDir, "2.root.json")); err != nil {
		t.Fatal(err)
	}
	selected, err = Select(context.Background(), options)
	if err != nil || selected.RootVersion != 2 {
		t.Fatalf("Select cached rotated root = %#v, %v", selected, err)
	}
}

func TestSelectDurablyRetainsAcceptedRootWhenLaterMetadataFails(t *testing.T) {
	repository := newFixtureRepository(t).rotateRoot(t)
	root := t.TempDir()
	brokenMetadataDir := repository.writeMetadataDir(t, filepath.Join(root, "broken-metadata"), fixtureOptions{})
	if err := os.Remove(filepath.Join(brokenMetadataDir, "timestamp.json")); err != nil {
		t.Fatal(err)
	}
	stateDir := mkdirFixture(t, filepath.Join(root, "state"))
	bundleDir := mkdirFixture(t, filepath.Join(root, "bundle"))
	if err := os.WriteFile(filepath.Join(bundleDir, "manifest.json"), repository.manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		TrustedRootPath: repository.trustedRoot, MetadataDir: brokenMetadataDir,
		StateDir: stateDir, BundleDir: bundleDir, TargetPath: fixtureTargetPath,
	}
	if _, err := Select(context.Background(), options); err == nil || !strings.Contains(err.Error(), "refresh trusted metadata") {
		t.Fatalf("Select with missing timestamp = %v", err)
	}

	cachedRoot, err := os.ReadFile(filepath.Join(stateDir, "root.json"))
	if err != nil {
		t.Fatalf("read accepted root: %v", err)
	}
	var envelope struct {
		Signed struct {
			Version int64 `json:"version"`
		} `json:"signed"`
	}
	if err := json.Unmarshal(cachedRoot, &envelope); err != nil || envelope.Signed.Version != 2 {
		t.Fatalf("cached root after partial refresh = version %d, %v", envelope.Signed.Version, err)
	}

	validMetadataDir := repository.writeMetadataDir(t, filepath.Join(root, "valid-metadata"), fixtureOptions{})
	if err := os.Remove(filepath.Join(validMetadataDir, "2.root.json")); err != nil {
		t.Fatal(err)
	}
	options.MetadataDir = validMetadataDir
	selected, err := Select(context.Background(), options)
	if err != nil || selected.RootVersion != 2 {
		t.Fatalf("Select from root retained after partial refresh = %#v, %v", selected, err)
	}
}

func TestSelectReportsDurabilityFailureAfterPartialRefresh(t *testing.T) {
	repository := newFixtureRepository(t).rotateRoot(t)
	root := t.TempDir()
	metadataDir := repository.writeMetadataDir(t, filepath.Join(root, "metadata"), fixtureOptions{})
	if err := os.WriteFile(filepath.Join(metadataDir, "targets.json"), []byte(`{"tampered":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stateDir := mkdirFixture(t, filepath.Join(root, "state"))
	bundleDir := mkdirFixture(t, filepath.Join(root, "bundle"))
	if err := os.WriteFile(filepath.Join(bundleDir, "manifest.json"), repository.manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	forced := errors.New("forced state sync failure")
	synced := make(map[string]bool)
	_, err := selectWithDependencies(context.Background(), Options{
		TrustedRootPath: repository.trustedRoot, MetadataDir: metadataDir,
		StateDir: stateDir, BundleDir: bundleDir, TargetPath: fixtureTargetPath,
	}, selectionDependencies{syncFile: func(file *os.File) error {
		name := filepath.Base(file.Name())
		synced[name] = true
		if name == "root.json" {
			return forced
		}
		return file.Sync()
	}})
	if !errors.Is(err, forced) || !strings.Contains(err.Error(), "refresh trusted metadata") {
		t.Fatalf("Select partial refresh with sync failure = %v, want refresh and durability errors", err)
	}
	for _, name := range []string{"root.json", "timestamp.json", "snapshot.json"} {
		if !synced[name] {
			t.Errorf("accepted trusted metadata %s was not offered for sync after another sync failed; got %v", name, synced)
		}
	}
}

func TestSelectRejectsExpiredMetadataAndRollback(t *testing.T) {
	t.Run("expired timestamp", func(t *testing.T) {
		fixture := writeChannelFixture(t, fixtureOptions{expiredTimestamp: true})
		if _, err := Select(context.Background(), fixture.selectOptions()); err == nil {
			t.Fatal("Select accepted expired timestamp")
		}
	})

	t.Run("timestamp rollback", func(t *testing.T) {
		repository := newFixtureRepository(t)
		root := t.TempDir()
		stateDir := mkdirFixture(t, filepath.Join(root, "state"))
		bundleDir := mkdirFixture(t, filepath.Join(root, "bundle"))
		if err := os.WriteFile(filepath.Join(bundleDir, "manifest.json"), repository.manifestRaw, 0o600); err != nil {
			t.Fatal(err)
		}
		newer := repository.writeMetadataDir(t, filepath.Join(root, "newer"), fixtureOptions{version: 2})
		older := repository.writeMetadataDir(t, filepath.Join(root, "older"), fixtureOptions{version: 1})
		options := Options{
			TrustedRootPath: repository.trustedRoot, MetadataDir: newer,
			StateDir: stateDir, BundleDir: bundleDir, TargetPath: fixtureTargetPath,
		}
		if _, err := Select(context.Background(), options); err != nil {
			t.Fatalf("Select newer: %v", err)
		}
		options.MetadataDir = older
		if _, err := Select(context.Background(), options); err == nil {
			t.Fatal("Select accepted timestamp rollback")
		}
		options.MetadataDir = newer
		if selected, err := Select(context.Background(), options); err != nil || selected.TimestampVersion != 2 {
			t.Fatalf("Select retained newer state = %#v, %v", selected, err)
		}
	})
}

func TestSelectHonorsCancellationAndExclusiveStateLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Select(ctx, Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Select canceled = %v", err)
	}

	fixture := writeChannelFixture(t, fixtureOptions{})
	if _, err := Select(context.Background(), fixture.selectOptions()); err != nil {
		t.Fatalf("initial Select: %v", err)
	}
	state, err := bolt.Open(filepath.Join(fixture.stateDir, stateDatabaseName), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if _, err := Select(context.Background(), fixture.selectOptions()); !errors.Is(err, ErrStateBusy) {
		t.Fatalf("Select locked state = %v, want ErrStateBusy", err)
	}
}

func TestSelectRejectsOverlappingTrustAndArtifactTrees(t *testing.T) {
	tests := []struct {
		name                  string
		mutate                func(*Options)
		assertNoStateMutation bool
	}{
		{name: "metadata is state", mutate: func(options *Options) { options.MetadataDir = options.StateDir }},
		{name: "bundle is state", mutate: func(options *Options) { options.BundleDir = options.StateDir }, assertNoStateMutation: true},
		{name: "bootstrap inside state", mutate: func(options *Options) {
			path := filepath.Join(options.StateDir, "bootstrap.json")
			raw, err := os.ReadFile(options.TrustedRootPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			options.TrustedRootPath = path
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := writeChannelFixture(t, fixtureOptions{})
			options := fixture.selectOptions()
			test.mutate(&options)
			if _, err := Select(context.Background(), options); !errors.Is(err, secureconfigfile.ErrUnsafePath) {
				t.Fatalf("Select overlapping paths = %v, want ErrUnsafePath", err)
			}
			if test.assertNoStateMutation {
				entries, err := os.ReadDir(fixture.stateDir)
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) != 0 {
					t.Fatalf("overlap rejection mutated state: %v", entries)
				}
			}
		})
	}
}

func TestSelectRejectsOversizedCachedTrustedMetadataBeforeUpdaterRead(t *testing.T) {
	tests := []struct {
		name    string
		maximum int64
	}{
		{name: "timestamp.json", maximum: maxTimestampBytes},
		{name: "snapshot.json", maximum: maxSnapshotBytes},
		{name: "targets.json", maximum: maxTargetsBytes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := writeChannelFixture(t, fixtureOptions{})
			if _, err := Select(context.Background(), fixture.selectOptions()); err != nil {
				t.Fatalf("initial Select: %v", err)
			}
			oversized := make([]byte, test.maximum+1)
			for index := range oversized {
				oversized[index] = 'x'
			}
			if err := os.WriteFile(filepath.Join(fixture.stateDir, test.name), oversized, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Select(context.Background(), fixture.selectOptions())
			if err == nil || !strings.Contains(err.Error(), "cached trusted metadata "+test.name) || !strings.Contains(err.Error(), "size must be") {
				t.Fatalf("Select oversized cached %s = %v", test.name, err)
			}
		})
	}
}

type fixtureOptions struct {
	omitTarget       bool
	expiredTimestamp bool
	delegatedTargets bool
	version          int64
}

type channelFixture struct {
	trustedRoot string
	metadataDir string
	stateDir    string
	bundleDir   string
	manifestRaw []byte
}

func (fixture channelFixture) selectOptions() Options {
	return Options{
		TrustedRootPath: fixture.trustedRoot, MetadataDir: fixture.metadataDir,
		StateDir: fixture.stateDir, BundleDir: fixture.bundleDir, TargetPath: fixtureTargetPath,
	}
}

type fixtureRepository struct {
	now            time.Time
	trustedRoot    string
	manifestRaw    []byte
	keys           map[string]ed25519.PrivateKey
	rotatedRootRaw []byte
}

func writeChannelFixture(t *testing.T, options fixtureOptions) channelFixture {
	t.Helper()
	root := t.TempDir()
	repository := newFixtureRepository(t)
	metadataDir := repository.writeMetadataDir(t, filepath.Join(root, "metadata"), options)
	stateDir := mkdirFixture(t, filepath.Join(root, "state"))
	bundleDir := mkdirFixture(t, filepath.Join(root, "bundle"))
	if err := os.WriteFile(filepath.Join(bundleDir, "manifest.json"), repository.manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	return channelFixture{
		trustedRoot: repository.trustedRoot, metadataDir: metadataDir, stateDir: stateDir,
		bundleDir: bundleDir, manifestRaw: repository.manifestRaw,
	}
}

func newFixtureRepository(t *testing.T) fixtureRepository {
	t.Helper()
	root := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)
	manifestRaw, err := indexpack.EncodeManifest(channelManifest(now))
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	keys := make(map[string]ed25519.PrivateKey)
	rootMetadata := metadata.Root(now.Add(24 * time.Hour))
	rootMetadata.Signed.ConsistentSnapshot = false
	for _, role := range []string{metadata.ROOT, metadata.TARGETS, metadata.SNAPSHOT, metadata.TIMESTAMP} {
		_, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		keys[role] = privateKey
		key, err := metadata.KeyFromPublicKey(privateKey.Public())
		if err != nil {
			t.Fatal(err)
		}
		if err := rootMetadata.Signed.AddKey(key, role); err != nil {
			t.Fatal(err)
		}
	}
	signMetadata(t, rootMetadata, keys[metadata.ROOT])
	rootRaw := metadataBytes(t, rootMetadata)
	trustedRoot := filepath.Join(root, "trusted-root.json")
	if err := os.WriteFile(trustedRoot, rootRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	return fixtureRepository{now: now, trustedRoot: trustedRoot, manifestRaw: manifestRaw, keys: keys}
}

func (repository fixtureRepository) rotateRoot(t *testing.T) fixtureRepository {
	t.Helper()
	newKeys := make(map[string]ed25519.PrivateKey)
	rotated := metadata.Root(repository.now.Add(48 * time.Hour))
	rotated.Signed.Version = 2
	rotated.Signed.ConsistentSnapshot = false
	for _, role := range []string{metadata.ROOT, metadata.TARGETS, metadata.SNAPSHOT, metadata.TIMESTAMP} {
		_, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		newKeys[role] = privateKey
		key, err := metadata.KeyFromPublicKey(privateKey.Public())
		if err != nil {
			t.Fatal(err)
		}
		if err := rotated.Signed.AddKey(key, role); err != nil {
			t.Fatal(err)
		}
	}
	signMetadata(t, rotated, repository.keys[metadata.ROOT])
	signMetadata(t, rotated, newKeys[metadata.ROOT])
	repository.keys = newKeys
	repository.rotatedRootRaw = metadataBytes(t, rotated)
	return repository
}

func (repository fixtureRepository) writeMetadataDir(t *testing.T, metadataDir string, options fixtureOptions) string {
	t.Helper()
	mkdirFixture(t, metadataDir)
	version := options.version
	if version == 0 {
		version = 1
	}
	targets := metadata.Targets(repository.now.Add(6 * time.Hour))
	targets.Signed.Version = version
	if !options.omitTarget {
		target, err := metadata.TargetFile().FromBytes(fixtureTargetPath, repository.manifestRaw, "sha256")
		if err != nil {
			t.Fatal(err)
		}
		customRaw, err := json.Marshal(map[string]any{"fetchmark_open_pack": map[string]any{
			"schema": 1, "pack_id": "developer-en", "kind": "snapshot", "revision": 1,
			"manifest_sha256": indexpack.ManifestDigest(repository.manifestRaw),
		}})
		if err != nil {
			t.Fatal(err)
		}
		custom := json.RawMessage(customRaw)
		target.Custom = &custom
		targets.Signed.Targets[fixtureTargetPath] = target
	}
	if options.delegatedTargets {
		key, err := metadata.KeyFromPublicKey(repository.keys[metadata.TARGETS].Public())
		if err != nil {
			t.Fatal(err)
		}
		keyID, err := key.ID()
		if err != nil {
			t.Fatal(err)
		}
		targets.Signed.Delegations = &metadata.Delegations{
			Keys: map[string]*metadata.Key{keyID: key},
			Roles: []metadata.DelegatedRole{{
				Name: "unsupported", KeyIDs: []string{keyID}, Threshold: 1,
				Paths: []string{"packs/*"},
			}},
		}
	}
	signMetadata(t, targets, repository.keys[metadata.TARGETS])
	targetsRaw := metadataBytes(t, targets)

	snapshot := metadata.Snapshot(repository.now.Add(4 * time.Hour))
	snapshot.Signed.Version = version
	snapshot.Signed.Meta["targets.json"] = metaFileFor(targets.Signed.Version, targetsRaw)
	signMetadata(t, snapshot, repository.keys[metadata.SNAPSHOT])
	snapshotRaw := metadataBytes(t, snapshot)

	timestampExpiry := repository.now.Add(2 * time.Hour)
	if options.expiredTimestamp {
		timestampExpiry = repository.now.Add(-time.Hour)
	}
	timestamp := metadata.Timestamp(timestampExpiry)
	timestamp.Signed.Version = version
	timestamp.Signed.Meta["snapshot.json"] = metaFileFor(snapshot.Signed.Version, snapshotRaw)
	signMetadata(t, timestamp, repository.keys[metadata.TIMESTAMP])
	timestampRaw := metadataBytes(t, timestamp)

	for name, raw := range map[string][]byte{
		"timestamp.json": timestampRaw,
		"snapshot.json":  snapshotRaw,
		"targets.json":   targetsRaw,
	} {
		if err := os.WriteFile(filepath.Join(metadataDir, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if len(repository.rotatedRootRaw) > 0 {
		if err := os.WriteFile(filepath.Join(metadataDir, "2.root.json"), repository.rotatedRootRaw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return metadataDir
}

func mkdirFixture(t *testing.T, path string) string {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func channelManifest(now time.Time) indexpack.Manifest {
	shardDigest := strings.Repeat("1", 64)
	return indexpack.Manifest{
		Version: 1, Kind: indexpack.KindSnapshot, PackID: "developer-en", Revision: 1,
		CreatedAt: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(12 * time.Hour).Format(time.RFC3339),
		SigningKeyID: strings.Repeat("4", 64),
		Publisher: indexpack.Publisher{
			Name: "Example Publisher", ContactURI: "mailto:operator@example.com",
			TakedownURI: "https://example.com/takedown", RightsNotice: "CC0-1.0",
		},
		Policy:    indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages: []string{"en"}, RecordCount: 1,
		Shards: []indexpack.Shard{{
			Path: indexpack.ShardPath(shardDigest), Compression: "zstd", SHA256: shardDigest,
			CompressedSizeBytes: 80, UncompressedSHA256: strings.Repeat("5", 64),
			UncompressedSizeBytes: 100, RecordCount: 1,
		}},
		Build: indexpack.Build{
			Generator: "fetchmark-pack-builder", GeneratorVersion: "1.0.0",
			Analyzer: "unicode-lexical", AnalyzerVersion: "1.0.0",
			PolicySHA256: strings.Repeat("6", 64), ExclusionsSHA256: strings.Repeat("7", 64),
			Inputs: []indexpack.BuildInput{{
				Name: "example-input", URI: "https://example.com/input",
				RetrievedAt: now.Add(-2 * time.Hour).Format(time.RFC3339),
				SHA256:      strings.Repeat("2", 64), RightsNotice: "source metadata used under CC0-1.0",
			}},
		},
	}
}

func metaFileFor(version int64, raw []byte) *metadata.MetaFiles {
	digest := sha256.Sum256(raw)
	return &metadata.MetaFiles{
		Version: version, Length: int64(len(raw)),
		Hashes: metadata.Hashes{"sha256": metadata.HexBytes(digest[:])},
	}
}

func signMetadata[T metadata.Roles](t *testing.T, document *metadata.Metadata[T], privateKey ed25519.PrivateKey) {
	t.Helper()
	signer, err := signature.LoadSigner(privateKey, crypto.Hash(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := document.Sign(signer); err != nil {
		t.Fatal(err)
	}
}

func metadataBytes[T metadata.Roles](t *testing.T, document *metadata.Metadata[T]) []byte {
	t.Helper()
	raw, err := document.ToBytes(false)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
