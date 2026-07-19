package openpackbuilder

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"

	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const (
	DeltaBuildReportVersion         = 1
	DeltaBuildReportSignatureDomain = "fetchmark-open-index-pack-delta-build-report-v1\n"
	deltaStateFilename              = ".delta-state.db"
	deltaBatchSize                  = 1_000
)

var (
	parentBucket = []byte("parent")
	targetBucket = []byte("target")

	ErrDeltaBuildCommitted = errors.New("open pack delta bundle committed but command completion failed")
)

type deltaBuildHooks struct {
	activate         func(*os.Root, string, string) error
	checkRoot        func(*os.Root, string) error
	syncOutputParent func(*os.Root) error
}

func defaultDeltaBuildHooks() deltaBuildHooks {
	return deltaBuildHooks{activate: activateNoReplace, checkRoot: rootStillAtPath, syncOutputParent: syncRoot}
}

type DeltaOptions struct {
	ParentBundleDir string
	SpecRaw         []byte
	ExclusionsRaw   []byte
	Candidates      io.Reader
	Identity        Identity
	BuilderVersion  string
	OutputDir       string
}

type DeltaResult struct {
	Path                  string `json:"path"`
	PackID                string `json:"pack_id"`
	Revision              uint64 `json:"revision"`
	ParentManifestSHA256  string `json:"parent_manifest_sha256"`
	ManifestSHA256        string `json:"manifest_sha256"`
	BuildReportSHA256     string `json:"build_report_sha256"`
	OperationCount        uint64 `json:"operation_count"`
	ProjectionRecordCount uint64 `json:"projection_record_count"`
	UpsertCount           uint64 `json:"upsert_count"`
	TombstoneCount        uint64 `json:"tombstone_count"`
	ShardCount            int    `json:"shard_count"`
	OrdinaryInstallable   bool   `json:"ordinary_installable"`
	ProjectionBytes       uint64 `json:"projection_bytes,omitempty"`
}

type DeltaOperationReport struct {
	Upserts               uint64 `json:"upserts"`
	Tombstones            uint64 `json:"tombstones"`
	Total                 uint64 `json:"total"`
	ProjectionRecordCount uint64 `json:"projection_record_count"`
}

type DeltaBuildReport struct {
	Version               int                       `json:"version"`
	Builder               string                    `json:"builder"`
	BuilderVersion        string                    `json:"builder_version"`
	Profile               string                    `json:"profile"`
	OrdinaryInstallable   bool                      `json:"ordinary_installable"`
	PublisherID           string                    `json:"publisher_id"`
	KeyID                 string                    `json:"key_id"`
	ManifestSHA256        string                    `json:"manifest_sha256"`
	ParentManifestSHA256  string                    `json:"parent_manifest_sha256"`
	PackID                string                    `json:"pack_id"`
	Revision              uint64                    `json:"revision"`
	CreatedAt             string                    `json:"created_at"`
	TargetSelection       indexpackselection.Report `json:"target_selection"`
	Operations            DeltaOperationReport      `json:"operations"`
	Shards                []ShardEvidence           `json:"shards"`
	OperationStreamSHA256 string                    `json:"operation_stream_sha256"`
}

type DeltaVerification struct {
	PackID                string `json:"pack_id"`
	Revision              uint64 `json:"revision"`
	ParentManifestSHA256  string `json:"parent_manifest_sha256"`
	ManifestSHA256        string `json:"manifest_sha256"`
	BuildReportSHA256     string `json:"build_report_sha256"`
	OperationCount        uint64 `json:"operation_count"`
	ProjectionRecordCount uint64 `json:"projection_record_count"`
	ShardCount            int    `json:"shard_count"`
	OrdinaryInstallable   bool   `json:"ordinary_installable"`
	ProjectionBytes       uint64 `json:"projection_bytes,omitempty"`
}

func BuildDelta(ctx context.Context, options DeltaOptions) (DeltaResult, error) {
	return buildDelta(ctx, options, time.Now)
}

func buildDelta(ctx context.Context, options DeltaOptions, now func() time.Time) (result DeltaResult, returnErr error) {
	return buildDeltaWithHooks(ctx, options, now, defaultDeltaBuildHooks())
}

func buildDeltaWithHooks(ctx context.Context, options DeltaOptions, now func() time.Time, hooks deltaBuildHooks) (result DeltaResult, returnErr error) {
	committed := false
	defer func() {
		if committed && returnErr != nil && !errors.Is(returnErr, ErrDeltaBuildCommitted) {
			returnErr = DeltaCommittedError(result, returnErr)
		}
	}()
	if ctx == nil || options.Candidates == nil || now == nil {
		return DeltaResult{}, errors.New("open pack delta builder: context, candidates, and clock are required")
	}
	if hooks.activate == nil || hooks.checkRoot == nil || hooks.syncOutputParent == nil {
		return DeltaResult{}, errors.New("open pack delta builder: build hooks are incomplete")
	}
	if options.Identity.publisherID == "" || options.Identity.keyID == "" || len(options.Identity.privateKey) != ed25519.PrivateKeySize {
		return DeltaResult{}, errors.New("open pack delta builder: valid publisher identity is required")
	}
	if strings.TrimSpace(options.BuilderVersion) != options.BuilderVersion || options.BuilderVersion == "" || len(options.BuilderVersion) > 128 {
		return DeltaResult{}, errors.New("open pack delta builder: builder version is required")
	}
	if !cleanAbsolute(options.ParentBundleDir) || !cleanAbsolute(options.OutputDir) || options.ParentBundleDir == options.OutputDir {
		return DeltaResult{}, errors.New("open pack delta builder: distinct clean absolute parent and output paths are required")
	}
	if pathsOverlap(options.ParentBundleDir, options.OutputDir) {
		return DeltaResult{}, errors.New("open pack delta builder: parent and output directory trees must not overlap")
	}
	specRaw := bytes.Clone(options.SpecRaw)
	exclusionsRaw := bytes.Clone(options.ExclusionsRaw)
	spec, _, err := indexpackselection.DecodeSpec(specRaw)
	if err != nil {
		return DeltaResult{}, err
	}
	if _, _, err := indexpackselection.DecodeExclusions(exclusionsRaw); err != nil {
		return DeltaResult{}, err
	}

	parentPath, err := secureconfigfile.ValidateDirectory(options.ParentBundleDir)
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: validate parent bundle: %w", err)
	}
	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: anchor parent bundle: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, parentRoot.Close()) }()
	publicIdentity := PublicIdentity{PublisherID: options.Identity.publisherID, KeyID: options.Identity.keyID}
	parentVerification, parent, parentReport, err := verifySnapshotBundleFromRoot(ctx, parentRoot, publicIdentity, options.Identity.PublicKey())
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: verify parent snapshot: %w", err)
	}
	if err := validateDeltaSpec(spec, parent.Manifest, parentReport.Profile, options.Identity); err != nil {
		return DeltaResult{}, err
	}

	outputBase := filepath.Base(options.OutputDir)
	if outputBase == "." || outputBase == string(filepath.Separator) {
		return DeltaResult{}, errors.New("open pack delta builder: output must name a new bundle directory")
	}
	outputParent, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.OutputDir))
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: validate output parent: %w", err)
	}
	outputDir := filepath.Join(outputParent, outputBase)
	outputRoot, err := os.OpenRoot(outputParent)
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: anchor output parent: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, outputRoot.Close()) }()
	overlaps, err := anchoredDirectoryTreeContains(parentRoot, outputRoot)
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: verify parent/output isolation: %w", err)
	}
	if overlaps {
		return DeltaResult{}, errors.New("open pack delta builder: output parent resolves inside the signed parent bundle")
	}
	nameDigest := sha256.Sum256([]byte(outputBase))
	nameSuffix := hex.EncodeToString(nameDigest[:])
	lockName := ".fetchmark-pack-lock-" + nameSuffix
	lock, err := outputRoot.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: claim output lock without overwrite: %w", err)
	}
	if err := errors.Join(lock.Sync(), lock.Close()); err != nil {
		_ = outputRoot.Remove(lockName)
		return DeltaResult{}, fmt.Errorf("open pack delta builder: persist output lock: %w", err)
	}
	defer func() {
		if err := outputRoot.Remove(lockName); err != nil && !errors.Is(err, os.ErrNotExist) {
			returnErr = errors.Join(returnErr, fmt.Errorf("open pack delta builder: remove output lock: %w", err))
		}
		returnErr = errors.Join(returnErr, syncRoot(outputRoot))
	}()
	if _, err := outputRoot.Lstat(outputBase); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return DeltaResult{}, errors.New("open pack delta builder: output already exists")
		}
		return DeltaResult{}, fmt.Errorf("open pack delta builder: inspect output: %w", err)
	}
	stagingName := ".fetchmark-pack-build-" + nameSuffix
	if err := outputRoot.Mkdir(stagingName, 0o700); err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: create private staging directory: %w", err)
	}
	stagingRoot, err := outputRoot.OpenRoot(stagingName)
	if err != nil {
		_ = outputRoot.RemoveAll(stagingName)
		return DeltaResult{}, fmt.Errorf("open pack delta builder: anchor private staging directory: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, stagingRoot.Close()) }()
	stagingCreated := true
	defer func() {
		if stagingCreated {
			returnErr = errors.Join(returnErr, removeStaging(outputRoot, stagingName))
		}
	}()

	store, err := openDeltaStore(stagingRoot)
	if err != nil {
		return DeltaResult{}, err
	}
	storeOpen := true
	defer func() {
		if storeOpen {
			returnErr = errors.Join(returnErr, store.Close())
		}
		if err := stagingRoot.Remove(deltaStateFilename); err != nil && !errors.Is(err, os.ErrNotExist) {
			returnErr = errors.Join(returnErr, fmt.Errorf("open pack delta builder: remove private state: %w", err))
		}
	}()
	if err := loadSnapshotRecords(ctx, parentRoot, parent.Manifest, store, parentBucket); err != nil {
		return DeltaResult{}, err
	}
	targetWriter, err := newBucketWriter(store, targetBucket)
	if err != nil {
		return DeltaResult{}, err
	}
	selection, err := indexpackselection.Select(ctx, now().UTC(), specRaw, exclusionsRaw, options.Candidates, targetWriter.Put)
	if err != nil {
		_ = targetWriter.Close()
		return DeltaResult{}, err
	}
	if err := targetWriter.Close(); err != nil {
		return DeltaResult{}, err
	}
	if err := indexpackselection.ValidateCompletion(selection, now().UTC()); err != nil {
		return DeltaResult{}, err
	}

	writer := newShardWriter(ctx, stagingRoot, indexpack.KindDelta)
	defer writer.abort()
	operations, err := emitDeltaOperations(ctx, store, writer.add, selection.Accepted)
	if err != nil {
		return DeltaResult{}, err
	}
	if err := writer.finish(); err != nil {
		return DeltaResult{}, err
	}
	if err := store.Close(); err != nil {
		return DeltaResult{}, err
	}
	storeOpen = false
	if err := stagingRoot.Remove(deltaStateFilename); err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: remove private state: %w", err)
	}

	descriptors := append([]indexpack.Shard(nil), writer.descriptors...)
	sort.Slice(descriptors, func(left, right int) bool { return descriptors[left].Path < descriptors[right].Path })
	manifest := indexpack.Manifest{
		Version: indexpack.VersionURLMetadata, Kind: indexpack.KindDelta,
		PackID: spec.PackID, Revision: spec.Revision, CreatedAt: spec.CreatedAt, ExpiresAt: spec.ExpiresAt,
		ParentManifestSHA256: parent.Digest, SigningKeyID: options.Identity.keyID,
		Publisher: spec.Publisher, Policy: parent.Manifest.Policy, Languages: append([]string(nil), parent.Manifest.Languages...),
		RecordCount: operations.Total, Shards: descriptors,
		Build: indexpack.Build{
			Generator: "fetchmark-pack-build-delta", GeneratorVersion: options.BuilderVersion,
			Analyzer: "url-derived-lexical", AnalyzerVersion: "1",
			PolicySHA256: selection.PolicySHA256, ExclusionsSHA256: selection.ExclusionsSHA256,
			CandidateSHA256: selection.CandidateSHA256, Inputs: spec.Inputs,
		},
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: encode manifest: %w", err)
	}
	manifestSignature, err := indexpack.SignManifest(manifestRaw, options.Identity.privateKey)
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: sign manifest: %w", err)
	}
	manifestSignatureRaw, err := indexpack.EncodeSignature(manifestSignature)
	if err != nil {
		return DeltaResult{}, err
	}
	deltaVerified, err := indexpack.VerifyManifest(manifestRaw, manifestSignatureRaw, map[string]ed25519.PublicKey{options.Identity.keyID: options.Identity.PublicKey()})
	if err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: self-verify manifest signature: %w", err)
	}
	if err := indexpack.ValidateDeltaTransition(parent, deltaVerified); err != nil {
		return DeltaResult{}, err
	}
	if err := writeExclusive(stagingRoot, ManifestFilename, manifestRaw); err != nil {
		return DeltaResult{}, err
	}
	if err := writeExclusive(stagingRoot, SignatureFilename, manifestSignatureRaw); err != nil {
		return DeltaResult{}, err
	}

	ordinary := spec.Profile == indexpackselection.ProfileLightweight
	var projectionBytes uint64
	if ordinary {
		projectionBytes, err = verifyOrdinaryDelta(ctx, parentRoot, stagingRoot, parent, deltaVerified, selection.Accepted, options.Identity.PublicKey())
		if err != nil {
			return DeltaResult{}, fmt.Errorf("open pack delta builder: lightweight consumer gate: %w", err)
		}
	}
	report := DeltaBuildReport{
		Version: DeltaBuildReportVersion, Builder: "fetchmark-pack-build", BuilderVersion: options.BuilderVersion,
		Profile: spec.Profile, OrdinaryInstallable: ordinary, PublisherID: options.Identity.publisherID,
		KeyID: options.Identity.keyID, ManifestSHA256: deltaVerified.Digest, ParentManifestSHA256: parent.Digest,
		PackID: spec.PackID, Revision: spec.Revision, CreatedAt: spec.CreatedAt, TargetSelection: selection,
		Operations: operations, Shards: writer.evidence, OperationStreamSHA256: hex.EncodeToString(writer.selectedHasher.Sum(nil)),
	}
	if err := report.validate(); err != nil {
		return DeltaResult{}, err
	}
	reportRaw, err := json.Marshal(report)
	if err != nil {
		return DeltaResult{}, err
	}
	reportSignatureRaw, err := signDeltaReport(reportRaw, options.Identity)
	if err != nil {
		return DeltaResult{}, err
	}
	if err := VerifyDeltaBuildReport(reportRaw, reportSignatureRaw, options.Identity.PublicKey()); err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: self-verify build report signature: %w", err)
	}
	if err := writeExclusive(stagingRoot, BuildReportFilename, reportRaw); err != nil {
		return DeltaResult{}, err
	}
	if err := writeExclusive(stagingRoot, ReportSignatureFilename, reportSignatureRaw); err != nil {
		return DeltaResult{}, err
	}
	if err := syncBundleDirectories(stagingRoot); err != nil {
		return DeltaResult{}, err
	}
	if err := indexpackselection.ValidateCompletion(selection, now().UTC()); err != nil {
		return DeltaResult{}, err
	}
	expiresAt, _ := time.Parse(time.RFC3339, spec.ExpiresAt)
	if !now().UTC().Before(expiresAt) {
		return DeltaResult{}, errors.New("open pack delta builder: target manifest expired before activation")
	}
	if err := ctx.Err(); err != nil {
		return DeltaResult{}, err
	}
	if err := hooks.checkRoot(parentRoot, parentPath); err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: parent bundle path changed: %w", err)
	}
	if err := hooks.checkRoot(outputRoot, outputParent); err != nil {
		return DeltaResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return DeltaResult{}, err
	}
	candidateResult := DeltaResult{
		Path: outputDir, PackID: spec.PackID, Revision: spec.Revision,
		ParentManifestSHA256: parentVerification.ManifestSHA256, ManifestSHA256: deltaVerified.Digest,
		BuildReportSHA256: indexpack.Digest(reportRaw), OperationCount: operations.Total,
		ProjectionRecordCount: selection.Accepted, UpsertCount: operations.Upserts, TombstoneCount: operations.Tombstones,
		ShardCount: len(descriptors), OrdinaryInstallable: ordinary, ProjectionBytes: projectionBytes,
	}
	if err := hooks.activate(outputRoot, stagingName, outputBase); err != nil {
		return DeltaResult{}, fmt.Errorf("open pack delta builder: atomically activate bundle: %w", err)
	}
	stagingCreated = false
	committed = true
	result = candidateResult
	if err := hooks.syncOutputParent(outputRoot); err != nil {
		return result, fmt.Errorf("open pack delta builder: sync activated bundle parent: %w", err)
	}
	if err := hooks.checkRoot(outputRoot, outputParent); err != nil {
		return result, err
	}
	return result, nil
}

// DeltaCommittedError preserves enough non-secret identity for an operator to
// verify an already-created bundle instead of blindly retrying or deleting it.
func DeltaCommittedError(result DeltaResult, cause error) error {
	if cause == nil {
		return nil
	}
	return fmt.Errorf(
		"%w: output=%q parent_manifest_sha256=%s manifest_sha256=%s build_report_sha256=%s: %w; run fetchmark-pack-build verify-delta-run against the exact parent and output before retrying or removing anything",
		ErrDeltaBuildCommitted, result.Path, result.ParentManifestSHA256, result.ManifestSHA256, result.BuildReportSHA256, cause,
	)
}

func validateDeltaSpec(spec indexpackselection.Spec, parent indexpack.Manifest, parentProfile string, identity Identity) error {
	if parentProfile != spec.Profile {
		return errors.New("open pack delta builder: target profile must match the parent snapshot")
	}
	if parent.Revision == ^uint64(0) || spec.Revision != parent.Revision+1 || spec.PackID != parent.PackID {
		return errors.New("open pack delta builder: target pack ID and revision must immediately follow the parent")
	}
	if spec.Publisher != parent.Publisher || !slices.Equal(spec.Languages, parent.Languages) {
		return errors.New("open pack delta builder: publisher and languages must match the parent snapshot")
	}
	if parent.SigningKeyID != identity.keyID {
		return errors.New("open pack delta builder: parent signing key differs from publisher identity")
	}
	created, _ := time.Parse(time.RFC3339, spec.CreatedAt)
	expires, _ := time.Parse(time.RFC3339, spec.ExpiresAt)
	parentCreated, _ := time.Parse(time.RFC3339, parent.CreatedAt)
	parentExpires, _ := time.Parse(time.RFC3339, parent.ExpiresAt)
	if created.Before(parentCreated) || !created.Before(parentExpires) || expires.After(parentExpires) {
		return errors.New("open pack delta builder: target validity must remain inside the parent validity window")
	}
	return nil
}

func pathsOverlap(left, right string) bool {
	contains := func(parent, child string) bool {
		relative, err := filepath.Rel(parent, child)
		return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
	}
	return contains(left, right) || contains(right, left)
}

func anchoredDirectoryTreeContains(root, target *os.Root) (bool, error) {
	if root == nil || target == nil {
		return false, errors.New("open pack delta builder: directory roots are required")
	}
	targetInfo, err := target.Stat(".")
	if err != nil {
		return false, err
	}
	const maximumDirectories = indexpack.MaxManifestShards + 16
	queue := []string{"."}
	visited := 0
	for len(queue) > 0 {
		relative := queue[0]
		queue = queue[1:]
		visited++
		if visited > maximumDirectories {
			return false, fmt.Errorf("parent bundle contains more than %d directories", maximumDirectories)
		}
		info, err := root.Stat(relative)
		if err != nil {
			return false, err
		}
		if os.SameFile(info, targetInfo) {
			return true, nil
		}
		directory, err := root.Open(relative)
		if err != nil {
			return false, err
		}
		for {
			entries, readErr := directory.ReadDir(128)
			for _, entry := range entries {
				if entry.Type()&os.ModeSymlink != 0 {
					continue
				}
				entryInfo, infoErr := entry.Info()
				if infoErr != nil {
					_ = directory.Close()
					return false, infoErr
				}
				if entryInfo.IsDir() {
					queue = append(queue, filepath.Join(relative, entry.Name()))
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = directory.Close()
				return false, readErr
			}
		}
		if err := directory.Close(); err != nil {
			return false, err
		}
	}
	return false, nil
}

func openDeltaStore(root *os.Root) (*bolt.DB, error) {
	if root == nil {
		return nil, errors.New("open pack delta builder: staging root is required")
	}
	db, err := bolt.Open(deltaStateFilename, 0o600, &bolt.Options{
		OpenFile: func(_ string, flags int, mode os.FileMode) (*os.File, error) {
			return root.OpenFile(deltaStateFilename, flags, mode)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open pack delta builder: open private state: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket(parentBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucket(targetBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open pack delta builder: initialize private state: %w", err)
	}
	return db, nil
}

type bucketWriter struct {
	db     *bolt.DB
	bucket []byte
	tx     *bolt.Tx
	count  int
}

func newBucketWriter(db *bolt.DB, bucket []byte) (*bucketWriter, error) {
	writer := &bucketWriter{db: db, bucket: bucket}
	if err := writer.begin(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (writer *bucketWriter) begin() error {
	tx, err := writer.db.Begin(true)
	if err != nil {
		return err
	}
	writer.tx = tx
	writer.count = 0
	return nil
}

func (writer *bucketWriter) Put(record indexpack.Record) error {
	if writer.tx == nil {
		return errors.New("open pack delta builder: record store writer is closed")
	}
	bucket := writer.tx.Bucket(writer.bucket)
	key := []byte(record.URL)
	if bucket.Get(key) != nil {
		return fmt.Errorf("open pack delta builder: duplicate URL in %s state: %q", writer.bucket, record.URL)
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := bucket.Put(key, raw); err != nil {
		return err
	}
	writer.count++
	if writer.count < deltaBatchSize {
		return nil
	}
	if err := writer.tx.Commit(); err != nil {
		writer.tx = nil
		return err
	}
	writer.tx = nil
	return writer.begin()
}

func (writer *bucketWriter) Close() error {
	if writer.tx == nil {
		return nil
	}
	err := writer.tx.Commit()
	writer.tx = nil
	return err
}

func loadSnapshotRecords(ctx context.Context, root *os.Root, manifest indexpack.Manifest, db *bolt.DB, bucket []byte) error {
	writer, err := newBucketWriter(db, bucket)
	if err != nil {
		return err
	}
	for _, descriptor := range manifest.Shards {
		if err := scanBuilderSnapshotShard(ctx, root, descriptor, manifest.Build.Inputs[0].Name, writer.Put); err != nil {
			_ = writer.Close()
			return err
		}
	}
	return writer.Close()
}

func scanBuilderSnapshotShard(ctx context.Context, root *os.Root, descriptor indexpack.Shard, expectedSource string, visit func(indexpack.Record) error) error {
	file, err := openRegularExact(root, filepath.FromSlash(descriptor.Path), descriptor.CompressedSizeBytes)
	if err != nil {
		return err
	}
	if err := indexpack.VerifyCompressedShard(file, descriptor); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return err
	}
	decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(indexpack.MaxUncompressedShardBytes))
	if err != nil {
		_ = file.Close()
		return err
	}
	err = indexpack.ScanRecordStreamVersion(ctx, decoder, indexpack.VersionURLMetadata, indexpack.KindSnapshot, descriptor, func(record indexpack.Record) error {
		if err := validateBuilderRecordProvenance(record, expectedSource); err != nil {
			return err
		}
		return visit(record)
	})
	decoder.Close()
	return errors.Join(err, file.Close())
}

func emitDeltaOperations(ctx context.Context, db *bolt.DB, emit func(indexpack.Record) error, finalCount uint64) (DeltaOperationReport, error) {
	if ctx == nil || db == nil || emit == nil || finalCount == 0 {
		return DeltaOperationReport{}, errors.New("open pack delta builder: delta comparison inputs are required")
	}
	report := DeltaOperationReport{ProjectionRecordCount: finalCount}
	err := db.View(func(tx *bolt.Tx) error {
		parentCursor := tx.Bucket(parentBucket).Cursor()
		targetCursor := tx.Bucket(targetBucket).Cursor()
		parentKey, parentValue := parentCursor.First()
		targetKey, targetValue := targetCursor.First()
		for parentKey != nil || targetKey != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			switch {
			case targetKey == nil || (parentKey != nil && bytes.Compare(parentKey, targetKey) < 0):
				if err := emit(indexpack.Record{Operation: indexpack.OperationTombstone, URL: string(parentKey)}); err != nil {
					return err
				}
				report.Tombstones++
				parentKey, parentValue = parentCursor.Next()
			case parentKey == nil || bytes.Compare(targetKey, parentKey) < 0:
				record, err := decodeStoredRecord(targetValue)
				if err != nil {
					return err
				}
				if err := emit(record); err != nil {
					return err
				}
				report.Upserts++
				targetKey, targetValue = targetCursor.Next()
			default:
				if !bytes.Equal(parentValue, targetValue) {
					record, err := decodeStoredRecord(targetValue)
					if err != nil {
						return err
					}
					if err := emit(record); err != nil {
						return err
					}
					report.Upserts++
				}
				parentKey, parentValue = parentCursor.Next()
				targetKey, targetValue = targetCursor.Next()
			}
		}
		return nil
	})
	if err != nil {
		return DeltaOperationReport{}, err
	}
	report.Total = report.Upserts + report.Tombstones
	if report.Total == 0 {
		return DeltaOperationReport{}, errors.New("open pack delta builder: target snapshot is identical to its parent")
	}
	return report, nil
}

func decodeStoredRecord(raw []byte) (indexpack.Record, error) {
	var record indexpack.Record
	if err := indexpack.DecodeStrictJSON(raw, &record); err != nil {
		return indexpack.Record{}, err
	}
	return record, nil
}

func verifyOrdinaryDelta(
	ctx context.Context,
	parentRoot, deltaRoot *os.Root,
	parent, delta indexpack.VerifiedManifest,
	projectionCount uint64,
	publicKey ed25519.PublicKey,
) (projectionBytes uint64, returnErr error) {
	root, err := os.MkdirTemp("", "fetchmark-pack-delta-verify-")
	if err != nil {
		return 0, err
	}
	defer func() { returnErr = errors.Join(returnErr, os.RemoveAll(root)) }()
	createdAt, err := time.Parse(time.RFC3339, delta.Manifest.CreatedAt)
	if err != nil {
		return 0, err
	}
	installed, err := openpackindex.ApplyDeltaFromRoots(ctx, parentRoot, deltaRoot, openpackindex.DeltaInstallOptions{
		Root: root, TrustedKeys: map[string]ed25519.PublicKey{delta.KeyID: publicKey},
		DeltaAcceptance: indexpack.Acceptance{
			Now: createdAt, ExpectedPackID: delta.Manifest.PackID, ExpectedManifestSHA256: delta.Digest,
			ExpectedKeyID: delta.KeyID, MinimumRevision: delta.Manifest.Revision, ExpectedRevision: delta.Manifest.Revision,
			ExpectedRecordCount: delta.Manifest.RecordCount, ExpectedCreatedAt: delta.Manifest.CreatedAt, ExpectedExpiresAt: delta.Manifest.ExpiresAt,
		},
		ExpectedParentManifestSHA256: parent.Digest, ExpectedProjectionRecordCount: projectionCount,
		MaxProjectionBytes: openpackindex.MaxProjectionBytes,
	})
	if err != nil {
		return 0, err
	}
	return installed.ProjectionBytes, nil
}

func signDeltaReport(reportRaw []byte, identity Identity) ([]byte, error) {
	message := append([]byte(DeltaBuildReportSignatureDomain), reportRaw...)
	signature := indexpack.DetachedSignature{
		Version: indexpack.SignatureVersion, Algorithm: indexpack.SignatureAlgorithm,
		KeyID: identity.keyID, Signature: encodeSignatureBytes(ed25519.Sign(identity.privateKey, message)),
	}
	return indexpack.EncodeSignature(signature)
}

func VerifyDeltaBuildReport(reportRaw, signatureRaw []byte, publicKey ed25519.PublicKey) error {
	signature, err := indexpack.DecodeSignature(signatureRaw)
	if err != nil {
		return err
	}
	if len(publicKey) != ed25519.PublicKeySize || signature.KeyID != indexpack.KeyID(publicKey) {
		return errors.New("open pack delta builder: report signing key mismatch")
	}
	decoded, err := decodeSignatureBytes(signature.Signature)
	if err != nil {
		return err
	}
	message := append([]byte(DeltaBuildReportSignatureDomain), reportRaw...)
	if !ed25519.Verify(publicKey, message, decoded) {
		return errors.New("open pack delta builder: report signature verification failed")
	}
	var report DeltaBuildReport
	if err := indexpack.DecodeStrictJSON(reportRaw, &report); err != nil {
		return fmt.Errorf("open pack delta builder: invalid report: %w", err)
	}
	if err := report.validate(); err != nil {
		return err
	}
	if report.KeyID != signature.KeyID {
		return errors.New("open pack delta builder: report identity is invalid")
	}
	return nil
}

func (report DeltaBuildReport) validate() error {
	if report.Version != DeltaBuildReportVersion || report.Builder != "fetchmark-pack-build" ||
		strings.TrimSpace(report.BuilderVersion) != report.BuilderVersion || report.BuilderVersion == "" || len(report.BuilderVersion) > 128 {
		return errors.New("open pack delta builder: report builder identity is invalid")
	}
	if !publisherIDPattern.MatchString(report.PublisherID) || !publisherIDPattern.MatchString(report.PackID) || report.Revision < 2 ||
		!validDigest(report.KeyID) || !validDigest(report.ManifestSHA256) || !validDigest(report.ParentManifestSHA256) || !validDigest(report.OperationStreamSHA256) {
		return errors.New("open pack delta builder: report identity is invalid")
	}
	createdAt, err := time.Parse(time.RFC3339, report.CreatedAt)
	if err != nil || createdAt.UTC().Format(time.RFC3339) != report.CreatedAt {
		return errors.New("open pack delta builder: report created_at is invalid")
	}
	selection := report.TargetSelection
	permissionValidUntil, permissionErr := time.Parse(time.RFC3339, selection.PermissionValidUntil)
	if selection.Version != indexpackselection.ReportVersion || !validDigest(selection.PolicySHA256) || !validDigest(selection.ExclusionsSHA256) || !validDigest(selection.CandidateSHA256) ||
		selection.Accepted == 0 || selection.UniqueHosts == 0 || selection.UniqueHosts > selection.Accepted || selection.PermissionOldestAge > indexpackselection.MaxPermissionAgeHours ||
		permissionErr != nil || permissionValidUntil.UTC().Format(time.RFC3339) != selection.PermissionValidUntil || !permissionValidUntil.After(createdAt) ||
		selection.Candidates < selection.Accepted || selection.Candidates-selection.Accepted != selection.Rejected ||
		sumCounts(selection.RejectedByReason) != selection.Rejected || sumCounts(selection.AcceptedByLanguage) != selection.Accepted {
		return errors.New("open pack delta builder: report target selection accounting is invalid")
	}
	operations := report.Operations
	if operations.Total == 0 || operations.Total != operations.Upserts+operations.Tombstones || operations.ProjectionRecordCount != selection.Accepted {
		return errors.New("open pack delta builder: report operation accounting is invalid")
	}
	switch report.Profile {
	case indexpackselection.ProfileLightweight:
		if !report.OrdinaryInstallable || selection.Accepted > indexpack.RecommendedInstallRecordLimit || operations.Total > indexpack.RecommendedInstallRecordLimit || len(report.Shards) != 1 {
			return errors.New("open pack delta builder: lightweight report exceeds the ordinary consumer profile")
		}
	case indexpackselection.ProfileFormatPrototype:
		if report.OrdinaryInstallable || selection.Accepted > 5_000_000 {
			return errors.New("open pack delta builder: format prototype report has invalid consumer claims")
		}
	default:
		return errors.New("open pack delta builder: report profile is invalid")
	}
	if len(report.Shards) == 0 || len(report.Shards) > indexpack.MaxManifestShards {
		return errors.New("open pack delta builder: report shard count is invalid")
	}
	seen := make(map[string]struct{}, len(report.Shards))
	var records uint64
	for _, shard := range report.Shards {
		if shard.Path != indexpack.ShardPath(shard.SHA256) || !validDigest(shard.SHA256) || !validDigest(shard.UncompressedSHA256) ||
			shard.RecordCount == 0 || shard.RecordCount > indexpack.MaxShardRecords || shard.CompressedSizeBytes == 0 || shard.CompressedSizeBytes > indexpack.MaxCompressedShardBytes ||
			shard.UncompressedSizeBytes == 0 || shard.UncompressedSizeBytes > indexpack.MaxUncompressedShardBytes {
			return errors.New("open pack delta builder: report shard evidence is invalid")
		}
		if _, duplicate := seen[shard.Path]; duplicate {
			return errors.New("open pack delta builder: report repeats a shard")
		}
		seen[shard.Path] = struct{}{}
		if records > indexpack.MaxPackRecords-shard.RecordCount {
			return errors.New("open pack delta builder: report shard record count overflows")
		}
		records += shard.RecordCount
	}
	if records != operations.Total {
		return errors.New("open pack delta builder: report shard records differ from operations")
	}
	return nil
}
