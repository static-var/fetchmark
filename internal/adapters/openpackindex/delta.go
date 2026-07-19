package openpackindex

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/blevesearch/bleve/v2"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

const (
	stagedParentShardFilename = "verified-parent-shard.zst"
	stagedDeltaShardFilename  = "verified-delta-shard.zst"
)

// DeltaInstallOptions binds a signed delta to one exact signed snapshot and
// one operator-pinned final projection count. ApplyDelta always builds a new
// immutable object; it never reads or mutates an installed Bleve projection.
type DeltaInstallOptions struct {
	ParentBundleDir               string
	BundleDir                     string
	Root                          string
	TrustedKeys                   map[string]ed25519.PublicKey
	DeltaAcceptance               indexpack.Acceptance
	ExpectedParentManifestSHA256  string
	ExpectedProjectionRecordCount uint64
	MaxProjectionBytes            uint64
}

// ApplyDelta re-verifies the exact parent snapshot and delta bundles, then
// materializes their resulting state under objects/<delta manifest digest>.
func ApplyDelta(ctx context.Context, options DeltaInstallOptions) (Installed, error) {
	if ctx == nil {
		return Installed{}, errors.New("open pack index: context is required")
	}
	if err := ctx.Err(); err != nil {
		return Installed{}, err
	}
	parentDir, err := requireSecureExistingDirectory(options.ParentBundleDir)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: parent bundle: %w", err)
	}
	deltaDir, err := requireSecureExistingDirectory(options.BundleDir)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: delta bundle: %w", err)
	}
	parentRoot, err := os.OpenRoot(parentDir)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: anchor parent bundle: %w", err)
	}
	deltaRoot, err := os.OpenRoot(deltaDir)
	if err != nil {
		return Installed{}, errors.Join(fmt.Errorf("open pack index: anchor delta bundle: %w", err), parentRoot.Close())
	}
	return applyDeltaAndClose(ctx, parentRoot, deltaRoot, options, func() error {
		return errors.Join(deltaRoot.Close(), parentRoot.Close())
	}, os.RemoveAll, verifyClosedProjection, measureProjectionBytes, productionInstallPolicy())
}

// ApplyDeltaFromRoots verifies and materializes a delta through caller-owned,
// descriptor-anchored bundle roots. It is intended for trusted offline
// publishers that must prove a just-built delta before activating its bundle.
// The caller retains ownership of both roots and BundleDir/ParentBundleDir must
// be empty so path-based input cannot be mixed with anchored input.
func ApplyDeltaFromRoots(ctx context.Context, parentRoot, deltaRoot *os.Root, options DeltaInstallOptions) (Installed, error) {
	if options.ParentBundleDir != "" || options.BundleDir != "" {
		return Installed{}, errors.New("open pack index: bundle paths must be empty for descriptor-anchored delta install")
	}
	return applyDeltaFromRoots(
		ctx, parentRoot, deltaRoot, options, os.RemoveAll, verifyClosedProjection,
		measureProjectionBytes, productionInstallPolicy(),
	)
}

func applyDeltaAndClose(
	ctx context.Context,
	parentRoot, deltaRoot *os.Root,
	options DeltaInstallOptions,
	closeRoots func() error,
	removeAll func(string) error,
	verifyProjection func(string, OpenOptions, uint64) error,
	measureProjection func(context.Context, string, uint64) (uint64, error),
	policy installPolicy,
) (Installed, error) {
	if closeRoots == nil {
		return Installed{}, errors.New("open pack index: bundle cleanup is required")
	}
	installed, applyErr := applyDeltaFromRoots(
		ctx, parentRoot, deltaRoot, options, removeAll, verifyProjection, measureProjection, policy,
	)
	closeErr := closeRoots()
	if applyErr != nil {
		return Installed{}, errors.Join(applyErr, closeErr)
	}
	// A successful return from applyDeltaFromRoots means the no-replace rename
	// committed the immutable object. A descriptor-close error cannot undo that
	// commit and must not turn it into an ambiguous reported failure.
	return installed, nil
}

func applyDeltaFromRoots(
	ctx context.Context,
	parentRoot, deltaRoot *os.Root,
	options DeltaInstallOptions,
	removeAll func(string) error,
	verifyProjection func(string, OpenOptions, uint64) error,
	measureProjection func(context.Context, string, uint64) (uint64, error),
	policy installPolicy,
) (installed Installed, returnErr error) {
	if ctx == nil || removeAll == nil || verifyProjection == nil || measureProjection == nil {
		return Installed{}, errors.New("open pack index: delta dependencies are incomplete")
	}
	if err := policy.validate(); err != nil {
		return Installed{}, err
	}
	if err := validateBundleRoot(parentRoot); err != nil {
		return Installed{}, fmt.Errorf("open pack index: parent bundle: %w", err)
	}
	if err := validateBundleRoot(deltaRoot); err != nil {
		return Installed{}, fmt.Errorf("open pack index: delta bundle: %w", err)
	}
	if !validDigest(options.ExpectedParentManifestSHA256) {
		return Installed{}, errors.New("open pack index: expected parent manifest digest is required")
	}
	if options.ExpectedProjectionRecordCount == 0 || options.ExpectedProjectionRecordCount > policy.maxRecords {
		return Installed{}, fmt.Errorf("%w: projected records must be 1..%d", ErrInstallLimit, policy.maxRecords)
	}
	projectionByteLimit := options.MaxProjectionBytes
	if projectionByteLimit == 0 {
		projectionByteLimit = policy.maxProjectionBytes
	}
	if projectionByteLimit > policy.maxProjectionBytes {
		return Installed{}, fmt.Errorf("%w: projection bytes %d > %d", ErrInstallLimit, projectionByteLimit, policy.maxProjectionBytes)
	}
	parent, err := verifyBundleManifest(parentRoot, options.TrustedKeys, nil)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: verify parent manifest: %w", err)
	}
	if parent.Digest != options.ExpectedParentManifestSHA256 {
		return Installed{}, errors.New("open pack index: parent manifest does not match operator expectation")
	}
	delta, err := verifyBundleManifest(deltaRoot, options.TrustedKeys, &options.DeltaAcceptance)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: verify delta manifest: %w", err)
	}
	if err := indexpack.ValidateDeltaTransition(parent, delta); err != nil {
		return Installed{}, err
	}
	now := options.DeltaAcceptance.Now.UTC()
	parentCreated, _ := time.Parse(time.RFC3339, parent.Manifest.CreatedAt)
	parentExpires, _ := time.Parse(time.RFC3339, parent.Manifest.ExpiresAt)
	if now.Before(parentCreated) || !now.Before(parentExpires) {
		return Installed{}, fmt.Errorf("%w: parent snapshot is outside its validity window", ErrOutsideValidity)
	}
	if err := preflight(parent.Manifest, policy); err != nil {
		return Installed{}, fmt.Errorf("open pack index: parent preflight: %w", err)
	}
	if delta.Manifest.Kind != indexpack.KindDelta {
		return Installed{}, errors.New("open pack index: target manifest is not a delta")
	}
	if err := preflightResources(delta.Manifest, policy); err != nil {
		return Installed{}, fmt.Errorf("open pack index: delta preflight: %w", err)
	}
	if err := preflightCombinedDeltaResources(parent.Manifest, delta.Manifest, policy); err != nil {
		return Installed{}, err
	}

	root, err := prepareSecureRoot(options.Root)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: root: %w", err)
	}
	objects := filepath.Join(root, "objects")
	if err := ensurePrivateDirectory(objects); err != nil {
		return Installed{}, err
	}
	destination := filepath.Join(objects, delta.Digest)
	if err := ensureMissing(destination); err != nil {
		return Installed{}, err
	}
	temporary, err := os.MkdirTemp(objects, ".delta-"+delta.Digest[:12]+"-")
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: create private delta staging directory: %w", err)
	}
	if err := os.Chmod(temporary, 0o700); err != nil {
		return Installed{}, errors.Join(fmt.Errorf("open pack index: secure delta staging directory: %w", err), removeAll(temporary))
	}
	activated := false
	defer func() {
		if !activated {
			if err := removeAll(temporary); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("open pack index: remove delta staging directory: %w", err))
			}
		}
	}()

	deltaRecords, err := readDeltaRecords(ctx, deltaRoot, temporary, delta.Manifest, policy)
	if err != nil {
		return Installed{}, err
	}
	shadow := make(map[string]int, len(deltaRecords))
	for index, record := range deltaRecords {
		shadow[record.URL] = index
	}
	matched := make([]bool, len(deltaRecords))

	projectionPath := filepath.Join(temporary, projectionDirectoryName)
	projection, err := bleve.New(projectionPath, projectionMapping())
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: create delta projection: %w", err)
	}
	projectionOpen := true
	defer func() {
		if projectionOpen {
			_ = projection.Close()
		}
	}()
	writer := newDeltaProjectionWriter(projection)
	for _, descriptor := range parent.Manifest.Shards {
		stagedPath := filepath.Join(temporary, stagedParentShardFilename)
		if err := stageVerifiedShard(ctx, parentRoot, stagedPath, descriptor); err != nil {
			return Installed{}, err
		}
		if err := validateSingleZstdFrame(stagedPath, descriptor); err != nil {
			return Installed{}, err
		}
		err = scanStagedShard(ctx, stagedPath, parent.Manifest.Version, parent.Manifest.Kind, descriptor, policy.maxDecoderMemoryBytes, func(record indexpack.Record) error {
			if index, replaced := shadow[record.URL]; replaced {
				matched[index] = true
				return nil
			}
			return writer.index(record)
		})
		if err != nil {
			return Installed{}, err
		}
		if err := os.Remove(stagedPath); err != nil {
			return Installed{}, fmt.Errorf("open pack index: remove parent staging shard: %w", err)
		}
	}
	for index, record := range deltaRecords {
		if record.Operation == indexpack.OperationTombstone {
			if !matched[index] {
				return Installed{}, fmt.Errorf("open pack index: delta tombstone URL is absent from parent: %q", record.URL)
			}
			continue
		}
		if err := writer.index(record); err != nil {
			return Installed{}, err
		}
	}
	if err := writer.flush(); err != nil {
		return Installed{}, err
	}
	if writer.records != options.ExpectedProjectionRecordCount {
		return Installed{}, fmt.Errorf("open pack index: projected %d records, expected %d", writer.records, options.ExpectedProjectionRecordCount)
	}
	marker := projectionMarker{
		MarkerKind: "open-pack-projection", SchemaVersion: projectionSchemaVersion,
		ManifestSHA256: delta.Digest, PackID: delta.Manifest.PackID,
		Revision: strconv.FormatUint(delta.Manifest.Revision, 10), RecordCount: strconv.FormatUint(writer.records, 10),
		SigningKeyID: delta.KeyID, CreatedAt: delta.Manifest.CreatedAt, ExpiresAt: delta.Manifest.ExpiresAt,
	}
	if err := projection.Index(markerDocumentID, marker); err != nil {
		return Installed{}, fmt.Errorf("open pack index: write delta projection marker: %w", err)
	}
	if err := projection.Close(); err != nil {
		return Installed{}, fmt.Errorf("open pack index: close delta projection: %w", err)
	}
	projectionOpen = false
	if err := ctx.Err(); err != nil {
		return Installed{}, err
	}
	if err := verifyProjection(projectionPath, OpenOptions{
		Path: temporary, ExpectedManifestSHA256: delta.Digest, ExpectedPackID: delta.Manifest.PackID,
		ExpectedRevision: delta.Manifest.Revision, ExpectedRecordCount: writer.records, ExpectedKeyID: delta.KeyID,
		ExpectedCreatedAt: delta.Manifest.CreatedAt, ExpectedExpiresAt: delta.Manifest.ExpiresAt, Now: now,
	}, policy.maxRecords); err != nil {
		return Installed{}, fmt.Errorf("open pack index: verify staged delta projection: %w", err)
	}
	projectionBytes, err := measureProjection(ctx, projectionPath, projectionByteLimit)
	if err != nil {
		return Installed{}, err
	}
	if err := ctx.Err(); err != nil {
		return Installed{}, err
	}
	if err := renameNoReplace(temporary, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return Installed{}, fmt.Errorf("%w: %s", ErrAlreadyExists, destination)
		}
		return Installed{}, fmt.Errorf("open pack index: activate delta projection: %w", err)
	}
	activated = true
	return Installed{
		Path: destination, ManifestSHA256: delta.Digest, PackID: delta.Manifest.PackID,
		Revision: delta.Manifest.Revision, RecordCount: writer.records, OperationCount: uint64(len(deltaRecords)),
		ProjectionBytes: projectionBytes,
	}, nil
}

func verifyBundleManifest(root *os.Root, trustedKeys map[string]ed25519.PublicKey, acceptance *indexpack.Acceptance) (indexpack.VerifiedManifest, error) {
	manifestRaw, err := readRegularFileFromRoot(root, ManifestFilename, MaxInstallManifestBytes)
	if err != nil {
		return indexpack.VerifiedManifest{}, err
	}
	signatureRaw, err := readRegularFileFromRoot(root, SignatureFilename, indexpack.MaxSignatureBytes)
	if err != nil {
		return indexpack.VerifiedManifest{}, err
	}
	if acceptance != nil {
		return indexpack.VerifyManifestFor(manifestRaw, signatureRaw, trustedKeys, *acceptance)
	}
	return indexpack.VerifyManifest(manifestRaw, signatureRaw, trustedKeys)
}

func readDeltaRecords(ctx context.Context, root *os.Root, temporary string, manifest indexpack.Manifest, policy installPolicy) ([]indexpack.Record, error) {
	records := make([]indexpack.Record, 0, manifest.RecordCount)
	for _, descriptor := range manifest.Shards {
		stagedPath := filepath.Join(temporary, stagedDeltaShardFilename)
		if err := stageVerifiedShard(ctx, root, stagedPath, descriptor); err != nil {
			return nil, err
		}
		if err := validateSingleZstdFrame(stagedPath, descriptor); err != nil {
			return nil, err
		}
		err := scanStagedShard(ctx, stagedPath, manifest.Version, manifest.Kind, descriptor, policy.maxDecoderMemoryBytes, func(record indexpack.Record) error {
			if record.Operation == indexpack.OperationUpsert {
				if err := enforceRecordCaps(record); err != nil {
					return err
				}
			}
			records = append(records, record)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if err := os.Remove(stagedPath); err != nil {
			return nil, fmt.Errorf("open pack index: remove delta staging shard: %w", err)
		}
	}
	if uint64(len(records)) != manifest.RecordCount {
		return nil, errors.New("open pack index: delta operation count changed")
	}
	return records, nil
}

type deltaProjectionWriter struct {
	projection bleve.Index
	batch      *bleve.Batch
	batchCount int
	records    uint64
}

func newDeltaProjectionWriter(projection bleve.Index) *deltaProjectionWriter {
	return &deltaProjectionWriter{projection: projection, batch: projection.NewBatch()}
}

func (writer *deltaProjectionWriter) index(record indexpack.Record) error {
	if err := enforceRecordCaps(record); err != nil {
		return err
	}
	document, err := makeStoredDocument(record)
	if err != nil {
		return err
	}
	if err := writer.batch.Index(documentID(record.URL), document); err != nil {
		return fmt.Errorf("open pack index: index delta materialization record: %w", err)
	}
	writer.batchCount++
	writer.records++
	if writer.batchCount == indexBatchSize {
		return writer.flush()
	}
	return nil
}

func (writer *deltaProjectionWriter) flush() error {
	if writer.batchCount == 0 {
		return nil
	}
	if err := writer.projection.Batch(writer.batch); err != nil {
		return fmt.Errorf("open pack index: write delta projection batch: %w", err)
	}
	writer.batch = writer.projection.NewBatch()
	writer.batchCount = 0
	return nil
}

func preflightCombinedDeltaResources(parent, delta indexpack.Manifest, policy installPolicy) error {
	parentCompressed, parentUncompressed := manifestResourceBytes(parent)
	deltaCompressed, deltaUncompressed := manifestResourceBytes(delta)
	if _, ok := checkedAdd(parentCompressed, deltaCompressed, policy.maxCompressedBytes); !ok {
		return fmt.Errorf("%w: parent and delta compressed aggregate exceeds %d bytes", ErrInstallLimit, policy.maxCompressedBytes)
	}
	if _, ok := checkedAdd(parentUncompressed, deltaUncompressed, policy.maxUncompressedBytes); !ok {
		return fmt.Errorf("%w: parent and delta uncompressed aggregate exceeds %d bytes", ErrInstallLimit, policy.maxUncompressedBytes)
	}
	return nil
}

func manifestResourceBytes(manifest indexpack.Manifest) (compressed, uncompressed uint64) {
	for _, shard := range manifest.Shards {
		compressed += shard.CompressedSizeBytes
		uncompressed += shard.UncompressedSizeBytes
	}
	return compressed, uncompressed
}
