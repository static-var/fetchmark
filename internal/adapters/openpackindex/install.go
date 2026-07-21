// Package openpackindex verifies signed local open-index packs and projects
// their discovery metadata into an immutable, CPU-only Bleve index.
package openpackindex

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/klauspost/compress/zstd"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

const (
	ManifestFilename  = "manifest.json"
	SignatureFilename = "manifest.ed25519"

	MaxInstallManifestBytes   = 64 << 10
	MaxInstallShards          = 1
	MaxInstallRecords         = 10_000
	MaxInstallRecordBytes     = 16 << 10
	MaxCompressedBytes        = 64 << 20
	MaxUncompressedBytes      = 256 << 20
	MaxProjectionBytes        = 512 << 20
	MaxInstallTitleBytes      = 512
	MaxInstallHeadings        = 16
	MaxInstallHeadingBytes    = 256
	MaxInstallAnchorTerms     = 32
	MaxInstallAnchorTermBytes = 128
	MaxInstallSalientSketch   = 1_024
	maxDecoderWindowBytes     = 64 << 20
	projectionSchemaVersion   = "2"
	projectionDirectoryName   = "index"
	stagedCompressedFilename  = "verified-shard.zst"
	markerDocumentID          = "__fetchmark_open_pack_schema__"
	indexBatchSize            = 512
)

var (
	ErrAlreadyExists   = errors.New("open pack index: projection already exists")
	ErrUnsafePath      = errors.New("open pack index: unsafe path")
	ErrInstallLimit    = errors.New("open pack index: install limit exceeded")
	ErrSchemaMismatch  = errors.New("open pack index: projection marker mismatch")
	ErrOutsideValidity = errors.New("open pack index: manifest is outside its validity window")
	ErrClosed          = errors.New("open pack index: closed")
)

// InstallOptions binds a local bundle to operator-selected trust and identity.
// BundleDir is a clean absolute path for Install and must be empty for
// InstallFromRoot. Root is always absolute and receives immutable projections
// at objects/<manifest sha256>/.
type InstallOptions struct {
	BundleDir          string
	Root               string
	TrustedKeys        map[string]ed25519.PublicKey
	Acceptance         indexpack.Acceptance
	MaxProjectionBytes uint64
}

type Installed struct {
	Path            string
	ManifestSHA256  string
	PackID          string
	Revision        uint64
	RecordCount     uint64
	OperationCount  uint64
	ProjectionBytes uint64
}

type installPolicy struct {
	maxShards             int
	maxRecords            uint64
	maxCompressedBytes    uint64
	maxUncompressedBytes  uint64
	maxProjectionBytes    uint64
	maxDecoderMemoryBytes uint64
}

func Install(ctx context.Context, options InstallOptions) (Installed, error) {
	return install(ctx, options, os.RemoveAll, verifyClosedProjection, productionInstallPolicy())
}

// InstallFromRoot verifies a bundle through a caller-owned descriptor-anchored
// root. It exists for trusted producers that must keep verification bound to a
// directory even if its path is renamed while an offline build is running.
// BundleDir must be empty; the caller retains ownership of bundleRoot.
func InstallFromRoot(ctx context.Context, bundleRoot *os.Root, options InstallOptions) (Installed, error) {
	if options.BundleDir != "" {
		return Installed{}, errors.New("open pack index: BundleDir must be empty for descriptor-anchored install")
	}
	return installFromRoot(ctx, bundleRoot, options, os.RemoveAll, verifyClosedProjection, productionInstallPolicy())
}

func productionInstallPolicy() installPolicy {
	return installPolicy{
		maxShards: MaxInstallShards, maxRecords: MaxInstallRecords,
		maxCompressedBytes: MaxCompressedBytes, maxUncompressedBytes: MaxUncompressedBytes,
		maxProjectionBytes: MaxProjectionBytes, maxDecoderMemoryBytes: MaxUncompressedBytes,
	}
}

func install(
	ctx context.Context,
	options InstallOptions,
	removeAll func(string) error,
	verifyProjection func(string, OpenOptions, uint64) error,
	policy installPolicy,
) (installed Installed, returnErr error) {
	if ctx == nil {
		return Installed{}, errors.New("open pack index: context is required")
	}
	if removeAll == nil {
		return Installed{}, errors.New("open pack index: staging cleanup is required")
	}
	if verifyProjection == nil {
		return Installed{}, errors.New("open pack index: staged projection verification is required")
	}
	if err := policy.validate(); err != nil {
		return Installed{}, err
	}
	if err := ctx.Err(); err != nil {
		return Installed{}, err
	}
	bundleDir, err := requireSecureExistingDirectory(options.BundleDir)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: bundle: %w", err)
	}
	bundleRoot, err := os.OpenRoot(bundleDir)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: anchor bundle: %w", err)
	}
	installed, installErr := installFromRoot(ctx, bundleRoot, options, removeAll, verifyProjection, policy)
	closeErr := bundleRoot.Close()
	if installErr != nil || closeErr != nil {
		return Installed{}, errors.Join(installErr, closeErr)
	}
	return installed, nil
}

func installFromRoot(
	ctx context.Context,
	bundleRoot *os.Root,
	options InstallOptions,
	removeAll func(string) error,
	verifyProjection func(string, OpenOptions, uint64) error,
	policy installPolicy,
) (installed Installed, returnErr error) {
	if ctx == nil {
		return Installed{}, errors.New("open pack index: context is required")
	}
	if removeAll == nil {
		return Installed{}, errors.New("open pack index: staging cleanup is required")
	}
	if verifyProjection == nil {
		return Installed{}, errors.New("open pack index: staged projection verification is required")
	}
	if err := policy.validate(); err != nil {
		return Installed{}, err
	}
	if err := ctx.Err(); err != nil {
		return Installed{}, err
	}
	if err := validateBundleRoot(bundleRoot); err != nil {
		return Installed{}, fmt.Errorf("open pack index: bundle: %w", err)
	}
	projectionByteLimit := options.MaxProjectionBytes
	if projectionByteLimit == 0 {
		projectionByteLimit = policy.maxProjectionBytes
	}
	if projectionByteLimit > policy.maxProjectionBytes {
		return Installed{}, fmt.Errorf("%w: projection bytes %d > %d", ErrInstallLimit, projectionByteLimit, policy.maxProjectionBytes)
	}
	root, err := prepareSecureRoot(options.Root)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: root: %w", err)
	}
	manifestRaw, err := readRegularFileFromRoot(bundleRoot, ManifestFilename, MaxInstallManifestBytes)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: read manifest: %w", err)
	}
	signatureRaw, err := readRegularFileFromRoot(bundleRoot, SignatureFilename, indexpack.MaxSignatureBytes)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: read signature: %w", err)
	}
	verified, err := indexpack.VerifyManifestFor(manifestRaw, signatureRaw, options.TrustedKeys, options.Acceptance)
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: verify manifest: %w", err)
	}
	if err := preflight(verified.Manifest, policy); err != nil {
		return Installed{}, err
	}

	objects := filepath.Join(root, "objects")
	if err := ensurePrivateDirectory(objects); err != nil {
		return Installed{}, err
	}
	destination := filepath.Join(objects, verified.Digest)
	if err := ensureMissing(destination); err != nil {
		return Installed{}, err
	}
	temporary, err := os.MkdirTemp(objects, ".import-"+verified.Digest[:12]+"-")
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: create private staging directory: %w", err)
	}
	if err := os.Chmod(temporary, 0o700); err != nil {
		cleanupErr := removeAll(temporary)
		if cleanupErr != nil {
			cleanupErr = fmt.Errorf("open pack index: remove insecure staging directory: %w", cleanupErr)
		}
		return Installed{}, errors.Join(fmt.Errorf("open pack index: secure staging directory: %w", err), cleanupErr)
	}
	activated := false
	defer func() {
		if !activated {
			if err := removeAll(temporary); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("open pack index: remove staging directory: %w", err))
			}
		}
	}()

	projectionPath := filepath.Join(temporary, projectionDirectoryName)
	projection, err := bleve.New(projectionPath, projectionMapping())
	if err != nil {
		return Installed{}, fmt.Errorf("open pack index: create projection: %w", err)
	}
	projectionOpen := true
	defer func() {
		if projectionOpen {
			_ = projection.Close()
		}
	}()

	var seenURLDigests map[[sha256.Size]byte]struct{}
	if len(verified.Manifest.Shards) > 1 {
		seenURLDigests = make(map[[sha256.Size]byte]struct{}, verified.Manifest.RecordCount)
	}
	var imported uint64
	for _, descriptor := range verified.Manifest.Shards {
		if err := ctx.Err(); err != nil {
			return Installed{}, err
		}
		stagedPath := filepath.Join(temporary, stagedCompressedFilename)
		if err := stageVerifiedShard(ctx, bundleRoot, stagedPath, descriptor); err != nil {
			return Installed{}, err
		}
		if err := validateSingleZstdFrame(stagedPath, descriptor); err != nil {
			return Installed{}, err
		}
		if err := importShard(ctx, projection, stagedPath, verified.Manifest.Version, verified.Manifest.Kind, descriptor, seenURLDigests, &imported, policy.maxDecoderMemoryBytes); err != nil {
			return Installed{}, err
		}
		if err := os.Remove(stagedPath); err != nil {
			return Installed{}, fmt.Errorf("open pack index: remove verified staging shard: %w", err)
		}
	}
	if imported != verified.Manifest.RecordCount {
		return Installed{}, fmt.Errorf("open pack index: imported %d records, expected %d", imported, verified.Manifest.RecordCount)
	}
	marker := projectionMarker{
		MarkerKind:     "open-pack-projection",
		SchemaVersion:  projectionSchemaVersion,
		ManifestSHA256: verified.Digest,
		PackID:         verified.Manifest.PackID,
		Revision:       strconv.FormatUint(verified.Manifest.Revision, 10),
		RecordCount:    strconv.FormatUint(imported, 10),
		SigningKeyID:   verified.KeyID,
		CreatedAt:      verified.Manifest.CreatedAt,
		ExpiresAt:      verified.Manifest.ExpiresAt,
	}
	if err := projection.Index(markerDocumentID, marker); err != nil {
		return Installed{}, fmt.Errorf("open pack index: write projection marker: %w", err)
	}
	if err := projection.Close(); err != nil {
		return Installed{}, fmt.Errorf("open pack index: close projection: %w", err)
	}
	projectionOpen = false
	if err := ctx.Err(); err != nil {
		return Installed{}, err
	}
	if err := verifyProjection(projectionPath, OpenOptions{
		Path:                   temporary,
		ExpectedManifestSHA256: verified.Digest,
		ExpectedPackID:         verified.Manifest.PackID,
		ExpectedRevision:       verified.Manifest.Revision,
		ExpectedRecordCount:    imported,
		ExpectedKeyID:          verified.KeyID,
		ExpectedCreatedAt:      verified.Manifest.CreatedAt,
		ExpectedExpiresAt:      verified.Manifest.ExpiresAt,
		Now:                    options.Acceptance.Now,
	}, policy.maxRecords); err != nil {
		return Installed{}, fmt.Errorf("open pack index: verify staged projection: %w", err)
	}
	projectionBytes, err := measureProjectionBytes(ctx, projectionPath, projectionByteLimit)
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
		return Installed{}, fmt.Errorf("open pack index: activate projection: %w", err)
	}
	activated = true
	return Installed{
		Path: destination, ManifestSHA256: verified.Digest, PackID: verified.Manifest.PackID,
		Revision: verified.Manifest.Revision, RecordCount: imported, OperationCount: imported,
		ProjectionBytes: projectionBytes,
	}, nil
}

func (policy installPolicy) validate() error {
	if policy.maxShards < 1 || policy.maxShards > indexpack.MaxManifestShards ||
		policy.maxRecords == 0 || policy.maxRecords > indexpack.MaxPackRecords ||
		policy.maxCompressedBytes == 0 || policy.maxUncompressedBytes == 0 || policy.maxProjectionBytes == 0 ||
		policy.maxDecoderMemoryBytes == 0 || policy.maxDecoderMemoryBytes > indexpack.MaxUncompressedShardBytes {
		return fmt.Errorf("%w: invalid install policy", ErrInstallLimit)
	}
	return nil
}

func verifyClosedProjection(path string, options OpenOptions, maxRecords uint64) (returnErr error) {
	projection, err := bleve.Open(path)
	if err != nil {
		return fmt.Errorf("reopen projection: %w", err)
	}
	defer func() {
		if err := projection.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close reopened projection: %w", err))
		}
	}()
	marker, err := readAndVerifyMarkerBound(projection, options, maxRecords)
	if err != nil {
		return fmt.Errorf("verify projection identity: %w", err)
	}
	if marker.SchemaVersion != projectionSchemaVersion {
		return fmt.Errorf("verify projection identity: %w: staged projection schema is not current", ErrSchemaMismatch)
	}
	return nil
}

func measureProjectionBytes(ctx context.Context, root string, maximum uint64) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: projection contains non-regular path %s", ErrUnsafePath, path)
		}
		if info.Size() < 0 {
			return fmt.Errorf("%w: projection contains negative file size", ErrUnsafePath)
		}
		var ok bool
		total, ok = checkedAdd(total, uint64(info.Size()), maximum)
		if !ok {
			return fmt.Errorf("%w: projection exceeds %d logical bytes", ErrInstallLimit, maximum)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("open pack index: measure projection: %w", err)
	}
	return total, nil
}

func preflight(manifest indexpack.Manifest, policy installPolicy) error {
	if err := policy.validate(); err != nil {
		return err
	}
	if manifest.Kind != indexpack.KindSnapshot {
		return errors.New("open pack index: only snapshot packs are supported")
	}
	return preflightResources(manifest, policy)
}

func preflightResources(manifest indexpack.Manifest, policy installPolicy) error {
	if err := policy.validate(); err != nil {
		return err
	}
	if len(manifest.Shards) > policy.maxShards {
		return fmt.Errorf("%w: shards %d > %d", ErrInstallLimit, len(manifest.Shards), policy.maxShards)
	}
	if manifest.RecordCount > policy.maxRecords {
		return fmt.Errorf("%w: records %d > %d", ErrInstallLimit, manifest.RecordCount, policy.maxRecords)
	}
	var compressed, uncompressed, records uint64
	for _, shard := range manifest.Shards {
		var ok bool
		compressed, ok = checkedAdd(compressed, shard.CompressedSizeBytes, policy.maxCompressedBytes)
		if !ok {
			return fmt.Errorf("%w: compressed aggregate exceeds %d bytes", ErrInstallLimit, policy.maxCompressedBytes)
		}
		uncompressed, ok = checkedAdd(uncompressed, shard.UncompressedSizeBytes, policy.maxUncompressedBytes)
		if !ok {
			return fmt.Errorf("%w: uncompressed aggregate exceeds %d bytes", ErrInstallLimit, policy.maxUncompressedBytes)
		}
		records, ok = checkedAdd(records, shard.RecordCount, policy.maxRecords)
		if !ok {
			return fmt.Errorf("%w: record aggregate exceeds %d", ErrInstallLimit, policy.maxRecords)
		}
	}
	if records != manifest.RecordCount {
		return errors.New("open pack index: inconsistent record aggregate")
	}
	return nil
}

func checkedAdd(total, value, maximum uint64) (uint64, bool) {
	if value > maximum || total > maximum-value {
		return 0, false
	}
	return total + value, true
}

func stageVerifiedShard(ctx context.Context, bundleRoot *os.Root, destination string, descriptor indexpack.Shard) error {
	source, err := openRegularFileFromRoot(bundleRoot, descriptor.Path, descriptor.CompressedSizeBytes)
	if err != nil {
		return fmt.Errorf("open pack index: open shard %q: %w", descriptor.Path, err)
	}
	defer source.Close()
	staged, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("open pack index: create verified shard staging file: %w", err)
	}
	verifyErr := copyVerifiedShard(ctx, source, staged, descriptor)
	closeErr := staged.Close()
	if verifyErr != nil {
		return fmt.Errorf("open pack index: verify compressed shard: %w", verifyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("open pack index: close staged shard: %w", closeErr)
	}
	return nil
}

func copyVerifiedShard(ctx context.Context, source io.Reader, destination io.Writer, descriptor indexpack.Shard) error {
	if ctx == nil {
		return errors.New("open pack index: context is required")
	}
	verifyErr := indexpack.VerifyCompressedShard(io.TeeReader(contextReader{ctx: ctx, reader: source}, destination), descriptor)
	if err := ctx.Err(); err != nil {
		return err
	}
	return verifyErr
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

func importShard(
	ctx context.Context,
	projection bleve.Index,
	compressedPath string,
	manifestVersion int,
	kind indexpack.Kind,
	descriptor indexpack.Shard,
	seenURLDigests map[[sha256.Size]byte]struct{},
	imported *uint64,
	decoderMemoryLimit uint64,
) error {
	batch := projection.NewBatch()
	batchCount := 0
	flush := func() error {
		if batchCount == 0 {
			return nil
		}
		if err := projection.Batch(batch); err != nil {
			return fmt.Errorf("open pack index: write projection batch: %w", err)
		}
		batch = projection.NewBatch()
		batchCount = 0
		return nil
	}
	err := scanStagedShard(ctx, compressedPath, manifestVersion, kind, descriptor, decoderMemoryLimit, func(record indexpack.Record) error {
		if err := enforceRecordCaps(record); err != nil {
			return err
		}
		if seenURLDigests != nil {
			digest := sha256.Sum256([]byte(record.URL))
			if _, duplicate := seenURLDigests[digest]; duplicate {
				return fmt.Errorf("duplicate canonical URL across pack: %q", record.URL)
			}
			seenURLDigests[digest] = struct{}{}
		}
		document, err := makeStoredDocument(record)
		if err != nil {
			return err
		}
		if err := batch.Index(documentID(record.URL), document); err != nil {
			return fmt.Errorf("index record: %w", err)
		}
		batchCount++
		*imported++
		if batchCount == indexBatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	return nil
}

func scanStagedShard(
	ctx context.Context,
	compressedPath string,
	manifestVersion int,
	kind indexpack.Kind,
	descriptor indexpack.Shard,
	decoderMemoryLimit uint64,
	visit func(indexpack.Record) error,
) error {
	compressed, err := os.Open(compressedPath)
	if err != nil {
		return fmt.Errorf("open pack index: reopen verified shard: %w", err)
	}
	defer compressed.Close()
	decoder, err := zstd.NewReader(compressed,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(decoderMemoryLimit),
		zstd.WithDecoderMaxWindow(maxDecoderWindowBytes),
		zstd.WithDecodeBuffersBelow(0),
	)
	if err != nil {
		return fmt.Errorf("open pack index: initialize zstd decoder: %w", err)
	}
	defer decoder.Close()
	boundedRecords := newLineLimitReader(decoder, MaxInstallRecordBytes)
	err = indexpack.ScanRecordStreamVersion(ctx, boundedRecords, manifestVersion, kind, descriptor, visit)
	if boundedRecords.limitErr != nil {
		return fmt.Errorf("open pack index: scan shard: %w", boundedRecords.limitErr)
	}
	if err != nil {
		return fmt.Errorf("open pack index: scan shard: %w", err)
	}
	return nil
}

func enforceRecordCaps(record indexpack.Record) error {
	if len(record.Title) > MaxInstallTitleBytes {
		return fmt.Errorf("%w: title exceeds %d bytes", ErrInstallLimit, MaxInstallTitleBytes)
	}
	if len(record.Headings) > MaxInstallHeadings {
		return fmt.Errorf("%w: headings exceed %d entries", ErrInstallLimit, MaxInstallHeadings)
	}
	for _, heading := range record.Headings {
		if len(heading) > MaxInstallHeadingBytes {
			return fmt.Errorf("%w: heading exceeds %d bytes", ErrInstallLimit, MaxInstallHeadingBytes)
		}
	}
	if len(record.AnchorTerms) > MaxInstallAnchorTerms {
		return fmt.Errorf("%w: anchor terms exceed %d entries", ErrInstallLimit, MaxInstallAnchorTerms)
	}
	for _, term := range record.AnchorTerms {
		if len(term) > MaxInstallAnchorTermBytes {
			return fmt.Errorf("%w: anchor term exceeds %d bytes", ErrInstallLimit, MaxInstallAnchorTermBytes)
		}
	}
	if len(record.SalientSketch) > MaxInstallSalientSketch {
		return fmt.Errorf("%w: salient sketch exceeds %d bytes", ErrInstallLimit, MaxInstallSalientSketch)
	}
	return nil
}

type lineLimitReader struct {
	reader   *bufio.Reader
	maximum  int
	pending  []byte
	after    error
	limitErr error
}

func newLineLimitReader(reader io.Reader, maximum int) *lineLimitReader {
	return &lineLimitReader{reader: bufio.NewReaderSize(reader, maximum+1), maximum: maximum}
}

func (bounded *lineLimitReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if len(bounded.pending) == 0 {
		if bounded.after != nil {
			return 0, bounded.after
		}
		line, err := bounded.reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) || (len(line) > bounded.maximum && (len(line) == 0 || line[len(line)-1] != '\n')) ||
			(len(line) > bounded.maximum+1) {
			bounded.limitErr = fmt.Errorf("%w: NDJSON record exceeds %d bytes", ErrInstallLimit, bounded.maximum)
			return 0, bounded.limitErr
		}
		if len(line) > 0 && line[len(line)-1] == '\n' && len(line)-1 > bounded.maximum {
			bounded.limitErr = fmt.Errorf("%w: NDJSON record exceeds %d bytes", ErrInstallLimit, bounded.maximum)
			return 0, bounded.limitErr
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		if len(line) == 0 {
			return 0, err
		}
		bounded.pending = line
		bounded.after = err
	}
	read := copy(buffer, bounded.pending)
	bounded.pending = bounded.pending[read:]
	return read, nil
}

func validateSingleZstdFrame(path string, descriptor indexpack.Shard) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open pack index: inspect zstd frame: %w", err)
	}
	defer file.Close()
	headerBytes := make([]byte, zstd.HeaderMaxSize)
	n, readErr := file.ReadAt(headerBytes, 0)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fmt.Errorf("open pack index: read zstd header: %w", readErr)
	}
	var header zstd.Header
	if err := header.Decode(headerBytes[:n]); err != nil {
		return fmt.Errorf("open pack index: invalid zstd header: %w", err)
	}
	if header.Skippable || header.DictionaryID != 0 {
		return errors.New("open pack index: skippable and dictionary zstd frames are unsupported")
	}
	if header.HasFCS && header.FrameContentSize != descriptor.UncompressedSizeBytes {
		return errors.New("open pack index: zstd frame content size does not match descriptor")
	}
	offset := int64(header.HeaderSize)
	limit := int64(descriptor.CompressedSizeBytes)
	for {
		var raw [3]byte
		if _, err := file.ReadAt(raw[:], offset); err != nil {
			return fmt.Errorf("open pack index: truncated zstd block header: %w", err)
		}
		value := uint32(raw[0]) | uint32(raw[1])<<8 | uint32(raw[2])<<16
		last := value&1 != 0
		blockType := (value >> 1) & 3
		blockSize := int64(value >> 3)
		if blockType == 3 {
			return errors.New("open pack index: reserved zstd block type")
		}
		payload := blockSize
		if blockType == 1 {
			payload = 1
		}
		if payload < 0 || offset > limit-3 || payload > limit-(offset+3) {
			return errors.New("open pack index: truncated zstd block")
		}
		offset += 3 + payload
		if last {
			break
		}
	}
	if header.HasCheckSum {
		if offset > limit-4 {
			return errors.New("open pack index: truncated zstd checksum")
		}
		offset += 4
	}
	if offset != limit {
		return errors.New("open pack index: trailing or concatenated zstd data is forbidden")
	}
	return nil
}

func ensureMissing(path string) error {
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: destination is a symlink", ErrUnsafePath)
		}
		return fmt.Errorf("%w: %s", ErrAlreadyExists, path)
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return fmt.Errorf("open pack index: inspect destination: %w", err)
	}
}

func cleanProviderID(value, packID string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "open-pack:" + packID
	}
	if len(value) > 256 || strings.ContainsAny(value, "\x00\r\n\t") {
		return "", errors.New("open pack index: provider ID is invalid")
	}
	return value, nil
}
