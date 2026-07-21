package tufrepository

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackchannel"
	"github.com/theupdateframework/go-tuf/v2/metadata"
)

type MirrorActivationStatus string

const (
	MirrorStatusPrepared      MirrorActivationStatus = "prepared"
	MirrorStatusInitialized   MirrorActivationStatus = "initialized"
	MirrorStatusActivated     MirrorActivationStatus = "activated"
	MirrorStatusAlreadyActive MirrorActivationStatus = "already_applied"
)

var (
	ErrInvalidMirrorActivation = errors.New("TUF repository: invalid mirror activation")
	ErrMirrorCASMismatch       = errors.New("TUF repository: mirror head compare-and-swap mismatch")
)

type MirrorActivationOptions struct {
	TrustedRootPath                  string
	MirrorDir                        string
	CandidateDir                     string
	TargetPath                       string
	PublisherKeys                    map[string]ed25519.PublicKey
	ExpectedCurrentTimestampSHA256   string
	ExpectedCandidateTimestampSHA256 string
	InitializeEmpty                  bool
}

type MirrorActivationResult struct {
	Status                  MirrorActivationStatus `json:"status"`
	Path                    string                 `json:"path"`
	HeadPath                string                 `json:"head_path"`
	TargetBasePath          string                 `json:"target_base_path"`
	TargetPath              string                 `json:"target_path"`
	FromTimestampSHA256     string                 `json:"from_timestamp_sha256,omitempty"`
	ToTimestampSHA256       string                 `json:"to_timestamp_sha256"`
	InitializedFromEmpty    bool                   `json:"initialized_from_empty"`
	Version                 int64                  `json:"version"`
	RootVersion             int64                  `json:"root_version"`
	PackID                  string                 `json:"pack_id,omitempty"`
	Revision                uint64                 `json:"revision,omitempty"`
	Kind                    indexpack.Kind         `json:"kind,omitempty"`
	ManifestSHA256          string                 `json:"manifest_sha256,omitempty"`
	ImmutableFileCount      int                    `json:"immutable_file_count"`
	ImmutableBytes          uint64                 `json:"immutable_bytes"`
	ImmutableArtifactsReady bool                   `json:"immutable_artifacts_ready"`
	HeadCommitted           bool                   `json:"head_committed"`
}

type PreparedMirrorActivationError struct {
	Result MirrorActivationResult
	Cause  error
}

func (err *PreparedMirrorActivationError) Error() string {
	return fmt.Sprintf(
		"TUF repository: mirror dependencies prepared at %s for timestamp_sha256=%s but the live head is unchanged: %v; retain the immutable files and retry with the exact live head",
		err.Result.Path, err.Result.ToTimestampSHA256, err.Cause,
	)
}

func (err *PreparedMirrorActivationError) Unwrap() error { return err.Cause }

type CommittedMirrorActivationError struct {
	Result MirrorActivationResult
	Cause  error
}

func (err *CommittedMirrorActivationError) Error() string {
	return fmt.Sprintf(
		"TUF repository: mirror head committed at %s but completion failed: from_timestamp_sha256=%s to_timestamp_sha256=%s: %v; inspect the exact live timestamp and immutable dependencies before retrying",
		err.Result.HeadPath, err.Result.FromTimestampSHA256, err.Result.ToTimestampSHA256, err.Cause,
	)
}

func (err *CommittedMirrorActivationError) Unwrap() error { return err.Cause }

type mirrorActivationDependencies struct {
	now                       func() time.Time
	beforeFinalRead           func()
	afterFinalRead            func()
	afterPreparedVerification func()
	closeLock                 func(*os.File) error
	closeMirror               func(*os.Root) error
	closeDirectory            func(*os.File) error
	syncAfterPrepare          func(*os.Root) error
	syncAfterCommit           func(*os.Root) error
}

func defaultMirrorActivationDependencies() mirrorActivationDependencies {
	return mirrorActivationDependencies{
		now:              func() time.Time { return time.Now().UTC() },
		closeLock:        func(file *os.File) error { return file.Close() },
		closeMirror:      func(root *os.Root) error { return root.Close() },
		closeDirectory:   func(file *os.File) error { return file.Close() },
		syncAfterPrepare: syncRoot,
		syncAfterCommit:  syncRoot,
	}
}

func ActivateMirror(ctx context.Context, options MirrorActivationOptions) (MirrorActivationResult, error) {
	return activateMirrorWithDependencies(ctx, options, defaultMirrorActivationDependencies())
}

func activateMirrorWithDependencies(ctx context.Context, options MirrorActivationOptions, dependencies mirrorActivationDependencies) (result MirrorActivationResult, returnErr error) {
	if ctx == nil {
		return MirrorActivationResult{}, invalidMirrorActivation(errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return MirrorActivationResult{}, err
	}
	if dependencies.now == nil || dependencies.closeLock == nil || dependencies.closeMirror == nil ||
		dependencies.closeDirectory == nil || dependencies.syncAfterPrepare == nil || dependencies.syncAfterCommit == nil {
		return MirrorActivationResult{}, invalidMirrorActivation(errors.New("activation dependencies are incomplete"))
	}
	validated, trustedRootRaw, err := validateMirrorActivationOptions(options)
	if err != nil {
		return MirrorActivationResult{}, err
	}
	options = validated
	now, err := mirrorActivationTime(dependencies.now)
	if err != nil {
		return MirrorActivationResult{}, err
	}
	candidate, err := verifyMirrorRepository(ctx, options.CandidateDir, trustedRootRaw, options.TargetPath, options.PublisherKeys, now, true)
	if err != nil {
		return MirrorActivationResult{}, fmt.Errorf("TUF repository: verify mirror candidate: %w", err)
	}
	if candidate.timestampDigest != options.ExpectedCandidateTimestampSHA256 {
		return MirrorActivationResult{}, fmt.Errorf("%w: candidate timestamp sha256=%s, expected=%s", ErrMirrorCASMismatch, candidate.timestampDigest, options.ExpectedCandidateTimestampSHA256)
	}
	result = MirrorActivationResult{
		Path: options.MirrorDir, HeadPath: filepath.Join(options.MirrorDir, "metadata", "timestamp.json"),
		TargetBasePath: filepath.Join(options.MirrorDir, "targets"), TargetPath: options.TargetPath,
		FromTimestampSHA256: options.ExpectedCurrentTimestampSHA256, ToTimestampSHA256: candidate.timestampDigest,
		InitializedFromEmpty: options.InitializeEmpty, Version: candidate.version, RootVersion: candidate.rootVersion,
	}
	if candidate.target != nil {
		result.PackID, result.Revision, result.Kind = candidate.target.PackID, candidate.target.Revision, candidate.target.Kind
		result.ManifestSHA256 = candidate.target.ManifestSHA256
	}

	mirror, err := os.OpenRoot(options.MirrorDir)
	if err != nil {
		return MirrorActivationResult{}, fmt.Errorf("TUF repository: anchor mirror: %w", err)
	}
	prepared := false
	committed := false
	defer func() {
		if closeErr := dependencies.closeMirror(mirror); closeErr != nil {
			returnErr = errors.Join(returnErr, mirrorLifecycleError(result, prepared, committed, fmt.Errorf("close mirror: %w", closeErr)))
		}
	}()
	if err := rootStillAtPath(mirror, options.MirrorDir); err != nil {
		return MirrorActivationResult{}, err
	}
	lock, err := openMirrorLock(ctx, mirror)
	if err != nil {
		return MirrorActivationResult{}, err
	}
	lockHeld := false
	defer func() {
		var lockErr error
		if lockHeld {
			lockErr = unlockRepositoryFile(lock)
		}
		lockErr = errors.Join(lockErr, dependencies.closeLock(lock))
		if lockErr != nil {
			returnErr = errors.Join(returnErr, mirrorLifecycleError(result, prepared, committed, fmt.Errorf("release mirror lock: %w", lockErr)))
		}
	}()
	if err := waitForMirrorLock(ctx, lock); err != nil {
		return MirrorActivationResult{}, err
	}
	lockHeld = true
	if err := rootStillAtPath(mirror, options.MirrorDir); err != nil {
		return MirrorActivationResult{}, err
	}

	liveRaw, liveDigest, liveExists, err := readMirrorHead(mirror)
	if err != nil {
		return MirrorActivationResult{}, err
	}
	if liveExists && liveDigest == candidate.timestampDigest {
		if !bytes.Equal(liveRaw, candidate.timestampRaw) {
			return MirrorActivationResult{}, fmt.Errorf("%w: candidate digest collision", ErrMirrorCASMismatch)
		}
		active, err := verifyMirrorRepositoryRoot(ctx, mirror, trustedRootRaw, options.TargetPath, options.PublisherKeys, now, liveRaw, true)
		if err != nil {
			return MirrorActivationResult{}, fmt.Errorf("TUF repository: verify already-applied mirror: %w", err)
		}
		if err := rootStillAtPath(mirror, options.MirrorDir); err != nil {
			return MirrorActivationResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return MirrorActivationResult{}, err
		}
		activeNow, err := mirrorActivationTime(dependencies.now)
		if err != nil {
			return MirrorActivationResult{}, err
		}
		if err := active.requireFreshAt(activeNow); err != nil {
			return MirrorActivationResult{}, err
		}
		if !equalRootChains(active.rootChain, candidate.rootChain) || active.rootVersion != candidate.rootVersion ||
			!bytes.Equal(active.rootRaw, candidate.rootRaw) {
			return MirrorActivationResult{}, fmt.Errorf("%w: live timestamp matches but root chain differs from the candidate", ErrMirrorCASMismatch)
		}
		confirmedRaw, confirmedDigest, confirmedExists, err := readMirrorHead(mirror)
		if err != nil || !confirmedExists || confirmedDigest != candidate.timestampDigest || !bytes.Equal(confirmedRaw, candidate.timestampRaw) {
			return MirrorActivationResult{}, errors.Join(fmt.Errorf("%w: already-applied mirror head changed during verification", ErrMirrorCASMismatch), err)
		}
		result.Status = MirrorStatusAlreadyActive
		result.ImmutableArtifactsReady = true
		result.HeadCommitted = true
		return result, nil
	}
	var current verifiedMirrorRepository
	if options.InitializeEmpty {
		if liveExists {
			return MirrorActivationResult{}, fmt.Errorf("%w: initialization requires an absent timestamp head", ErrMirrorCASMismatch)
		}
		if candidate.version != 1 || candidate.target == nil {
			return MirrorActivationResult{}, invalidMirrorActivation(errors.New("initial publication must be a present version-1 target"))
		}
		result.FromTimestampSHA256 = ""
	} else {
		if !liveExists || liveDigest != options.ExpectedCurrentTimestampSHA256 {
			return MirrorActivationResult{}, fmt.Errorf("%w: live timestamp sha256=%s, expected=%s", ErrMirrorCASMismatch, liveDigest, options.ExpectedCurrentTimestampSHA256)
		}
		current, err = verifyMirrorRepositoryRoot(ctx, mirror, trustedRootRaw, options.TargetPath, options.PublisherKeys, now, liveRaw, false)
		if err != nil {
			return MirrorActivationResult{}, fmt.Errorf("TUF repository: verify live mirror: %w", err)
		}
		if err := validateMirrorTransition(current, candidate); err != nil {
			return MirrorActivationResult{}, err
		}
	}

	created, count, copied, err := prepareMirrorDependencies(ctx, mirror, options.CandidateDir, candidate)
	result.ImmutableFileCount, result.ImmutableBytes = count, copied
	prepared = created
	if err != nil {
		if prepared {
			result.Status = MirrorStatusPrepared
			return result, &PreparedMirrorActivationError{Result: result, Cause: err}
		}
		return MirrorActivationResult{}, err
	}
	prepared = true
	result.Status = MirrorStatusPrepared
	result.ImmutableArtifactsReady = true
	if err := dependencies.syncAfterPrepare(mirror); err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: fmt.Errorf("sync immutable mirror dependencies: %w", err)}
	}
	if dependencies.beforeFinalRead != nil {
		dependencies.beforeFinalRead()
	}
	if err := ctx.Err(); err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	finalRaw, finalDigest, finalExists, err := readMirrorHead(mirror)
	if err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	if finalExists != liveExists || finalDigest != liveDigest || !bytes.Equal(finalRaw, liveRaw) {
		return result, &PreparedMirrorActivationError{Result: result, Cause: fmt.Errorf("%w: live timestamp changed before replacement", ErrMirrorCASMismatch)}
	}
	if dependencies.afterFinalRead != nil {
		dependencies.afterFinalRead()
	}
	if err := ctx.Err(); err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	now, err = mirrorActivationTime(dependencies.now)
	if err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	preparedCandidate, err := verifyMirrorRepositoryRoot(ctx, mirror, trustedRootRaw, options.TargetPath, options.PublisherKeys, now, candidate.timestampRaw, true)
	if err != nil || preparedCandidate.timestampDigest != candidate.timestampDigest ||
		preparedCandidate.rootVersion != candidate.rootVersion || !bytes.Equal(preparedCandidate.rootRaw, candidate.rootRaw) ||
		!equalRootChains(preparedCandidate.rootChain, candidate.rootChain) {
		return result, &PreparedMirrorActivationError{Result: result, Cause: errors.Join(errors.New("prepared candidate revalidation failed"), err)}
	}
	if dependencies.afterPreparedVerification != nil {
		dependencies.afterPreparedVerification()
	}
	if err := rootStillAtPath(mirror, options.MirrorDir); err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	if err := ctx.Err(); err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	commitNow, err := mirrorActivationTime(dependencies.now)
	if err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	if err := preparedCandidate.requireFreshAt(commitNow); err != nil {
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	commitRoot, commitChain, err := readMirrorRootChain(mirror, trustedRootRaw)
	if err != nil || commitRoot.Signed.Version != candidate.rootVersion || !bytes.Equal(commitChain[len(commitChain)-1], candidate.rootRaw) ||
		!equalRootChains(commitChain, candidate.rootChain) {
		return result, &PreparedMirrorActivationError{Result: result, Cause: errors.Join(errors.New("prepared root chain changed before commit"), err)}
	}
	headCommitted, err := commitMirrorHead(mirror, candidate.timestampRaw, liveExists, dependencies.closeDirectory)
	if headCommitted {
		committed = true
		result.HeadCommitted = true
		if options.InitializeEmpty {
			result.Status = MirrorStatusInitialized
		} else {
			result.Status = MirrorStatusActivated
		}
	}
	if err != nil {
		if committed {
			return result, &CommittedMirrorActivationError{Result: result, Cause: err}
		}
		return result, &PreparedMirrorActivationError{Result: result, Cause: err}
	}
	if err := dependencies.syncAfterCommit(mirror); err != nil {
		return result, &CommittedMirrorActivationError{Result: result, Cause: fmt.Errorf("sync committed mirror: %w", err)}
	}
	if err := rootStillAtPath(mirror, options.MirrorDir); err != nil {
		return result, &CommittedMirrorActivationError{Result: result, Cause: err}
	}
	activeRaw, activeDigest, activeExists, err := readMirrorHead(mirror)
	if err != nil || !activeExists || activeDigest != candidate.timestampDigest || !bytes.Equal(activeRaw, candidate.timestampRaw) {
		return result, &CommittedMirrorActivationError{Result: result, Cause: errors.Join(errors.New("committed timestamp readback differs from candidate"), err)}
	}
	committedRoot, committedChain, err := readMirrorRootChain(mirror, trustedRootRaw)
	if err != nil || committedRoot.Signed.Version != candidate.rootVersion ||
		!bytes.Equal(committedChain[len(committedChain)-1], candidate.rootRaw) || !equalRootChains(committedChain, candidate.rootChain) {
		return result, &CommittedMirrorActivationError{Result: result, Cause: errors.Join(errors.New("committed root chain differs from candidate"), err)}
	}
	return result, nil
}

type verifiedMirrorRepository struct {
	rootRaw          []byte
	rootChain        [][]byte
	rootVersion      int64
	timestampRaw     []byte
	snapshotRaw      []byte
	targetsRaw       []byte
	timestampDigest  string
	version          int64
	target           *openpackchannel.Target
	manifest         *indexpack.Manifest
	manifestRaw      []byte
	signatureRaw     []byte
	files            []mirrorFile
	rootExpires      time.Time
	timestampExpires time.Time
	snapshotExpires  time.Time
	targetsExpires   time.Time
	manifestCreated  time.Time
	manifestExpires  time.Time
}

type mirrorFile struct {
	path   string
	size   uint64
	digest string
	raw    []byte
}

func verifyMirrorRepository(ctx context.Context, repositoryPath string, trustedRootRaw []byte, targetPath string, publisherKeys map[string]ed25519.PublicKey, now time.Time, requireFresh bool) (verifiedMirrorRepository, error) {
	root, err := os.OpenRoot(repositoryPath)
	if err != nil {
		return verifiedMirrorRepository{}, err
	}
	defer root.Close()
	timestampRaw, err := readStageFile(root, "metadata/timestamp.json", 64<<10)
	if err != nil {
		return verifiedMirrorRepository{}, err
	}
	return verifyMirrorRepositoryRoot(ctx, root, trustedRootRaw, targetPath, publisherKeys, now, timestampRaw, requireFresh)
}

func verifyMirrorRepositoryRoot(ctx context.Context, root *os.Root, trustedRootRaw []byte, targetPath string, publisherKeys map[string]ed25519.PublicKey, now time.Time, timestampRaw []byte, requireFresh bool) (verifiedMirrorRepository, error) {
	if err := ctx.Err(); err != nil {
		return verifiedMirrorRepository{}, err
	}
	result := verifiedMirrorRepository{timestampRaw: bytes.Clone(timestampRaw), timestampDigest: indexpack.Digest(timestampRaw)}
	parsedRoot, rootChain, err := readMirrorRootChain(root, trustedRootRaw)
	if err != nil {
		return verifiedMirrorRepository{}, err
	}
	if requireFresh && !parsedRoot.Signed.Expires.After(now) {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("repository active root is expired"))
	}
	result.rootRaw = rootChain[len(rootChain)-1]
	result.rootChain = rootChain
	result.rootVersion = parsedRoot.Signed.Version
	result.rootExpires = parsedRoot.Signed.Expires
	timestamp, err := metadata.Timestamp().FromBytes(timestampRaw)
	if err != nil {
		return verifiedMirrorRepository{}, err
	}
	if err := parsedRoot.VerifyDelegate(metadata.TIMESTAMP, timestamp); err != nil {
		return verifiedMirrorRepository{}, invalidMirrorActivation(err)
	}
	if requireFresh && timestamp.Signed.Expires.Sub(now) < MinimumPublicationHorizon {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("timestamp expiry is inside the minimum publication horizon"))
	}
	result.timestampExpires = timestamp.Signed.Expires
	snapshotInfo, found := timestamp.Signed.Meta["snapshot.json"]
	if !found || len(timestamp.Signed.Meta) != 1 {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("timestamp must bind only snapshot.json"))
	}
	snapshotRaw, err := readStageFile(root, fmt.Sprintf("metadata/%d.snapshot.json", snapshotInfo.Version), 2<<20)
	if err != nil || !metadataFileMatches(snapshotInfo, snapshotRaw) {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.Join(errors.New("snapshot bytes do not match timestamp"), err))
	}
	snapshot, err := metadata.Snapshot().FromBytes(snapshotRaw)
	if err != nil {
		return verifiedMirrorRepository{}, err
	}
	if err := parsedRoot.VerifyDelegate(metadata.SNAPSHOT, snapshot); err != nil {
		return verifiedMirrorRepository{}, invalidMirrorActivation(err)
	}
	if requireFresh && !snapshot.Signed.Expires.After(now) {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("snapshot metadata is expired"))
	}
	result.snapshotExpires = snapshot.Signed.Expires
	targetsInfo, found := snapshot.Signed.Meta["targets.json"]
	if !found || len(snapshot.Signed.Meta) != 1 {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("snapshot must bind only targets.json"))
	}
	targetsRaw, err := readStageFile(root, fmt.Sprintf("metadata/%d.targets.json", targetsInfo.Version), 5<<20)
	if err != nil || !metadataFileMatches(targetsInfo, targetsRaw) {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.Join(errors.New("targets bytes do not match snapshot"), err))
	}
	targets, err := metadata.Targets().FromBytes(targetsRaw)
	if err != nil {
		return verifiedMirrorRepository{}, err
	}
	if err := parsedRoot.VerifyDelegate(metadata.TARGETS, targets); err != nil {
		return verifiedMirrorRepository{}, invalidMirrorActivation(err)
	}
	if (requireFresh && !targets.Signed.Expires.After(now)) || targets.Signed.Delegations != nil || len(targets.Signed.Targets) > 1 {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("targets metadata is expired or outside the single-target profile"))
	}
	result.targetsExpires = targets.Signed.Expires
	version := timestamp.Signed.Version
	if version < 1 || snapshot.Signed.Version != version || targets.Signed.Version != version || snapshotInfo.Version != version || targetsInfo.Version != version {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("timestamp, snapshot, and targets versions must be synchronized"))
	}
	result.version, result.snapshotRaw, result.targetsRaw = version, snapshotRaw, targetsRaw
	result.files = append(result.files, mirrorFile{path: "root.json", raw: rootChain[0]})
	for index, versionRaw := range rootChain {
		result.files = append(result.files, mirrorFile{path: fmt.Sprintf("metadata/%d.root.json", index+1), raw: versionRaw})
	}
	result.files = append(result.files,
		mirrorFile{path: fmt.Sprintf("metadata/%d.snapshot.json", version), raw: snapshotRaw},
		mirrorFile{path: fmt.Sprintf("metadata/%d.targets.json", version), raw: targetsRaw},
	)
	if len(targets.Signed.Targets) == 0 {
		if requireFresh {
			if err := result.requireFreshAt(now); err != nil {
				return verifiedMirrorRepository{}, err
			}
		}
		return result, nil
	}
	targetFile, found := targets.Signed.Targets[targetPath]
	if !found || len(targets.Signed.Targets) != 1 {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("repository target path differs from the requested target"))
	}
	custom, err := openpackchannel.DecodeCustom(targetFile.Custom)
	if err != nil {
		return verifiedMirrorRepository{}, invalidMirrorActivation(fmt.Errorf("target custom metadata: %w", err))
	}
	manifestPath := path.Join("targets", path.Dir(targetPath), custom.ManifestSHA256+".manifest.json")
	manifestRaw, err := readStageFile(root, manifestPath, indexpack.MaxManifestBytes)
	if err != nil || targetFile.VerifyLengthHashes(manifestRaw) != nil {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.Join(errors.New("manifest bytes do not match targets metadata"), err))
	}
	signaturePath := path.Join("targets", path.Dir(targetPath), custom.ManifestSHA256+".manifest.ed25519")
	signatureRaw, err := readStageFile(root, signaturePath, indexpack.MaxSignatureBytes)
	if err != nil {
		return verifiedMirrorRepository{}, err
	}
	verified, err := indexpack.VerifyManifest(manifestRaw, signatureRaw, publisherKeys)
	if err != nil || verified.Digest != custom.ManifestSHA256 {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.Join(errors.New("publisher signature or manifest digest is invalid"), err))
	}
	manifest := verified.Manifest
	if requireFresh {
		manifest, err = custom.VerifyManifest(manifestRaw, now)
		if err != nil {
			return verifiedMirrorRepository{}, err
		}
	} else if verified.Digest != custom.ManifestSHA256 || manifest.PackID != custom.PackID || manifest.Kind != custom.Kind ||
		manifest.Revision != custom.Revision || manifest.ParentManifestSHA256 != custom.ParentManifestSHA256 {
		return verifiedMirrorRepository{}, invalidMirrorActivation(errors.New("manifest identity does not match target custom metadata"))
	}
	result.target, result.manifest = &custom, &manifest
	result.manifestCreated, _ = time.Parse(time.RFC3339, manifest.CreatedAt)
	result.manifestExpires, _ = time.Parse(time.RFC3339, manifest.ExpiresAt)
	result.manifestRaw, result.signatureRaw = manifestRaw, signatureRaw
	result.files = append(result.files,
		mirrorFile{path: manifestPath, raw: manifestRaw},
		mirrorFile{path: signaturePath, raw: signatureRaw},
	)
	for _, shard := range manifest.Shards {
		shardPath := path.Join("targets", path.Dir(targetPath), shard.Path)
		if err := verifyMirrorShard(ctx, root, shardPath, shard.CompressedSizeBytes, shard.SHA256); err != nil {
			return verifiedMirrorRepository{}, err
		}
		result.files = append(result.files, mirrorFile{path: shardPath, size: shard.CompressedSizeBytes, digest: shard.SHA256})
	}
	if requireFresh {
		if err := result.requireFreshAt(now); err != nil {
			return verifiedMirrorRepository{}, err
		}
	}
	return result, nil
}

func readMirrorRootChain(root *os.Root, trustedRootRaw []byte) (*metadata.Metadata[metadata.RootType], [][]byte, error) {
	bootstrapRaw, err := readStageFile(root, "root.json", 512<<10)
	if err != nil || !bytes.Equal(bootstrapRaw, trustedRootRaw) {
		return nil, nil, invalidMirrorActivation(errors.Join(errors.New("repository bootstrap root differs from the pinned root"), err))
	}
	versionOneRaw, err := readStageFile(root, "metadata/1.root.json", 512<<10)
	if err != nil || !bytes.Equal(versionOneRaw, bootstrapRaw) {
		return nil, nil, invalidMirrorActivation(errors.Join(errors.New("version-1 root differs from the repository bootstrap"), err))
	}
	chain := [][]byte{bootstrapRaw}
	foundGap := false
	for version := 2; version <= MaxRootRotations+2; version++ {
		relative := fmt.Sprintf("metadata/%d.root.json", version)
		_, statErr := root.Lstat(filepath.FromSlash(relative))
		if errors.Is(statErr, os.ErrNotExist) {
			foundGap = true
			continue
		}
		if statErr != nil {
			return nil, nil, statErr
		}
		if foundGap {
			return nil, nil, invalidMirrorActivation(errors.New("repository root chain contains a version gap"))
		}
		if version == MaxRootRotations+2 {
			return nil, nil, invalidMirrorActivation(fmt.Errorf("repository root chain exceeds %d rotations", MaxRootRotations))
		}
		updateRaw, err := readStageFile(root, relative, 512<<10)
		if err != nil {
			return nil, nil, err
		}
		chain = append(chain, updateRaw)
	}
	active, err := validateRootChain(chain[0], chain[1:], time.Time{})
	if err != nil {
		return nil, nil, invalidMirrorActivation(err)
	}
	return active, chain, nil
}

func validateMirrorTransition(current, candidate verifiedMirrorRepository) error {
	if candidate.version != current.version+1 {
		return invalidMirrorActivation(errors.New("metadata roles must advance together by exactly one version"))
	}
	if candidate.rootVersion < current.rootVersion || len(candidate.rootChain) < len(current.rootChain) {
		return invalidMirrorActivation(errors.New("candidate root chain cannot roll back the live root"))
	}
	for index := range current.rootChain {
		if !bytes.Equal(current.rootChain[index], candidate.rootChain[index]) {
			return invalidMirrorActivation(errors.New("candidate root chain diverges from the live root"))
		}
	}
	if current.target == nil && candidate.target != nil {
		return invalidMirrorActivation(errors.New("reintroducing a withdrawn target is not supported"))
	}
	if candidate.target == nil {
		return nil
	}
	if current.target == nil || current.manifest == nil {
		return invalidMirrorActivation(errors.New("present candidate requires a present live target"))
	}
	if candidate.target.PackID != current.target.PackID || candidate.target.Revision <= current.target.Revision ||
		candidate.target.ManifestSHA256 == current.target.ManifestSHA256 {
		return invalidMirrorActivation(errors.New("pack identity must stay fixed while revision and manifest digest advance"))
	}
	if candidate.target.Kind == indexpack.KindDelta &&
		(current.target.Kind != indexpack.KindSnapshot || candidate.target.ParentManifestSHA256 != current.target.ManifestSHA256) {
		return invalidMirrorActivation(errors.New("delta must name the exact live snapshot parent"))
	}
	return nil
}

func equalRootChains(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}

func (repository verifiedMirrorRepository) requireFreshAt(now time.Time) error {
	if !repository.rootExpires.After(now) || repository.timestampExpires.Sub(now) < MinimumPublicationHorizon ||
		!repository.snapshotExpires.After(now) || !repository.targetsExpires.After(now) {
		return invalidMirrorActivation(errors.New("candidate metadata is no longer fresh at mirror commit"))
	}
	if repository.timestampExpires.After(repository.snapshotExpires) || repository.snapshotExpires.After(repository.targetsExpires) ||
		!repository.rootExpires.After(repository.targetsExpires) {
		return invalidMirrorActivation(errors.New("candidate expiry profile must satisfy timestamp <= snapshot <= targets < root"))
	}
	if repository.target != nil && (now.Before(repository.manifestCreated) || !now.Before(repository.manifestExpires)) {
		return invalidMirrorActivation(errors.New("candidate manifest is no longer valid at mirror commit"))
	}
	if repository.target != nil && repository.targetsExpires.After(repository.manifestExpires) {
		return invalidMirrorActivation(errors.New("candidate targets metadata must not outlive the pack manifest"))
	}
	return nil
}

func validateMirrorActivationOptions(options MirrorActivationOptions) (MirrorActivationOptions, []byte, error) {
	if !validTargetPath(options.TargetPath) || !cleanAbsolute(options.MirrorDir) || !cleanAbsolute(options.CandidateDir) || !cleanAbsolute(options.TrustedRootPath) ||
		!validDigest(options.ExpectedCandidateTimestampSHA256) {
		return MirrorActivationOptions{}, nil, invalidMirrorActivation(errors.New("clean absolute paths, target path, and exact candidate timestamp digest are required"))
	}
	if options.InitializeEmpty {
		if options.ExpectedCurrentTimestampSHA256 != "" {
			return MirrorActivationOptions{}, nil, invalidMirrorActivation(errors.New("initialize-empty forbids an expected current timestamp digest"))
		}
	} else if !validDigest(options.ExpectedCurrentTimestampSHA256) || options.ExpectedCurrentTimestampSHA256 == options.ExpectedCandidateTimestampSHA256 {
		return MirrorActivationOptions{}, nil, invalidMirrorActivation(errors.New("normal activation requires distinct exact current and candidate timestamp digests"))
	}
	trustedRootRaw, err := secureconfigfile.Read(options.TrustedRootPath, secureconfigfile.Options{MaxBytes: 512 << 10, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return MirrorActivationOptions{}, nil, fmt.Errorf("TUF repository: trusted root: %w", err)
	}
	mirror, err := secureconfigfile.ValidateDirectory(options.MirrorDir)
	if err != nil {
		return MirrorActivationOptions{}, nil, fmt.Errorf("TUF repository: mirror directory: %w", err)
	}
	candidate, err := secureconfigfile.ValidateDirectory(options.CandidateDir)
	if err != nil {
		return MirrorActivationOptions{}, nil, fmt.Errorf("TUF repository: candidate directory: %w", err)
	}
	if err := requireDisjointStagePaths(mirror, candidate); err != nil {
		return MirrorActivationOptions{}, nil, invalidMirrorActivation(errors.New("mirror and candidate directories must be disjoint"))
	}
	options.MirrorDir, options.CandidateDir = mirror, candidate
	return options, trustedRootRaw, nil
}

func prepareMirrorDependencies(ctx context.Context, mirror *os.Root, candidatePath string, candidate verifiedMirrorRepository) (created bool, count int, copied uint64, returnErr error) {
	source, err := os.OpenRoot(candidatePath)
	if err != nil {
		return false, 0, 0, err
	}
	defer source.Close()
	directories := map[string]struct{}{".": {}}
	for _, file := range candidate.files {
		if err := ctx.Err(); err != nil {
			return created, count, copied, err
		}
		fileCreated, written, err := prepareMirrorFile(ctx, mirror, source, file)
		if fileCreated {
			created = true
		}
		if err != nil {
			return created, count, copied, err
		}
		count++
		copied += written
		for directory := filepath.Dir(filepath.FromSlash(file.path)); directory != "."; directory = filepath.Dir(directory) {
			directories[filepath.ToSlash(directory)] = struct{}{}
		}
	}
	if err := syncStageDirectories(mirror, directories); err != nil {
		return created, count, copied, fmt.Errorf("TUF repository: sync prepared mirror directories: %w", err)
	}
	return created, count, copied, nil
}

func prepareMirrorFile(ctx context.Context, mirror, source *os.Root, descriptor mirrorFile) (bool, uint64, error) {
	if !validStageRelativePath(descriptor.path) || descriptor.path == "metadata/timestamp.json" {
		return false, 0, invalidMirrorActivation(errors.New("immutable mirror path is unsafe"))
	}
	exists, matches, inspectErr := inspectPreparedMirrorFile(ctx, mirror, descriptor)
	if exists {
		if inspectErr != nil || !matches {
			return false, 0, errors.Join(invalidMirrorActivation(fmt.Errorf("existing immutable file %s differs", descriptor.path)), inspectErr)
		}
		return false, 0, nil
	} else if inspectErr != nil {
		return false, 0, inspectErr
	}
	directory := filepath.Dir(filepath.FromSlash(descriptor.path))
	if err := ensureStageDirectory(mirror, directory, map[string]struct{}{}); err != nil {
		return false, 0, err
	}
	temporary, err := mirrorTemporaryPath(directory)
	if err != nil {
		return false, 0, err
	}
	output, err := mirror.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return false, 0, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = mirror.Remove(temporary)
		}
	}()
	var input io.Reader
	var inputFile *os.File
	if descriptor.raw != nil {
		input = bytes.NewReader(descriptor.raw)
	} else {
		inputFile, err = source.Open(filepath.FromSlash(descriptor.path))
		if err != nil {
			_ = output.Close()
			return false, 0, err
		}
		defer inputFile.Close()
		input = io.LimitReader(inputFile, int64(descriptor.size)+1)
	}
	hasher := sha256.New()
	written, copyErr := copyMirrorFile(ctx, io.MultiWriter(output, hasher), input)
	finishErr := errors.Join(output.Sync(), output.Close())
	if copyErr != nil || finishErr != nil {
		return false, 0, errors.Join(copyErr, finishErr)
	}
	if !mirrorStreamMatches(descriptor, written, hex.EncodeToString(hasher.Sum(nil))) {
		return false, 0, invalidMirrorActivation(fmt.Errorf("candidate file %s changed while preparing", descriptor.path))
	}
	directoryFile, err := mirror.Open(directory)
	if err != nil {
		return false, 0, err
	}
	renameErr := renameNoReplaceAt(directoryFile, filepath.Base(temporary), filepath.Base(filepath.FromSlash(descriptor.path)))
	closeErr := directoryFile.Close()
	if errors.Is(renameErr, os.ErrExist) {
		exists, matches, inspectErr := inspectPreparedMirrorFile(ctx, mirror, descriptor)
		if !exists || inspectErr != nil || !matches {
			return false, 0, errors.Join(invalidMirrorActivation(fmt.Errorf("raced immutable file %s differs", descriptor.path)), inspectErr, closeErr)
		}
		return false, 0, closeErr
	}
	if renameErr != nil || closeErr != nil {
		return renameErr == nil, uint64(written), errors.Join(renameErr, closeErr)
	}
	cleanup = false
	return true, uint64(written), nil
}

func commitMirrorHead(mirror *os.Root, raw []byte, replace bool, closeDirectory func(*os.File) error) (bool, error) {
	if err := mirror.MkdirAll("metadata", 0o700); err != nil {
		return false, err
	}
	temporary, err := mirrorTemporaryPath("metadata")
	if err != nil {
		return false, err
	}
	if err := writeMirrorRawFile(mirror, temporary, raw); err != nil {
		return false, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = mirror.Remove(temporary)
		}
	}()
	directory, err := mirror.Open("metadata")
	if err != nil {
		return false, err
	}
	var renameErr error
	if replace {
		renameErr = replaceRepositoryAt(directory, filepath.Base(temporary), "timestamp.json")
	} else {
		renameErr = renameNoReplaceAt(directory, filepath.Base(temporary), "timestamp.json")
	}
	var syncErr error
	if renameErr == nil {
		syncErr = directory.Sync()
	}
	closeErr := closeDirectory(directory)
	if renameErr == nil {
		cleanup = false
	}
	return renameErr == nil, errors.Join(renameErr, syncErr, closeErr)
}

func readMirrorHead(root *os.Root) ([]byte, string, bool, error) {
	raw, err := readOptionalMirrorFile(root, "metadata/timestamp.json", 64<<10)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	return raw, indexpack.Digest(raw), true, nil
}

func readOptionalMirrorFile(root *os.Root, relative string, maximum int64) ([]byte, error) {
	raw, err := readStageFile(root, relative, maximum)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return raw, nil
}

func writeMirrorRawFile(root *os.Root, relative string, raw []byte) error {
	file, err := root.OpenFile(relative, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := io.Copy(file, bytes.NewReader(raw))
	if writeErr == nil && written != int64(len(raw)) {
		writeErr = io.ErrShortWrite
	}
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func mirrorStreamMatches(descriptor mirrorFile, written int64, digest string) bool {
	if descriptor.raw != nil {
		return written == int64(len(descriptor.raw)) && digest == indexpack.Digest(descriptor.raw)
	}
	return written == int64(descriptor.size) && digest == descriptor.digest
}

func inspectPreparedMirrorFile(ctx context.Context, root *os.Root, descriptor mirrorFile) (bool, bool, error) {
	info, err := root.Lstat(filepath.FromSlash(descriptor.path))
	if errors.Is(err, os.ErrNotExist) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return true, false, nil
	}
	if descriptor.raw != nil {
		raw, err := readStageFile(root, descriptor.path, int64(len(descriptor.raw)))
		if err != nil {
			return true, false, err
		}
		return true, bytes.Equal(raw, descriptor.raw), nil
	}
	if uint64(info.Size()) != descriptor.size {
		return true, false, nil
	}
	if err := verifyMirrorShard(ctx, root, descriptor.path, descriptor.size, descriptor.digest); err != nil {
		return true, false, err
	}
	return true, true, nil
}

func verifyMirrorShard(ctx context.Context, root *os.Root, relative string, size uint64, digest string) error {
	info, err := inspectStageInputPath(root, relative)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || uint64(info.Size()) != size {
		return fmt.Errorf("TUF repository: shard %s is not the declared regular file", relative)
	}
	file, err := root.Open(filepath.FromSlash(relative))
	if err != nil {
		return err
	}
	hasher := sha256.New()
	written, readErr := copyMirrorFile(ctx, hasher, io.LimitReader(file, int64(size)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	if written != int64(size) || hex.EncodeToString(hasher.Sum(nil)) != digest {
		return errors.New("TUF repository: shard size or digest mismatch")
	}
	return nil
}

func copyMirrorFile(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 64<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			written, writeErr := destination.Write(buffer[:read])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != read {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
		if read == 0 {
			return total, io.ErrNoProgress
		}
	}
}

func mirrorTemporaryPath(directory string) (string, error) {
	randomBytes := make([]byte, 16)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}
	return filepath.Join(directory, ".fetchmark-mirror-stage-"+hex.EncodeToString(randomBytes)), nil
}

func openMirrorLock(ctx context.Context, mirror *os.Root) (*os.File, error) {
	const name = ".fetchmark-mirror-lock"
	for range 8 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file, err := mirror.OpenFile(name, os.O_RDWR, 0)
		if err == nil {
			if err := validateMirrorLock(mirror, name, file); err != nil {
				_ = file.Close()
				return nil, err
			}
			return file, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		file, err = mirror.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			if err := validateMirrorLock(mirror, name, file); err != nil {
				_ = file.Close()
				return nil, err
			}
			return file, nil
		}
		if !errors.Is(err, os.ErrExist) && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return nil, errors.New("TUF repository: mirror lock changed repeatedly while opening")
}

func validateMirrorLock(mirror *os.Root, name string, file *os.File) error {
	info, statErr := file.Stat()
	pathInfo, pathErr := mirror.Lstat(name)
	if statErr != nil || pathErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 ||
		pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, pathInfo) {
		return invalidMirrorActivation(errors.New("mirror lock must be the private anchored regular file"))
	}
	return nil
}

func waitForMirrorLock(ctx context.Context, lock *os.File) error {
	for {
		locked, err := tryLockRepositoryFile(lock)
		if err != nil {
			return err
		}
		if locked {
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func mirrorActivationTime(now func() time.Time) (time.Time, error) {
	current := now().UTC()
	if current.IsZero() {
		return time.Time{}, invalidMirrorActivation(errors.New("activation clock returned zero time"))
	}
	return current, nil
}

func mirrorLifecycleError(result MirrorActivationResult, prepared, committed bool, cause error) error {
	if committed {
		return &CommittedMirrorActivationError{Result: result, Cause: cause}
	}
	if prepared {
		return &PreparedMirrorActivationError{Result: result, Cause: cause}
	}
	return cause
}

func invalidMirrorActivation(cause error) error {
	return fmt.Errorf("%w: %v", ErrInvalidMirrorActivation, cause)
}
