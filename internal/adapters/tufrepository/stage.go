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
	"sort"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackchannel"
	"github.com/theupdateframework/go-tuf/v2/metadata"
)

type StageStatus string

const (
	StatusStaged    StageStatus = "staged"
	StatusWithdrawn StageStatus = "withdrawn"
)

var (
	ErrInvalidStage = errors.New("TUF repository: invalid stage")
	ErrStageExists  = errors.New("TUF repository: output already exists")
)

const MinimumPublicationHorizon = 15 * time.Minute

type StageOptions struct {
	Identity               Identity
	BundleDir              string
	PreviousRepositoryDir  string
	OutputDir              string
	TargetPath             string
	Version                int64
	Now                    time.Time
	TimestampExpires       time.Time
	SnapshotExpires        time.Time
	TargetsExpires         time.Time
	PublisherKeys          map[string]ed25519.PublicKey
	ExpectedManifestSHA256 string
	Withdraw               bool
}

type StageResult struct {
	Status             StageStatus    `json:"status"`
	Path               string         `json:"path"`
	RepositoryID       string         `json:"repository_id"`
	TargetPath         string         `json:"target_path"`
	Version            int64          `json:"version"`
	PackID             string         `json:"pack_id,omitempty"`
	Revision           uint64         `json:"revision,omitempty"`
	Kind               indexpack.Kind `json:"kind,omitempty"`
	ManifestSHA256     string         `json:"manifest_sha256,omitempty"`
	RootSHA256         string         `json:"root_sha256"`
	RootVersion        int64          `json:"root_version"`
	TargetsSHA256      string         `json:"targets_sha256"`
	SnapshotSHA256     string         `json:"snapshot_sha256"`
	TimestampSHA256    string         `json:"timestamp_sha256"`
	TimestampExpiresAt string         `json:"timestamp_expires_at"`
	SnapshotExpiresAt  string         `json:"snapshot_expires_at"`
	TargetsExpiresAt   string         `json:"targets_expires_at"`
	FileCount          int            `json:"file_count"`
	StagedBytes        uint64         `json:"staged_bytes"`
}

// CommittedStageError means the repository directory was atomically renamed
// into place. Operators must inspect the result before retrying.
type CommittedStageError struct {
	Result StageResult
	Cause  error
}

func (err *CommittedStageError) Error() string {
	return fmt.Sprintf(
		"TUF repository: stage committed at %s but completion failed: root_sha256=%s targets_sha256=%s snapshot_sha256=%s timestamp_sha256=%s manifest_sha256=%s: %v; inspect the committed generation before retrying or removing it",
		err.Result.Path, err.Result.RootSHA256, err.Result.TargetsSHA256, err.Result.SnapshotSHA256,
		err.Result.TimestampSHA256, err.Result.ManifestSHA256, err.Cause,
	)
}

func (err *CommittedStageError) Unwrap() error { return err.Cause }

type stageDependencies struct {
	now                      func() time.Time
	beforeActivate           func()
	closeActivationDirectory func(*os.File) error
	closeParent              func(*os.Root) error
	syncParent               func(*os.Root) error
}

func defaultStageDependencies() stageDependencies {
	return stageDependencies{
		now:                      func() time.Time { return time.Now().UTC() },
		closeActivationDirectory: func(file *os.File) error { return file.Close() },
		closeParent:              func(root *os.Root) error { return root.Close() },
		syncParent:               syncRoot,
	}
}

func Stage(ctx context.Context, options StageOptions) (StageResult, error) {
	return stageWithDependencies(ctx, options, defaultStageDependencies())
}

func stageWithDependencies(ctx context.Context, options StageOptions, dependencies stageDependencies) (result StageResult, returnErr error) {
	if ctx == nil {
		return StageResult{}, invalidStage(errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return StageResult{}, err
	}
	if dependencies.now == nil || dependencies.closeActivationDirectory == nil || dependencies.closeParent == nil || dependencies.syncParent == nil {
		return StageResult{}, invalidStage(errors.New("stage dependencies are incomplete"))
	}
	validated, err := validateStageOptions(options)
	if err != nil {
		return StageResult{}, err
	}
	options = validated
	bootstrapRootRaw, err := options.Identity.BootstrapRoot()
	if err != nil {
		return StageResult{}, err
	}
	rootRaw, err := options.Identity.ActiveRoot()
	if err != nil {
		return StageResult{}, err
	}
	rootChain, err := options.Identity.RootChain()
	if err != nil {
		return StageResult{}, err
	}
	activeRoot, err := decodeCurrentRoot(rootRaw)
	if err != nil {
		return StageResult{}, err
	}
	if !activeRoot.Signed.Expires.After(options.TargetsExpires) {
		return StageResult{}, invalidStage(errors.New("root expiry must be after targets expiry"))
	}
	var manifest indexpack.Manifest
	var manifestRaw, signatureRaw []byte
	if !options.Withdraw {
		manifest, manifestRaw, signatureRaw, err = verifyBundle(options)
		if err != nil {
			return StageResult{}, err
		}
		manifestExpires, _ := time.Parse(time.RFC3339, manifest.ExpiresAt)
		if options.TargetsExpires.After(manifestExpires) {
			return StageResult{}, invalidStage(errors.New("targets metadata must not outlive the advertised pack manifest"))
		}
	}
	if err := verifyPreviousGeneration(options, bootstrapRootRaw, rootChain, manifest, indexpack.ManifestDigest(manifestRaw)); err != nil {
		return StageResult{}, err
	}
	targetsRaw, snapshotRaw, timestampRaw, err := buildMetadata(options, rootRaw, manifest, manifestRaw)
	if err != nil {
		return StageResult{}, err
	}
	result = StageResult{
		Status: StatusStaged, RepositoryID: options.Identity.repositoryID, TargetPath: options.TargetPath,
		Version: options.Version, RootSHA256: indexpack.Digest(rootRaw), RootVersion: activeRoot.Signed.Version,
		TargetsSHA256:  indexpack.Digest(targetsRaw),
		SnapshotSHA256: indexpack.Digest(snapshotRaw), TimestampSHA256: indexpack.Digest(timestampRaw),
		TimestampExpiresAt: options.TimestampExpires.Format(time.RFC3339),
		SnapshotExpiresAt:  options.SnapshotExpires.Format(time.RFC3339), TargetsExpiresAt: options.TargetsExpires.Format(time.RFC3339),
	}
	if options.Withdraw {
		result.Status = StatusWithdrawn
	} else {
		result.PackID, result.Revision, result.Kind = manifest.PackID, manifest.Revision, manifest.Kind
		result.ManifestSHA256 = indexpack.ManifestDigest(manifestRaw)
	}

	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.OutputDir))
	if err != nil {
		return StageResult{}, fmt.Errorf("TUF repository: output parent: %w", err)
	}
	outputBase := filepath.Base(options.OutputDir)
	outputPath := filepath.Join(parentPath, outputBase)
	result.Path = outputPath
	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return StageResult{}, fmt.Errorf("TUF repository: anchor output parent: %w", err)
	}
	committed := false
	defer func() {
		if closeErr := dependencies.closeParent(parentRoot); closeErr != nil {
			if committed {
				closeErr = &CommittedStageError{Result: result, Cause: fmt.Errorf("close output parent: %w", closeErr)}
			}
			returnErr = errors.Join(returnErr, closeErr)
		}
	}()
	if err := rootStillAtPath(parentRoot, parentPath); err != nil {
		return StageResult{}, err
	}
	if _, err := parentRoot.Lstat(outputBase); err == nil {
		return StageResult{}, ErrStageExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return StageResult{}, fmt.Errorf("TUF repository: inspect output: %w", err)
	}
	stagingName, err := createStagingName(parentRoot)
	if err != nil {
		return StageResult{}, err
	}
	if err := parentRoot.Mkdir(stagingName, 0o700); err != nil {
		return StageResult{}, fmt.Errorf("TUF repository: create staging directory: %w", err)
	}
	stagingExists := true
	defer func() {
		if stagingExists {
			if removeErr := parentRoot.RemoveAll(stagingName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				returnErr = errors.Join(returnErr, fmt.Errorf("TUF repository: remove incomplete stage: %w", removeErr))
			}
		}
	}()
	stagingRoot, err := parentRoot.OpenRoot(stagingName)
	if err != nil {
		return StageResult{}, fmt.Errorf("TUF repository: anchor staging directory: %w", err)
	}
	stagingOpen := true
	defer func() {
		if stagingOpen {
			returnErr = errors.Join(returnErr, stagingRoot.Close())
		}
	}()
	directories := map[string]struct{}{".": {}, "metadata": {}, "targets": {}}
	for directory := range directories {
		if directory != "." {
			if err := stagingRoot.MkdirAll(directory, 0o700); err != nil {
				return StageResult{}, fmt.Errorf("TUF repository: create %s: %w", directory, err)
			}
		}
	}
	write := func(relative string, raw []byte) error {
		if err := writeStageFile(stagingRoot, relative, raw, directories); err != nil {
			return err
		}
		result.FileCount++
		result.StagedBytes += uint64(len(raw))
		return nil
	}
	if err := write("root.json", bootstrapRootRaw); err != nil {
		return StageResult{}, err
	}
	for index, rootVersionRaw := range rootChain {
		if err := write(fmt.Sprintf("metadata/%d.root.json", index+1), rootVersionRaw); err != nil {
			return StageResult{}, err
		}
	}
	if !options.Withdraw {
		prefix := path.Dir(options.TargetPath)
		remoteManifest := path.Join("targets", prefix, result.ManifestSHA256+".manifest.json")
		if err := write(remoteManifest, manifestRaw); err != nil {
			return StageResult{}, err
		}
		remoteSignature := path.Join("targets", prefix, result.ManifestSHA256+".manifest.ed25519")
		if err := write(remoteSignature, signatureRaw); err != nil {
			return StageResult{}, err
		}
		bundleRoot, err := os.OpenRoot(options.BundleDir)
		if err != nil {
			return StageResult{}, fmt.Errorf("TUF repository: reopen bundle: %w", err)
		}
		for _, shard := range manifest.Shards {
			if err := ctx.Err(); err != nil {
				_ = bundleRoot.Close()
				return StageResult{}, err
			}
			written, err := copyVerifiedStageFile(stagingRoot, bundleRoot, shard.Path, path.Join("targets", prefix, shard.Path), shard.CompressedSizeBytes, shard.SHA256, directories)
			if err != nil {
				_ = bundleRoot.Close()
				return StageResult{}, err
			}
			result.FileCount++
			result.StagedBytes += written
		}
		if err := bundleRoot.Close(); err != nil {
			return StageResult{}, fmt.Errorf("TUF repository: close bundle: %w", err)
		}
	}
	if err := write(fmt.Sprintf("metadata/%d.targets.json", options.Version), targetsRaw); err != nil {
		return StageResult{}, err
	}
	if err := write(fmt.Sprintf("metadata/%d.snapshot.json", options.Version), snapshotRaw); err != nil {
		return StageResult{}, err
	}
	// timestamp.json is written last because it is the repository publication
	// commit point when an operator later mirrors this immutable generation.
	if err := write("metadata/timestamp.json", timestampRaw); err != nil {
		return StageResult{}, err
	}
	if err := syncStageDirectories(stagingRoot, directories); err != nil {
		return StageResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return StageResult{}, err
	}
	if err := rootStillAtPath(parentRoot, parentPath); err != nil {
		return StageResult{}, err
	}
	if err := stagingRoot.Close(); err != nil {
		return StageResult{}, fmt.Errorf("TUF repository: close staging directory: %w", err)
	}
	stagingOpen = false
	if dependencies.beforeActivate != nil {
		dependencies.beforeActivate()
	}
	if err := ctx.Err(); err != nil {
		return StageResult{}, err
	}
	activationNow := dependencies.now().UTC()
	if options.TimestampExpires.Sub(activationNow) < MinimumPublicationHorizon {
		return StageResult{}, invalidStage(fmt.Errorf(
			"timestamp expiry must remain at least %s after final staging", MinimumPublicationHorizon,
		))
	}
	directory, err := parentRoot.Open(".")
	if err != nil {
		return StageResult{}, fmt.Errorf("TUF repository: open activation directory: %w", err)
	}
	renameErr := renameNoReplaceAt(directory, stagingName, outputBase)
	closeErr := dependencies.closeActivationDirectory(directory)
	if renameErr != nil {
		if errors.Is(renameErr, os.ErrExist) {
			renameErr = ErrStageExists
		}
		return StageResult{}, errors.Join(renameErr, closeErr)
	}
	committed = true
	stagingExists = false
	if closeErr != nil {
		return result, &CommittedStageError{Result: result, Cause: fmt.Errorf("close activation directory: %w", closeErr)}
	}
	if err := rootStillAtPath(parentRoot, parentPath); err != nil {
		return result, &CommittedStageError{Result: result, Cause: err}
	}
	if err := dependencies.syncParent(parentRoot); err != nil {
		return result, &CommittedStageError{Result: result, Cause: fmt.Errorf("sync output parent: %w", err)}
	}
	if err := rootStillAtPath(parentRoot, parentPath); err != nil {
		return result, &CommittedStageError{Result: result, Cause: err}
	}
	return result, nil
}

func validateStageOptions(options StageOptions) (StageOptions, error) {
	if err := options.Identity.validate(); err != nil {
		return StageOptions{}, invalidStage(err)
	}
	if options.Version < 1 || !validTargetPath(options.TargetPath) || !cleanAbsolute(options.OutputDir) {
		return StageOptions{}, invalidStage(errors.New("positive version, clean target path, and clean absolute output are required"))
	}
	if !exactUTCFutureSecond(options.Now, time.Time{}) {
		return StageOptions{}, invalidStage(errors.New("now must be an exact UTC second"))
	}
	if !exactUTCFutureSecond(options.TimestampExpires, options.Now) || !exactUTCFutureSecond(options.SnapshotExpires, options.Now) ||
		!exactUTCFutureSecond(options.TargetsExpires, options.Now) || options.TimestampExpires.After(options.SnapshotExpires) ||
		options.SnapshotExpires.After(options.TargetsExpires) {
		return StageOptions{}, invalidStage(errors.New("expiries must be exact future UTC seconds ordered timestamp <= snapshot <= targets"))
	}
	if options.TimestampExpires.Sub(options.Now) < MinimumPublicationHorizon {
		return StageOptions{}, invalidStage(fmt.Errorf("timestamp expiry must be at least %s after staging starts", MinimumPublicationHorizon))
	}
	if options.PreviousRepositoryDir == "" {
		if options.Version != 1 || options.Withdraw {
			return StageOptions{}, invalidStage(errors.New("an initial generation must be version 1 and cannot withdraw"))
		}
	} else if !cleanAbsolute(options.PreviousRepositoryDir) {
		return StageOptions{}, invalidStage(errors.New("previous repository must be a clean absolute path"))
	}
	if options.Withdraw {
		if options.BundleDir != "" || len(options.PublisherKeys) != 0 || options.ExpectedManifestSHA256 != "" {
			return StageOptions{}, invalidStage(errors.New("withdrawal forbids bundle and publisher keys"))
		}
	} else if !cleanAbsolute(options.BundleDir) || len(options.PublisherKeys) == 0 || !validDigest(options.ExpectedManifestSHA256) {
		return StageOptions{}, invalidStage(errors.New("publication requires a clean absolute bundle, publisher keys, and exact verified manifest digest"))
	}
	if options.BundleDir != "" {
		canonical, err := secureconfigfile.ValidateDirectory(options.BundleDir)
		if err != nil {
			return StageOptions{}, invalidStage(fmt.Errorf("bundle directory: %w", err))
		}
		options.BundleDir = canonical
	}
	if options.PreviousRepositoryDir != "" {
		canonical, err := secureconfigfile.ValidateDirectory(options.PreviousRepositoryDir)
		if err != nil {
			return StageOptions{}, invalidStage(fmt.Errorf("previous repository directory: %w", err))
		}
		options.PreviousRepositoryDir = canonical
	}
	paths := []string{options.OutputDir}
	if options.BundleDir != "" {
		paths = append(paths, options.BundleDir)
	}
	if options.PreviousRepositoryDir != "" {
		paths = append(paths, options.PreviousRepositoryDir)
	}
	if err := requireDisjointStagePaths(paths...); err != nil {
		return StageOptions{}, err
	}
	return options, nil
}

func verifyBundle(options StageOptions) (indexpack.Manifest, []byte, []byte, error) {
	bundle, err := os.OpenRoot(options.BundleDir)
	if err != nil {
		return indexpack.Manifest{}, nil, nil, fmt.Errorf("TUF repository: anchor bundle: %w", err)
	}
	defer bundle.Close()
	manifestRaw, err := readStageFile(bundle, "manifest.json", indexpack.MaxManifestBytes)
	if err != nil {
		return indexpack.Manifest{}, nil, nil, err
	}
	signatureRaw, err := readStageFile(bundle, "manifest.ed25519", indexpack.MaxSignatureBytes)
	if err != nil {
		return indexpack.Manifest{}, nil, nil, err
	}
	verified, err := indexpack.VerifyManifest(manifestRaw, signatureRaw, options.PublisherKeys)
	if err != nil {
		return indexpack.Manifest{}, nil, nil, fmt.Errorf("TUF repository: verify pack manifest: %w", err)
	}
	if verified.Digest != options.ExpectedManifestSHA256 {
		return indexpack.Manifest{}, nil, nil, invalidStage(errors.New("bundle manifest differs from the exact fully verified manifest"))
	}
	target := openpackchannel.Target{
		Schema: openpackchannel.Schema, PackID: verified.Manifest.PackID, Kind: verified.Manifest.Kind,
		Revision: verified.Manifest.Revision, ManifestSHA256: verified.Digest,
		ParentManifestSHA256: verified.Manifest.ParentManifestSHA256,
	}
	if _, err := target.VerifyManifest(manifestRaw, options.Now); err != nil {
		return indexpack.Manifest{}, nil, nil, err
	}
	for _, shard := range verified.Manifest.Shards {
		if err := verifyStageFile(bundle, shard.Path, shard.CompressedSizeBytes, shard.SHA256); err != nil {
			return indexpack.Manifest{}, nil, nil, err
		}
	}
	return verified.Manifest, manifestRaw, signatureRaw, nil
}

func buildMetadata(options StageOptions, rootRaw []byte, manifest indexpack.Manifest, manifestRaw []byte) ([]byte, []byte, []byte, error) {
	root, err := metadata.Root().FromBytes(rootRaw)
	if err != nil {
		return nil, nil, nil, err
	}
	targets := metadata.Targets(options.TargetsExpires)
	targets.Signed.Version = options.Version
	if !options.Withdraw {
		targetFile, err := metadata.TargetFile().FromBytes(options.TargetPath, manifestRaw, "sha256")
		if err != nil {
			return nil, nil, nil, err
		}
		custom, err := openpackchannel.EncodeCustom(openpackchannel.Target{
			Schema: openpackchannel.Schema, PackID: manifest.PackID, Kind: manifest.Kind, Revision: manifest.Revision,
			ManifestSHA256: indexpack.ManifestDigest(manifestRaw), ParentManifestSHA256: manifest.ParentManifestSHA256,
		})
		if err != nil {
			return nil, nil, nil, err
		}
		targetFile.Custom = &custom
		targets.Signed.Targets[options.TargetPath] = targetFile
	}
	if err := signMetadata(targets, options.Identity.targetsKey); err != nil {
		return nil, nil, nil, err
	}
	if err := root.VerifyDelegate(metadata.TARGETS, targets); err != nil {
		return nil, nil, nil, err
	}
	targetsRaw, err := targets.ToBytes(false)
	if err != nil {
		return nil, nil, nil, err
	}
	snapshot := metadata.Snapshot(options.SnapshotExpires)
	snapshot.Signed.Version = options.Version
	snapshot.Signed.Meta["targets.json"] = metadataFile(options.Version, targetsRaw)
	if err := signMetadata(snapshot, options.Identity.snapshotKey); err != nil {
		return nil, nil, nil, err
	}
	if err := root.VerifyDelegate(metadata.SNAPSHOT, snapshot); err != nil {
		return nil, nil, nil, err
	}
	snapshotRaw, err := snapshot.ToBytes(false)
	if err != nil {
		return nil, nil, nil, err
	}
	timestamp := metadata.Timestamp(options.TimestampExpires)
	timestamp.Signed.Version = options.Version
	timestamp.Signed.Meta["snapshot.json"] = metadataFile(options.Version, snapshotRaw)
	if err := signMetadata(timestamp, options.Identity.timestampKey); err != nil {
		return nil, nil, nil, err
	}
	if err := root.VerifyDelegate(metadata.TIMESTAMP, timestamp); err != nil {
		return nil, nil, nil, err
	}
	timestampRaw, err := timestamp.ToBytes(false)
	return targetsRaw, snapshotRaw, timestampRaw, err
}

func verifyPreviousGeneration(options StageOptions, expectedBootstrap []byte, expectedChain [][]byte, nextManifest indexpack.Manifest, nextDigest string) error {
	if options.PreviousRepositoryDir == "" {
		return nil
	}
	previousRoot, err := os.OpenRoot(options.PreviousRepositoryDir)
	if err != nil {
		return fmt.Errorf("TUF repository: anchor previous repository: %w", err)
	}
	defer previousRoot.Close()
	rootRaw, err := readStageFile(previousRoot, "root.json", 512<<10)
	if err != nil {
		return err
	}
	if !bytes.Equal(rootRaw, expectedBootstrap) {
		return invalidStage(errors.New("previous repository uses a different bootstrap root"))
	}
	if len(expectedChain) == 0 || !bytes.Equal(expectedChain[0], expectedBootstrap) {
		return invalidStage(errors.New("identity root chain does not begin with its bootstrap root"))
	}
	root, previousChain, err := readMirrorRootChain(previousRoot, expectedBootstrap)
	if err != nil {
		return invalidStage(fmt.Errorf("previous repository root chain: %w", err))
	}
	if len(previousChain) > len(expectedChain) {
		return invalidStage(errors.New("previous repository has a root beyond the identity chain"))
	}
	for index, actual := range previousChain {
		if !bytes.Equal(actual, expectedChain[index]) {
			return invalidStage(fmt.Errorf("previous repository root version %d differs from the identity chain", index+1))
		}
	}
	timestampRaw, err := readStageFile(previousRoot, "metadata/timestamp.json", 64<<10)
	if err != nil {
		return err
	}
	timestamp, err := metadata.Timestamp().FromBytes(timestampRaw)
	if err != nil {
		return err
	}
	if err := root.VerifyDelegate(metadata.TIMESTAMP, timestamp); err != nil {
		return invalidStage(err)
	}
	snapshotInfo, found := timestamp.Signed.Meta["snapshot.json"]
	if !found || len(timestamp.Signed.Meta) != 1 {
		return invalidStage(errors.New("previous timestamp must bind only snapshot.json"))
	}
	snapshotRaw, err := readStageFile(previousRoot, fmt.Sprintf("metadata/%d.snapshot.json", snapshotInfo.Version), 2<<20)
	if err != nil || !metadataFileMatches(snapshotInfo, snapshotRaw) {
		return invalidStage(errors.New("previous snapshot bytes do not match timestamp"))
	}
	snapshot, err := metadata.Snapshot().FromBytes(snapshotRaw)
	if err != nil {
		return err
	}
	if err := root.VerifyDelegate(metadata.SNAPSHOT, snapshot); err != nil {
		return invalidStage(err)
	}
	targetsInfo, found := snapshot.Signed.Meta["targets.json"]
	if !found || len(snapshot.Signed.Meta) != 1 {
		return invalidStage(errors.New("previous snapshot must bind only targets.json"))
	}
	targetsRaw, err := readStageFile(previousRoot, fmt.Sprintf("metadata/%d.targets.json", targetsInfo.Version), 5<<20)
	if err != nil || !metadataFileMatches(targetsInfo, targetsRaw) {
		return invalidStage(errors.New("previous targets bytes do not match snapshot"))
	}
	targets, err := metadata.Targets().FromBytes(targetsRaw)
	if err != nil {
		return err
	}
	if err := root.VerifyDelegate(metadata.TARGETS, targets); err != nil {
		return invalidStage(err)
	}
	previousVersion := timestamp.Signed.Version
	if snapshot.Signed.Version != previousVersion || targets.Signed.Version != previousVersion ||
		snapshotInfo.Version != previousVersion || targetsInfo.Version != previousVersion || options.Version != previousVersion+1 {
		return invalidStage(errors.New("metadata roles must advance together by exactly one version"))
	}
	if targets.Signed.Delegations != nil || len(targets.Signed.Targets) > 1 {
		return invalidStage(errors.New("previous repository is outside the single-target profile"))
	}
	if !options.Withdraw && len(targets.Signed.Targets) == 0 {
		return invalidStage(errors.New("reintroducing a withdrawn target is not supported without an explicit rollback floor"))
	}
	for targetPath, targetFile := range targets.Signed.Targets {
		if targetPath != options.TargetPath {
			return invalidStage(errors.New("previous repository target path differs"))
		}
		if options.Withdraw {
			continue
		}
		previous, err := openpackchannel.DecodeCustom(targetFile.Custom)
		if err != nil {
			return invalidStage(fmt.Errorf("previous target custom metadata: %w", err))
		}
		if previous.PackID != nextManifest.PackID || nextManifest.Revision <= previous.Revision ||
			nextDigest == previous.ManifestSHA256 {
			return invalidStage(errors.New("pack identity must stay fixed while revision and manifest digest advance"))
		}
		if nextManifest.Kind == indexpack.KindDelta &&
			(previous.Kind != indexpack.KindSnapshot || nextManifest.ParentManifestSHA256 != previous.ManifestSHA256) {
			return invalidStage(errors.New("delta must name the exact previously advertised snapshot parent"))
		}
	}
	return nil
}

func metadataFile(version int64, raw []byte) *metadata.MetaFiles {
	digest := sha256.Sum256(raw)
	return &metadata.MetaFiles{
		Version: version, Length: int64(len(raw)),
		Hashes: metadata.Hashes{"sha256": metadata.HexBytes(digest[:])},
	}
}

func metadataFileMatches(expected *metadata.MetaFiles, raw []byte) bool {
	if expected == nil || expected.Length != int64(len(raw)) {
		return false
	}
	digest, found := expected.Hashes["sha256"]
	actual := sha256.Sum256(raw)
	return found && bytes.Equal(digest, actual[:])
}

func writeStageFile(root *os.Root, relative string, raw []byte, directories map[string]struct{}) error {
	if !validStageRelativePath(relative) {
		return invalidStage(errors.New("output path is unsafe"))
	}
	if err := ensureStageDirectory(root, filepath.Dir(filepath.FromSlash(relative)), directories); err != nil {
		return err
	}
	file, err := root.OpenFile(filepath.FromSlash(relative), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("TUF repository: create %s: %w", relative, err)
	}
	written, writeErr := io.Copy(file, bytes.NewReader(raw))
	if writeErr == nil && written != int64(len(raw)) {
		writeErr = io.ErrShortWrite
	}
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func copyVerifiedStageFile(destination, source *os.Root, sourcePath, destinationPath string, size uint64, digest string, directories map[string]struct{}) (uint64, error) {
	if !validStageRelativePath(sourcePath) || !validStageRelativePath(destinationPath) || size > indexpack.MaxCompressedShardBytes {
		return 0, invalidStage(errors.New("shard path or size is unsafe"))
	}
	info, err := inspectStageInputPath(source, sourcePath)
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || uint64(info.Size()) != size {
		return 0, fmt.Errorf("TUF repository: shard %s is not the declared regular file", sourcePath)
	}
	input, err := source.Open(filepath.FromSlash(sourcePath))
	if err != nil {
		return 0, err
	}
	defer input.Close()
	opened, err := input.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return 0, invalidStage(errors.New("shard changed while opening"))
	}
	directory := filepath.Dir(filepath.FromSlash(destinationPath))
	if err := ensureStageDirectory(destination, directory, directories); err != nil {
		return 0, err
	}
	output, err := destination.OpenFile(filepath.FromSlash(destinationPath), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(output, hasher), io.LimitReader(input, int64(size)+1))
	finishErr := errors.Join(output.Sync(), output.Close())
	if copyErr != nil || finishErr != nil {
		return 0, errors.Join(copyErr, finishErr)
	}
	if written != int64(size) || hex.EncodeToString(hasher.Sum(nil)) != digest {
		return 0, errors.New("TUF repository: shard size or digest mismatch")
	}
	after, err := input.Stat()
	if err != nil || !os.SameFile(opened, after) || opened.Size() != after.Size() || !opened.ModTime().Equal(after.ModTime()) {
		return 0, invalidStage(errors.New("shard changed while copying"))
	}
	return uint64(written), nil
}

func verifyStageFile(root *os.Root, relative string, size uint64, digest string) error {
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
	written, readErr := io.Copy(hasher, io.LimitReader(file, int64(size)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	if written != int64(size) || hex.EncodeToString(hasher.Sum(nil)) != digest {
		return errors.New("TUF repository: shard size or digest mismatch")
	}
	return nil
}

func readStageFile(root *os.Root, relative string, maximum int64) ([]byte, error) {
	if maximum < 1 || !validStageRelativePath(relative) {
		return nil, invalidStage(errors.New("input path is unsafe"))
	}
	info, err := inspectStageInputPath(root, relative)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > maximum {
		return nil, fmt.Errorf("TUF repository: %s is not a bounded regular file", relative)
	}
	file, err := root.Open(filepath.FromSlash(relative))
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maximum+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if int64(len(raw)) != info.Size() {
		return nil, invalidStage(errors.New("input changed while reading"))
	}
	return raw, nil
}

func inspectStageInputPath(root *os.Root, relative string) (os.FileInfo, error) {
	if root == nil || !validStageRelativePath(relative) {
		return nil, invalidStage(errors.New("input path is unsafe"))
	}
	components := strings.Split(filepath.FromSlash(relative), string(filepath.Separator))
	current := ""
	for index, component := range components {
		if current == "" {
			current = component
		} else {
			current = filepath.Join(current, component)
		}
		info, err := root.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, invalidStage(fmt.Errorf("input path component %s is a symlink", current))
		}
		if index < len(components)-1 && !info.IsDir() {
			return nil, invalidStage(fmt.Errorf("input path component %s is not a directory", current))
		}
		if index == len(components)-1 {
			return info, nil
		}
	}
	return nil, invalidStage(errors.New("input path is empty"))
}

func ensureStageDirectory(root *os.Root, directory string, directories map[string]struct{}) error {
	if directory == "." {
		return nil
	}
	if err := root.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	for current := directory; current != "."; current = filepath.Dir(current) {
		directories[filepath.ToSlash(current)] = struct{}{}
	}
	return nil
}

func syncStageDirectories(root *os.Root, directories map[string]struct{}) error {
	ordered := make([]string, 0, len(directories))
	for directory := range directories {
		ordered = append(ordered, directory)
	}
	sort.Slice(ordered, func(left, right int) bool {
		return strings.Count(ordered[left], "/") > strings.Count(ordered[right], "/")
	})
	for _, directory := range ordered {
		file, err := root.Open(filepath.FromSlash(directory))
		if err != nil {
			return err
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			return err
		}
	}
	return nil
}

func syncRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func rootStillAtPath(root *os.Root, rootPath string) error {
	anchored, err := root.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(rootPath)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return invalidStage(errors.New("output parent path changed"))
	}
	return nil
}

func createStagingName(root *os.Root) (string, error) {
	for range 8 {
		randomBytes := make([]byte, 16)
		if _, err := rand.Read(randomBytes); err != nil {
			return "", err
		}
		name := ".fetchmark-tuf-stage-" + hex.EncodeToString(randomBytes)
		if _, err := root.Lstat(name); errors.Is(err, os.ErrNotExist) {
			return name, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("TUF repository: could not allocate staging directory")
}

func requireDisjointStagePaths(paths ...string) error {
	canonical := make([]string, len(paths))
	for index, raw := range paths {
		if index == 0 {
			parent, err := filepath.EvalSymlinks(filepath.Dir(raw))
			if err != nil {
				return invalidStage(err)
			}
			canonical[index] = filepath.Join(parent, filepath.Base(raw))
		} else {
			value, err := filepath.EvalSymlinks(raw)
			if err != nil {
				return invalidStage(err)
			}
			canonical[index] = value
		}
	}
	for left := 0; left < len(canonical); left++ {
		for right := left + 1; right < len(canonical); right++ {
			if pathContains(canonical[left], canonical[right]) || pathContains(canonical[right], canonical[left]) {
				return invalidStage(errors.New("output, bundle, and previous repository paths must be disjoint"))
			}
		}
	}
	return nil
}

func pathContains(directory, candidate string) bool {
	relative, err := filepath.Rel(directory, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func cleanAbsolute(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && filepath.IsAbs(value) && filepath.Clean(value) == value
}

func validTargetPath(value string) bool {
	return len(value) > 0 && len(value) <= 512 && !strings.Contains(value, "\\") && !strings.Contains(value, "%") &&
		!path.IsAbs(value) && path.Clean(value) == value && !strings.HasPrefix(value, "../") &&
		strings.HasPrefix(value, "packs/") && strings.HasSuffix(value, "/manifest.json")
}

func validStageRelativePath(value string) bool {
	return value != "" && !strings.Contains(value, "\\") && !strings.Contains(value, "%") &&
		!path.IsAbs(value) && path.Clean(value) == value && !strings.HasPrefix(value, "../")
}

func validDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func invalidStage(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidStage, err)
}
