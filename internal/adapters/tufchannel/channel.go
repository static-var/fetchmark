// Package tufchannel verifies operator-supplied, offline TUF metadata and
// selects exact open-pack manifests. Select is network-free; the separate
// Retrieve entry point performs explicit bounded artifact retrieval. Neither
// path mutates an open-pack registry or installs a pack.
package tufchannel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackchannel"
	"github.com/theupdateframework/go-tuf/v2/metadata"
	"github.com/theupdateframework/go-tuf/v2/metadata/config"
	"github.com/theupdateframework/go-tuf/v2/metadata/trustedmetadata"
	"github.com/theupdateframework/go-tuf/v2/metadata/updater"
)

const (
	offlineMetadataURL = "https://offline.fetchmark.invalid/metadata"
	stateDatabaseName  = "channel-state.db"
	bootstrapKey       = "bootstrap_root_sha256"
	maxRootBytes       = 512 << 10
	maxTimestampBytes  = 64 << 10
	maxSnapshotBytes   = 2 << 20
	maxTargetsBytes    = 5 << 20
	maxTargetPathBytes = 512
)

var (
	ErrInvalidOptions    = errors.New("TUF channel: invalid options")
	ErrBootstrapChanged  = errors.New("TUF channel: bootstrap root changed")
	ErrStateBusy         = errors.New("TUF channel: state is in use")
	metadataFilenameExpr = regexp.MustCompile(`^(?:[1-9][0-9]*\.)?(?:root|timestamp|snapshot|targets)\.json$`)
	stateBucket          = []byte("fetchmark-open-pack-channel-v1")
)

type Status string

const (
	StatusSelected Status = "selected"
	// StatusAbsent is a verified absence in the current top-level targets role.
	// Operators may use it as a revocation signal for a previously known path.
	StatusAbsent Status = "absent"
)

type Options struct {
	TrustedRootPath string
	MetadataDir     string
	StateDir        string
	BundleDir       string
	TargetPath      string
}

type selectionDependencies struct {
	syncFile     func(*os.File) error
	loadManifest func(context.Context, verifiedManifestTarget) ([]byte, error)
}

type verifiedManifestTarget struct {
	Path               string
	Target             *metadata.TargetFiles
	Custom             openpackchannel.Target
	ConsistentSnapshot bool
}

type Selection struct {
	Status               Status         `json:"status"`
	TargetPath           string         `json:"target_path"`
	PackID               string         `json:"pack_id,omitempty"`
	Kind                 indexpack.Kind `json:"kind,omitempty"`
	Revision             uint64         `json:"revision,omitempty"`
	ManifestSHA256       string         `json:"manifest_sha256,omitempty"`
	ParentManifestSHA256 string         `json:"parent_manifest_sha256,omitempty"`
	SigningKeyID         string         `json:"signing_key_id,omitempty"`
	ManifestRecordCount  uint64         `json:"manifest_record_count,omitempty"`
	CreatedAt            string         `json:"created_at,omitempty"`
	ExpiresAt            string         `json:"expires_at,omitempty"`
	TargetLength         int64          `json:"target_length,omitempty"`
	RootVersion          int64          `json:"root_version"`
	TimestampVersion     int64          `json:"timestamp_version"`
	SnapshotVersion      int64          `json:"snapshot_version"`
	TargetsVersion       int64          `json:"targets_version"`
	TimestampExpiresAt   string         `json:"timestamp_expires_at"`
	SnapshotExpiresAt    string         `json:"snapshot_expires_at"`
	TargetsExpiresAt     string         `json:"targets_expires_at"`
}

// Select advances durable trusted TUF metadata from an operator-supplied
// directory and verifies one exact bundle manifest selected by the current
// top-level targets role. The state directory must already exist and be safe.
func Select(ctx context.Context, options Options) (selection Selection, returnErr error) {
	return selectWithDependencies(ctx, options, defaultSelectionDependencies())
}

func defaultSelectionDependencies() selectionDependencies {
	return selectionDependencies{syncFile: func(file *os.File) error { return file.Sync() }}
}

func selectWithDependencies(ctx context.Context, options Options, dependencies selectionDependencies) (selection Selection, returnErr error) {
	if ctx == nil {
		return Selection{}, invalidOptions(errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return Selection{}, err
	}
	if err := validateTargetPath(options.TargetPath); err != nil {
		return Selection{}, invalidOptions(err)
	}
	metadataDir, err := secureconfigfile.ValidateDirectory(options.MetadataDir)
	if err != nil {
		return Selection{}, fmt.Errorf("TUF channel: metadata directory: %w", err)
	}
	stateDir, err := secureconfigfile.ValidateDirectory(options.StateDir)
	if err != nil {
		return Selection{}, fmt.Errorf("TUF channel: state directory: %w", err)
	}
	bundleDir := ""
	disjointPaths := []string{stateDir, metadataDir, options.TrustedRootPath}
	if options.BundleDir != "" {
		bundleDir, err = secureconfigfile.ValidateDirectory(options.BundleDir)
		if err != nil {
			return Selection{}, fmt.Errorf("TUF channel: bundle directory: %w", err)
		}
		disjointPaths = append(disjointPaths, bundleDir)
	}
	if err := requireDisjointPaths(disjointPaths...); err != nil {
		return Selection{}, err
	}
	bootstrapRoot, err := secureconfigfile.Read(options.TrustedRootPath, secureconfigfile.Options{
		MaxBytes: maxRootBytes, Mode: secureconfigfile.PublicConfig,
	})
	if err != nil {
		return Selection{}, fmt.Errorf("TUF channel: trusted root: %w", err)
	}
	if _, err := trustedmetadata.New(bootstrapRoot); err != nil {
		return Selection{}, fmt.Errorf("TUF channel: validate bootstrap root before pinning: %w", err)
	}

	state, err := openState(stateDir, bootstrapRoot)
	if err != nil {
		return Selection{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, state.Close()) }()
	stateRoot, err := os.OpenRoot(stateDir)
	if err != nil {
		return Selection{}, fmt.Errorf("TUF channel: anchor state directory: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, stateRoot.Close()) }()
	if err := preflightCachedTrustedMetadata(stateRoot, stateDir); err != nil {
		return Selection{}, err
	}

	trustedRoot := bootstrapRoot
	cachedRootPath := filepath.Join(stateDir, "root.json")
	if _, err := os.Lstat(cachedRootPath); err == nil {
		trustedRoot, err = secureconfigfile.Read(cachedRootPath, secureconfigfile.Options{
			MaxBytes: maxRootBytes, Mode: secureconfigfile.PublicConfig,
		})
		if err != nil {
			return Selection{}, fmt.Errorf("TUF channel: cached trusted root: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Selection{}, fmt.Errorf("TUF channel: inspect cached trusted root: %w", err)
	}

	metadataRoot, err := os.OpenRoot(metadataDir)
	if err != nil {
		return Selection{}, fmt.Errorf("TUF channel: anchor metadata directory: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, metadataRoot.Close()) }()

	cfg, err := config.New(offlineMetadataURL, trustedRoot)
	if err != nil {
		return Selection{}, fmt.Errorf("TUF channel: configure verifier: %w", err)
	}
	cfg.Fetcher = &rootedMetadataFetcher{ctx: ctx, root: metadataRoot}
	cfg.LocalMetadataDir = stateDir
	cfg.LocalTargetsDir = filepath.Join(stateDir, "targets")
	cfg.MaxRootRotations = 32
	cfg.MaxDelegations = 0
	cfg.RootMaxLength = maxRootBytes
	cfg.TimestampMaxLength = maxTimestampBytes
	cfg.SnapshotMaxLength = maxSnapshotBytes
	cfg.TargetsMaxLength = maxTargetsBytes
	cfg.PrefixTargetsWithHash = true
	cfg.UnsafeLocalMode = false

	client, err := updater.New(cfg)
	if err != nil {
		return Selection{}, fmt.Errorf("TUF channel: initialize verifier: %w", err)
	}
	refreshErr := client.Refresh()
	syncErr := syncTrustedState(stateRoot, stateDir, refreshErr == nil, dependencies.syncFile)
	if refreshErr != nil || syncErr != nil {
		if refreshErr != nil {
			refreshErr = fmt.Errorf("TUF channel: refresh trusted metadata: %w", refreshErr)
		}
		return Selection{}, errors.Join(refreshErr, syncErr)
	}
	if err := ctx.Err(); err != nil {
		return Selection{}, err
	}

	trusted := client.GetTrustedMetadataSet()
	selection, err = metadataSelection(options.TargetPath, trusted)
	if err != nil {
		return Selection{}, err
	}
	if trusted.Targets[metadata.TARGETS].Signed.Delegations != nil {
		return Selection{}, errors.New("TUF channel: delegated targets are not supported by this channel profile")
	}
	target, found := client.GetTopLevelTargets()[options.TargetPath]
	if !found {
		selection.Status = StatusAbsent
		return selection, nil
	}
	if options.BundleDir == "" && dependencies.loadManifest == nil {
		return Selection{}, invalidOptions(errors.New("bundle directory is required for a selected target"))
	}
	if target.Length < 1 || target.Length > indexpack.MaxManifestBytes {
		return Selection{}, fmt.Errorf("TUF channel: selected manifest length must be 1..%d bytes", indexpack.MaxManifestBytes)
	}
	expectedSHA256, ok := target.Hashes["sha256"]
	if !ok || len(expectedSHA256) != sha256.Size {
		return Selection{}, errors.New("TUF channel: selected target must declare SHA-256")
	}
	custom, err := openpackchannel.DecodeCustom(target.Custom)
	if err != nil {
		return Selection{}, err
	}
	if !bytes.Equal(expectedSHA256, mustDecodeDigest(custom.ManifestSHA256)) {
		return Selection{}, errors.New("TUF channel: target SHA-256 does not match manifest_sha256")
	}

	var manifestRaw []byte
	if dependencies.loadManifest != nil {
		manifestRaw, err = dependencies.loadManifest(ctx, verifiedManifestTarget{
			Path: options.TargetPath, Target: target, Custom: custom,
			ConsistentSnapshot: trusted.Root.Signed.ConsistentSnapshot,
		})
		if err != nil {
			return Selection{}, err
		}
	} else {
		bundleRoot, err := os.OpenRoot(bundleDir)
		if err != nil {
			return Selection{}, fmt.Errorf("TUF channel: anchor bundle directory: %w", err)
		}
		var readErr error
		manifestRaw, readErr = readBoundedRegular(bundleRoot, "manifest.json", indexpack.MaxManifestBytes)
		closeErr := bundleRoot.Close()
		if readErr != nil || closeErr != nil {
			return Selection{}, errors.Join(readErr, closeErr)
		}
	}
	if err := target.VerifyLengthHashes(manifestRaw); err != nil {
		return Selection{}, fmt.Errorf("TUF channel: verify selected manifest target: %w", err)
	}
	manifest, err := custom.VerifyManifest(manifestRaw, time.Now().UTC())
	if err != nil {
		return Selection{}, err
	}
	selection.Status = StatusSelected
	selection.PackID = manifest.PackID
	selection.Kind = manifest.Kind
	selection.Revision = manifest.Revision
	selection.ManifestSHA256 = custom.ManifestSHA256
	selection.ParentManifestSHA256 = manifest.ParentManifestSHA256
	selection.SigningKeyID = manifest.SigningKeyID
	selection.ManifestRecordCount = manifest.RecordCount
	selection.CreatedAt = manifest.CreatedAt
	selection.ExpiresAt = manifest.ExpiresAt
	selection.TargetLength = target.Length
	return selection, nil
}

func validateTargetPath(value string) error {
	if value == "" || len(value) > maxTargetPathBytes || strings.Contains(value, "\\") ||
		path.IsAbs(value) || path.Clean(value) != value || strings.HasPrefix(value, "../") ||
		!strings.HasPrefix(value, "packs/") || !strings.HasSuffix(value, "/manifest.json") {
		return errors.New("target path must be a clean packs/.../manifest.json path")
	}
	return nil
}

func requireDisjointPaths(paths ...string) error {
	for left := 0; left < len(paths); left++ {
		for right := left + 1; right < len(paths); right++ {
			leftContainsRight, err := pathContains(paths[left], paths[right])
			if err != nil {
				return fmt.Errorf("TUF channel: inspect path isolation: %w", err)
			}
			rightContainsLeft, err := pathContains(paths[right], paths[left])
			if err != nil {
				return fmt.Errorf("TUF channel: inspect path isolation: %w", err)
			}
			if leftContainsRight || rightContainsLeft {
				return fmt.Errorf("%w: trusted root, metadata, state, and bundle paths must be disjoint", secureconfigfile.ErrUnsafePath)
			}
		}
	}
	return nil
}

func pathContains(directoryPath, candidatePath string) (bool, error) {
	directory, err := os.Lstat(directoryPath)
	if err != nil {
		return false, err
	}
	current := candidatePath
	for {
		info, err := os.Lstat(current)
		if err != nil {
			return false, err
		}
		if os.SameFile(directory, info) {
			return true, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false, nil
		}
		current = parent
	}
}

func openState(stateDir string, bootstrapRoot []byte) (*bolt.DB, error) {
	path := filepath.Join(stateDir, stateDatabaseName)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: state database must be a regular file", secureconfigfile.ErrUnsafePath)
		}
		if _, err := secureconfigfile.Read(path, secureconfigfile.Options{
			MaxBytes: 1 << 20, Mode: secureconfigfile.PrivateIdentity,
		}); err != nil {
			return nil, fmt.Errorf("TUF channel: validate state database: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("TUF channel: inspect state database: %w", err)
	}
	database, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 100 * time.Millisecond})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, ErrStateBusy
		}
		return nil, fmt.Errorf("TUF channel: open state database: %w", err)
	}
	digest := sha256.Sum256(bootstrapRoot)
	err = database.Update(func(transaction *bolt.Tx) error {
		bucket, err := transaction.CreateBucketIfNotExists(stateBucket)
		if err != nil {
			return err
		}
		stored := bucket.Get([]byte(bootstrapKey))
		if stored == nil {
			return bucket.Put([]byte(bootstrapKey), digest[:])
		}
		if !bytes.Equal(stored, digest[:]) {
			return ErrBootstrapChanged
		}
		return nil
	})
	if err != nil {
		return nil, errors.Join(err, database.Close())
	}
	return database, nil
}

func preflightCachedTrustedMetadata(root *os.Root, statePath string) error {
	if err := rootStillAtPath(root, statePath); err != nil {
		return err
	}
	for _, role := range []struct {
		name    string
		maximum int64
	}{
		{name: "timestamp.json", maximum: maxTimestampBytes},
		{name: "snapshot.json", maximum: maxSnapshotBytes},
		{name: "targets.json", maximum: maxTargetsBytes},
	} {
		trustedPath := filepath.Join(statePath, role.name)
		if _, err := os.Lstat(trustedPath); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return fmt.Errorf("TUF channel: inspect cached trusted metadata %s: %w", role.name, err)
		}
		if _, err := secureconfigfile.Read(trustedPath, secureconfigfile.Options{
			MaxBytes: role.maximum, Mode: secureconfigfile.PublicConfig,
		}); err != nil {
			return fmt.Errorf("TUF channel: cached trusted metadata %s: %w", role.name, err)
		}
	}
	return rootStillAtPath(root, statePath)
}

func syncTrustedState(root *os.Root, statePath string, requireComplete bool, syncFile func(*os.File) error) error {
	if err := rootStillAtPath(root, statePath); err != nil {
		return err
	}
	if syncFile == nil {
		return errors.New("TUF channel: trusted metadata sync function is required")
	}
	var syncErrors []error
	for _, name := range []string{"root.json", "timestamp.json", "snapshot.json", "targets.json"} {
		before, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) && !requireComplete {
			continue
		}
		if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
			syncErrors = append(syncErrors, fmt.Errorf("TUF channel: trusted metadata %s is not a regular non-symlink file", name))
			continue
		}
		file, err := root.OpenFile(name, os.O_RDWR, 0)
		if err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("TUF channel: open trusted metadata %s for sync: %w", name, err))
			continue
		}
		opened, statErr := file.Stat()
		if statErr != nil || !os.SameFile(before, opened) {
			if statErr == nil {
				statErr = errors.New("file identity changed")
			}
			syncErrors = append(syncErrors, errors.Join(
				fmt.Errorf("TUF channel: trusted metadata %s changed before sync: %w", name, statErr),
				file.Close(),
			))
			continue
		}
		if err := errors.Join(syncFile(file), file.Close()); err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("TUF channel: sync trusted metadata %s: %w", name, err))
		}
	}
	directory, err := root.Open(".")
	if err != nil {
		syncErrors = append(syncErrors, fmt.Errorf("TUF channel: open state directory for sync: %w", err))
	} else if err := errors.Join(syncFile(directory), directory.Close()); err != nil {
		syncErrors = append(syncErrors, fmt.Errorf("TUF channel: sync state directory: %w", err))
	}
	syncErrors = append(syncErrors, rootStillAtPath(root, statePath))
	return errors.Join(syncErrors...)
}

func rootStillAtPath(root *os.Root, statePath string) error {
	anchored, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("TUF channel: inspect anchored state directory: %w", err)
	}
	current, err := os.Lstat(statePath)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return errors.New("TUF channel: state directory path changed during verification")
	}
	return nil
}

type rootedMetadataFetcher struct {
	ctx  context.Context
	root *os.Root
}

func (fetcher *rootedMetadataFetcher) DownloadFile(rawURL string, maximum int64, _ time.Duration) ([]byte, error) {
	if err := fetcher.ctx.Err(); err != nil {
		return nil, err
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "offline.fetchmark.invalid" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("TUF channel: verifier requested an unexpected metadata URL")
	}
	relative := strings.TrimPrefix(parsed.EscapedPath(), "/metadata/")
	if relative == parsed.EscapedPath() || strings.Contains(relative, "%") || !metadataFilenameExpr.MatchString(relative) {
		return nil, errors.New("TUF channel: verifier requested an unsupported metadata path")
	}
	if maximum < 1 || maximum > maxTargetsBytes {
		return nil, errors.New("TUF channel: verifier requested an invalid metadata size")
	}
	file, err := fetcher.root.Open(relative)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &metadata.ErrDownloadHTTP{StatusCode: http.StatusNotFound, URL: rawURL}
		}
		return nil, fmt.Errorf("TUF channel: open metadata: %w", err)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.Join(errors.New("TUF channel: metadata path is not a regular file"), file.Close())
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("TUF channel: read metadata: %w", err), file.Close())
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("TUF channel: close metadata: %w", err)
	}
	if int64(len(raw)) > maximum {
		return nil, &metadata.ErrDownloadLengthMismatch{Msg: "offline metadata exceeds verifier limit"}
	}
	if err := fetcher.ctx.Err(); err != nil {
		return nil, err
	}
	return raw, nil
}

func metadataSelection(targetPath string, trusted trustedmetadata.TrustedMetadata) (Selection, error) {
	if trusted.Root == nil || trusted.Timestamp == nil || trusted.Snapshot == nil || trusted.Targets[metadata.TARGETS] == nil {
		return Selection{}, errors.New("TUF channel: verifier returned an incomplete trusted metadata set")
	}
	return Selection{
		TargetPath:         targetPath,
		RootVersion:        trusted.Root.Signed.Version,
		TimestampVersion:   trusted.Timestamp.Signed.Version,
		SnapshotVersion:    trusted.Snapshot.Signed.Version,
		TargetsVersion:     trusted.Targets[metadata.TARGETS].Signed.Version,
		TimestampExpiresAt: trusted.Timestamp.Signed.Expires.UTC().Format(time.RFC3339),
		SnapshotExpiresAt:  trusted.Snapshot.Signed.Expires.UTC().Format(time.RFC3339),
		TargetsExpiresAt:   trusted.Targets[metadata.TARGETS].Signed.Expires.UTC().Format(time.RFC3339),
	}, nil
}

func readBoundedRegular(root *os.Root, relative string, maximum int) ([]byte, error) {
	before, err := root.Lstat(relative)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() < 1 || before.Size() > int64(maximum) {
		return nil, errors.New("TUF channel: bundle manifest must be a bounded regular non-symlink file")
	}
	file, err := root.Open(relative)
	if err != nil {
		return nil, fmt.Errorf("TUF channel: open bundle manifest: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(before, info) || info.Size() < 1 || info.Size() > int64(maximum) {
		return nil, errors.New("TUF channel: bundle manifest must be a bounded regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil || len(raw) > maximum {
		return nil, errors.New("TUF channel: read bounded bundle manifest")
	}
	return raw, nil
}

func mustDecodeDigest(value string) []byte {
	raw, _ := hex.DecodeString(value)
	return raw
}

func invalidOptions(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidOptions, err)
}
