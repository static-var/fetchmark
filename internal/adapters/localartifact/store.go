// Package localartifact persists permitted source representations as
// content-addressed, immutable per-URL objects with an atomic current pointer.
// It deliberately does not share objects across URLs so noindex and takedown
// purges remain exact without reference counting.
package localartifact

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	coreartifact "github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
)

const (
	storeSchemaVersion   = 1
	archiveSchemaVersion = 2
	maxValidatorBytes    = 4096
	maxPointerBytes      = 64 << 10
	defaultMaxEntries    = 100_000
)

var (
	ErrArtifactTooLarge = errors.New("local artifact: artifact exceeds configured limit")
	ErrBudgetExceeded   = errors.New("local artifact: aggregate budget exceeded")
	ErrEntryLimit       = errors.New("local artifact: URL-state entry limit exceeded")
	ErrVersionLimit     = errors.New("local artifact: archive version limit exceeded")
	ErrPointerTooLarge  = errors.New("local artifact: pointer metadata exceeds limit")
	ErrCorruptArtifact  = errors.New("local artifact: corrupt artifact")
	ErrNotPermitted     = errors.New("local artifact: only explicitly permitted content may be stored")
	ErrStaleObservation = errors.New("local artifact: observation is older than retained state")
	ErrStoreInUse       = errors.New("local artifact: store is already open by another process")
	ErrClosed           = errors.New("local artifact: closed")
)

type Options struct {
	Path              string
	MaxBytes          int64
	MaxArtifactBytes  int64
	MaxEntries        int
	Archive           bool
	MaxVersions       int
	MaxVersionsPerURL int
}

type Store struct {
	mu                sync.RWMutex
	root              string
	rootFS            *os.Root
	ownership         *storeOwnership
	maxBytes          int64
	maxArtifactBytes  int64
	usedBytes         int64
	entries           int
	maxEntries        int
	archive           bool
	schemaVersion     int
	versions          int
	maxVersions       int
	maxVersionsPerURL int
	closed            bool
}

type pointerState struct {
	SchemaVersion        int                              `json:"schema_version"`
	URL                  string                           `json:"url"`
	EffectiveURL         string                           `json:"effective_url,omitempty"`
	ContentHash          string                           `json:"content_hash,omitempty"`
	ETag                 string                           `json:"etag,omitempty"`
	LastModified         string                           `json:"last_modified,omitempty"`
	MIME                 string                           `json:"mime,omitempty"`
	PolicyAgent          string                           `json:"policy_agent,omitempty"`
	FetchedAt            time.Time                        `json:"fetched_at,omitempty"`
	ObservedAt           time.Time                        `json:"observed_at"`
	ValidatedAt          time.Time                        `json:"validated_at,omitempty"`
	ExpiresAt            *time.Time                       `json:"expires_at,omitempty"`
	SafetyClassification localcorpus.SafetyClassification `json:"safety_classification,omitempty"`
	IndexingDisposition  localcorpus.IndexingDisposition  `json:"indexing_disposition"`
	VersionHashes        []string                         `json:"version_hashes,omitempty"`
}

type schemaMarker struct {
	Version int    `json:"version"`
	Mode    string `json:"mode,omitempty"`
}

func Open(options Options) (*Store, error) {
	root := filepath.Clean(strings.TrimSpace(options.Path))
	if root == "." || !filepath.IsAbs(root) {
		return nil, errors.New("local artifact: absolute path is required")
	}
	if options.MaxBytes <= 0 || options.MaxArtifactBytes <= 0 {
		return nil, errors.New("local artifact: byte budgets must be > 0")
	}
	if options.MaxArtifactBytes > options.MaxBytes {
		return nil, errors.New("local artifact: per-artifact budget exceeds aggregate budget")
	}
	if options.Archive && (options.MaxVersions <= 0 || options.MaxVersionsPerURL <= 0) {
		return nil, errors.New("local artifact: archive version limits must be > 0")
	}
	if options.Archive && options.MaxVersionsPerURL > options.MaxVersions {
		return nil, fmt.Errorf("%w: per-URL limit exceeds aggregate limit", ErrVersionLimit)
	}
	if info, err := os.Lstat(root); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("local artifact: path must not be a symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("local artifact: inspect path: %w", err)
	}
	resolvedRoot, err := resolveStorePath(root)
	if err != nil {
		return nil, err
	}
	root = resolvedRoot
	if options.MaxEntries <= 0 {
		options.MaxEntries = defaultMaxEntries
	}
	if err := ensureRealDirectory(root, 0o700); err != nil {
		return nil, fmt.Errorf("local artifact: create root: %w", err)
	}
	for _, directory := range []string{filepath.Join(root, "objects"), filepath.Join(root, "urls")} {
		if err := ensureRealDirectory(directory, 0o700); err != nil {
			return nil, fmt.Errorf("local artifact: create layout: %w", err)
		}
	}
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("local artifact: open confined root: %w", err)
	}
	ownership, err := acquireStoreOwnership(rootFS)
	if err != nil {
		_ = rootFS.Close()
		return nil, err
	}
	closeOpenResources := func() {
		_ = ownership.Close()
		_ = rootFS.Close()
	}
	if err := ensureSchema(rootFS, options.Archive); err != nil {
		closeOpenResources()
		return nil, err
	}
	if options.Archive {
		created, err := ensureRealDirectoryCreated(filepath.Join(root, "versions"), 0o700)
		if err != nil {
			closeOpenResources()
			return nil, fmt.Errorf("local artifact: create archive layout: %w", err)
		}
		if created {
			if err := syncDirectory(root); err != nil {
				closeOpenResources()
				return nil, fmt.Errorf("local artifact: sync archive layout: %w", err)
			}
		}
	}
	var usedBytes int64
	var entries, versions int
	if options.Archive {
		usedBytes, entries, versions, err = repairArchiveStore(root, rootFS, options)
	} else {
		usedBytes, entries, err = repairStore(root, rootFS, options.MaxArtifactBytes, options.MaxEntries)
	}
	if err != nil {
		closeOpenResources()
		return nil, err
	}
	if usedBytes > options.MaxBytes {
		closeOpenResources()
		return nil, fmt.Errorf("%w: retained=%d configured=%d", ErrBudgetExceeded, usedBytes, options.MaxBytes)
	}
	schemaVersion := storeSchemaVersion
	if options.Archive {
		schemaVersion = archiveSchemaVersion
	}
	return &Store{
		root: root, rootFS: rootFS, ownership: ownership, maxBytes: options.MaxBytes,
		maxArtifactBytes: options.MaxArtifactBytes, usedBytes: usedBytes, entries: entries,
		maxEntries: options.MaxEntries, archive: options.Archive, schemaVersion: schemaVersion,
		versions: versions, maxVersions: options.MaxVersions, maxVersionsPerURL: options.MaxVersionsPerURL,
	}, nil
}

func repairStore(root string, rootFS *os.Root, maxArtifactBytes int64, maxEntries int) (int64, int, error) {
	if err := repairMetadataDirectories(rootFS, false); err != nil {
		return 0, 0, err
	}
	referenced := make(map[string]int64)
	urlEntries, err := os.ReadDir(filepath.Join(root, "urls"))
	if err != nil {
		return 0, 0, fmt.Errorf("local artifact: enumerate pointers: %w", err)
	}
	entries := 0
	now := time.Now().UTC()
	for _, entry := range urlEntries {
		if entry.Type()&os.ModeSymlink != 0 {
			return 0, 0, fmt.Errorf("local artifact: pointer symlink is not allowed: %s", entry.Name())
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		entries++
		raw, err := readRegularBoundedRoot(rootFS, filepath.Join("urls", entry.Name()), maxPointerBytes)
		if err != nil {
			return 0, 0, fmt.Errorf("%w: read pointer %s: %v", ErrCorruptArtifact, entry.Name(), err)
		}
		var state pointerState
		if err := json.Unmarshal(raw, &state); err != nil || state.SchemaVersion != storeSchemaVersion || entry.Name() != URLID(state.URL)+".json" || validateValidators(state.ETag, state.LastModified) != nil || validateSafetyClassification(state.SafetyClassification) != nil {
			return 0, 0, fmt.Errorf("%w: invalid pointer %s", ErrCorruptArtifact, entry.Name())
		}
		canonical, canonicalErr := canonicalURL(state.URL)
		if canonicalErr != nil || canonical != state.URL {
			return 0, 0, fmt.Errorf("%w: non-canonical pointer %s", ErrCorruptArtifact, entry.Name())
		}
		if state.IndexingDisposition != localcorpus.DispositionPermitted {
			continue
		}
		if state.ExpiresAt != nil && !now.Before(state.ExpiresAt.UTC()) {
			if err := rootFS.Remove(filepath.Join("urls", entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return 0, 0, err
			}
			if validContentHash(state.ContentHash) {
				if err := rootFS.Remove(filepath.Join("objects", URLID(state.URL), state.ContentHash+".body")); err != nil && !errors.Is(err, os.ErrNotExist) {
					return 0, 0, err
				}
			}
			_ = rootFS.Remove(filepath.Join("objects", URLID(state.URL)))
			entries--
			continue
		}
		if !validContentHash(state.ContentHash) {
			return 0, 0, fmt.Errorf("%w: invalid content hash for %s", ErrCorruptArtifact, entry.Name())
		}
		bodyPath := filepath.Join(root, "objects", URLID(state.URL), state.ContentHash+".body")
		body, err := readRegularBoundedRoot(rootFS, filepath.Join("objects", URLID(state.URL), state.ContentHash+".body"), maxArtifactBytes)
		if err != nil || hashBytes(body) != state.ContentHash {
			return 0, 0, fmt.Errorf("%w: current body for %s", ErrCorruptArtifact, entry.Name())
		}
		referenced[bodyPath] = int64(len(body))
	}

	objectsRoot := filepath.Join(root, "objects")
	orphanDirectories := make([]string, 0)
	err = filepath.WalkDir(objectsRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("local artifact: object symlink is not allowed: %s", path)
		}
		if entry.IsDir() {
			if path != objectsRoot {
				orphanDirectories = append(orphanDirectories, path)
			}
			return nil
		}
		if _, keep := referenced[path]; keep {
			return nil
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		if err := rootFS.Remove(relative); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("local artifact: purge orphan %s: %w", path, err)
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	for index := len(orphanDirectories) - 1; index >= 0; index-- {
		relative, relErr := filepath.Rel(root, orphanDirectories[index])
		if relErr != nil {
			return 0, 0, relErr
		}
		_ = rootFS.Remove(relative)
	}
	total, measuredEntries, err := measureStore(root)
	if err != nil {
		return 0, 0, err
	}
	if measuredEntries != entries {
		return 0, 0, ErrCorruptArtifact
	}
	if entries > maxEntries {
		return 0, 0, ErrEntryLimit
	}
	return total, entries, nil
}

const atomicTemporaryPrefix = ".fetchmark-"

func repairMetadataDirectories(rootFS *os.Root, archive bool) error {
	allowed := map[string]bool{
		".fetchmark.lock": false,
		"schema.json":     false,
		"objects":         true,
		"urls":            true,
	}
	if archive {
		allowed["versions"] = true
	}
	if err := repairMetadataDirectory(rootFS, ".", allowed); err != nil {
		return err
	}
	return repairMetadataDirectory(rootFS, "urls", nil)
}

func repairMetadataDirectory(rootFS *os.Root, directory string, allowed map[string]bool) error {
	handle, err := rootFS.Open(directory)
	if err != nil {
		return fmt.Errorf("local artifact: inspect managed directory %s: %w", directory, err)
	}
	entries, readErr := handle.ReadDir(-1)
	closeErr := handle.Close()
	if readErr != nil {
		return fmt.Errorf("local artifact: inspect managed directory %s: %w", directory, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("local artifact: close managed directory %s: %w", directory, closeErr)
	}
	for _, entry := range entries {
		relative := filepath.Join(directory, entry.Name())
		if isAtomicTemporaryName(entry.Name()) {
			info, err := rootFS.Lstat(relative)
			if err != nil {
				return fmt.Errorf("local artifact: inspect temporary file %s: %w", relative, err)
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return fmt.Errorf("%w: invalid temporary entry %s", ErrCorruptArtifact, relative)
			}
			if err := rootFS.Remove(relative); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("local artifact: purge temporary file %s: %w", relative, err)
			}
			continue
		}
		if allowed != nil {
			wantDirectory, ok := allowed[entry.Name()]
			if !ok || entry.IsDir() != wantDirectory || entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("%w: unexpected managed entry %s", ErrCorruptArtifact, relative)
			}
			if !wantDirectory {
				info, err := rootFS.Lstat(relative)
				if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("%w: invalid managed entry %s", ErrCorruptArtifact, relative)
				}
			}
			continue
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(entry.Name(), ".json") {
			return fmt.Errorf("%w: unexpected pointer entry %s", ErrCorruptArtifact, relative)
		}
	}
	return nil
}

func isAtomicTemporaryName(name string) bool {
	if !strings.HasPrefix(name, atomicTemporaryPrefix) || len(name) != len(atomicTemporaryPrefix)+16 {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(name, atomicTemporaryPrefix))
	return err == nil && len(decoded) == 8 && name == strings.ToLower(name)
}

func writePointerFileRoot(root *os.Root, path string, state pointerState) error {
	encoded, err := encodePointer(state)
	if err != nil {
		return err
	}
	if err := atomicWriteRoot(root, path, encoded); err != nil {
		return fmt.Errorf("local artifact: write pointer: %w", err)
	}
	return nil
}

func encodePointer(state pointerState) ([]byte, error) {
	return encodePointerVersion(state, storeSchemaVersion)
}

func encodePointerVersion(state pointerState, schemaVersion int) ([]byte, error) {
	state.SchemaVersion = schemaVersion
	encoded, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("local artifact: encode pointer: %w", err)
	}
	if len(encoded) > maxPointerBytes {
		return nil, ErrPointerTooLarge
	}
	return encoded, nil
}

func ensureSchema(root *os.Root, archive bool) error {
	const path = "schema.json"
	want := schemaMarker{Version: storeSchemaVersion}
	if archive {
		want = schemaMarker{Version: archiveSchemaVersion, Mode: "archive"}
	}
	raw, err := readRegularBoundedRoot(root, path, maxPointerBytes)
	if errors.Is(err, os.ErrNotExist) {
		encoded, marshalErr := json.Marshal(want)
		if marshalErr != nil {
			return marshalErr
		}
		return atomicWriteRoot(root, path, encoded)
	}
	if err != nil {
		return fmt.Errorf("local artifact: read schema: %w", err)
	}
	var marker schemaMarker
	if json.Unmarshal(raw, &marker) != nil || marker != want {
		return fmt.Errorf("local artifact: incompatible schema; expected version %d mode %q", want.Version, want.Mode)
	}
	return nil
}

func (store *Store) Current(ctx context.Context, rawURL string) (coreartifact.Version, bool, error) {
	if err := ctx.Err(); err != nil {
		return coreartifact.Version{}, false, err
	}
	canonical, err := canonicalURL(rawURL)
	if err != nil {
		return coreartifact.Version{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return coreartifact.Version{}, false, ErrClosed
	}
	if err := store.validateLayout(); err != nil {
		return coreartifact.Version{}, false, err
	}
	state, exists, err := store.readState(canonical)
	if err != nil || !exists {
		return coreartifact.Version{}, false, err
	}
	if state.IndexingDisposition != localcorpus.DispositionPermitted || state.ContentHash == "" {
		return coreartifact.Version{}, false, nil
	}
	if state.ExpiresAt != nil && !time.Now().UTC().Before(state.ExpiresAt.UTC()) {
		return coreartifact.Version{}, false, nil
	}
	body, err := readRegularBoundedRoot(store.rootFS, store.bodyRelative(canonical, state.ContentHash), store.maxArtifactBytes)
	if err != nil {
		if errors.Is(err, ErrArtifactTooLarge) {
			return coreartifact.Version{}, false, err
		}
		return coreartifact.Version{}, false, fmt.Errorf("%w: %v", ErrCorruptArtifact, err)
	}
	if hashBytes(body) != state.ContentHash {
		return coreartifact.Version{}, false, ErrCorruptArtifact
	}
	return state.version(body), true, nil
}

func (store *Store) Put(ctx context.Context, version coreartifact.Version) error {
	if store.archive {
		return store.putArchive(ctx, version)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if version.IndexingDisposition != localcorpus.DispositionPermitted {
		return ErrNotPermitted
	}
	if len(version.Body) == 0 {
		return errors.New("local artifact: non-empty body is required")
	}
	if int64(len(version.Body)) > store.maxArtifactBytes {
		return ErrArtifactTooLarge
	}
	if err := validateValidators(version.ETag, version.LastModified); err != nil {
		return err
	}
	classification, err := localcorpus.ParseSafetyClassification(string(version.SafetyClassification))
	if err != nil {
		return fmt.Errorf("local artifact: %w", err)
	}
	canonical, err := canonicalURL(version.URL)
	if err != nil {
		return err
	}
	effective := version.EffectiveURL
	if effective == "" {
		effective = canonical
	} else if effective, err = canonicalURL(effective); err != nil {
		return fmt.Errorf("local artifact: effective URL: %w", err)
	}
	observedAt := observationTime(version.ObservedAt, version.FetchedAt)
	fetchedAt := version.FetchedAt.UTC()
	if fetchedAt.IsZero() {
		fetchedAt = observedAt
	}
	validatedAt := version.ValidatedAt.UTC()
	if validatedAt.IsZero() {
		validatedAt = observedAt
	}
	contentHash := hashBytes(version.Body)
	if version.ContentHash != "" && !strings.EqualFold(version.ContentHash, contentHash) {
		return ErrCorruptArtifact
	}
	state := pointerState{
		SchemaVersion: storeSchemaVersion, URL: canonical, EffectiveURL: effective, ContentHash: contentHash,
		ETag: version.ETag, LastModified: version.LastModified, MIME: strings.TrimSpace(version.MIME),
		PolicyAgent: strings.TrimSpace(version.PolicyAgent), FetchedAt: fetchedAt, ObservedAt: observedAt,
		ValidatedAt: validatedAt, ExpiresAt: copyTime(version.ExpiresAt), SafetyClassification: classification,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}
	encodedState, err := encodePointerVersion(state, store.schemaVersion)
	if err != nil {
		return err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if err := store.validateLayout(); err != nil {
		return err
	}
	if int64(len(version.Body))+int64(len(encodedState)) > store.maxBytes-store.usedBytes {
		if _, err := store.sweepExpiredLocked(ctx, time.Now().UTC()); err != nil {
			return err
		}
	}
	if store.entries >= store.maxEntries {
		if _, err := store.sweepExpiredLocked(ctx, time.Now().UTC()); err != nil {
			return err
		}
	}
	previous, exists, err := store.readState(canonical)
	if err != nil {
		return err
	}
	if exists && observationIsStale(previous, observedAt, localcorpus.DispositionPermitted) {
		return ErrStaleObservation
	}
	if !exists && store.entries >= store.maxEntries {
		return ErrEntryLimit
	}
	bodyPath := store.bodyPath(canonical, contentHash)
	newSize := int64(len(version.Body))
	oldBodySize := int64(0)
	if exists && previous.ContentHash != "" && previous.ContentHash != contentHash {
		if info, statErr := os.Lstat(store.bodyPath(canonical, previous.ContentHash)); statErr == nil && info.Mode().IsRegular() {
			oldBodySize = info.Size()
		}
	}
	oldPointerSize := store.pointerSize(canonical)
	alreadyExists := false
	existingBodySize := int64(0)
	if existing, readErr := readRegularBoundedRoot(store.rootFS, store.bodyRelative(canonical, contentHash), store.maxArtifactBytes); readErr == nil {
		existingBodySize = int64(len(existing))
		if hashBytes(existing) != contentHash {
		} else {
			alreadyExists = true
			newSize = 0
		}
	} else if errors.Is(readErr, ErrArtifactTooLarge) {
		if info, statErr := os.Lstat(bodyPath); statErr == nil && info.Mode().IsRegular() {
			existingBodySize = info.Size()
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	retainedAfterReplacement := store.usedBytes - oldPointerSize - oldBodySize - existingBodySize
	if alreadyExists {
		retainedAfterReplacement += existingBodySize
	}
	if retainedAfterReplacement < 0 {
		return ErrCorruptArtifact
	}
	if int64(len(encodedState))+newSize > store.maxBytes-retainedAfterReplacement {
		return ErrBudgetExceeded
	}
	objectDir := store.objectDir(canonical)
	directoryCreated, err := ensureRealDirectoryCreated(objectDir, 0o700)
	if err != nil {
		return fmt.Errorf("local artifact: create object directory: %w", err)
	}
	if directoryCreated {
		if err := syncDirectory(filepath.Join(store.root, "objects")); err != nil {
			_ = store.rootFS.Remove(filepath.Join("objects", URLID(canonical)))
			return fmt.Errorf("local artifact: sync object directory parent: %w", err)
		}
	}
	if !alreadyExists {
		if err := atomicWriteRoot(store.rootFS, store.bodyRelative(canonical, contentHash), version.Body); err != nil {
			if !exists || previous.ContentHash != contentHash {
				_ = store.rootFS.Remove(store.bodyRelative(canonical, contentHash))
				if directoryCreated {
					_ = store.rootFS.Remove(filepath.Join("objects", URLID(canonical)))
				}
			}
			_ = store.refreshUsageLocked()
			return fmt.Errorf("local artifact: write body: %w", err)
		}
	}
	if err := store.writeState(canonical, state); err != nil {
		current, currentExists, readErr := store.readState(canonical)
		pointerAdvanced := readErr == nil && currentExists && current.ContentHash == contentHash && current.ObservedAt.Equal(observedAt)
		if !alreadyExists && !pointerAdvanced {
			_ = store.rootFS.Remove(store.bodyRelative(canonical, contentHash))
			if directoryCreated {
				_ = store.rootFS.Remove(filepath.Join("objects", URLID(canonical)))
			}
		}
		_ = store.refreshUsageLocked()
		return errors.Join(err, readErr)
	}
	if exists && previous.ContentHash != "" && previous.ContentHash != contentHash {
		if err := store.rootFS.Remove(store.bodyRelative(canonical, previous.ContentHash)); err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = store.refreshUsageLocked()
			return fmt.Errorf("local artifact: remove superseded body: %w", err)
		}
	}
	if !exists {
		store.entries++
	}
	store.usedBytes = retainedAfterReplacement + int64(len(encodedState)) + newSize
	return nil
}

// Sweep removes permitted versions whose expiry is at or before now. Tombstones
// are retained so revocation ordering remains sticky across later observations.
func (store *Store) Sweep(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		return 0, errors.New("local artifact: sweep time is required")
	}
	now = now.UTC()

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrClosed
	}
	if store.archive {
		return 0, nil
	}
	return store.sweepExpiredLocked(ctx, now)
}

func (store *Store) sweepExpiredLocked(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.validateLayout(); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(filepath.Join(store.root, "urls"))
	if err != nil {
		return 0, fmt.Errorf("local artifact: enumerate expiry pointers: %w", err)
	}
	removed := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := readRegularBoundedRoot(store.rootFS, filepath.Join("urls", entry.Name()), maxPointerBytes)
		if err != nil {
			return removed, fmt.Errorf("%w: expiry pointer %s", ErrCorruptArtifact, entry.Name())
		}
		var state pointerState
		if err := json.Unmarshal(raw, &state); err != nil || state.SchemaVersion != storeSchemaVersion || entry.Name() != URLID(state.URL)+".json" || validateValidators(state.ETag, state.LastModified) != nil || validateSafetyClassification(state.SafetyClassification) != nil {
			return removed, fmt.Errorf("%w: expiry pointer %s", ErrCorruptArtifact, entry.Name())
		}
		canonical, canonicalErr := canonicalURL(state.URL)
		if canonicalErr != nil || canonical != state.URL || (state.IndexingDisposition == localcorpus.DispositionPermitted && !validContentHash(state.ContentHash)) {
			return removed, fmt.Errorf("%w: expiry pointer %s", ErrCorruptArtifact, entry.Name())
		}
		if state.IndexingDisposition != localcorpus.DispositionPermitted || state.ExpiresAt == nil || now.Before(state.ExpiresAt.UTC()) {
			continue
		}
		contentHash := state.ContentHash
		if err := store.rootFS.Remove(store.stateRelative(state.URL)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, err
		}
		if err := store.rootFS.Remove(store.bodyRelative(state.URL, contentHash)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, fmt.Errorf("local artifact: remove expired body: %w", err)
		}
		_ = store.rootFS.Remove(filepath.Join("objects", URLID(state.URL)))
		removed++
	}
	if err := store.refreshUsageLocked(); err != nil {
		return removed, err
	}
	return removed, nil
}

func (store *Store) Revalidate(ctx context.Context, observation coreartifact.Observation) error {
	if store.archive {
		return store.revalidateArchive(ctx, observation)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateValidators(observation.ETag, observation.LastModified); err != nil {
		return err
	}
	canonical, err := canonicalURL(observation.URL)
	if err != nil {
		return err
	}
	observedAt := observationTime(observation.ObservedAt, observation.ValidatedAt)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if err := store.validateLayout(); err != nil {
		return err
	}
	state, exists, err := store.readState(canonical)
	if err != nil {
		return err
	}
	if !exists || state.IndexingDisposition != localcorpus.DispositionPermitted || state.ContentHash == "" {
		return ErrNotPermitted
	}
	if observationIsStale(state, observedAt, localcorpus.DispositionPermitted) {
		return ErrStaleObservation
	}
	state.ObservedAt = observedAt
	state.ValidatedAt = observation.ValidatedAt.UTC()
	if state.ValidatedAt.IsZero() {
		state.ValidatedAt = observedAt
	}
	if observation.ETag != "" {
		state.ETag = observation.ETag
	}
	if observation.LastModified != "" {
		state.LastModified = observation.LastModified
	}
	state.ExpiresAt = copyTime(observation.ExpiresAt)
	encoded, err := encodePointerVersion(state, store.schemaVersion)
	if err != nil {
		return err
	}
	oldSize := store.pointerSize(canonical)
	if int64(len(encoded)) > store.maxBytes-(store.usedBytes-oldSize) {
		return ErrBudgetExceeded
	}
	if err := store.writeState(canonical, state); err != nil {
		_ = store.refreshUsageLocked()
		return err
	}
	store.usedBytes = store.usedBytes - oldSize + int64(len(encoded))
	return nil
}

func (store *Store) Revoke(ctx context.Context, rawURL string, disposition localcorpus.IndexingDisposition, observedAt time.Time) error {
	_, err := store.revoke(ctx, rawURL, disposition, observedAt, false)
	return err
}

// RevokeExisting revokes and purges only an already-retained URL state. It is
// the artifact-side counterpart to Bleve ReconcileExisting: ordinary curated
// policy observations can remove artifact-only crash/rebuild state without
// allowing arbitrary denied URLs to consume durable tombstone capacity.
func (store *Store) RevokeExisting(ctx context.Context, rawURL string, disposition localcorpus.IndexingDisposition, observedAt time.Time) (bool, error) {
	return store.revoke(ctx, rawURL, disposition, observedAt, true)
}

func (store *Store) revoke(ctx context.Context, rawURL string, disposition localcorpus.IndexingDisposition, observedAt time.Time, requireExisting bool) (bool, error) {
	if store.archive {
		return store.revokeArchive(ctx, rawURL, disposition, observedAt, requireExisting)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if disposition == "" || disposition == localcorpus.DispositionPermitted {
		return false, ErrNotPermitted
	}
	canonical, err := canonicalURL(rawURL)
	if err != nil {
		return false, err
	}
	observedAt = observationTime(observedAt, time.Time{})
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return false, ErrClosed
	}
	if err := store.validateLayout(); err != nil {
		return false, err
	}
	if !requireExisting && store.entries >= store.maxEntries {
		if _, err := store.sweepExpiredLocked(ctx, time.Now().UTC()); err != nil {
			return false, err
		}
	}
	previous, exists, err := store.readState(canonical)
	if err != nil {
		return false, err
	}
	if requireExisting && !exists {
		return false, nil
	}
	if exists && observationIsStale(previous, observedAt, disposition) {
		return false, ErrStaleObservation
	}
	if !exists && store.entries >= store.maxEntries {
		return false, ErrEntryLimit
	}
	state := pointerState{
		SchemaVersion: storeSchemaVersion, URL: canonical, ObservedAt: observedAt, IndexingDisposition: disposition,
	}
	encoded, err := encodePointerVersion(state, store.schemaVersion)
	if err != nil {
		return false, err
	}
	oldPointerSize := store.pointerSize(canonical)
	if int64(len(encoded)) > store.maxBytes-(store.usedBytes-oldPointerSize) {
		return false, ErrBudgetExceeded
	}
	if err := store.writeState(canonical, state); err != nil {
		_ = store.refreshUsageLocked()
		return false, err
	}
	objectDir := store.objectDir(canonical)
	entries, readErr := os.ReadDir(objectDir)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		_ = store.refreshUsageLocked()
		return false, fmt.Errorf("local artifact: enumerate revoked objects: %w", readErr)
	}
	purgedBodyBytes := int64(0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".body") {
			continue
		}
		relative := filepath.Join("objects", URLID(canonical), entry.Name())
		info, statErr := store.rootFS.Lstat(relative)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			_ = store.refreshUsageLocked()
			return false, fmt.Errorf("local artifact: inspect revoked object: %w", statErr)
		}
		if statErr == nil {
			if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
				_ = store.rootFS.Remove(relative)
				_ = store.refreshUsageLocked()
				return false, ErrCorruptArtifact
			}
			purgedBodyBytes += info.Size()
		}
		if err := store.rootFS.Remove(relative); err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = store.refreshUsageLocked()
			return false, fmt.Errorf("local artifact: purge revoked object: %w", err)
		}
	}
	_ = store.rootFS.Remove(filepath.Join("objects", URLID(canonical)))
	if !exists {
		store.entries++
	}
	retained := store.usedBytes - oldPointerSize - purgedBodyBytes
	if retained < 0 || int64(len(encoded)) > store.maxBytes-retained {
		_ = store.refreshUsageLocked()
		return false, ErrCorruptArtifact
	}
	store.usedBytes = retained + int64(len(encoded))
	return true, nil
}

func observationIsStale(retained pointerState, observedAt time.Time, incoming localcorpus.IndexingDisposition) bool {
	if retained.IndexingDisposition == localcorpus.DispositionTakedown && incoming != localcorpus.DispositionTakedown {
		return true
	}
	if retained.ObservedAt.After(observedAt) {
		return true
	}
	return retained.ObservedAt.Equal(observedAt) && retained.IndexingDisposition != localcorpus.DispositionPermitted && incoming == localcorpus.DispositionPermitted
}

func (store *Store) readState(canonical string) (pointerState, bool, error) {
	raw, err := readRegularBoundedRoot(store.rootFS, store.stateRelative(canonical), maxPointerBytes)
	if errors.Is(err, os.ErrNotExist) {
		return pointerState{}, false, nil
	}
	if err != nil {
		return pointerState{}, false, fmt.Errorf("local artifact: read pointer: %w", err)
	}
	var state pointerState
	if err := json.Unmarshal(raw, &state); err != nil || state.SchemaVersion != store.schemaVersion || state.URL != canonical || validateValidators(state.ETag, state.LastModified) != nil || validateSafetyClassification(state.SafetyClassification) != nil {
		return pointerState{}, false, ErrCorruptArtifact
	}
	if state.IndexingDisposition == localcorpus.DispositionPermitted && !validContentHash(state.ContentHash) {
		return pointerState{}, false, ErrCorruptArtifact
	}
	if store.archive && validateArchivePointer(state) != nil {
		return pointerState{}, false, ErrCorruptArtifact
	}
	return state, true, nil
}

func (store *Store) writeState(canonical string, state pointerState) error {
	encoded, err := encodePointerVersion(state, store.schemaVersion)
	if err != nil {
		return err
	}
	if err := atomicWriteRoot(store.rootFS, store.stateRelative(canonical), encoded); err != nil {
		return fmt.Errorf("local artifact: write pointer: %w", err)
	}
	return nil
}

func (store *Store) pointerSize(canonical string) int64 {
	info, err := store.rootFS.Lstat(store.stateRelative(canonical))
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return 0
	}
	return info.Size()
}

func (store *Store) validateLayout() error {
	directories := []string{store.root, filepath.Join(store.root, "objects"), filepath.Join(store.root, "urls")}
	if store.archive {
		directories = append(directories, filepath.Join(store.root, "versions"))
	}
	for _, directory := range directories {
		info, err := os.Lstat(directory)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("local artifact: managed layout must contain only real directories")
		}
	}
	return nil
}

func (store *Store) refreshUsageLocked() error {
	used, entries, err := measureStore(store.root)
	if err != nil {
		return err
	}
	store.usedBytes = used
	store.entries = entries
	if used > store.maxBytes {
		return ErrBudgetExceeded
	}
	if entries > store.maxEntries {
		return ErrEntryLimit
	}
	if store.archive {
		versions, err := countCommittedVersions(store.rootFS, store.root, store.maxVersionsPerURL)
		if err != nil {
			return err
		}
		store.versions = versions
		if versions > store.maxVersions {
			return ErrVersionLimit
		}
	}
	return nil
}

func (state pointerState) version(body []byte) coreartifact.Version {
	return coreartifact.Version{
		URL: state.URL, EffectiveURL: state.EffectiveURL, Body: append([]byte(nil), body...), ContentHash: state.ContentHash,
		ETag: state.ETag, LastModified: state.LastModified, MIME: state.MIME, PolicyAgent: state.PolicyAgent,
		FetchedAt: state.FetchedAt, ObservedAt: state.ObservedAt, ValidatedAt: state.ValidatedAt,
		ExpiresAt: copyTime(state.ExpiresAt), SafetyClassification: state.SafetyClassification,
		IndexingDisposition: state.IndexingDisposition,
	}
}

func (store *Store) objectDir(canonical string) string {
	return filepath.Join(store.root, "objects", URLID(canonical))
}

func (store *Store) bodyPath(canonical, hash string) string {
	return filepath.Join(store.objectDir(canonical), hash+".body")
}

func (store *Store) statePath(canonical string) string {
	return filepath.Join(store.root, "urls", URLID(canonical)+".json")
}

func (store *Store) bodyRelative(canonical, hash string) string {
	return filepath.Join("objects", URLID(canonical), hash+".body")
}

func (store *Store) stateRelative(canonical string) string {
	return filepath.Join("urls", URLID(canonical)+".json")
}

func URLID(rawURL string) string {
	canonical, err := cache.CanonicalURL(rawURL)
	if err != nil {
		canonical = rawURL
	}
	return hashBytes([]byte(canonical))
}

func canonicalURL(rawURL string) (string, error) {
	canonical, err := cache.CanonicalURL(rawURL)
	if err != nil {
		return "", fmt.Errorf("local artifact: canonical URL: %w", err)
	}
	return canonical, nil
}

func validateValidators(etag, lastModified string) error {
	for name, value := range map[string]string{"ETag": etag, "Last-Modified": lastModified} {
		if len(value) > maxValidatorBytes {
			return fmt.Errorf("local artifact: %s exceeds %d bytes", name, maxValidatorBytes)
		}
		if strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("local artifact: %s contains a line break", name)
		}
	}
	return nil
}

func validateSafetyClassification(value localcorpus.SafetyClassification) error {
	_, err := localcorpus.ParseSafetyClassification(string(value))
	return err
}

func observationTime(primary, fallback time.Time) time.Time {
	value := primary.UTC()
	if value.IsZero() {
		value = fallback.UTC()
	}
	if value.IsZero() {
		value = time.Now().UTC()
	}
	return value
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := value.UTC()
	return &copied
}

func hashBytes(value []byte) string {
	hash := sha256.Sum256(value)
	return hex.EncodeToString(hash[:])
}

func validContentHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func readRegularBoundedRoot(root *os.Root, path string, max int64) ([]byte, error) {
	info, err := root.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("local artifact: expected a regular file")
	}
	file, err := root.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > max {
		return nil, ErrArtifactTooLarge
	}
	return raw, nil
}

func atomicWriteRoot(root *os.Root, path string, value []byte) error {
	directory := filepath.Dir(path)
	info, err := root.Lstat(directory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		if err == nil {
			err = errors.New("not a real directory")
		}
		return err
	}
	random := make([]byte, 8)
	if _, err := cryptorand.Read(random); err != nil {
		return err
	}
	temporaryPath := filepath.Join(directory, atomicTemporaryPrefix+hex.EncodeToString(random))
	temporary, err := root.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(temporaryPath) }()
	if _, err := temporary.Write(value); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := root.Rename(temporaryPath, path); err != nil {
		return err
	}
	directoryHandle, err := root.Open(directory)
	if err != nil {
		return err
	}
	defer directoryHandle.Close()
	return directoryHandle.Sync()
}

func ensureRealDirectory(path string, mode os.FileMode) error {
	_, err := ensureRealDirectoryCreated(path, mode)
	return err
}

func resolveStorePath(path string) (string, error) {
	tail := make([]string, 0, 4)
	current := filepath.Clean(path)
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(tail) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, tail[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("local artifact: resolve path: %w", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("local artifact: resolve path: %w", err)
		}
		tail = append(tail, filepath.Base(current))
		current = parent
	}
}

func ensureRealDirectoryCreated(path string, mode os.FileMode) (bool, error) {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return false, errors.New("path is not a real directory")
		}
		return false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.Mkdir(path, mode); err != nil {
		return false, err
	}
	info, err = os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return false, errors.New("created path is not a real directory")
	}
	return true, nil
}

func syncDirectory(path string) error {
	handle, err := os.Open(path)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}

func measureStore(root string) (int64, int, error) {
	var total int64
	entries := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("local artifact: symlink is not allowed: %s", path)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("local artifact: non-regular entry is not allowed: %s", path)
		}
		if info.Size() > int64(^uint64(0)>>1)-total {
			return ErrBudgetExceeded
		}
		total += info.Size()
		if filepath.Dir(path) == filepath.Join(root, "urls") && strings.HasSuffix(entry.Name(), ".json") {
			entries++
		}
		return nil
	})
	return total, entries, err
}

func (store *Store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	return errors.Join(store.rootFS.Close(), store.ownership.Close())
}
