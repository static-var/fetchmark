package localartifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	coreartifact "github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
)

type archiveManifest struct {
	SchemaVersion        int                              `json:"schema_version"`
	URL                  string                           `json:"url"`
	EffectiveURL         string                           `json:"effective_url,omitempty"`
	ContentHash          string                           `json:"content_hash"`
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
}

func (store *Store) putArchive(ctx context.Context, version coreartifact.Version) error {
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
		SchemaVersion: archiveSchemaVersion, URL: canonical, EffectiveURL: effective, ContentHash: contentHash,
		ETag: version.ETag, LastModified: version.LastModified, MIME: strings.TrimSpace(version.MIME),
		PolicyAgent: strings.TrimSpace(version.PolicyAgent), FetchedAt: fetchedAt, ObservedAt: observedAt,
		ValidatedAt: validatedAt, ExpiresAt: nil, SafetyClassification: classification,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}
	manifest := manifestFromPointer(state)
	encodedManifest, err := encodeManifest(manifest)
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
	committed := containsHash(previous.VersionHashes, contentHash)
	state.VersionHashes = append([]string(nil), previous.VersionHashes...)
	if !committed {
		if len(state.VersionHashes) >= store.maxVersionsPerURL || store.versions >= store.maxVersions {
			return ErrVersionLimit
		}
		state.VersionHashes = append(state.VersionHashes, contentHash)
	}
	encodedPointer, err := encodePointerVersion(state, archiveSchemaVersion)
	if err != nil {
		return err
	}

	oldPointerSize := store.pointerSize(canonical)
	bodyRelative := store.bodyRelative(canonical, contentHash)
	manifestRelative := store.manifestRelative(canonical, contentHash)
	bodySize, bodyExists, err := validateExistingBody(store.rootFS, bodyRelative, version.Body, store.maxArtifactBytes)
	if err != nil {
		return err
	}
	var manifestSize int64
	var manifestExists bool
	if committed {
		if _, err := store.readManifest(canonical, contentHash); err != nil {
			return err
		}
		info, err := store.rootFS.Lstat(manifestRelative)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return ErrCorruptArtifact
		}
		manifestSize, manifestExists = info.Size(), true
	} else {
		manifestSize, manifestExists, err = validateExistingManifest(store.rootFS, manifestRelative, encodedManifest)
		if err != nil {
			return err
		}
	}
	if committed && (!bodyExists || !manifestExists) {
		return ErrCorruptArtifact
	}
	additional := int64(len(encodedPointer)) - oldPointerSize
	if !bodyExists {
		additional += int64(len(version.Body))
	} else {
		_ = bodySize
	}
	if !manifestExists {
		additional += int64(len(encodedManifest))
	} else {
		_ = manifestSize
	}
	if additional > store.maxBytes-store.usedBytes {
		return ErrBudgetExceeded
	}

	objectCreated, err := ensureArchiveURLDirectory(store, "objects", canonical)
	if err != nil {
		return err
	}
	versionCreated, err := ensureArchiveURLDirectory(store, "versions", canonical)
	if err != nil {
		if objectCreated {
			_ = store.rootFS.Remove(filepath.Join("objects", URLID(canonical)))
		}
		return err
	}
	wroteBody := false
	wroteManifest := false
	cleanupUncommitted := func() {
		if wroteManifest {
			_ = store.rootFS.Remove(manifestRelative)
		}
		if wroteBody {
			_ = store.rootFS.Remove(bodyRelative)
		}
		if versionCreated {
			_ = store.rootFS.Remove(filepath.Join("versions", URLID(canonical)))
		}
		if objectCreated {
			_ = store.rootFS.Remove(filepath.Join("objects", URLID(canonical)))
		}
	}
	if !bodyExists {
		if err := atomicWriteRoot(store.rootFS, bodyRelative, version.Body); err != nil {
			cleanupUncommitted()
			_ = store.refreshUsageLocked()
			return fmt.Errorf("local artifact: write archive body: %w", err)
		}
		wroteBody = true
	}
	if !manifestExists {
		if err := atomicWriteRoot(store.rootFS, manifestRelative, encodedManifest); err != nil {
			cleanupUncommitted()
			_ = store.refreshUsageLocked()
			return fmt.Errorf("local artifact: write archive manifest: %w", err)
		}
		wroteManifest = true
	}
	if err := store.writeState(canonical, state); err != nil {
		current, currentExists, readErr := store.readState(canonical)
		pointerAdvanced := readErr == nil && currentExists && current.ContentHash == contentHash && current.ObservedAt.Equal(observedAt) && containsHash(current.VersionHashes, contentHash)
		if !pointerAdvanced && !committed {
			cleanupUncommitted()
		}
		_ = store.refreshUsageLocked()
		return errors.Join(err, readErr)
	}
	if !exists {
		store.entries++
	}
	if !committed {
		store.versions++
	}
	store.usedBytes += additional
	return nil
}

func (store *Store) revalidateArchive(ctx context.Context, observation coreartifact.Observation) error {
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
	state.ExpiresAt = nil
	encoded, err := encodePointerVersion(state, archiveSchemaVersion)
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

func (store *Store) revokeArchive(ctx context.Context, rawURL string, disposition localcorpus.IndexingDisposition, observedAt time.Time, requireExisting bool) (bool, error) {
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
	purgeBytes, err := store.archiveURLBytes(canonical)
	if err != nil {
		return false, err
	}
	state := pointerState{SchemaVersion: archiveSchemaVersion, URL: canonical, ObservedAt: observedAt, IndexingDisposition: disposition, VersionHashes: []string{}}
	encoded, err := encodePointerVersion(state, archiveSchemaVersion)
	if err != nil {
		return false, err
	}
	oldPointerSize := store.pointerSize(canonical)
	retained := store.usedBytes - oldPointerSize - purgeBytes
	if retained < 0 || int64(len(encoded)) > store.maxBytes-retained {
		return false, ErrCorruptArtifact
	}
	// Pointer-last makes new versions visible; revocation deliberately reverses
	// that order so the empty-history tombstone is durable before any purge.
	if err := store.writeState(canonical, state); err != nil {
		current, currentExists, readErr := store.readState(canonical)
		pointerAdvanced := readErr == nil && currentExists && current.IndexingDisposition == disposition && current.ObservedAt.Equal(observedAt) && len(current.VersionHashes) == 0
		var purgeErr error
		if pointerAdvanced {
			purgeErr = store.purgeArchiveURL(canonical)
		}
		_ = store.refreshUsageLocked()
		return false, errors.Join(err, readErr, purgeErr)
	}
	if err := store.purgeArchiveURL(canonical); err != nil {
		_ = store.refreshUsageLocked()
		return false, err
	}
	if !exists {
		store.entries++
	}
	store.versions -= len(previous.VersionHashes)
	if store.versions < 0 {
		_ = store.refreshUsageLocked()
		return false, ErrCorruptArtifact
	}
	store.usedBytes = retained + int64(len(encoded))
	return true, nil
}

func (store *Store) History(ctx context.Context, rawURL string) (coreartifact.History, bool, error) {
	if err := ctx.Err(); err != nil {
		return coreartifact.History{}, false, err
	}
	if !store.archive {
		return coreartifact.History{}, false, ErrNotPermitted
	}
	canonical, err := canonicalURL(rawURL)
	if err != nil {
		return coreartifact.History{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return coreartifact.History{}, false, ErrClosed
	}
	if err := store.validateLayout(); err != nil {
		return coreartifact.History{}, false, err
	}
	state, exists, err := store.readState(canonical)
	if err != nil || !exists {
		return coreartifact.History{}, false, err
	}
	history := coreartifact.History{
		URL: canonical, CurrentHash: state.ContentHash, ObservedAt: state.ObservedAt,
		IndexingDisposition: state.IndexingDisposition,
		Versions:            make([]coreartifact.VersionInfo, 0, len(state.VersionHashes)),
	}
	for _, contentHash := range state.VersionHashes {
		manifest, err := store.readManifest(canonical, contentHash)
		if err != nil {
			return coreartifact.History{}, false, err
		}
		history.Versions = append(history.Versions, manifest.info())
	}
	return history, true, nil
}

func (store *Store) Version(ctx context.Context, rawURL, contentHash string) (coreartifact.Version, bool, error) {
	if err := ctx.Err(); err != nil {
		return coreartifact.Version{}, false, err
	}
	if !store.archive {
		return coreartifact.Version{}, false, ErrNotPermitted
	}
	canonical, err := canonicalURL(rawURL)
	if err != nil {
		return coreartifact.Version{}, false, err
	}
	if !validContentHash(contentHash) {
		return coreartifact.Version{}, false, nil
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
	if err != nil || !exists || !containsHash(state.VersionHashes, contentHash) {
		return coreartifact.Version{}, false, err
	}
	manifest, err := store.readManifest(canonical, contentHash)
	if err != nil {
		return coreartifact.Version{}, false, err
	}
	body, err := readRegularBoundedRoot(store.rootFS, store.bodyRelative(canonical, contentHash), store.maxArtifactBytes)
	if err != nil || hashBytes(body) != contentHash {
		return coreartifact.Version{}, false, ErrCorruptArtifact
	}
	return manifest.version(body), true, nil
}

func repairArchiveStore(root string, rootFS *os.Root, options Options) (int64, int, int, error) {
	if err := repairMetadataDirectories(rootFS, true); err != nil {
		return 0, 0, 0, err
	}
	referencedBodies := make(map[string]struct{})
	referencedManifests := make(map[string]struct{})
	urlEntries, err := os.ReadDir(filepath.Join(root, "urls"))
	if err != nil {
		return 0, 0, 0, fmt.Errorf("local artifact: enumerate archive pointers: %w", err)
	}
	entries := 0
	versions := 0
	for _, entry := range urlEntries {
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return 0, 0, 0, fmt.Errorf("%w: invalid archive pointer entry %s", ErrCorruptArtifact, entry.Name())
		}
		raw, err := readRegularBoundedRoot(rootFS, filepath.Join("urls", entry.Name()), maxPointerBytes)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("%w: read archive pointer %s", ErrCorruptArtifact, entry.Name())
		}
		var state pointerState
		if json.Unmarshal(raw, &state) != nil || state.SchemaVersion != archiveSchemaVersion || entry.Name() != URLID(state.URL)+".json" || validateArchivePointer(state) != nil {
			return 0, 0, 0, fmt.Errorf("%w: invalid archive pointer %s", ErrCorruptArtifact, entry.Name())
		}
		canonical, canonicalErr := canonicalURL(state.URL)
		if canonicalErr != nil || canonical != state.URL {
			return 0, 0, 0, fmt.Errorf("%w: non-canonical archive pointer %s", ErrCorruptArtifact, entry.Name())
		}
		if len(state.VersionHashes) > options.MaxVersionsPerURL {
			return 0, 0, 0, ErrVersionLimit
		}
		entries++
		versions += len(state.VersionHashes)
		for _, contentHash := range state.VersionHashes {
			manifestRelative := filepath.Join("versions", URLID(state.URL), contentHash+".json")
			bodyRelative := filepath.Join("objects", URLID(state.URL), contentHash+".body")
			manifest, err := readArchiveManifest(rootFS, manifestRelative, state.URL, contentHash)
			if err != nil {
				return 0, 0, 0, err
			}
			body, err := readRegularBoundedRoot(rootFS, bodyRelative, options.MaxArtifactBytes)
			if err != nil || hashBytes(body) != contentHash {
				return 0, 0, 0, fmt.Errorf("%w: archive body %s", ErrCorruptArtifact, contentHash)
			}
			if manifest.ContentHash != contentHash {
				return 0, 0, 0, ErrCorruptArtifact
			}
			referencedBodies[bodyRelative] = struct{}{}
			referencedManifests[manifestRelative] = struct{}{}
		}
	}
	if entries > options.MaxEntries {
		return 0, 0, 0, ErrEntryLimit
	}
	if versions > options.MaxVersions {
		return 0, 0, 0, ErrVersionLimit
	}
	if err := purgeArchiveOrphans(root, rootFS, "objects", referencedBodies); err != nil {
		return 0, 0, 0, err
	}
	if err := purgeArchiveOrphans(root, rootFS, "versions", referencedManifests); err != nil {
		return 0, 0, 0, err
	}
	total, measuredEntries, err := measureStore(root)
	if err != nil {
		return 0, 0, 0, err
	}
	if measuredEntries != entries {
		return 0, 0, 0, ErrCorruptArtifact
	}
	return total, entries, versions, nil
}

func validateArchivePointer(state pointerState) error {
	if validateValidators(state.ETag, state.LastModified) != nil || validateSafetyClassification(state.SafetyClassification) != nil {
		return ErrCorruptArtifact
	}
	if !validIndexingDisposition(state.IndexingDisposition) || state.ExpiresAt != nil {
		return ErrCorruptArtifact
	}
	seen := make(map[string]struct{}, len(state.VersionHashes))
	for _, contentHash := range state.VersionHashes {
		if !validContentHash(contentHash) {
			return ErrCorruptArtifact
		}
		if _, exists := seen[contentHash]; exists {
			return ErrCorruptArtifact
		}
		seen[contentHash] = struct{}{}
	}
	if state.IndexingDisposition == localcorpus.DispositionPermitted {
		if !validContentHash(state.ContentHash) || len(state.VersionHashes) == 0 || !containsHash(state.VersionHashes, state.ContentHash) {
			return ErrCorruptArtifact
		}
		effective, err := canonicalURL(state.EffectiveURL)
		if state.EffectiveURL == "" || err != nil || effective != state.EffectiveURL || state.ObservedAt.IsZero() {
			return ErrCorruptArtifact
		}
		return nil
	}
	if state.ContentHash != "" || len(state.VersionHashes) != 0 || state.ObservedAt.IsZero() {
		return ErrCorruptArtifact
	}
	return nil
}

func validIndexingDisposition(disposition localcorpus.IndexingDisposition) bool {
	switch disposition {
	case localcorpus.DispositionPermitted,
		localcorpus.DispositionRobotsBlocked,
		localcorpus.DispositionNoIndexHeader,
		localcorpus.DispositionNoIndexMetadata,
		localcorpus.DispositionNoArchiveHeader,
		localcorpus.DispositionNoArchiveMetadata,
		localcorpus.DispositionTakedown,
		localcorpus.DispositionUnknown:
		return true
	default:
		return false
	}
}

func encodeManifest(manifest archiveManifest) ([]byte, error) {
	manifest.SchemaVersion = archiveSchemaVersion
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("local artifact: encode archive manifest: %w", err)
	}
	if len(encoded) > maxPointerBytes {
		return nil, ErrPointerTooLarge
	}
	return encoded, nil
}

func manifestFromPointer(state pointerState) archiveManifest {
	return archiveManifest{
		SchemaVersion: archiveSchemaVersion, URL: state.URL, EffectiveURL: state.EffectiveURL,
		ContentHash: state.ContentHash, ETag: state.ETag, LastModified: state.LastModified,
		MIME: state.MIME, PolicyAgent: state.PolicyAgent, FetchedAt: state.FetchedAt,
		ObservedAt: state.ObservedAt, ValidatedAt: state.ValidatedAt, ExpiresAt: copyTime(state.ExpiresAt),
		SafetyClassification: state.SafetyClassification, IndexingDisposition: localcorpus.DispositionPermitted,
	}
}

func readArchiveManifest(root *os.Root, path, canonical, contentHash string) (archiveManifest, error) {
	raw, err := readRegularBoundedRoot(root, path, maxPointerBytes)
	if err != nil {
		return archiveManifest{}, fmt.Errorf("%w: read archive manifest: %v", ErrCorruptArtifact, err)
	}
	var manifest archiveManifest
	if json.Unmarshal(raw, &manifest) != nil || manifest.SchemaVersion != archiveSchemaVersion || manifest.URL != canonical || manifest.ContentHash != contentHash || manifest.IndexingDisposition != localcorpus.DispositionPermitted || manifest.ExpiresAt != nil || manifest.ObservedAt.IsZero() || validateValidators(manifest.ETag, manifest.LastModified) != nil || validateSafetyClassification(manifest.SafetyClassification) != nil {
		return archiveManifest{}, ErrCorruptArtifact
	}
	effective, err := canonicalURL(manifest.EffectiveURL)
	if manifest.EffectiveURL == "" || err != nil || effective != manifest.EffectiveURL {
		return archiveManifest{}, ErrCorruptArtifact
	}
	return manifest, nil
}

func (store *Store) readManifest(canonical, contentHash string) (archiveManifest, error) {
	return readArchiveManifest(store.rootFS, store.manifestRelative(canonical, contentHash), canonical, contentHash)
}

func (manifest archiveManifest) info() coreartifact.VersionInfo {
	return coreartifact.VersionInfo{
		ContentHash: manifest.ContentHash, EffectiveURL: manifest.EffectiveURL, MIME: manifest.MIME,
		PolicyAgent: manifest.PolicyAgent, FetchedAt: manifest.FetchedAt, ObservedAt: manifest.ObservedAt,
		ValidatedAt: manifest.ValidatedAt, ExpiresAt: copyTime(manifest.ExpiresAt),
		SafetyClassification: manifest.SafetyClassification,
	}
}

func (manifest archiveManifest) version(body []byte) coreartifact.Version {
	return coreartifact.Version{
		URL: manifest.URL, EffectiveURL: manifest.EffectiveURL, Body: append([]byte(nil), body...),
		ContentHash: manifest.ContentHash, ETag: manifest.ETag, LastModified: manifest.LastModified,
		MIME: manifest.MIME, PolicyAgent: manifest.PolicyAgent, FetchedAt: manifest.FetchedAt,
		ObservedAt: manifest.ObservedAt, ValidatedAt: manifest.ValidatedAt, ExpiresAt: copyTime(manifest.ExpiresAt),
		SafetyClassification: manifest.SafetyClassification, IndexingDisposition: manifest.IndexingDisposition,
	}
}

func containsHash(hashes []string, target string) bool {
	for _, contentHash := range hashes {
		if contentHash == target {
			return true
		}
	}
	return false
}

func validateExistingBody(root *os.Root, path string, expected []byte, max int64) (int64, bool, error) {
	body, err := readRegularBoundedRoot(root, path, max)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil || hashBytes(body) != hashBytes(expected) {
		return 0, false, ErrCorruptArtifact
	}
	return int64(len(body)), true, nil
}

func validateExistingManifest(root *os.Root, path string, expected []byte) (int64, bool, error) {
	raw, err := readRegularBoundedRoot(root, path, maxPointerBytes)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil || string(raw) != string(expected) {
		return 0, false, ErrCorruptArtifact
	}
	return int64(len(raw)), true, nil
}

func ensureArchiveURLDirectory(store *Store, lane, canonical string) (bool, error) {
	directory := filepath.Join(store.root, lane, URLID(canonical))
	created, err := ensureRealDirectoryCreated(directory, 0o700)
	if err != nil {
		return false, fmt.Errorf("local artifact: create archive %s directory: %w", lane, err)
	}
	if created {
		if err := syncDirectory(filepath.Join(store.root, lane)); err != nil {
			_ = store.rootFS.Remove(filepath.Join(lane, URLID(canonical)))
			return false, fmt.Errorf("local artifact: sync archive %s parent: %w", lane, err)
		}
	}
	return created, nil
}

func (store *Store) manifestRelative(canonical, contentHash string) string {
	return filepath.Join("versions", URLID(canonical), contentHash+".json")
}

func (store *Store) archiveURLBytes(canonical string) (int64, error) {
	var total int64
	for _, lane := range []string{"objects", "versions"} {
		directory := filepath.Join(lane, URLID(canonical))
		entries, err := store.rootFS.Open(directory)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		listed, readErr := entries.ReadDir(-1)
		closeErr := entries.Close()
		if readErr != nil || closeErr != nil {
			return 0, errors.Join(readErr, closeErr)
		}
		for _, entry := range listed {
			relative := filepath.Join(directory, entry.Name())
			info, err := store.rootFS.Lstat(relative)
			if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return 0, ErrCorruptArtifact
			}
			if info.Size() > int64(^uint64(0)>>1)-total {
				return 0, ErrBudgetExceeded
			}
			total += info.Size()
		}
	}
	return total, nil
}

func (store *Store) purgeArchiveURL(canonical string) error {
	for _, lane := range []string{"objects", "versions"} {
		directory := filepath.Join(lane, URLID(canonical))
		handle, err := store.rootFS.Open(directory)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		entries, readErr := handle.ReadDir(-1)
		closeErr := handle.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		for _, entry := range entries {
			relative := filepath.Join(directory, entry.Name())
			info, err := store.rootFS.Lstat(relative)
			if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return ErrCorruptArtifact
			}
			if err := store.rootFS.Remove(relative); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err := store.rootFS.Remove(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func purgeArchiveOrphans(root string, rootFS *os.Root, lane string, referenced map[string]struct{}) error {
	laneRoot := filepath.Join(root, lane)
	directories := make([]string, 0)
	err := filepath.WalkDir(laneRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: archive symlink %s", ErrCorruptArtifact, path)
		}
		if entry.IsDir() {
			if path != laneRoot {
				directories = append(directories, path)
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("%w: archive non-regular entry %s", ErrCorruptArtifact, path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if _, keep := referenced[relative]; keep {
			return nil
		}
		if err := rootFS.Remove(relative); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	for index := len(directories) - 1; index >= 0; index-- {
		relative, err := filepath.Rel(root, directories[index])
		if err != nil {
			return err
		}
		_ = rootFS.Remove(relative)
	}
	return nil
}

func countCommittedVersions(rootFS *os.Root, root string, maxVersionsPerURL int) (int, error) {
	entries, err := os.ReadDir(filepath.Join(root, "urls"))
	if err != nil {
		return 0, err
	}
	total := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := readRegularBoundedRoot(rootFS, filepath.Join("urls", entry.Name()), maxPointerBytes)
		if err != nil {
			return 0, err
		}
		var state pointerState
		if json.Unmarshal(raw, &state) != nil || state.SchemaVersion != archiveSchemaVersion || validateArchivePointer(state) != nil {
			return 0, ErrCorruptArtifact
		}
		if len(state.VersionHashes) > maxVersionsPerURL {
			return 0, ErrVersionLimit
		}
		total += len(state.VersionHashes)
	}
	return total, nil
}

var _ coreartifact.ArchiveReader = (*Store)(nil)
