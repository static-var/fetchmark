package crawlfrontier

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	sourcesBucket        = []byte("discovery_sources")
	sourcePagesBucket    = []byte("source_pages")
	pageSourcesBucket    = []byte("page_sources")
	sourceChildrenBucket = []byte("source_children")
	childParentsBucket   = []byte("child_parents")
)

const (
	maxValidatorBytes            = 4 << 10
	reasonPageOwnershipRemoved   = "discovery ownership removed"
	reasonSourceOwnershipRemoved = "source ownership removed"
)

type DiscoverySourceSeed struct {
	Key       string
	JobID     string
	URL       string
	Kind      DiscoverySourceKind
	RootKey   string
	ParentKey string
	Depth     int
}

type DiscoverySourceKind string

const (
	DiscoverySourceAuto    DiscoverySourceKind = "auto"
	DiscoverySourceSitemap DiscoverySourceKind = "sitemap"
	DiscoverySourceFeed    DiscoverySourceKind = "feed"
)

type DiscoverySourceEntry struct {
	Key             string              `json:"key"`
	JobID           string              `json:"job_id"`
	URL             string              `json:"url"`
	Kind            DiscoverySourceKind `json:"kind"`
	RootKey         string              `json:"root_key"`
	ParentKey       string              `json:"parent_key,omitempty"`
	Depth           int                 `json:"depth"`
	State           State               `json:"state"`
	Configured      bool                `json:"configured,omitempty"`
	Attempts        int                 `json:"attempts"`
	NextEligible    time.Time           `json:"next_eligible,omitempty"`
	LeaseUntil      time.Time           `json:"lease_until,omitempty"`
	LeaseGeneration uint64              `json:"lease_generation,omitempty"`
	LastReason      string              `json:"last_reason,omitempty"`
	ETag            string              `json:"etag,omitempty"`
	LastModified    string              `json:"last_modified,omitempty"`
	HasSnapshot     bool                `json:"has_snapshot,omitempty"`
}

type DiscoveredPage struct {
	Key string
	URL string
}

type SourceValidators struct {
	ETag         string
	LastModified string
}

type DiscoverySnapshot struct {
	SourceKey       string
	LeaseGeneration uint64
	Pages           []DiscoveredPage
	Children        []DiscoverySourceSeed
	Validators      SourceValidators
	NextEligible    time.Time
}

// SyncDiscoverySources atomically replaces the configured root-source set.
// Discovered descendants and their last committed snapshots retain identity,
// but become unschedulable whenever they are unreachable from a configured root.
func (f *Frontier) SyncDiscoverySources(seeds []DiscoverySourceSeed, now time.Time) error {
	if now.IsZero() {
		return ErrInvalidOptions
	}
	db, release, err := f.acquire()
	if err != nil {
		return err
	}
	defer release()
	if len(seeds) > f.maxSources {
		return ErrMaxEntries
	}
	now = now.UTC()
	normalized := make(map[string]DiscoverySourceSeed, len(seeds))
	for _, seed := range seeds {
		seed = normalizeRootSource(seed)
		if err := validateDiscoverySourceSeed(seed); err != nil {
			return err
		}
		if _, found := normalized[seed.Key]; found {
			return fmt.Errorf("%w: duplicate source %q", ErrInvalidSeed, seed.Key)
		}
		normalized[seed.Key] = seed
	}
	return db.Update(func(tx *bolt.Tx) error {
		sources := tx.Bucket(sourcesBucket)
		if sources == nil {
			return ErrSchemaMismatch
		}
		newCount := 0
		for key, seed := range normalized {
			raw := sources.Get([]byte(key))
			if raw == nil {
				newCount++
				continue
			}
			source, err := decodeDiscoverySource([]byte(key), raw)
			if err != nil {
				return err
			}
			if source.JobID != seed.JobID || source.URL != seed.URL || source.RootKey != seed.RootKey || source.ParentKey != seed.ParentKey || source.Depth != seed.Depth {
				return fmt.Errorf("%w: source %q", ErrKeyConflict, key)
			}
		}
		if sources.Stats().KeyN+newCount > f.maxSources {
			return ErrMaxEntries
		}
		for key, seed := range normalized {
			raw := sources.Get([]byte(key))
			if raw == nil {
				source := DiscoverySourceEntry{Key: key, JobID: seed.JobID, URL: seed.URL, Kind: seed.Kind, RootKey: seed.RootKey, ParentKey: seed.ParentKey, Depth: seed.Depth, State: StateQueued, Configured: true, NextEligible: now}
				if err := putDiscoverySource(sources, source); err != nil {
					return err
				}
				continue
			}
			source, err := decodeDiscoverySource([]byte(key), raw)
			if err != nil {
				return err
			}
			kindChanged := source.Kind != seed.Kind
			source.Kind = seed.Kind
			source.Configured = true
			if kindChanged {
				if err := removeSourcePageMemberships(tx, source.Key); err != nil {
					return err
				}
				if err := removeSourceChildMemberships(tx, source.Key); err != nil {
					return err
				}
				source.State = StateQueued
				source.Attempts = 0
				source.NextEligible = now
				source.LeaseUntil = time.Time{}
				source.LastReason = "source kind changed"
				source.ETag = ""
				source.LastModified = ""
				source.HasSnapshot = false
			} else if source.State == StateDisabled {
				source.State = StateQueued
				source.NextEligible = now
				source.LeaseUntil = time.Time{}
				source.LastReason = ""
			}
			if err := putDiscoverySource(sources, source); err != nil {
				return err
			}
		}
		removed := make([]DiscoverySourceEntry, 0)
		if err := sources.ForEach(func(key, raw []byte) error {
			source, err := decodeDiscoverySource(key, raw)
			if err != nil {
				return err
			}
			if _, present := normalized[string(key)]; source.Configured && !present {
				source.Configured = false
				removed = append(removed, source)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, source := range removed {
			if err := putDiscoverySource(sources, source); err != nil {
				return err
			}
		}
		return f.reconcileDiscoveryState(tx, now)
	})
}

func (f *Frontier) LeaseDueDiscoverySources(now time.Time, limit int, leaseTTL time.Duration) ([]DiscoverySourceEntry, error) {
	return f.leaseDueDiscoverySources("", now, limit, leaseTTL)
}

func (f *Frontier) LeaseDueDiscoverySourcesJob(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]DiscoverySourceEntry, error) {
	if strings.TrimSpace(jobID) == "" || len(jobID) > maxJobIDBytes {
		return nil, ErrInvalidOptions
	}
	return f.leaseDueDiscoverySources(jobID, now, limit, leaseTTL)
}

func (f *Frontier) leaseDueDiscoverySources(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]DiscoverySourceEntry, error) {
	if limit <= 0 || leaseTTL <= 0 {
		return nil, ErrInvalidOptions
	}
	db, release, err := f.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	if limit > f.maxSources {
		limit = f.maxSources
	}
	now = now.UTC()
	var leased []DiscoverySourceEntry
	err = db.Update(func(tx *bolt.Tx) error {
		if err := recoverExpiredSourceLeases(tx, now); err != nil {
			return err
		}
		selected, err := dueSourcesIndexed(tx, jobID, now, limit)
		if err != nil {
			return err
		}
		sources := tx.Bucket(sourcesBucket)
		meta := tx.Bucket(metaBucket)
		if sources == nil || meta == nil {
			return ErrSchemaMismatch
		}
		sequenceRaw := meta.Get(leaseSeqKey)
		if len(sequenceRaw) != 8 {
			return ErrSchemaMismatch
		}
		sequence := binary.BigEndian.Uint64(sequenceRaw)
		leased = make([]DiscoverySourceEntry, 0, len(selected))
		for _, indexed := range selected {
			raw := sources.Get([]byte(indexed.entryKey))
			if raw == nil {
				return fmt.Errorf("%w: indexed source missing %q", ErrCorrupt, indexed.entryKey)
			}
			source, err := decodeDiscoverySource([]byte(indexed.entryKey), raw)
			if err != nil {
				return err
			}
			if !sourceSchedulable(source.State) || source.NextEligible.After(now) || (jobID != "" && source.JobID != jobID) {
				return fmt.Errorf("%w: source due mismatch %q", ErrCorrupt, source.Key)
			}
			expected := sourceDueGlobalKey(source)
			if jobID != "" {
				expected = sourceDueByJobKey(source)
			}
			if !bytes.Equal(expected, indexed.indexKey) || sequence == ^uint64(0) {
				return fmt.Errorf("%w: source due key or sequence", ErrCorrupt)
			}
			if err := removeSourceDue(tx, source); err != nil {
				return err
			}
			sequence++
			source.State = StateLeased
			source.Attempts++
			source.LeaseGeneration = sequence
			source.LeaseUntil = now.Add(leaseTTL)
			if err := putDiscoverySource(sources, source); err != nil {
				return err
			}
			if err := addSourceLease(tx, source); err != nil {
				return err
			}
			leased = append(leased, source)
		}
		if len(selected) > 0 {
			next := make([]byte, 8)
			binary.BigEndian.PutUint64(next, sequence)
			if err := meta.Put(leaseSeqKey, next); err != nil {
				return err
			}
		}
		return nil
	})
	return leased, err
}

func (f *Frontier) CompleteDiscoverySource(key string, generation uint64, outcome State, nextEligible time.Time, reason string) error {
	if outcome != StateRetry {
		return ErrInvalidTransition
	}
	if nextEligible.IsZero() {
		return ErrInvalidTransition
	}
	return f.completeDiscoverySource(key, generation, outcome, nextEligible, reason, SourceValidators{}, false)
}

// PreserveDiscoverySnapshot completes a conditional not-modified poll without
// changing memberships. The supplied validator pair replaces the stored pair.
func (f *Frontier) PreserveDiscoverySnapshot(key string, generation uint64, validators SourceValidators, nextEligible time.Time) error {
	return f.completeDiscoverySource(key, generation, StateSucceeded, nextEligible, "not modified", validators, true)
}

func (f *Frontier) completeDiscoverySource(key string, generation uint64, outcome State, nextEligible time.Time, reason string, validators SourceValidators, preserve bool) error {
	if strings.TrimSpace(key) == "" || generation == 0 || len(reason) > maxReasonBytes || nextEligible.IsZero() || !validValidators(validators) {
		return ErrInvalidTransition
	}
	db, release, err := f.acquire()
	if err != nil {
		return err
	}
	defer release()
	return db.Update(func(tx *bolt.Tx) error {
		sources := tx.Bucket(sourcesBucket)
		if sources == nil {
			return ErrSchemaMismatch
		}
		raw := sources.Get([]byte(key))
		if raw == nil {
			return ErrNotFound
		}
		source, err := decodeDiscoverySource([]byte(key), raw)
		if err != nil {
			return err
		}
		if source.State != StateLeased {
			return ErrInvalidTransition
		}
		if source.LeaseGeneration != generation {
			return ErrLeaseMismatch
		}
		if preserve && !source.HasSnapshot {
			return ErrInvalidTransition
		}
		if err := removeSourceLease(tx, source); err != nil {
			return err
		}
		source.State = outcome
		source.LeaseUntil = time.Time{}
		source.LastReason = reason
		source.NextEligible = nextEligible.UTC()
		if preserve {
			source.ETag = validators.ETag
			source.LastModified = validators.LastModified
			source.Attempts = 0
		}
		if err := putDiscoverySource(sources, source); err != nil {
			return err
		}
		return f.reconcileDiscoveryState(tx, nextEligible.UTC())
	})
}

// ApplyDiscoverySnapshot atomically exact-replaces one leased source's page
// memberships and child edges, commits validators, and schedules its next poll.
func (f *Frontier) ApplyDiscoverySnapshot(snapshot DiscoverySnapshot, now time.Time) error {
	if f == nil {
		return ErrClosed
	}
	if now.IsZero() || strings.TrimSpace(snapshot.SourceKey) == "" || snapshot.LeaseGeneration == 0 || snapshot.NextEligible.IsZero() || !validValidators(snapshot.Validators) || len(snapshot.Pages) > f.maxPagesPerSource || len(snapshot.Children) > f.maxChildrenPerSource {
		return ErrInvalidOptions
	}
	pageSeeds := make(map[string]DiscoveredPage, len(snapshot.Pages))
	for _, page := range snapshot.Pages {
		seed := Seed{Key: page.Key, JobID: "placeholder", URL: page.URL}
		if strings.TrimSpace(page.Key) == "" || len(page.Key) > maxKeyBytes || len(page.URL) > maxURLBytes || validateSeed(seed) != nil {
			return ErrInvalidSeed
		}
		if _, duplicate := pageSeeds[page.Key]; duplicate {
			return ErrInvalidSeed
		}
		pageSeeds[page.Key] = page
	}
	children := make(map[string]DiscoverySourceSeed, len(snapshot.Children))
	for _, child := range snapshot.Children {
		if child.Kind == "" {
			child.Kind = DiscoverySourceAuto
		}
		if err := validateSeed(Seed{Key: child.Key, JobID: child.JobID, URL: child.URL}); err != nil {
			return err
		}
		if !validSourceKind(child.Kind) {
			return ErrInvalidSeed
		}
		if _, duplicate := children[child.Key]; duplicate {
			return ErrInvalidSeed
		}
		children[child.Key] = child
	}
	db, release, err := f.acquire()
	if err != nil {
		return err
	}
	defer release()
	now = now.UTC()
	return db.Update(func(tx *bolt.Tx) error {
		sources := tx.Bucket(sourcesBucket)
		entries := tx.Bucket(entriesBucket)
		if sources == nil || entries == nil {
			return ErrSchemaMismatch
		}
		raw := sources.Get([]byte(snapshot.SourceKey))
		if raw == nil {
			return ErrNotFound
		}
		source, err := decodeDiscoverySource([]byte(snapshot.SourceKey), raw)
		if err != nil {
			return err
		}
		if source.State != StateLeased {
			return ErrInvalidTransition
		}
		if source.LeaseGeneration != snapshot.LeaseGeneration {
			return ErrLeaseMismatch
		}
		initialEntries := entries.Stats().KeyN
		initialSources := sources.Stats().KeyN
		for key, child := range children {
			normalizedChild, err := normalizeChildSource(child, source, f.maxSourceDepth)
			if err != nil {
				return err
			}
			children[key] = normalizedChild
		}
		if err := removeSourcePageMemberships(tx, source.Key); err != nil {
			return err
		}
		if err := removeSourceChildMemberships(tx, source.Key); err != nil {
			return err
		}
		newPages := 0
		for _, page := range pageSeeds {
			raw := entries.Get([]byte(page.Key))
			if raw == nil {
				newPages++
				entry := Entry{Key: page.Key, JobID: source.JobID, URL: page.URL, State: StateQueued, NextEligible: now}
				if err := putEntry(entries, entry); err != nil {
					return err
				}
			} else {
				entry, err := decodeEntry([]byte(page.Key), raw)
				if err != nil {
					return err
				}
				if entry.JobID != source.JobID || entry.URL != page.URL {
					return fmt.Errorf("%w: page %q", ErrKeyConflict, page.Key)
				}
				if entry.State == StateDisabled && entry.LastReason == reasonPageOwnershipRemoved {
					entry.State = StateQueued
					entry.NextEligible = now
					entry.LastReason = ""
					if err := putEntry(entries, entry); err != nil {
						return err
					}
				}
			}
			if err := addMembership(tx, sourcePagesBucket, pageSourcesBucket, source.Key, page.Key); err != nil {
				return err
			}
		}
		if initialEntries > f.maxEntries || newPages > f.maxEntries-initialEntries {
			return ErrMaxEntries
		}
		newSources := 0
		for _, child := range children {
			raw := sources.Get([]byte(child.Key))
			if raw == nil {
				newSources++
				childSource := DiscoverySourceEntry{Key: child.Key, JobID: child.JobID, URL: child.URL, Kind: child.Kind, RootKey: child.RootKey, ParentKey: child.ParentKey, Depth: child.Depth, State: StateQueued, NextEligible: now}
				if err := putDiscoverySource(sources, childSource); err != nil {
					return err
				}
			} else {
				childSource, err := decodeDiscoverySource([]byte(child.Key), raw)
				if err != nil {
					return err
				}
				if childSource.JobID != child.JobID || childSource.URL != child.URL || childSource.Kind != child.Kind || childSource.RootKey != child.RootKey {
					return fmt.Errorf("%w: child source %q", ErrKeyConflict, child.Key)
				}
				if childSource.State == StateDisabled {
					childSource.State = StateQueued
					childSource.NextEligible = now
					if err := putDiscoverySource(sources, childSource); err != nil {
						return err
					}
				}
			}
			if err := addMembership(tx, sourceChildrenBucket, childParentsBucket, source.Key, child.Key); err != nil {
				return err
			}
		}
		if initialSources > f.maxSources || newSources > f.maxSources-initialSources || pageMembershipCount(tx) > f.maxMemberships || sourceEdgeCount(tx) > f.maxSourceEdges {
			return ErrMaxEntries
		}
		if err := removeSourceLease(tx, source); err != nil {
			return err
		}
		source.State = StateSucceeded
		source.LeaseUntil = time.Time{}
		source.NextEligible = snapshot.NextEligible.UTC()
		source.Attempts = 0
		source.LastReason = "snapshot applied"
		source.ETag = snapshot.Validators.ETag
		source.LastModified = snapshot.Validators.LastModified
		source.HasSnapshot = true
		if err := putDiscoverySource(sources, source); err != nil {
			return err
		}
		return f.reconcileDiscoveryState(tx, now)
	})
}

func (f *Frontier) reconcileDiscoveryState(tx *bolt.Tx, now time.Time) error {
	sources := tx.Bucket(sourcesBucket)
	entries := tx.Bucket(entriesBucket)
	children := tx.Bucket(sourceChildrenBucket)
	if sources == nil || entries == nil || children == nil {
		return ErrSchemaMismatch
	}
	all := make(map[string]DiscoverySourceEntry)
	if err := sources.ForEach(func(key, raw []byte) error {
		source, err := decodeDiscoverySource(key, raw)
		if err == nil {
			all[string(key)] = source
		}
		return err
	}); err != nil {
		return err
	}
	type ancestry struct {
		root   string
		parent string
		depth  int
	}
	reachable := make(map[string]ancestry)
	queue := make([]string, 0)
	configured := make([]string, 0)
	for key, source := range all {
		if source.Configured {
			configured = append(configured, key)
		}
	}
	sort.Strings(configured)
	for _, key := range configured {
		reachable[key] = ancestry{root: key}
		queue = append(queue, key)
	}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		parentSource := all[parent]
		parentAncestry := reachable[parent]
		for _, child := range membershipSeconds(children, parent) {
			childSource, found := all[child]
			if !found || childSource.JobID != parentSource.JobID || childSource.RootKey != parentAncestry.root {
				return fmt.Errorf("%w: invalid discovery edge %q -> %q", ErrCorrupt, parent, child)
			}
			candidate := ancestry{root: parentAncestry.root, parent: parent, depth: parentAncestry.depth + 1}
			if candidate.depth > f.maxSourceDepth {
				continue
			}
			current, seen := reachable[child]
			if !seen || candidate.depth < current.depth || candidate.depth == current.depth && candidate.parent < current.parent {
				reachable[child] = candidate
				queue = append(queue, child)
			}
		}
	}
	sourceUpdates := make([]DiscoverySourceEntry, 0)
	for key, source := range all {
		ancestry, active := reachable[key]
		if active {
			changed := source.RootKey != ancestry.root || source.ParentKey != ancestry.parent || source.Depth != ancestry.depth
			source.RootKey = ancestry.root
			source.ParentKey = ancestry.parent
			source.Depth = ancestry.depth
			if source.State == StateDisabled {
				source.State = StateQueued
				source.NextEligible = now
				source.LastReason = ""
				changed = true
			}
			if changed {
				sourceUpdates = append(sourceUpdates, source)
			}
			continue
		}
		if source.State != StateDisabled || !source.NextEligible.IsZero() || !source.LeaseUntil.IsZero() || source.LastReason != reasonSourceOwnershipRemoved {
			source.State = StateDisabled
			source.NextEligible = time.Time{}
			source.LeaseUntil = time.Time{}
			source.LastReason = reasonSourceOwnershipRemoved
			sourceUpdates = append(sourceUpdates, source)
		}
	}
	for _, source := range sourceUpdates {
		if err := putDiscoverySource(sources, source); err != nil {
			return err
		}
	}
	effectivePages := make(map[string]bool)
	pages := tx.Bucket(sourcePagesBucket)
	if pages == nil {
		return ErrSchemaMismatch
	}
	for source := range reachable {
		for _, page := range membershipSeconds(pages, source) {
			effectivePages[page] = true
		}
	}
	entryUpdates := make([]Entry, 0)
	if err := entries.ForEach(func(key, raw []byte) error {
		entry, err := decodeEntry(key, raw)
		if err != nil {
			return err
		}
		baseOwned := entry.Explicit || effectivePages[entry.Key]
		owned := baseOwned || hasActiveLinkParent(tx, entry.Key)
		if !owned && entry.State != StateDisabled {
			entry.State = StateDisabled
			entry.NextEligible = time.Time{}
			entry.LeaseUntil = time.Time{}
			entry.LastReason = reasonPageOwnershipRemoved
			entryUpdates = append(entryUpdates, entry)
		} else if owned && entry.State == StateDisabled && entry.LastReason == reasonPageOwnershipRemoved {
			entry.State = StateQueued
			entry.NextEligible = now
			entry.LastReason = ""
			entryUpdates = append(entryUpdates, entry)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, entry := range entryUpdates {
		if err := putEntry(entries, entry); err != nil {
			return err
		}
	}
	if err := rebuildSchedulingIndexes(tx); err != nil {
		return err
	}
	if err := rebuildSourceSchedule(tx); err != nil {
		return err
	}
	return rebuildAuthorityRefs(tx)
}

func validateDiscoverySourceSeed(seed DiscoverySourceSeed) error {
	if err := validateSeed(Seed{Key: seed.Key, JobID: seed.JobID, URL: seed.URL}); err != nil {
		return err
	}
	if !validSourceKind(seed.Kind) || strings.TrimSpace(seed.RootKey) == "" || len(seed.RootKey) > maxKeyBytes || len(seed.ParentKey) > maxKeyBytes || seed.Depth < 0 || seed.Depth > 256 {
		return ErrInvalidSeed
	}
	return nil
}

func validSourceKind(kind DiscoverySourceKind) bool {
	return kind == DiscoverySourceAuto || kind == DiscoverySourceSitemap || kind == DiscoverySourceFeed
}

func normalizeRootSource(seed DiscoverySourceSeed) DiscoverySourceSeed {
	if seed.Kind == "" {
		seed.Kind = DiscoverySourceAuto
	}
	seed.RootKey = seed.Key
	seed.ParentKey = ""
	seed.Depth = 0
	return seed
}

func normalizeChildSource(seed DiscoverySourceSeed, parent DiscoverySourceEntry, maxDepth int) (DiscoverySourceSeed, error) {
	if seed.Kind == "" {
		seed.Kind = DiscoverySourceAuto
	}
	if seed.JobID != parent.JobID || parent.Depth >= maxDepth {
		return seed, ErrInvalidSeed
	}
	expectedDepth := parent.Depth + 1
	if seed.RootKey != "" && seed.RootKey != parent.RootKey || seed.ParentKey != "" && seed.ParentKey != parent.Key || seed.Depth != 0 && seed.Depth != expectedDepth {
		return seed, ErrKeyConflict
	}
	seed.RootKey = parent.RootKey
	seed.ParentKey = parent.Key
	seed.Depth = expectedDepth
	if err := validateDiscoverySourceSeed(seed); err != nil {
		return seed, err
	}
	return seed, nil
}

func validValidators(validators SourceValidators) bool {
	return len(validators.ETag) <= maxValidatorBytes && len(validators.LastModified) <= maxValidatorBytes
}

func putDiscoverySource(bucket *bolt.Bucket, source DiscoverySourceEntry) error {
	raw, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(source.Key), raw)
}

func decodeDiscoverySource(key, raw []byte) (DiscoverySourceEntry, error) {
	var source DiscoverySourceEntry
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&source); err != nil {
		return source, fmt.Errorf("%w: decode source %q: %v", ErrCorrupt, key, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return source, fmt.Errorf("%w: trailing source data %q", ErrCorrupt, key)
	}
	if source.Key != string(key) || validateDiscoverySourceSeed(DiscoverySourceSeed{Key: source.Key, JobID: source.JobID, URL: source.URL, Kind: source.Kind, RootKey: source.RootKey, ParentKey: source.ParentKey, Depth: source.Depth}) != nil || source.Attempts < 0 || len(source.LastReason) > maxReasonBytes || !validValidators(SourceValidators{ETag: source.ETag, LastModified: source.LastModified}) {
		return source, fmt.Errorf("%w: invalid source %q", ErrCorrupt, key)
	}
	switch source.State {
	case StateQueued, StateRetry, StateSucceeded:
		if source.NextEligible.IsZero() || !source.LeaseUntil.IsZero() {
			return source, fmt.Errorf("%w: invalid scheduled source %q", ErrCorrupt, key)
		}
	case StateLeased:
		if source.LeaseUntil.IsZero() || source.LeaseGeneration == 0 {
			return source, fmt.Errorf("%w: invalid leased source %q", ErrCorrupt, key)
		}
	case StateDisabled:
		if !source.NextEligible.IsZero() || !source.LeaseUntil.IsZero() {
			return source, fmt.Errorf("%w: invalid disabled source %q", ErrCorrupt, key)
		}
	default:
		return source, fmt.Errorf("%w: invalid source state %q", ErrCorrupt, source.State)
	}
	return source, nil
}

func membershipKey(first, second string) []byte {
	return orderedString(orderedString(nil, first), second)
}

func addMembership(tx *bolt.Tx, forwardName, reverseName []byte, first, second string) error {
	forward := tx.Bucket(forwardName)
	reverse := tx.Bucket(reverseName)
	if forward == nil || reverse == nil {
		return ErrSchemaMismatch
	}
	if err := forward.Put(membershipKey(first, second), indexMarker); err != nil {
		return err
	}
	return reverse.Put(membershipKey(second, first), indexMarker)
}

func removeMembership(tx *bolt.Tx, forwardName, reverseName []byte, first, second string) error {
	forward := tx.Bucket(forwardName)
	reverse := tx.Bucket(reverseName)
	if forward == nil || reverse == nil {
		return ErrSchemaMismatch
	}
	if err := forward.Delete(membershipKey(first, second)); err != nil {
		return err
	}
	return reverse.Delete(membershipKey(second, first))
}

func membershipSeconds(bucket *bolt.Bucket, first string) []string {
	prefix := orderedString(nil, first)
	values := make([]string, 0)
	cursor := bucket.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		second, trailing, err := decodeOrderedString(key[len(prefix):])
		if err == nil && len(trailing) == 0 {
			values = append(values, second)
		}
	}
	return values
}

func removeSourcePageMemberships(tx *bolt.Tx, source string) error {
	forward := tx.Bucket(sourcePagesBucket)
	if forward == nil {
		return ErrSchemaMismatch
	}
	for _, page := range membershipSeconds(forward, source) {
		if err := removeMembership(tx, sourcePagesBucket, pageSourcesBucket, source, page); err != nil {
			return err
		}
	}
	return nil
}

func removeSourceChildMemberships(tx *bolt.Tx, source string) error {
	forward := tx.Bucket(sourceChildrenBucket)
	if forward == nil {
		return ErrSchemaMismatch
	}
	for _, child := range membershipSeconds(forward, source) {
		if err := removeMembership(tx, sourceChildrenBucket, childParentsBucket, source, child); err != nil {
			return err
		}
	}
	return nil
}

func hasPageMembership(tx *bolt.Tx, page string) bool {
	bucket := tx.Bucket(pageSourcesBucket)
	return bucket != nil && len(membershipSeconds(bucket, page)) > 0
}

func pageMembershipCount(tx *bolt.Tx) int {
	pages := tx.Bucket(sourcePagesBucket)
	if pages == nil {
		return int(^uint(0) >> 1)
	}
	count := 0
	_ = pages.ForEach(func(_, _ []byte) error {
		count++
		return nil
	})
	return count
}

func sourceEdgeCount(tx *bolt.Tx) int {
	edges := tx.Bucket(sourceChildrenBucket)
	if edges == nil {
		return int(^uint(0) >> 1)
	}
	count := 0
	_ = edges.ForEach(func(_, _ []byte) error {
		count++
		return nil
	})
	return count
}

func decodeMembershipKey(raw []byte) (string, string, error) {
	first, rest, err := decodeOrderedString(raw)
	if err != nil {
		return "", "", err
	}
	second, trailing, err := decodeOrderedString(rest)
	if err != nil || len(trailing) != 0 {
		return "", "", fmt.Errorf("%w: malformed membership key", ErrCorrupt)
	}
	return first, second, nil
}

func validateDiscoveryState(tx *bolt.Tx, f *Frontier, enforceReachability bool) error {
	sources := tx.Bucket(sourcesBucket)
	entries := tx.Bucket(entriesBucket)
	sourcePages := tx.Bucket(sourcePagesBucket)
	pageSources := tx.Bucket(pageSourcesBucket)
	sourceChildren := tx.Bucket(sourceChildrenBucket)
	childParents := tx.Bucket(childParentsBucket)
	if sources == nil || entries == nil || sourcePages == nil || pageSources == nil || sourceChildren == nil || childParents == nil {
		return ErrSchemaMismatch
	}
	allSources := make(map[string]DiscoverySourceEntry)
	if err := sources.ForEach(func(key, raw []byte) error {
		source, err := decodeDiscoverySource(key, raw)
		if err != nil {
			return err
		}
		if source.Configured && (source.RootKey != source.Key || source.ParentKey != "" || source.Depth != 0) {
			return fmt.Errorf("%w: configured source ancestry %q", ErrCorrupt, source.Key)
		}
		allSources[source.Key] = source
		return nil
	}); err != nil {
		return err
	}
	if len(allSources) > f.maxSources {
		return ErrMaxEntries
	}
	allEntries := make(map[string]Entry)
	if err := entries.ForEach(func(key, raw []byte) error {
		entry, err := decodeEntry(key, raw)
		if err == nil {
			allEntries[entry.Key] = entry
		}
		return err
	}); err != nil {
		return err
	}

	pageCounts := make(map[string]int)
	if err := sourcePages.ForEach(func(key, marker []byte) error {
		if !bytes.Equal(marker, indexMarker) {
			return fmt.Errorf("%w: invalid source-page marker", ErrCorrupt)
		}
		sourceKey, pageKey, err := decodeMembershipKey(key)
		if err != nil {
			return err
		}
		source, sourceFound := allSources[sourceKey]
		entry, pageFound := allEntries[pageKey]
		if !sourceFound || !pageFound || source.JobID != entry.JobID || !source.HasSnapshot || !bytes.Equal(pageSources.Get(membershipKey(pageKey, sourceKey)), indexMarker) {
			return fmt.Errorf("%w: invalid source-page membership %q -> %q", ErrCorrupt, sourceKey, pageKey)
		}
		pageCounts[sourceKey]++
		if pageCounts[sourceKey] > f.maxPagesPerSource {
			return ErrMaxEntries
		}
		return nil
	}); err != nil {
		return err
	}
	if sourcePages.Stats().KeyN > f.maxMemberships || pageSources.Stats().KeyN != sourcePages.Stats().KeyN {
		return fmt.Errorf("%w: source-page membership cardinality", ErrCorrupt)
	}
	if err := pageSources.ForEach(func(key, marker []byte) error {
		if !bytes.Equal(marker, indexMarker) {
			return fmt.Errorf("%w: invalid page-source marker", ErrCorrupt)
		}
		pageKey, sourceKey, err := decodeMembershipKey(key)
		if err != nil || !bytes.Equal(sourcePages.Get(membershipKey(sourceKey, pageKey)), indexMarker) {
			return fmt.Errorf("%w: unmatched page-source membership", ErrCorrupt)
		}
		return nil
	}); err != nil {
		return err
	}

	childCounts := make(map[string]int)
	if err := sourceChildren.ForEach(func(key, marker []byte) error {
		if !bytes.Equal(marker, indexMarker) {
			return fmt.Errorf("%w: invalid source-child marker", ErrCorrupt)
		}
		parentKey, childKey, err := decodeMembershipKey(key)
		if err != nil {
			return err
		}
		parent, parentFound := allSources[parentKey]
		child, childFound := allSources[childKey]
		if !parentFound || !childFound || parent.JobID != child.JobID || parent.RootKey != child.RootKey || !parent.HasSnapshot || !bytes.Equal(childParents.Get(membershipKey(childKey, parentKey)), indexMarker) {
			return fmt.Errorf("%w: invalid source-child membership %q -> %q", ErrCorrupt, parentKey, childKey)
		}
		childCounts[parentKey]++
		if childCounts[parentKey] > f.maxChildrenPerSource {
			return ErrMaxEntries
		}
		return nil
	}); err != nil {
		return err
	}
	if sourceChildren.Stats().KeyN > f.maxSourceEdges || childParents.Stats().KeyN != sourceChildren.Stats().KeyN {
		return fmt.Errorf("%w: source edge cardinality", ErrCorrupt)
	}
	if err := childParents.ForEach(func(key, marker []byte) error {
		if !bytes.Equal(marker, indexMarker) {
			return fmt.Errorf("%w: invalid child-parent marker", ErrCorrupt)
		}
		childKey, parentKey, err := decodeMembershipKey(key)
		if err != nil || !bytes.Equal(sourceChildren.Get(membershipKey(parentKey, childKey)), indexMarker) {
			return fmt.Errorf("%w: unmatched child-parent membership", ErrCorrupt)
		}
		return nil
	}); err != nil {
		return err
	}
	if !enforceReachability {
		return nil
	}

	type ancestry struct {
		root   string
		parent string
		depth  int
	}
	reachable := make(map[string]ancestry)
	queue := make([]string, 0)
	configured := make([]string, 0)
	for key, source := range allSources {
		if source.Configured {
			configured = append(configured, key)
		}
	}
	sort.Strings(configured)
	for _, key := range configured {
		reachable[key] = ancestry{root: key}
		queue = append(queue, key)
	}
	for len(queue) > 0 {
		parentKey := queue[0]
		queue = queue[1:]
		parent := allSources[parentKey]
		parentAncestry := reachable[parentKey]
		for _, childKey := range membershipSeconds(sourceChildren, parentKey) {
			child := allSources[childKey]
			candidate := ancestry{root: parentAncestry.root, parent: parentKey, depth: parentAncestry.depth + 1}
			if child.JobID != parent.JobID || child.RootKey != candidate.root {
				return fmt.Errorf("%w: invalid reachable source edge", ErrCorrupt)
			}
			if candidate.depth > f.maxSourceDepth {
				continue
			}
			current, seen := reachable[childKey]
			if !seen || candidate.depth < current.depth || candidate.depth == current.depth && candidate.parent < current.parent {
				reachable[childKey] = candidate
				queue = append(queue, childKey)
			}
		}
	}
	effectivePages := make(map[string]bool)
	for key, source := range allSources {
		ancestry, active := reachable[key]
		if active {
			if source.State == StateDisabled || source.RootKey != ancestry.root || source.ParentKey != ancestry.parent || source.Depth != ancestry.depth {
				return fmt.Errorf("%w: reachable source state or ancestry %q", ErrCorrupt, key)
			}
			for _, page := range membershipSeconds(sourcePages, key) {
				effectivePages[page] = true
			}
		} else if source.State != StateDisabled {
			return fmt.Errorf("%w: unreachable source is schedulable %q", ErrCorrupt, key)
		}
	}
	for key, entry := range allEntries {
		owned := entry.Explicit || effectivePages[key] || hasActiveLinkParent(tx, key)
		if !owned && entry.State != StateDisabled || owned && entry.State == StateDisabled && entry.LastReason == reasonPageOwnershipRemoved {
			return fmt.Errorf("%w: page ownership state mismatch %q", ErrCorrupt, key)
		}
	}
	return nil
}
