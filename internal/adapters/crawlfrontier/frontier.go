// Package crawlfrontier provides the durable, bounded work frontier used by
// the optional Fetchmark crawler. It owns scheduling state only; URL policy,
// robots decisions, fetching, and admission remain outside this adapter.
package crawlfrontier

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	schemaVersion         uint64 = 6
	previousSchemaVersion uint64 = 5

	defaultLockTimeout = 50 * time.Millisecond
	privateFileMode    = os.FileMode(0o600)

	maxKeyBytes    = 512
	maxJobIDBytes  = 512
	maxURLBytes    = 16 << 10
	maxReasonBytes = 4 << 10
)

var (
	metaBucket          = []byte("meta")
	entriesBucket       = []byte("entries")
	hostsBucket         = []byte("hosts")
	authorityRefsBucket = []byte("authority_refs")
	schemaKey           = []byte("schema_version")
	leaseSeqKey         = []byte("lease_sequence")
)

var (
	ErrInvalidOptions    = errors.New("crawl frontier: invalid options")
	ErrInvalidSeed       = errors.New("crawl frontier: invalid seed")
	ErrKeyConflict       = errors.New("crawl frontier: key conflicts with persisted seed")
	ErrMaxEntries        = errors.New("crawl frontier: maximum entries exceeded")
	ErrOwned             = errors.New("crawl frontier: database is already owned")
	ErrUnsafePath        = errors.New("crawl frontier: unsafe database path")
	ErrSchemaMismatch    = errors.New("crawl frontier: schema mismatch")
	ErrCorrupt           = errors.New("crawl frontier: corrupt database")
	ErrClosed            = errors.New("crawl frontier: closed")
	ErrNotFound          = errors.New("crawl frontier: entry not found")
	ErrLeaseMismatch     = errors.New("crawl frontier: lease generation mismatch")
	ErrInvalidTransition = errors.New("crawl frontier: invalid state transition")
)

const ReasonLeaseExpired = "lease expired"

// State is the persisted scheduling state of one frontier entry.
type State string

const (
	StateQueued    State = "queued"
	StateLeased    State = "leased"
	StateSucceeded State = "succeeded"
	StateRejected  State = "rejected"
	StateRetry     State = "retry"
	StateDisabled  State = "disabled"
)

// Entry is a durable scheduling record. Attempts is incremented whenever the
// entry is leased, including after an expired lease is recovered.
type Entry struct {
	Key          string    `json:"key"`
	JobID        string    `json:"job_id"`
	URL          string    `json:"url"`
	State        State     `json:"state"`
	Attempts     int       `json:"attempts"`
	NextEligible time.Time `json:"next_eligible,omitempty"`
	LeaseUntil   time.Time `json:"lease_until,omitempty"`
	// LeaseGeneration monotonically identifies the current lease and prevents
	// a stale worker from completing work re-leased to another worker.
	LeaseGeneration uint64 `json:"lease_generation,omitempty"`
	LastReason      string `json:"last_reason,omitempty"`
	Explicit        bool   `json:"explicit,omitempty"`
	// CanExpand is derived at lease time from current base ownership and the
	// persisted job policy. It is intentionally never persisted.
	CanExpand bool `json:"-"`
}

// Seed is the caller-canonicalized identity synchronized into the frontier.
// A key must always identify the same job and URL.
type Seed struct {
	Key   string
	JobID string
	URL   string
}

type Options struct {
	MaxEntries           int
	MaxSources           int
	MaxPagesPerSource    int
	MaxChildrenPerSource int
	// MaxMemberships bounds source-to-page ownership records globally.
	MaxMemberships int
	// MaxSourceEdges separately bounds source-to-child graph edges globally.
	MaxSourceEdges int
	// MaxLinkEdges separately bounds page-to-page link ownership edges globally.
	MaxLinkEdges int
	// MaxLinksPerPage is the installation-wide hard ceiling for each job policy.
	// It may not exceed 64.
	MaxLinksPerPage int
	MaxSourceDepth  int
	FileMode        os.FileMode
	LockTimeout     time.Duration
}

// StateCounts is a point-in-time count of persisted records by state.
type StateCounts struct {
	Total     int
	Queued    int
	Leased    int
	Succeeded int
	Rejected  int
	Retry     int
	Disabled  int
}

// Frontier owns one exclusively locked bbolt database.
type Frontier struct {
	db                   *bolt.DB
	maxEntries           int
	maxSources           int
	maxPagesPerSource    int
	maxChildrenPerSource int
	maxMemberships       int
	maxSourceEdges       int
	maxLinkEdges         int
	maxLinksPerPage      int
	maxSourceDepth       int
	lifecycle            sync.RWMutex
	closed               bool
}

func Open(path string, options Options) (*Frontier, error) {
	if strings.TrimSpace(path) == "" || options.MaxEntries <= 0 || options.LockTimeout < 0 {
		return nil, ErrInvalidOptions
	}
	if options.MaxSources == 0 {
		options.MaxSources = options.MaxEntries
	}
	if options.MaxPagesPerSource == 0 {
		options.MaxPagesPerSource = options.MaxEntries
	}
	if options.MaxChildrenPerSource == 0 {
		options.MaxChildrenPerSource = options.MaxEntries
	}
	if options.MaxMemberships == 0 {
		options.MaxMemberships = options.MaxEntries
	}
	if options.MaxSourceEdges == 0 {
		options.MaxSourceEdges = options.MaxEntries
	}
	if options.MaxLinkEdges == 0 {
		options.MaxLinkEdges = options.MaxEntries
	}
	if options.MaxLinksPerPage == 0 {
		options.MaxLinksPerPage = hardMaxLinksPerPage
	}
	if options.MaxSourceDepth == 0 {
		options.MaxSourceDepth = 16
	}
	if options.MaxSources < 0 || options.MaxPagesPerSource < 0 || options.MaxChildrenPerSource < 0 || options.MaxMemberships < 0 || options.MaxSourceEdges < 0 || options.MaxLinkEdges < 0 || options.MaxLinksPerPage < 1 || options.MaxLinksPerPage > hardMaxLinksPerPage || options.MaxSourceDepth < 1 || options.MaxSourceDepth > 256 {
		return nil, ErrInvalidOptions
	}
	mode := options.FileMode
	if mode == 0 {
		mode = privateFileMode
	}
	if mode.Perm() != privateFileMode {
		return nil, fmt.Errorf("%w: file mode must be 0600", ErrInvalidOptions)
	}
	lockTimeout := options.LockTimeout
	if lockTimeout == 0 {
		lockTimeout = defaultLockTimeout
	}

	parentPath := filepath.Dir(path)
	parentInfo, err := os.Lstat(parentPath)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 || parentInfo.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("%w: direct parent must be a trusted non-writable directory", ErrUnsafePath)
	}

	preInfo, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: inspect path: %v", ErrUnsafePath, err)
	}
	if err == nil && !preInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: database must be a regular file", ErrUnsafePath)
	}
	newDatabase := errors.Is(err, os.ErrNotExist) || (err == nil && preInfo.Size() == 0)

	db, err := bolt.Open(path, mode, &bolt.Options{Timeout: lockTimeout, OpenFile: openNoFollow})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf("%w: %v", ErrOwned, err)
		}
		return nil, fmt.Errorf("%w: open: %v", ErrCorrupt, err)
	}
	fail := func(openErr error) (*Frontier, error) {
		_ = db.Close()
		return nil, openErr
	}

	postInfo, err := os.Lstat(path)
	if err != nil || !postInfo.Mode().IsRegular() {
		return fail(fmt.Errorf("%w: database changed during open", ErrUnsafePath))
	}
	if preInfo != nil && !os.SameFile(preInfo, postInfo) {
		return fail(fmt.Errorf("%w: database changed during open", ErrUnsafePath))
	}
	postParentInfo, err := os.Lstat(parentPath)
	if err != nil || !postParentInfo.IsDir() || !os.SameFile(parentInfo, postParentInfo) || postParentInfo.Mode().Perm()&0o022 != 0 {
		return fail(fmt.Errorf("%w: direct parent changed during open", ErrUnsafePath))
	}
	if err := os.Chmod(path, privateFileMode); err != nil {
		return fail(fmt.Errorf("%w: enforce private permissions: %v", ErrUnsafePath, err))
	}

	f := &Frontier{
		db: db, maxEntries: options.MaxEntries, maxSources: options.MaxSources,
		maxPagesPerSource: options.MaxPagesPerSource, maxChildrenPerSource: options.MaxChildrenPerSource,
		maxMemberships: options.MaxMemberships, maxSourceEdges: options.MaxSourceEdges,
		maxLinkEdges: options.MaxLinkEdges, maxLinksPerPage: options.MaxLinksPerPage, maxSourceDepth: options.MaxSourceDepth,
	}
	if err := f.initializeOrValidate(newDatabase, time.Now().UTC()); err != nil {
		return fail(err)
	}
	return f, nil
}

// SyncSeeds atomically applies a complete seed snapshot. New seeds are queued,
// missing seeds are disabled without deletion, and re-added disabled seeds are
// queued again. Terminal and active states of seeds still present are retained.
func (f *Frontier) SyncSeeds(seeds []Seed, now time.Time) error {
	db, release, err := f.acquire()
	if err != nil {
		return err
	}
	defer release()
	now = now.UTC()
	if len(seeds) > f.maxEntries {
		return ErrMaxEntries
	}
	normalized := make(map[string]Seed, len(seeds))
	for _, seed := range seeds {
		if err := validateSeed(seed); err != nil {
			return err
		}
		if _, exists := normalized[seed.Key]; exists {
			return fmt.Errorf("%w: duplicate key %q", ErrInvalidSeed, seed.Key)
		}
		normalized[seed.Key] = seed
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := requiredEntriesBucket(tx)
		if err != nil {
			return err
		}
		newCount := 0
		for key, seed := range normalized {
			raw := bucket.Get([]byte(key))
			if raw == nil {
				newCount++
				continue
			}
			entry, err := decodeEntry([]byte(key), raw)
			if err != nil {
				return err
			}
			if entry.JobID != seed.JobID || entry.URL != seed.URL {
				return fmt.Errorf("%w: %q", ErrKeyConflict, key)
			}
		}
		if bucket.Stats().KeyN+newCount > f.maxEntries {
			return ErrMaxEntries
		}

		for key, seed := range normalized {
			raw := bucket.Get([]byte(key))
			if raw == nil {
				entry := Entry{Key: seed.Key, JobID: seed.JobID, URL: seed.URL, State: StateQueued, NextEligible: now, Explicit: true}
				if err := putEntry(bucket, entry); err != nil {
					return err
				}
				continue
			}
			entry, err := decodeEntry([]byte(key), raw)
			if err != nil {
				return err
			}
			changed := !entry.Explicit
			entry.Explicit = true
			if entry.State == StateDisabled {
				entry.State = StateQueued
				entry.NextEligible = now
				entry.LeaseUntil = time.Time{}
				entry.LastReason = ""
				changed = true
			}
			if changed {
				if err := putEntry(bucket, entry); err != nil {
					return err
				}
			}
		}

		removed := make([]Entry, 0)
		if err := bucket.ForEach(func(key, raw []byte) error {
			if _, present := normalized[string(key)]; present {
				return nil
			}
			entry, err := decodeEntry(key, raw)
			if err != nil {
				return err
			}
			if !entry.Explicit {
				return nil
			}
			entry.Explicit = false
			if !hasPageMembership(tx, entry.Key) {
				entry.State = StateDisabled
				entry.NextEligible = time.Time{}
				entry.LeaseUntil = time.Time{}
				entry.LastReason = reasonPageOwnershipRemoved
			}
			removed = append(removed, entry)
			return nil
		}); err != nil {
			return err
		}
		for _, entry := range removed {
			if err := putEntry(bucket, entry); err != nil {
				return err
			}
		}
		return f.reconcileDiscoveryState(tx, now)
	})
}

// LeaseDue atomically leases due entries across all jobs.
func (f *Frontier) LeaseDue(now time.Time, limit int, leaseTTL time.Duration) ([]Entry, error) {
	return f.leaseDue("", now, limit, leaseTTL)
}

// LeaseDueJob atomically leases due entries for one job. Ordering is by next
// eligibility and then canonical key, making repeated runs deterministic.
func (f *Frontier) LeaseDueJob(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]Entry, error) {
	if strings.TrimSpace(jobID) == "" || len(jobID) > maxJobIDBytes {
		return nil, fmt.Errorf("%w: invalid job ID", ErrInvalidOptions)
	}
	return f.leaseDue(jobID, now, limit, leaseTTL)
}

func (f *Frontier) leaseDue(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]Entry, error) {
	if limit <= 0 || leaseTTL <= 0 {
		return nil, ErrInvalidOptions
	}
	db, release, err := f.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	if limit > f.maxEntries {
		limit = f.maxEntries
	}
	now = now.UTC()
	var leased []Entry
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := requiredEntriesBucket(tx)
		if err != nil {
			return err
		}
		if err := recoverExpiredLeasesIndexed(tx, now); err != nil {
			return err
		}
		candidates, err := dueEntriesIndexed(tx, jobID, now, limit)
		if err != nil {
			return err
		}
		meta := tx.Bucket(metaBucket)
		if meta == nil {
			return ErrSchemaMismatch
		}
		sequenceRaw := meta.Get(leaseSeqKey)
		if len(sequenceRaw) != 8 {
			return ErrSchemaMismatch
		}
		sequence := binary.BigEndian.Uint64(sequenceRaw)
		leased = make([]Entry, 0, len(candidates))
		for _, candidate := range candidates {
			raw := bucket.Get([]byte(candidate.entryKey))
			if raw == nil {
				return fmt.Errorf("%w: due index entry missing %q", ErrCorrupt, candidate.entryKey)
			}
			entry, err := decodeEntry([]byte(candidate.entryKey), raw)
			if err != nil {
				return err
			}
			if !isSchedulable(entry.State) || entry.NextEligible.After(now) || (jobID != "" && entry.JobID != jobID) {
				return fmt.Errorf("%w: due index disagrees with %q", ErrCorrupt, entry.Key)
			}
			expectedIndexKey := dueGlobalKey(entry)
			if jobID != "" {
				expectedIndexKey = dueByJobKey(entry)
			}
			if !bytes.Equal(expectedIndexKey, candidate.indexKey) {
				return fmt.Errorf("%w: due index key disagrees with %q", ErrCorrupt, entry.Key)
			}
			if sequence == ^uint64(0) {
				return fmt.Errorf("%w: lease generation exhausted", ErrCorrupt)
			}
			sequence++
			if err := removeDueIndexes(tx, entry); err != nil {
				return err
			}
			entry.State = StateLeased
			entry.Attempts++
			entry.LeaseGeneration = sequence
			entry.LeaseUntil = now.Add(leaseTTL)
			entry.CanExpand = canEntryExpand(tx, entry)
			if err := putEntry(bucket, entry); err != nil {
				return err
			}
			if err := addLeaseIndex(tx, entry); err != nil {
				return err
			}
			leased = append(leased, entry)
		}
		if len(candidates) > 0 {
			nextSequenceRaw := make([]byte, 8)
			binary.BigEndian.PutUint64(nextSequenceRaw, sequence)
			if err := meta.Put(leaseSeqKey, nextSequenceRaw); err != nil {
				return fmt.Errorf("%w: persist lease sequence: %v", ErrCorrupt, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return leased, nil
}

// Complete atomically transitions a leased entry to a refreshable success,
// rejection, retry, or disabled outcome. Every refreshable outcome requires a
// non-zero next-eligible time. Success and rejection begin a fresh attempt
// series; retry preserves the current attempt count.
func (f *Frontier) Complete(key string, leaseGeneration uint64, outcome State, nextEligible time.Time, reason string) error {
	if strings.TrimSpace(key) == "" || len(key) > maxKeyBytes || leaseGeneration == 0 || len(reason) > maxReasonBytes {
		return ErrInvalidTransition
	}
	if outcome != StateSucceeded && outcome != StateRejected && outcome != StateRetry && outcome != StateDisabled {
		return ErrInvalidTransition
	}
	if outcome != StateDisabled && nextEligible.IsZero() {
		return fmt.Errorf("%w: outcome requires next eligibility", ErrInvalidTransition)
	}
	nextEligible = nextEligible.UTC()
	db, release, err := f.acquire()
	if err != nil {
		return err
	}
	defer release()
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := requiredEntriesBucket(tx)
		if err != nil {
			return err
		}
		raw := bucket.Get([]byte(key))
		if raw == nil {
			return ErrNotFound
		}
		entry, err := decodeEntry([]byte(key), raw)
		if err != nil {
			return err
		}
		if entry.State != StateLeased {
			return ErrInvalidTransition
		}
		if entry.LeaseGeneration != leaseGeneration {
			return ErrLeaseMismatch
		}
		if err := removeLeaseIndex(tx, entry); err != nil {
			return err
		}
		entry.State = outcome
		entry.LeaseUntil = time.Time{}
		entry.LastReason = reason
		if outcome != StateDisabled {
			entry.NextEligible = nextEligible
		} else {
			entry.NextEligible = time.Time{}
		}
		if outcome == StateSucceeded || outcome == StateRejected {
			entry.Attempts = 0
		}
		if err := putEntry(bucket, entry); err != nil {
			return err
		}
		if isSchedulable(entry.State) {
			return addDueIndexes(tx, entry)
		}
		return nil
	})
}

func (f *Frontier) Counts() (StateCounts, error) {
	db, release, err := f.acquire()
	if err != nil {
		return StateCounts{}, err
	}
	defer release()
	var counts StateCounts
	err = db.View(func(tx *bolt.Tx) error {
		bucket, err := requiredEntriesBucket(tx)
		if err != nil {
			return err
		}
		return bucket.ForEach(func(key, raw []byte) error {
			entry, err := decodeEntry(key, raw)
			if err != nil {
				return err
			}
			counts.Total++
			switch entry.State {
			case StateQueued:
				counts.Queued++
			case StateLeased:
				counts.Leased++
			case StateSucceeded:
				counts.Succeeded++
			case StateRejected:
				counts.Rejected++
			case StateRetry:
				counts.Retry++
			case StateDisabled:
				counts.Disabled++
			}
			return nil
		})
	})
	return counts, err
}

// PruneDisabled explicitly deletes up to limit disabled identities in
// lexicographic key order. It is the only capacity-recovery operation; normal
// seed synchronization never evicts records silently.
func (f *Frontier) PruneDisabled(limit int) (int, error) {
	if limit <= 0 {
		return 0, ErrInvalidOptions
	}
	db, release, err := f.acquire()
	if err != nil {
		return 0, err
	}
	defer release()
	pruned := 0
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := requiredEntriesBucket(tx)
		if err != nil {
			return err
		}
		capacity := limit
		if capacity > f.maxEntries {
			capacity = f.maxEntries
		}
		keys := make([][]byte, 0, capacity)
		if err := bucket.ForEach(func(key, raw []byte) error {
			entry, err := decodeEntry(key, raw)
			if err != nil {
				return err
			}
			if entry.State == StateDisabled && !hasPageMembership(tx, entry.Key) && !hasAnyLinkRelationship(tx, entry.Key) && len(keys) < limit {
				keys = append(keys, append([]byte(nil), key...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, key := range keys {
			if err := bucket.Delete(key); err != nil {
				return fmt.Errorf("crawl frontier: prune disabled entry: %w", err)
			}
		}
		pruned = len(keys)
		if err := pruneOrphanHosts(tx); err != nil {
			return err
		}
		return rebuildSchedulingIndexes(tx)
	})
	if err != nil {
		return 0, err
	}
	return pruned, nil
}

func (f *Frontier) Close() error {
	if f == nil {
		return nil
	}
	f.lifecycle.Lock()
	defer f.lifecycle.Unlock()
	if f.closed || f.db == nil {
		return nil
	}
	f.closed = true
	return f.db.Close()
}

func (f *Frontier) acquire() (*bolt.DB, func(), error) {
	if f == nil {
		return nil, nil, ErrClosed
	}
	f.lifecycle.RLock()
	if f.closed || f.db == nil {
		f.lifecycle.RUnlock()
		return nil, nil, ErrClosed
	}
	return f.db, f.lifecycle.RUnlock, nil
}

func (f *Frontier) initializeOrValidate(newDatabase bool, now time.Time) error {
	return f.db.Update(func(tx *bolt.Tx) error {
		if newDatabase {
			meta, err := tx.CreateBucket(metaBucket)
			if err != nil {
				return fmt.Errorf("%w: create metadata: %v", ErrCorrupt, err)
			}
			version := make([]byte, 8)
			binary.BigEndian.PutUint64(version, schemaVersion)
			if err := meta.Put(schemaKey, version); err != nil {
				return fmt.Errorf("%w: write schema: %v", ErrCorrupt, err)
			}
			if err := meta.Put(leaseSeqKey, make([]byte, 8)); err != nil {
				return fmt.Errorf("%w: write lease sequence: %v", ErrCorrupt, err)
			}
			if _, err := tx.CreateBucket(entriesBucket); err != nil {
				return fmt.Errorf("%w: create entries: %v", ErrCorrupt, err)
			}
			if _, err := tx.CreateBucket(hostsBucket); err != nil {
				return fmt.Errorf("%w: create hosts: %v", ErrCorrupt, err)
			}
			if _, err := tx.CreateBucket(authorityRefsBucket); err != nil {
				return fmt.Errorf("%w: create authority references: %v", ErrCorrupt, err)
			}
			for _, bucket := range [][]byte{dueByJobBucket, dueGlobalBucket, leasesByExpiryBucket} {
				if _, err := tx.CreateBucket(bucket); err != nil {
					return fmt.Errorf("%w: create scheduling index: %v", ErrCorrupt, err)
				}
			}
			for _, bucket := range [][]byte{sourcesBucket, sourcePagesBucket, pageSourcesBucket, sourceChildrenBucket, childParentsBucket, sourceDueByJobBucket, sourceDueGlobalBucket, sourceLeasesByExpiryBucket, pageLinksBucket, linkParentsBucket, linkPoliciesBucket} {
				if _, err := tx.CreateBucket(bucket); err != nil {
					return fmt.Errorf("%w: create discovery state: %v", ErrCorrupt, err)
				}
			}
			return nil
		}

		meta := tx.Bucket(metaBucket)
		entries := tx.Bucket(entriesBucket)
		hosts := tx.Bucket(hostsBucket)
		authorityRefs := tx.Bucket(authorityRefsBucket)
		if meta == nil || entries == nil || hosts == nil || authorityRefs == nil {
			return ErrSchemaMismatch
		}
		version := meta.Get(schemaKey)
		if len(version) != 8 {
			return ErrSchemaMismatch
		}
		persistedVersion := binary.BigEndian.Uint64(version)
		if persistedVersion == previousSchemaVersion {
			if err := migrateLinkStateV5ToV6(tx); err != nil {
				return err
			}
			persistedVersion = schemaVersion
		}
		if persistedVersion != schemaVersion {
			return ErrSchemaMismatch
		}
		sequenceRaw := meta.Get(leaseSeqKey)
		if len(sequenceRaw) != 8 {
			return ErrSchemaMismatch
		}
		sequence := binary.BigEndian.Uint64(sequenceRaw)
		count := 0
		var maxLeaseGeneration uint64
		authorities := make(map[string]uint64)
		if err := entries.ForEach(func(key, raw []byte) error {
			entry, err := decodeEntry(key, raw)
			if err != nil {
				return err
			}
			if entry.LeaseGeneration > maxLeaseGeneration {
				maxLeaseGeneration = entry.LeaseGeneration
			}
			authority, err := canonicalAuthorityForURL(entry.URL)
			if err != nil {
				return fmt.Errorf("%w: invalid entry authority %q", ErrCorrupt, key)
			}
			authorities[authority]++
			count++
			return nil
		}); err != nil {
			return err
		}
		sources := tx.Bucket(sourcesBucket)
		if sources == nil {
			return ErrSchemaMismatch
		}
		if err := sources.ForEach(func(key, raw []byte) error {
			source, err := decodeDiscoverySource(key, raw)
			if err != nil {
				return err
			}
			if source.LeaseGeneration > maxLeaseGeneration {
				maxLeaseGeneration = source.LeaseGeneration
			}
			authority, err := canonicalAuthorityForURL(source.URL)
			if err != nil {
				return fmt.Errorf("%w: invalid source authority %q", ErrCorrupt, key)
			}
			authorities[authority]++
			return nil
		}); err != nil {
			return err
		}
		if count > f.maxEntries {
			return ErrMaxEntries
		}
		if sequence < maxLeaseGeneration {
			return fmt.Errorf("%w: lease sequence precedes persisted entry", ErrCorrupt)
		}
		if err := validateHostStates(hosts, authorities, len(authorities)); err != nil {
			return err
		}
		if err := validateAuthorityReferences(authorityRefs, authorities); err != nil {
			return err
		}
		if err := validateSchedulingIndexes(tx, f.maxEntries); err != nil {
			return err
		}
		if err := validateDiscoveryState(tx, f, false); err != nil {
			return err
		}
		if err := validateSourceSchedule(tx, f.maxSources); err != nil {
			return err
		}
		if err := validateLinkState(tx, f, false); err != nil {
			return err
		}
		if err := f.reconcileDiscoveryState(tx, now); err != nil {
			return err
		}
		if err := validateDiscoveryState(tx, f, true); err != nil {
			return err
		}
		if err := validateLinkState(tx, f, true); err != nil {
			return err
		}
		if err := recoverExpiredLeasesIndexed(tx, now); err != nil {
			return err
		}
		return recoverExpiredSourceLeases(tx, now)
	})
}

func isSchedulable(state State) bool {
	return state == StateQueued || state == StateRetry || state == StateSucceeded || state == StateRejected
}

func validateSeed(seed Seed) error {
	if strings.TrimSpace(seed.Key) == "" || len(seed.Key) > maxKeyBytes || strings.TrimSpace(seed.JobID) == "" || len(seed.JobID) > maxJobIDBytes || len(seed.URL) > maxURLBytes {
		return ErrInvalidSeed
	}
	parsed, err := url.Parse(seed.URL)
	if err != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("%w: URL must be absolute HTTP(S) without credentials", ErrInvalidSeed)
	}
	if _, err := canonicalAuthorityForURL(seed.URL); err != nil {
		return fmt.Errorf("%w: URL authority is not canonicalizable", ErrInvalidSeed)
	}
	return nil
}

func requiredEntriesBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	bucket := tx.Bucket(entriesBucket)
	if bucket == nil {
		return nil, ErrSchemaMismatch
	}
	return bucket, nil
}

func putEntry(bucket *bolt.Bucket, entry Entry) error {
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("%w: encode entry: %v", ErrCorrupt, err)
	}
	if err := bucket.Put([]byte(entry.Key), raw); err != nil {
		return fmt.Errorf("crawl frontier: store entry: %w", err)
	}
	return nil
}

func decodeEntry(key, raw []byte) (Entry, error) {
	var entry Entry
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entry); err != nil {
		return Entry{}, fmt.Errorf("%w: decode entry %q: %v", ErrCorrupt, key, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Entry{}, fmt.Errorf("%w: trailing entry data %q", ErrCorrupt, key)
	}
	if entry.Key != string(key) || entry.Attempts < 0 || len(entry.LastReason) > maxReasonBytes {
		return Entry{}, fmt.Errorf("%w: invalid entry %q", ErrCorrupt, key)
	}
	if err := validateSeed(Seed{Key: entry.Key, JobID: entry.JobID, URL: entry.URL}); err != nil {
		return Entry{}, fmt.Errorf("%w: invalid entry %q", ErrCorrupt, key)
	}
	switch entry.State {
	case StateQueued, StateSucceeded, StateRejected, StateRetry:
		if !entry.LeaseUntil.IsZero() {
			return Entry{}, fmt.Errorf("%w: non-leased entry has lease %q", ErrCorrupt, key)
		}
		if entry.NextEligible.IsZero() {
			return Entry{}, fmt.Errorf("%w: schedulable entry has no next eligibility %q", ErrCorrupt, key)
		}
	case StateDisabled:
		if !entry.LeaseUntil.IsZero() || !entry.NextEligible.IsZero() {
			return Entry{}, fmt.Errorf("%w: disabled entry has scheduling time %q", ErrCorrupt, key)
		}
	case StateLeased:
		if entry.LeaseUntil.IsZero() || entry.LeaseGeneration == 0 {
			return Entry{}, fmt.Errorf("%w: leased entry has no expiry %q", ErrCorrupt, key)
		}
	default:
		return Entry{}, fmt.Errorf("%w: invalid state %q", ErrCorrupt, entry.State)
	}
	return entry, nil
}
