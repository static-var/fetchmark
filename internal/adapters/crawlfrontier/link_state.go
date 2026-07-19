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

const (
	hardMaxLinksPerPage       = 64
	reasonLinkPolicyChanged   = "link expansion policy changed"
	reasonLinkOwnershipAdded  = "link ownership added"
	reasonLinkSnapshotApplied = "link snapshot applied"
)

var (
	pageLinksBucket    = []byte("page_links")
	linkParentsBucket  = []byte("link_parents")
	linkPoliciesBucket = []byte("link_policies")
)

var ErrMaxLinkEdges = errors.New("crawl frontier: maximum link edges exceeded")

// LinkPolicy is the persisted, per-job trust decision that permits directly
// owned pages to publish one-hop outbound-link snapshots. A zero limit disables
// expansion for that job when synchronizing policies.
type LinkPolicy struct {
	JobID           string `json:"job_id"`
	MaxLinksPerPage int    `json:"max_links_per_page"`
}

// LinkCompletion atomically completes one admitted success or authoritative
// rejection. Success exactly replaces Links; rejection clears the snapshot.
// Retry and failed paths must use Complete, which preserves the last snapshot.
type LinkCompletion struct {
	Key             string
	LeaseGeneration uint64
	Outcome         State
	NextEligible    time.Time
	Reason          string
	Links           []Seed
}

// LinkMutationCounts reports committed logical graph changes. AppliedEdges is
// the complete post-commit snapshot size, not merely newly inserted edges.
type LinkMutationCounts struct {
	Reported     int
	AppliedEdges int
	AddedEdges   int
	RemovedEdges int
	Created      int
	Reactivated  int
}

// SyncLinkPolicies atomically replaces the complete per-job policy set.
// Omitted and explicitly disabled jobs have their snapshots cleared. Any
// policy transition invalidates leases and immediately queues directly owned
// pages so new trust/configuration is applied without stale work completing.
func (f *Frontier) SyncLinkPolicies(policies []LinkPolicy, now time.Time) error {
	if f == nil {
		return ErrClosed
	}
	if now.IsZero() {
		return ErrInvalidOptions
	}
	if len(policies) > f.maxEntries && len(policies)-f.maxEntries > f.maxSources {
		return ErrMaxEntries
	}
	normalized := make(map[string]LinkPolicy, len(policies))
	for _, policy := range policies {
		if strings.TrimSpace(policy.JobID) == "" || len(policy.JobID) > maxJobIDBytes || policy.MaxLinksPerPage < 0 || policy.MaxLinksPerPage > f.maxLinksPerPage || policy.MaxLinksPerPage > hardMaxLinksPerPage {
			return ErrInvalidOptions
		}
		if _, duplicate := normalized[policy.JobID]; duplicate {
			return ErrInvalidOptions
		}
		// Disabled policies remain in this input snapshot for duplicate and
		// transition detection, but are not persisted as enabled policies.
		normalized[policy.JobID] = policy
	}
	now = now.UTC()
	db, release, err := f.acquire()
	if err != nil {
		return err
	}
	defer release()
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(linkPoliciesBucket)
		entries := tx.Bucket(entriesBucket)
		if bucket == nil || entries == nil {
			return ErrSchemaMismatch
		}
		current := make(map[string]LinkPolicy)
		if err := bucket.ForEach(func(key, raw []byte) error {
			policy, err := decodeLinkPolicy(key, raw)
			if err == nil {
				current[policy.JobID] = policy
			}
			return err
		}); err != nil {
			return err
		}
		changed := make(map[string]bool)
		for jobID, old := range current {
			next, found := normalized[jobID]
			if !found || next.MaxLinksPerPage == 0 || next.MaxLinksPerPage != old.MaxLinksPerPage {
				changed[jobID] = true
			}
		}
		for jobID, next := range normalized {
			old, found := current[jobID]
			if next.MaxLinksPerPage > 0 && (!found || old.MaxLinksPerPage != next.MaxLinksPerPage) {
				changed[jobID] = true
			}
		}
		if len(changed) == 0 {
			return nil
		}

		for jobID := range changed {
			if err := removeJobLinkSnapshots(tx, jobID); err != nil {
				return err
			}
		}
		if err := clearBucket(bucket); err != nil {
			return err
		}
		jobs := make([]string, 0, len(normalized))
		for jobID, policy := range normalized {
			if policy.MaxLinksPerPage > 0 {
				jobs = append(jobs, jobID)
			}
		}
		sort.Strings(jobs)
		for _, jobID := range jobs {
			if err := putLinkPolicy(bucket, normalized[jobID]); err != nil {
				return err
			}
		}

		updates := make([]Entry, 0)
		if err := entries.ForEach(func(key, raw []byte) error {
			entry, err := decodeEntry(key, raw)
			if err != nil {
				return err
			}
			if !changed[entry.JobID] || !entryBaseOwned(tx, entry) {
				return nil
			}
			entry.State = StateQueued
			entry.Attempts = 0
			entry.NextEligible = now
			entry.LeaseUntil = time.Time{}
			entry.LastReason = reasonLinkPolicyChanged
			updates = append(updates, entry)
			return nil
		}); err != nil {
			return err
		}
		for _, entry := range updates {
			if err := putEntry(entries, entry); err != nil {
				return err
			}
		}
		return f.reconcileDiscoveryState(tx, now)
	})
}

// CompleteWithLinks atomically verifies the lease and current direct
// ownership, exact-replaces (or clears) the bounded link snapshot, transitions
// the parent, and reconciles all affected child ownership.
func (f *Frontier) CompleteWithLinks(completion LinkCompletion, now time.Time) (LinkMutationCounts, error) {
	var counts LinkMutationCounts
	if f == nil {
		return counts, ErrClosed
	}
	if now.IsZero() || strings.TrimSpace(completion.Key) == "" || len(completion.Key) > maxKeyBytes || completion.LeaseGeneration == 0 || completion.NextEligible.IsZero() || len(completion.Reason) > maxReasonBytes || (completion.Outcome != StateSucceeded && completion.Outcome != StateRejected) {
		return counts, ErrInvalidTransition
	}
	if completion.Outcome == StateRejected && len(completion.Links) != 0 {
		return counts, ErrInvalidTransition
	}
	if len(completion.Links) > f.maxLinksPerPage || len(completion.Links) > hardMaxLinksPerPage {
		return counts, ErrInvalidTransition
	}
	counts.Reported = len(completion.Links)
	normalized := make(map[string]Seed, len(completion.Links))
	for _, link := range completion.Links {
		if err := validateSeed(link); err != nil {
			return LinkMutationCounts{}, err
		}
		if link.Key == completion.Key {
			return LinkMutationCounts{}, ErrInvalidSeed
		}
		if _, duplicate := normalized[link.Key]; duplicate {
			return LinkMutationCounts{}, ErrInvalidSeed
		}
		normalized[link.Key] = link
	}
	now = now.UTC()
	completion.NextEligible = completion.NextEligible.UTC()
	db, release, err := f.acquire()
	if err != nil {
		return LinkMutationCounts{}, err
	}
	defer release()
	err = db.Update(func(tx *bolt.Tx) error {
		entries := tx.Bucket(entriesBucket)
		forward := tx.Bucket(pageLinksBucket)
		if entries == nil || forward == nil {
			return ErrSchemaMismatch
		}
		raw := entries.Get([]byte(completion.Key))
		if raw == nil {
			return ErrNotFound
		}
		parent, err := decodeEntry([]byte(completion.Key), raw)
		if err != nil {
			return err
		}
		if parent.State != StateLeased {
			return ErrInvalidTransition
		}
		if parent.LeaseGeneration != completion.LeaseGeneration {
			return ErrLeaseMismatch
		}
		if !entryBaseOwned(tx, parent) {
			return ErrInvalidTransition
		}
		if completion.Outcome == StateSucceeded {
			policy, enabled, err := linkPolicyForJob(tx, parent.JobID)
			if err != nil {
				return err
			}
			if !enabled || len(normalized) > policy.MaxLinksPerPage || len(normalized) > f.maxLinksPerPage {
				return ErrInvalidTransition
			}
		}

		keys := make([]string, 0, len(normalized))
		newEntries := 0
		for key, link := range normalized {
			if link.JobID != parent.JobID {
				return ErrInvalidSeed
			}
			keys = append(keys, key)
			raw := entries.Get([]byte(key))
			if raw == nil {
				newEntries++
				continue
			}
			entry, err := decodeEntry([]byte(key), raw)
			if err != nil {
				return err
			}
			if entry.JobID != link.JobID || entry.URL != link.URL {
				return fmt.Errorf("%w: link target %q", ErrKeyConflict, key)
			}
		}
		if entries.Stats().KeyN > f.maxEntries || newEntries > f.maxEntries-entries.Stats().KeyN {
			return ErrMaxEntries
		}
		oldChildren := membershipSeconds(forward, parent.Key)
		if forward.Stats().KeyN > f.maxLinkEdges || len(normalized) > f.maxLinkEdges-(forward.Stats().KeyN-len(oldChildren)) {
			return ErrMaxLinkEdges
		}
		sort.Strings(keys)
		oldSet := make(map[string]bool, len(oldChildren))
		for _, child := range oldChildren {
			oldSet[child] = true
		}
		for _, child := range oldChildren {
			if !containsKey(normalized, child) {
				counts.RemovedEdges++
			}
			if err := removeMembership(tx, pageLinksBucket, linkParentsBucket, parent.Key, child); err != nil {
				return err
			}
		}
		for _, key := range keys {
			link := normalized[key]
			raw := entries.Get([]byte(key))
			if raw == nil {
				entry := Entry{Key: link.Key, JobID: link.JobID, URL: link.URL, State: StateQueued, NextEligible: now, LastReason: reasonLinkOwnershipAdded}
				if err := putEntry(entries, entry); err != nil {
					return err
				}
				counts.Created++
			} else {
				entry, err := decodeEntry([]byte(key), raw)
				if err != nil {
					return err
				}
				if entry.State == StateDisabled && !entryBaseOwned(tx, entry) && !hasActiveLinkParent(tx, entry.Key) {
					counts.Reactivated++
				}
			}
			if !oldSet[key] {
				counts.AddedEdges++
			}
			if err := addMembership(tx, pageLinksBucket, linkParentsBucket, parent.Key, key); err != nil {
				return err
			}
		}
		counts.AppliedEdges = len(keys)
		if err := removeLeaseIndex(tx, parent); err != nil {
			return err
		}
		parent.State = completion.Outcome
		parent.LeaseUntil = time.Time{}
		parent.NextEligible = completion.NextEligible
		parent.LastReason = completion.Reason
		if parent.LastReason == "" {
			parent.LastReason = reasonLinkSnapshotApplied
		}
		parent.Attempts = 0
		if err := putEntry(entries, parent); err != nil {
			return err
		}
		return f.reconcileDiscoveryState(tx, now)
	})
	if err != nil {
		return LinkMutationCounts{}, err
	}
	return counts, nil
}

func canEntryExpand(tx *bolt.Tx, entry Entry) bool {
	if !entryBaseOwned(tx, entry) {
		return false
	}
	_, enabled, err := linkPolicyForJob(tx, entry.JobID)
	return err == nil && enabled
}

func entryBaseOwned(tx *bolt.Tx, entry Entry) bool {
	if entry.Explicit {
		return true
	}
	reverse := tx.Bucket(pageSourcesBucket)
	sources := tx.Bucket(sourcesBucket)
	if reverse == nil || sources == nil {
		return false
	}
	for _, sourceKey := range membershipSeconds(reverse, entry.Key) {
		raw := sources.Get([]byte(sourceKey))
		if raw == nil {
			continue
		}
		source, err := decodeDiscoverySource([]byte(sourceKey), raw)
		if err == nil && source.State != StateDisabled {
			return true
		}
	}
	return false
}

func hasActiveLinkParent(tx *bolt.Tx, childKey string) bool {
	reverse := tx.Bucket(linkParentsBucket)
	entries := tx.Bucket(entriesBucket)
	if reverse == nil || entries == nil {
		return false
	}
	for _, parentKey := range membershipSeconds(reverse, childKey) {
		raw := entries.Get([]byte(parentKey))
		if raw == nil {
			continue
		}
		parent, err := decodeEntry([]byte(parentKey), raw)
		if err == nil && entryBaseOwned(tx, parent) {
			return true
		}
	}
	return false
}

func hasAnyLinkRelationship(tx *bolt.Tx, key string) bool {
	forward := tx.Bucket(pageLinksBucket)
	reverse := tx.Bucket(linkParentsBucket)
	return forward == nil || reverse == nil || len(membershipSeconds(forward, key)) > 0 || len(membershipSeconds(reverse, key)) > 0
}

func linkPolicyForJob(tx *bolt.Tx, jobID string) (LinkPolicy, bool, error) {
	bucket := tx.Bucket(linkPoliciesBucket)
	if bucket == nil {
		return LinkPolicy{}, false, ErrSchemaMismatch
	}
	raw := bucket.Get([]byte(jobID))
	if raw == nil {
		return LinkPolicy{}, false, nil
	}
	policy, err := decodeLinkPolicy([]byte(jobID), raw)
	return policy, err == nil, err
}

func removeJobLinkSnapshots(tx *bolt.Tx, jobID string) error {
	entries := tx.Bucket(entriesBucket)
	forward := tx.Bucket(pageLinksBucket)
	if entries == nil || forward == nil {
		return ErrSchemaMismatch
	}
	parents := make([]string, 0)
	if err := forward.ForEach(func(key, _ []byte) error {
		parentKey, _, err := decodeMembershipKey(key)
		if err != nil {
			return err
		}
		raw := entries.Get([]byte(parentKey))
		if raw == nil {
			return fmt.Errorf("%w: link parent missing %q", ErrCorrupt, parentKey)
		}
		entry, err := decodeEntry([]byte(parentKey), raw)
		if err != nil {
			return err
		}
		if entry.JobID == jobID && (len(parents) == 0 || parents[len(parents)-1] != parentKey) {
			parents = append(parents, parentKey)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, parent := range parents {
		for _, child := range membershipSeconds(forward, parent) {
			if err := removeMembership(tx, pageLinksBucket, linkParentsBucket, parent, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func putLinkPolicy(bucket *bolt.Bucket, policy LinkPolicy) error {
	raw, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(policy.JobID), raw)
}

func decodeLinkPolicy(key, raw []byte) (LinkPolicy, error) {
	var policy LinkPolicy
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return policy, fmt.Errorf("%w: decode link policy %q: %v", ErrCorrupt, key, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return policy, fmt.Errorf("%w: trailing link policy data %q", ErrCorrupt, key)
	}
	if policy.JobID != string(key) || strings.TrimSpace(policy.JobID) == "" || len(policy.JobID) > maxJobIDBytes || policy.MaxLinksPerPage < 1 || policy.MaxLinksPerPage > hardMaxLinksPerPage {
		return policy, fmt.Errorf("%w: invalid link policy %q", ErrCorrupt, key)
	}
	return policy, nil
}

func migrateLinkStateV5ToV6(tx *bolt.Tx) error {
	for _, bucket := range [][]byte{pageLinksBucket, linkParentsBucket, linkPoliciesBucket} {
		if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
			return fmt.Errorf("%w: migrate link state: %v", ErrCorrupt, err)
		}
	}
	meta := tx.Bucket(metaBucket)
	if meta == nil {
		return ErrSchemaMismatch
	}
	version := make([]byte, 8)
	binary.BigEndian.PutUint64(version, schemaVersion)
	if err := meta.Put(schemaKey, version); err != nil {
		return fmt.Errorf("%w: migrate schema marker: %v", ErrCorrupt, err)
	}
	return nil
}

func validateLinkState(tx *bolt.Tx, f *Frontier, enforceOwnership bool) error {
	entries := tx.Bucket(entriesBucket)
	forward := tx.Bucket(pageLinksBucket)
	reverse := tx.Bucket(linkParentsBucket)
	policies := tx.Bucket(linkPoliciesBucket)
	if entries == nil || forward == nil || reverse == nil || policies == nil {
		return ErrSchemaMismatch
	}
	if policies.Stats().KeyN > f.maxEntries && policies.Stats().KeyN-f.maxEntries > f.maxSources {
		return ErrMaxEntries
	}
	policyByJob := make(map[string]LinkPolicy)
	if err := policies.ForEach(func(key, raw []byte) error {
		policy, err := decodeLinkPolicy(key, raw)
		if err != nil {
			return err
		}
		if policy.MaxLinksPerPage > f.maxLinksPerPage {
			return ErrMaxEntries
		}
		policyByJob[policy.JobID] = policy
		return nil
	}); err != nil {
		return err
	}
	perParent := make(map[string]int)
	if err := forward.ForEach(func(key, marker []byte) error {
		if !bytes.Equal(marker, indexMarker) {
			return fmt.Errorf("%w: invalid page-link marker", ErrCorrupt)
		}
		parentKey, childKey, err := decodeMembershipKey(key)
		if err != nil {
			return err
		}
		parentRaw := entries.Get([]byte(parentKey))
		childRaw := entries.Get([]byte(childKey))
		if parentRaw == nil || childRaw == nil || parentKey == childKey {
			return fmt.Errorf("%w: invalid page-link edge %q -> %q", ErrCorrupt, parentKey, childKey)
		}
		parent, err := decodeEntry([]byte(parentKey), parentRaw)
		if err != nil {
			return err
		}
		child, err := decodeEntry([]byte(childKey), childRaw)
		if err != nil {
			return err
		}
		policy, enabled := policyByJob[parent.JobID]
		if parent.JobID != child.JobID || !enabled || !bytes.Equal(reverse.Get(membershipKey(childKey, parentKey)), indexMarker) {
			return fmt.Errorf("%w: invalid page-link ownership %q -> %q", ErrCorrupt, parentKey, childKey)
		}
		perParent[parentKey]++
		if perParent[parentKey] > policy.MaxLinksPerPage || perParent[parentKey] > f.maxLinksPerPage {
			return ErrMaxLinkEdges
		}
		return nil
	}); err != nil {
		return err
	}
	if forward.Stats().KeyN > f.maxLinkEdges {
		return ErrMaxLinkEdges
	}
	if reverse.Stats().KeyN != forward.Stats().KeyN {
		return fmt.Errorf("%w: page-link reverse cardinality", ErrCorrupt)
	}
	if err := reverse.ForEach(func(key, marker []byte) error {
		if !bytes.Equal(marker, indexMarker) {
			return fmt.Errorf("%w: invalid link-parent marker", ErrCorrupt)
		}
		childKey, parentKey, err := decodeMembershipKey(key)
		if err != nil || !bytes.Equal(forward.Get(membershipKey(parentKey, childKey)), indexMarker) {
			return fmt.Errorf("%w: unmatched link-parent membership", ErrCorrupt)
		}
		return nil
	}); err != nil {
		return err
	}
	if !enforceOwnership {
		return nil
	}
	return entries.ForEach(func(key, raw []byte) error {
		entry, err := decodeEntry(key, raw)
		if err != nil {
			return err
		}
		owned := entryBaseOwned(tx, entry) || hasActiveLinkParent(tx, entry.Key)
		if !owned && entry.State != StateDisabled || owned && entry.State == StateDisabled && entry.LastReason == reasonPageOwnershipRemoved {
			return fmt.Errorf("%w: link ownership state mismatch %q", ErrCorrupt, entry.Key)
		}
		return nil
	})
}

func containsKey(values map[string]Seed, key string) bool {
	_, found := values[key]
	return found
}
