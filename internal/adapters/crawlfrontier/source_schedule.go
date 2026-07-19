package crawlfrontier

import (
	"bytes"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	sourceDueByJobBucket       = []byte("source_due_by_job")
	sourceDueGlobalBucket      = []byte("source_due_global")
	sourceLeasesByExpiryBucket = []byte("source_leases_by_expiry")
)

func sourceDueByJobKey(source DiscoverySourceEntry) []byte {
	key := orderedString(nil, source.JobID)
	key = appendScheduleTime(key, source.NextEligible)
	return orderedString(key, source.Key)
}

func sourceDueGlobalKey(source DiscoverySourceEntry) []byte {
	key := appendScheduleTime(nil, source.NextEligible)
	return orderedString(key, source.Key)
}

func sourceLeaseExpiryKey(source DiscoverySourceEntry) []byte {
	key := appendScheduleTime(nil, source.LeaseUntil)
	return orderedString(key, source.Key)
}

func sourceSchedulable(state State) bool {
	return state == StateQueued || state == StateRetry || state == StateSucceeded
}

func requiredSourceScheduleBuckets(tx *bolt.Tx) (*bolt.Bucket, *bolt.Bucket, *bolt.Bucket, error) {
	byJob := tx.Bucket(sourceDueByJobBucket)
	global := tx.Bucket(sourceDueGlobalBucket)
	leases := tx.Bucket(sourceLeasesByExpiryBucket)
	if byJob == nil || global == nil || leases == nil {
		return nil, nil, nil, ErrSchemaMismatch
	}
	return byJob, global, leases, nil
}

func addSourceDue(tx *bolt.Tx, source DiscoverySourceEntry) error {
	byJob, global, _, err := requiredSourceScheduleBuckets(tx)
	if err != nil {
		return err
	}
	if err := byJob.Put(sourceDueByJobKey(source), indexMarker); err != nil {
		return err
	}
	return global.Put(sourceDueGlobalKey(source), indexMarker)
}

func removeSourceDue(tx *bolt.Tx, source DiscoverySourceEntry) error {
	byJob, global, _, err := requiredSourceScheduleBuckets(tx)
	if err != nil {
		return err
	}
	if err := byJob.Delete(sourceDueByJobKey(source)); err != nil {
		return err
	}
	return global.Delete(sourceDueGlobalKey(source))
}

func addSourceLease(tx *bolt.Tx, source DiscoverySourceEntry) error {
	_, _, leases, err := requiredSourceScheduleBuckets(tx)
	if err != nil {
		return err
	}
	return leases.Put(sourceLeaseExpiryKey(source), indexMarker)
}

func removeSourceLease(tx *bolt.Tx, source DiscoverySourceEntry) error {
	_, _, leases, err := requiredSourceScheduleBuckets(tx)
	if err != nil {
		return err
	}
	return leases.Delete(sourceLeaseExpiryKey(source))
}

func rebuildSourceSchedule(tx *bolt.Tx) error {
	sources := tx.Bucket(sourcesBucket)
	if sources == nil {
		return ErrSchemaMismatch
	}
	byJob, global, leases, err := requiredSourceScheduleBuckets(tx)
	if err != nil {
		return err
	}
	for _, bucket := range []*bolt.Bucket{byJob, global, leases} {
		if err := clearBucket(bucket); err != nil {
			return err
		}
	}
	return sources.ForEach(func(key, raw []byte) error {
		source, err := decodeDiscoverySource(key, raw)
		if err != nil {
			return err
		}
		if sourceSchedulable(source.State) {
			return addSourceDue(tx, source)
		}
		if source.State == StateLeased {
			return addSourceLease(tx, source)
		}
		return nil
	})
}

func validateSourceSchedule(tx *bolt.Tx, maxSources int) error {
	sources := tx.Bucket(sourcesBucket)
	if sources == nil {
		return ErrSchemaMismatch
	}
	byJob, global, leases, err := requiredSourceScheduleBuckets(tx)
	if err != nil {
		return err
	}
	dueCount := 0
	leaseCount := 0
	if err := sources.ForEach(func(key, raw []byte) error {
		source, err := decodeDiscoverySource(key, raw)
		if err != nil {
			return err
		}
		if sourceSchedulable(source.State) {
			if !bytes.Equal(byJob.Get(sourceDueByJobKey(source)), indexMarker) || !bytes.Equal(global.Get(sourceDueGlobalKey(source)), indexMarker) {
				return fmt.Errorf("%w: source due index missing %q", ErrCorrupt, key)
			}
			dueCount++
		} else if source.State == StateLeased {
			if !bytes.Equal(leases.Get(sourceLeaseExpiryKey(source)), indexMarker) {
				return fmt.Errorf("%w: source lease index missing %q", ErrCorrupt, key)
			}
			leaseCount++
		}
		return nil
	}); err != nil {
		return err
	}
	if dueCount > maxSources || leaseCount > maxSources || byJob.Stats().KeyN != dueCount || global.Stats().KeyN != dueCount || leases.Stats().KeyN != leaseCount {
		return fmt.Errorf("%w: source schedule cardinality mismatch", ErrCorrupt)
	}
	return nil
}

func recoverExpiredSourceLeases(tx *bolt.Tx, now time.Time) error {
	sources := tx.Bucket(sourcesBucket)
	_, _, leases, err := requiredSourceScheduleBuckets(tx)
	if sources == nil || err != nil {
		if err != nil {
			return err
		}
		return ErrSchemaMismatch
	}
	expired := make([]indexedEntry, 0)
	cursor := leases.Cursor()
	for key, marker := cursor.First(); key != nil; key, marker = cursor.Next() {
		if !bytes.Equal(marker, indexMarker) {
			return fmt.Errorf("%w: invalid source lease marker", ErrCorrupt)
		}
		expires, sourceKey, err := parseTimeAndKey(key)
		if err != nil {
			return err
		}
		if expires.After(now) {
			break
		}
		expired = append(expired, indexedEntry{indexKey: append([]byte(nil), key...), entryKey: sourceKey})
	}
	for _, indexed := range expired {
		raw := sources.Get([]byte(indexed.entryKey))
		if raw == nil {
			return fmt.Errorf("%w: source lease missing %q", ErrCorrupt, indexed.entryKey)
		}
		source, err := decodeDiscoverySource([]byte(indexed.entryKey), raw)
		if err != nil {
			return err
		}
		if source.State != StateLeased || !bytes.Equal(sourceLeaseExpiryKey(source), indexed.indexKey) {
			return fmt.Errorf("%w: source lease mismatch %q", ErrCorrupt, indexed.entryKey)
		}
		if err := leases.Delete(indexed.indexKey); err != nil {
			return err
		}
		source.State = StateRetry
		source.NextEligible = now
		source.LeaseUntil = time.Time{}
		source.LastReason = ReasonLeaseExpired
		if err := putDiscoverySource(sources, source); err != nil {
			return err
		}
		if err := addSourceDue(tx, source); err != nil {
			return err
		}
	}
	return nil
}

func dueSourcesIndexed(tx *bolt.Tx, jobID string, now time.Time, limit int) ([]indexedEntry, error) {
	byJob, global, _, err := requiredSourceScheduleBuckets(tx)
	if err != nil {
		return nil, err
	}
	bucket := global
	var prefix []byte
	if jobID != "" {
		bucket = byJob
		prefix = orderedString(nil, jobID)
	}
	cursor := bucket.Cursor()
	key, marker := cursor.First()
	if prefix != nil {
		key, marker = cursor.Seek(prefix)
	}
	selected := make([]indexedEntry, 0, limit)
	for ; key != nil && len(selected) < limit; key, marker = cursor.Next() {
		if prefix != nil && !bytes.HasPrefix(key, prefix) {
			break
		}
		if !bytes.Equal(marker, indexMarker) {
			return nil, fmt.Errorf("%w: invalid source due marker", ErrCorrupt)
		}
		due, sourceKey, err := parseDueIndexKey(key, prefix)
		if err != nil {
			return nil, err
		}
		if due.After(now) {
			break
		}
		selected = append(selected, indexedEntry{indexKey: append([]byte(nil), key...), entryKey: sourceKey})
	}
	return selected, nil
}
