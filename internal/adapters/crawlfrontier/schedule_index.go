package crawlfrontier

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	dueByJobBucket       = []byte("due_by_job")
	dueGlobalBucket      = []byte("due_global")
	leasesByExpiryBucket = []byte("leases_by_expiry")
	indexMarker          = []byte{1}
)

type indexedEntry struct {
	indexKey []byte
	entryKey string
}

func dueByJobKey(entry Entry) []byte {
	key := orderedString(nil, entry.JobID)
	key = appendScheduleTime(key, entry.NextEligible)
	return orderedString(key, entry.Key)
}

func dueGlobalKey(entry Entry) []byte {
	key := appendScheduleTime(nil, entry.NextEligible)
	return orderedString(key, entry.Key)
}

func leaseExpiryKey(entry Entry) []byte {
	key := appendScheduleTime(nil, entry.LeaseUntil)
	return orderedString(key, entry.Key)
}

func addDueIndexes(tx *bolt.Tx, entry Entry) error {
	if !isSchedulable(entry.State) {
		return fmt.Errorf("%w: cannot index non-schedulable entry %q", ErrCorrupt, entry.Key)
	}
	byJob, global, _, err := requiredSchedulingBuckets(tx)
	if err != nil {
		return err
	}
	if err := byJob.Put(dueByJobKey(entry), indexMarker); err != nil {
		return fmt.Errorf("crawl frontier: index due entry by job: %w", err)
	}
	if err := global.Put(dueGlobalKey(entry), indexMarker); err != nil {
		return fmt.Errorf("crawl frontier: index due entry globally: %w", err)
	}
	return nil
}

func removeDueIndexes(tx *bolt.Tx, entry Entry) error {
	byJob, global, _, err := requiredSchedulingBuckets(tx)
	if err != nil {
		return err
	}
	if err := byJob.Delete(dueByJobKey(entry)); err != nil {
		return fmt.Errorf("crawl frontier: remove job due index: %w", err)
	}
	if err := global.Delete(dueGlobalKey(entry)); err != nil {
		return fmt.Errorf("crawl frontier: remove global due index: %w", err)
	}
	return nil
}

func addLeaseIndex(tx *bolt.Tx, entry Entry) error {
	if entry.State != StateLeased {
		return fmt.Errorf("%w: cannot index non-leased entry %q", ErrCorrupt, entry.Key)
	}
	_, _, leases, err := requiredSchedulingBuckets(tx)
	if err != nil {
		return err
	}
	if err := leases.Put(leaseExpiryKey(entry), indexMarker); err != nil {
		return fmt.Errorf("crawl frontier: index lease expiry: %w", err)
	}
	return nil
}

func removeLeaseIndex(tx *bolt.Tx, entry Entry) error {
	_, _, leases, err := requiredSchedulingBuckets(tx)
	if err != nil {
		return err
	}
	if err := leases.Delete(leaseExpiryKey(entry)); err != nil {
		return fmt.Errorf("crawl frontier: remove lease expiry index: %w", err)
	}
	return nil
}

func rebuildSchedulingIndexes(tx *bolt.Tx) error {
	entries, err := requiredEntriesBucket(tx)
	if err != nil {
		return err
	}
	byJob, global, leases, err := requiredSchedulingBuckets(tx)
	if err != nil {
		return err
	}
	for _, bucket := range []*bolt.Bucket{byJob, global, leases} {
		if err := clearBucket(bucket); err != nil {
			return err
		}
	}
	return entries.ForEach(func(key, raw []byte) error {
		entry, err := decodeEntry(key, raw)
		if err != nil {
			return err
		}
		switch {
		case isSchedulable(entry.State):
			return addDueIndexes(tx, entry)
		case entry.State == StateLeased:
			return addLeaseIndex(tx, entry)
		default:
			return nil
		}
	})
}

func validateSchedulingIndexes(tx *bolt.Tx, maxEntries int) error {
	entries, err := requiredEntriesBucket(tx)
	if err != nil {
		return err
	}
	byJob, global, leases, err := requiredSchedulingBuckets(tx)
	if err != nil {
		return err
	}
	dueCount := 0
	leaseCount := 0
	if err := entries.ForEach(func(key, raw []byte) error {
		entry, err := decodeEntry(key, raw)
		if err != nil {
			return err
		}
		switch {
		case isSchedulable(entry.State):
			if !bytes.Equal(byJob.Get(dueByJobKey(entry)), indexMarker) || !bytes.Equal(global.Get(dueGlobalKey(entry)), indexMarker) {
				return fmt.Errorf("%w: missing due index for %q", ErrCorrupt, entry.Key)
			}
			dueCount++
		case entry.State == StateLeased:
			if !bytes.Equal(leases.Get(leaseExpiryKey(entry)), indexMarker) {
				return fmt.Errorf("%w: missing lease index for %q", ErrCorrupt, entry.Key)
			}
			leaseCount++
		}
		return nil
	}); err != nil {
		return err
	}
	if dueCount > maxEntries || leaseCount > maxEntries || byJob.Stats().KeyN != dueCount || global.Stats().KeyN != dueCount || leases.Stats().KeyN != leaseCount {
		return fmt.Errorf("%w: scheduling index cardinality mismatch", ErrCorrupt)
	}
	return nil
}

func recoverExpiredLeasesIndexed(tx *bolt.Tx, now time.Time) error {
	entries, err := requiredEntriesBucket(tx)
	if err != nil {
		return err
	}
	_, _, leases, err := requiredSchedulingBuckets(tx)
	if err != nil {
		return err
	}
	expired := make([]indexedEntry, 0)
	cursor := leases.Cursor()
	for key, marker := cursor.First(); key != nil; key, marker = cursor.Next() {
		if !bytes.Equal(marker, indexMarker) {
			return fmt.Errorf("%w: invalid lease index marker", ErrCorrupt)
		}
		expires, entryKey, err := parseTimeAndKey(key)
		if err != nil {
			return err
		}
		if expires.After(now) {
			break
		}
		expired = append(expired, indexedEntry{indexKey: append([]byte(nil), key...), entryKey: entryKey})
	}
	for _, indexed := range expired {
		raw := entries.Get([]byte(indexed.entryKey))
		if raw == nil {
			return fmt.Errorf("%w: lease index entry missing %q", ErrCorrupt, indexed.entryKey)
		}
		entry, err := decodeEntry([]byte(indexed.entryKey), raw)
		if err != nil {
			return err
		}
		if entry.State != StateLeased || !bytes.Equal(leaseExpiryKey(entry), indexed.indexKey) {
			return fmt.Errorf("%w: lease index disagrees with %q", ErrCorrupt, indexed.entryKey)
		}
		if err := leases.Delete(indexed.indexKey); err != nil {
			return fmt.Errorf("crawl frontier: recover lease index: %w", err)
		}
		entry.State = StateRetry
		entry.NextEligible = now
		entry.LeaseUntil = time.Time{}
		entry.LastReason = ReasonLeaseExpired
		if err := putEntry(entries, entry); err != nil {
			return err
		}
		if err := addDueIndexes(tx, entry); err != nil {
			return err
		}
	}
	return nil
}

func dueEntriesIndexed(tx *bolt.Tx, jobID string, now time.Time, limit int) ([]indexedEntry, error) {
	byJob, global, _, err := requiredSchedulingBuckets(tx)
	if err != nil {
		return nil, err
	}
	bucket := global
	prefix := []byte(nil)
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
			return nil, fmt.Errorf("%w: invalid due index marker", ErrCorrupt)
		}
		indexedTime, entryKey, err := parseDueIndexKey(key, prefix)
		if err != nil {
			return nil, err
		}
		if indexedTime.After(now) {
			break
		}
		selected = append(selected, indexedEntry{indexKey: append([]byte(nil), key...), entryKey: entryKey})
	}
	return selected, nil
}

func requiredSchedulingBuckets(tx *bolt.Tx) (*bolt.Bucket, *bolt.Bucket, *bolt.Bucket, error) {
	byJob := tx.Bucket(dueByJobBucket)
	global := tx.Bucket(dueGlobalBucket)
	leases := tx.Bucket(leasesByExpiryBucket)
	if byJob == nil || global == nil || leases == nil {
		return nil, nil, nil, ErrSchemaMismatch
	}
	return byJob, global, leases, nil
}

func clearBucket(bucket *bolt.Bucket) error {
	keys := make([][]byte, 0, bucket.Stats().KeyN)
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		keys = append(keys, append([]byte(nil), key...))
	}
	for _, key := range keys {
		if err := bucket.Delete(key); err != nil {
			return fmt.Errorf("crawl frontier: clear scheduling index: %w", err)
		}
	}
	return nil
}

func orderedString(dst []byte, value string) []byte {
	for _, char := range []byte(value) {
		if char == 0 {
			dst = append(dst, 0, 255)
		} else {
			dst = append(dst, char)
		}
	}
	return append(dst, 0, 0)
}

func decodeOrderedString(raw []byte) (string, []byte, error) {
	decoded := make([]byte, 0)
	for index := 0; index < len(raw); index++ {
		if raw[index] != 0 {
			decoded = append(decoded, raw[index])
			continue
		}
		if index+1 >= len(raw) {
			return "", nil, fmt.Errorf("%w: truncated scheduling key", ErrCorrupt)
		}
		switch raw[index+1] {
		case 0:
			return string(decoded), raw[index+2:], nil
		case 255:
			decoded = append(decoded, 0)
			index++
		default:
			return "", nil, fmt.Errorf("%w: invalid scheduling key escape", ErrCorrupt)
		}
	}
	return "", nil, fmt.Errorf("%w: unterminated scheduling key", ErrCorrupt)
}

func appendScheduleTime(dst []byte, value time.Time) []byte {
	raw := make([]byte, 12)
	binary.BigEndian.PutUint64(raw[:8], uint64(value.Unix())^(uint64(1)<<63))
	binary.BigEndian.PutUint32(raw[8:], uint32(value.Nanosecond()))
	return append(dst, raw...)
}

func decodeScheduleTime(raw []byte) (time.Time, []byte, error) {
	if len(raw) < 12 {
		return time.Time{}, nil, fmt.Errorf("%w: truncated scheduling time", ErrCorrupt)
	}
	seconds := int64(binary.BigEndian.Uint64(raw[:8]) ^ (uint64(1) << 63))
	nanoseconds := binary.BigEndian.Uint32(raw[8:12])
	if nanoseconds >= 1_000_000_000 {
		return time.Time{}, nil, fmt.Errorf("%w: invalid scheduling nanoseconds", ErrCorrupt)
	}
	return time.Unix(seconds, int64(nanoseconds)).UTC(), raw[12:], nil
}

func parseDueIndexKey(raw, prefix []byte) (time.Time, string, error) {
	if prefix != nil {
		if !bytes.HasPrefix(raw, prefix) {
			return time.Time{}, "", fmt.Errorf("%w: wrong job scheduling prefix", ErrCorrupt)
		}
		raw = raw[len(prefix):]
	}
	return parseTimeAndKey(raw)
}

func parseTimeAndKey(raw []byte) (time.Time, string, error) {
	indexedTime, rest, err := decodeScheduleTime(raw)
	if err != nil {
		return time.Time{}, "", err
	}
	entryKey, trailing, err := decodeOrderedString(rest)
	if err != nil {
		return time.Time{}, "", err
	}
	if len(trailing) != 0 {
		return time.Time{}, "", fmt.Errorf("%w: trailing scheduling key data", ErrCorrupt)
	}
	return indexedTime, entryKey, nil
}
