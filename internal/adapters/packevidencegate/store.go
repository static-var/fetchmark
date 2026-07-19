package packevidencegate

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	bolt "go.etcd.io/bbolt"
)

const (
	schemaVersion       = 1
	privateFileMode     = os.FileMode(0o600)
	defaultLockTimeout  = 50 * time.Millisecond
	maximumRunDuration  = 48 * time.Hour
	maximumControlGrace = time.Minute
	maximumHostBytes    = 253
	maximumAttemptBytes = 128
	pruneBatchSize      = 64
)

var (
	metaBucket     = []byte("meta")
	hostsBucket    = []byte("hosts")
	expiriesBucket = []byte("lease_expiries")
	prunesBucket   = []byte("host_prunes")
	schemaKey      = []byte("schema")
	policyKey      = []byte("policy")
	activeKey      = []byte("active")
	generationKey  = []byte("generation")
	lastClockKey   = []byte("last_clock")
)

var (
	ErrInvalidOptions  = errors.New("pack evidence gate: invalid options")
	ErrOwned           = errors.New("pack evidence gate: database is already owned")
	ErrUnsafePath      = errors.New("pack evidence gate: unsafe database path")
	ErrCorrupt         = errors.New("pack evidence gate: corrupt database")
	ErrSchemaMismatch  = errors.New("pack evidence gate: schema mismatch")
	ErrPolicyMismatch  = errors.New("pack evidence gate: policy mismatch")
	ErrLeaseMismatch   = errors.New("pack evidence gate: lease mismatch")
	ErrAttemptComplete = errors.New("pack evidence gate: attempt already completed")
	ErrClockRollback   = errors.New("pack evidence gate: clock moved backwards")
	ErrExpired         = errors.New("pack evidence gate: run expired")
	ErrClosed          = errors.New("pack evidence gate: closed")
)

var (
	runIDPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	attemptPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// Policy is immutable for the lifetime of one coordination database. Its
// digest binding prevents a worker from weakening pacing or concurrency.
type Policy struct {
	RunID             string
	ConfigSHA256      string
	MinHostInterval   time.Duration
	GlobalConcurrency uint64
	RequestTimeout    time.Duration
	MaxRetryAfter     time.Duration
	ControlGrace      time.Duration
	ExpiresAt         time.Time
}

type persistedPolicy struct {
	Version              int    `json:"version"`
	RunID                string `json:"run_id"`
	ConfigSHA256         string `json:"config_sha256"`
	MinHostIntervalNanos int64  `json:"min_host_interval_nanos"`
	GlobalConcurrency    uint64 `json:"global_concurrency"`
	RequestTimeoutNanos  int64  `json:"request_timeout_nanos"`
	MaxRetryAfterNanos   int64  `json:"max_retry_after_nanos"`
	ControlGraceNanos    int64  `json:"control_grace_nanos"`
	ExpiresAtUnixNano    int64  `json:"expires_at_unix_nano"`
}

type Lease struct {
	Host       string    `json:"host"`
	AttemptID  string    `json:"attempt_id"`
	Generation uint64    `json:"generation"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type Decision struct {
	Granted bool      `json:"granted"`
	Lease   Lease     `json:"lease,omitempty"`
	RetryAt time.Time `json:"retry_at,omitempty"`
}

type hostState struct {
	Host             string `json:"host"`
	Generation       uint64 `json:"generation"`
	AttemptID        string `json:"attempt_id"`
	Active           bool   `json:"active"`
	LeaseExpiresNano int64  `json:"lease_expires_unix_nano"`
	LastDispatchNano int64  `json:"last_dispatch_unix_nano"`
	NextEligibleNano int64  `json:"next_eligible_unix_nano"`
	CooldownNano     int64  `json:"cooldown_unix_nano"`
	PruneNano        int64  `json:"prune_unix_nano"`
}

type Store struct {
	db        *bolt.DB
	policy    Policy
	now       func() time.Time
	lifecycle sync.RWMutex
	closed    bool
}

func Open(path string, policy Policy, now func() time.Time) (*Store, error) {
	if !validPolicy(policy, now) || strings.TrimSpace(path) == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, ErrInvalidOptions
	}
	parent, err := secureconfigfile.ValidateDirectory(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("%w: validate parent: %v", ErrUnsafePath, err)
	}
	path = filepath.Join(parent, filepath.Base(path))
	before, statErr := os.Lstat(path)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: inspect database: %v", ErrUnsafePath, statErr)
	}
	if statErr == nil && (!before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm() != privateFileMode || !ownedByEffectiveUser(before)) {
		return nil, fmt.Errorf("%w: existing database must be an owner-only regular file", ErrUnsafePath)
	}
	newDatabase := errors.Is(statErr, os.ErrNotExist) || (statErr == nil && before.Size() == 0)
	db, err := bolt.Open(path, privateFileMode, &bolt.Options{Timeout: defaultLockTimeout, OpenFile: openNoFollow})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf("%w: %v", ErrOwned, err)
		}
		return nil, fmt.Errorf("%w: open database: %v", ErrCorrupt, err)
	}
	fail := func(openErr error) (*Store, error) {
		_ = db.Close()
		return nil, openErr
	}
	after, err := os.Lstat(path)
	if err != nil || !after.Mode().IsRegular() || after.Mode().Perm() != privateFileMode || !ownedByEffectiveUser(after) || (before != nil && !os.SameFile(before, after)) {
		return fail(fmt.Errorf("%w: database changed while opening", ErrUnsafePath))
	}
	encodedPolicy, err := encodePolicy(policy)
	if err != nil {
		return fail(err)
	}
	store := &Store{db: db, policy: normalizedPolicy(policy), now: now}
	if err := store.initialize(newDatabase, encodedPolicy); err != nil {
		return fail(err)
	}
	return store, nil
}

func validPolicy(policy Policy, now func() time.Time) bool {
	if now == nil || !runIDPattern.MatchString(policy.RunID) || len(policy.ConfigSHA256) != 64 {
		return false
	}
	if _, err := hex.DecodeString(policy.ConfigSHA256); err != nil || strings.ToLower(policy.ConfigSHA256) != policy.ConfigSHA256 {
		return false
	}
	current := now().UTC()
	return !current.IsZero() && policy.MinHostInterval >= time.Second && policy.MinHostInterval <= time.Hour &&
		policy.GlobalConcurrency >= 1 && policy.GlobalConcurrency <= 32 && policy.RequestTimeout >= time.Second && policy.RequestTimeout <= 2*time.Minute &&
		policy.MaxRetryAfter >= 0 && policy.MaxRetryAfter <= time.Hour && policy.ControlGrace >= 0 && policy.ControlGrace <= maximumControlGrace &&
		policy.ExpiresAt.After(current) && !policy.ExpiresAt.After(current.Add(maximumRunDuration))
}

func normalizedPolicy(policy Policy) Policy {
	policy.ExpiresAt = policy.ExpiresAt.UTC()
	return policy
}

func encodePolicy(policy Policy) ([]byte, error) {
	policy = normalizedPolicy(policy)
	return json.Marshal(persistedPolicy{
		Version: schemaVersion, RunID: policy.RunID, ConfigSHA256: policy.ConfigSHA256,
		MinHostIntervalNanos: int64(policy.MinHostInterval), GlobalConcurrency: policy.GlobalConcurrency,
		RequestTimeoutNanos: int64(policy.RequestTimeout), MaxRetryAfterNanos: int64(policy.MaxRetryAfter),
		ControlGraceNanos: int64(policy.ControlGrace), ExpiresAtUnixNano: policy.ExpiresAt.UnixNano(),
	})
}

func (store *Store) initialize(newDatabase bool, encodedPolicy []byte) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		if newDatabase {
			meta, err := tx.CreateBucket(metaBucket)
			if err != nil {
				return fmt.Errorf("%w: create meta: %v", ErrCorrupt, err)
			}
			for _, name := range [][]byte{hostsBucket, expiriesBucket, prunesBucket} {
				if _, err := tx.CreateBucket(name); err != nil {
					return fmt.Errorf("%w: create bucket: %v", ErrCorrupt, err)
				}
			}
			if err := meta.Put(schemaKey, uint64Bytes(schemaVersion)); err != nil {
				return err
			}
			if err := meta.Put(policyKey, encodedPolicy); err != nil {
				return err
			}
			if err := meta.Put(activeKey, uint64Bytes(0)); err != nil {
				return err
			}
			if err := meta.Put(generationKey, uint64Bytes(0)); err != nil {
				return err
			}
			if err := meta.Put(lastClockKey, int64Bytes(store.now().UTC().UnixNano())); err != nil {
				return err
			}
			return nil
		}
		meta := tx.Bucket(metaBucket)
		if meta == nil || tx.Bucket(hostsBucket) == nil || tx.Bucket(expiriesBucket) == nil || tx.Bucket(prunesBucket) == nil {
			return ErrCorrupt
		}
		if value, err := readUint64(meta.Get(schemaKey)); err != nil || value != schemaVersion {
			return ErrSchemaMismatch
		}
		if !bytes.Equal(meta.Get(policyKey), encodedPolicy) {
			return ErrPolicyMismatch
		}
		if _, err := readUint64(meta.Get(activeKey)); err != nil {
			return ErrCorrupt
		}
		if _, err := readUint64(meta.Get(generationKey)); err != nil {
			return ErrCorrupt
		}
		return touchClock(meta, store.now().UTC())
	})
}

func (store *Store) Acquire(attemptID, rawHost string) (Decision, error) {
	if !attemptPattern.MatchString(attemptID) {
		return Decision{}, ErrInvalidOptions
	}
	host, err := CanonicalHost(rawHost)
	if err != nil {
		return Decision{}, err
	}
	store.lifecycle.RLock()
	defer store.lifecycle.RUnlock()
	if store.closed {
		return Decision{}, ErrClosed
	}
	var decision Decision
	err = store.db.Update(func(tx *bolt.Tx) error {
		now := store.now().UTC()
		if !now.Before(store.policy.ExpiresAt) {
			return ErrExpired
		}
		meta, hosts, expiries, prunes, err := requiredBuckets(tx)
		if err != nil {
			return err
		}
		if err := touchClock(meta, now); err != nil {
			return err
		}
		if err := reapExpiredLeases(meta, hosts, expiries, prunes, now); err != nil {
			return err
		}
		if err := pruneExpiredHosts(hosts, prunes, now, pruneBatchSize); err != nil {
			return err
		}
		state, found, err := loadHost(hosts, host)
		if err != nil {
			return err
		}
		if found && state.AttemptID == attemptID {
			if !state.Active {
				return ErrAttemptComplete
			}
			decision = Decision{Granted: true, Lease: leaseFromState(state)}
			return nil
		}
		if found {
			retryAt := latestTime(state.LeaseExpiresNano, state.NextEligibleNano, state.CooldownNano)
			if state.Active || retryAt.After(now) {
				decision.RetryAt = retryAt
				return nil
			}
		}
		active, err := readUint64(meta.Get(activeKey))
		if err != nil {
			return ErrCorrupt
		}
		if active >= store.policy.GlobalConcurrency {
			retryAt, err := firstExpiry(expiries)
			if err != nil {
				return err
			}
			decision.RetryAt = retryAt
			return nil
		}
		generation, err := readUint64(meta.Get(generationKey))
		if err != nil || generation == ^uint64(0) {
			return ErrCorrupt
		}
		generation++
		dispatch := now
		expires := dispatch.Add(store.policy.RequestTimeout + store.policy.MaxRetryAfter + store.policy.ControlGrace)
		state = hostState{Host: host, Generation: generation, AttemptID: attemptID, Active: true,
			LeaseExpiresNano: expires.UnixNano(), LastDispatchNano: dispatch.UnixNano(),
			NextEligibleNano: dispatch.Add(store.policy.MinHostInterval).UnixNano()}
		if err := saveHost(hosts, state); err != nil {
			return err
		}
		if err := expiries.Put(scheduleKey(state.LeaseExpiresNano, host), uint64Bytes(generation)); err != nil {
			return err
		}
		if err := meta.Put(activeKey, uint64Bytes(active+1)); err != nil {
			return err
		}
		if err := meta.Put(generationKey, uint64Bytes(generation)); err != nil {
			return err
		}
		decision = Decision{Granted: true, Lease: leaseFromState(state)}
		return nil
	})
	return decision, err
}

func (store *Store) RecordResponse(lease Lease, status int, rawRetryAfter string) (time.Duration, error) {
	if err := validateLease(lease); err != nil || status < 100 || status > 599 || len(rawRetryAfter) > 256 || strings.ContainsAny(rawRetryAfter, "\x00\r\n") {
		return 0, ErrInvalidOptions
	}
	store.lifecycle.RLock()
	defer store.lifecycle.RUnlock()
	if store.closed {
		return 0, ErrClosed
	}
	var applied time.Duration
	err := store.db.Update(func(tx *bolt.Tx) error {
		now := store.now().UTC()
		meta, hosts, expiries, prunes, err := requiredBuckets(tx)
		if err != nil {
			return err
		}
		if err := touchClock(meta, now); err != nil {
			return err
		}
		state, found, err := loadHost(hosts, lease.Host)
		if err != nil {
			return err
		}
		if !found || state.Generation != lease.Generation || state.AttemptID != lease.AttemptID {
			return ErrLeaseMismatch
		}
		if status == 429 {
			applied = retryafter.Parse(rawRetryAfter, now, store.policy.MaxRetryAfter)
			if applied > 0 && now.Add(applied).UnixNano() > state.CooldownNano {
				state.CooldownNano = now.Add(applied).UnixNano()
			}
		}
		if state.Active {
			oldKey := scheduleKey(state.LeaseExpiresNano, state.Host)
			if err := expiries.Delete(oldKey); err != nil {
				return err
			}
			shorter := time.Unix(0, state.LastDispatchNano).UTC().Add(store.policy.RequestTimeout + store.policy.ControlGrace)
			if shorter.Before(now) {
				shorter = now.Add(store.policy.ControlGrace)
			}
			if shorter.UnixNano() < state.LeaseExpiresNano {
				state.LeaseExpiresNano = shorter.UnixNano()
			}
			if err := expiries.Put(scheduleKey(state.LeaseExpiresNano, state.Host), uint64Bytes(state.Generation)); err != nil {
				return err
			}
		}
		if state.PruneNano != 0 {
			_ = prunes.Delete(scheduleKey(state.PruneNano, state.Host))
			state.PruneNano = 0
		}
		return saveHost(hosts, state)
	})
	return applied, err
}

func (store *Store) Release(lease Lease) error {
	if err := validateLease(lease); err != nil {
		return ErrInvalidOptions
	}
	store.lifecycle.RLock()
	defer store.lifecycle.RUnlock()
	if store.closed {
		return ErrClosed
	}
	return store.db.Update(func(tx *bolt.Tx) error {
		now := store.now().UTC()
		meta, hosts, expiries, prunes, err := requiredBuckets(tx)
		if err != nil {
			return err
		}
		if err := touchClock(meta, now); err != nil {
			return err
		}
		state, found, err := loadHost(hosts, lease.Host)
		if err != nil {
			return err
		}
		if !found || state.Generation != lease.Generation || state.AttemptID != lease.AttemptID {
			return ErrLeaseMismatch
		}
		if !state.Active {
			return nil
		}
		if err := expiries.Delete(scheduleKey(state.LeaseExpiresNano, state.Host)); err != nil {
			return err
		}
		active, err := readUint64(meta.Get(activeKey))
		if err != nil || active == 0 {
			return ErrCorrupt
		}
		state.Active = false
		if err := meta.Put(activeKey, uint64Bytes(active-1)); err != nil {
			return err
		}
		pruneAt := latestTime(state.NextEligibleNano, state.CooldownNano)
		if !pruneAt.After(now) {
			return hosts.Delete([]byte(state.Host))
		}
		state.PruneNano = pruneAt.UnixNano()
		if err := prunes.Put(scheduleKey(state.PruneNano, state.Host), uint64Bytes(state.Generation)); err != nil {
			return err
		}
		return saveHost(hosts, state)
	})
}

func (store *Store) Close() error {
	if store == nil {
		return nil
	}
	store.lifecycle.Lock()
	defer store.lifecycle.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	return store.db.Close()
}

func requiredBuckets(tx *bolt.Tx) (*bolt.Bucket, *bolt.Bucket, *bolt.Bucket, *bolt.Bucket, error) {
	meta, hosts, expiries, prunes := tx.Bucket(metaBucket), tx.Bucket(hostsBucket), tx.Bucket(expiriesBucket), tx.Bucket(prunesBucket)
	if meta == nil || hosts == nil || expiries == nil || prunes == nil {
		return nil, nil, nil, nil, ErrCorrupt
	}
	return meta, hosts, expiries, prunes, nil
}

func reapExpiredLeases(meta, hosts, expiries, prunes *bolt.Bucket, now time.Time) error {
	cursor := expiries.Cursor()
	for key, rawGeneration := cursor.First(); key != nil; key, rawGeneration = cursor.First() {
		when, host, err := parseScheduleKey(key)
		if err != nil {
			return err
		}
		if when.After(now) {
			return nil
		}
		generation, err := readUint64(rawGeneration)
		if err != nil {
			return ErrCorrupt
		}
		state, found, err := loadHost(hosts, host)
		if err != nil {
			return err
		}
		if found && state.Active && state.Generation == generation && state.LeaseExpiresNano <= now.UnixNano() {
			active, err := readUint64(meta.Get(activeKey))
			if err != nil || active == 0 {
				return ErrCorrupt
			}
			state.Active = false
			if err := meta.Put(activeKey, uint64Bytes(active-1)); err != nil {
				return err
			}
			pruneAt := latestTime(state.NextEligibleNano, state.CooldownNano)
			if pruneAt.After(now) {
				state.PruneNano = pruneAt.UnixNano()
				if err := prunes.Put(scheduleKey(state.PruneNano, host), uint64Bytes(generation)); err != nil {
					return err
				}
				if err := saveHost(hosts, state); err != nil {
					return err
				}
			} else if err := hosts.Delete([]byte(host)); err != nil {
				return err
			}
		}
		if err := expiries.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func pruneExpiredHosts(hosts, prunes *bolt.Bucket, now time.Time, maximum int) error {
	cursor := prunes.Cursor()
	for count := 0; count < maximum; count++ {
		key, rawGeneration := cursor.First()
		if key == nil {
			return nil
		}
		when, host, err := parseScheduleKey(key)
		if err != nil {
			return err
		}
		if when.After(now) {
			return nil
		}
		generation, err := readUint64(rawGeneration)
		if err != nil {
			return ErrCorrupt
		}
		state, found, err := loadHost(hosts, host)
		if err != nil {
			return err
		}
		if found && !state.Active && state.Generation == generation && state.PruneNano <= now.UnixNano() {
			if err := hosts.Delete([]byte(host)); err != nil {
				return err
			}
		}
		if err := prunes.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func firstExpiry(bucket *bolt.Bucket) (time.Time, error) {
	key, _ := bucket.Cursor().First()
	if key == nil {
		return time.Time{}, ErrCorrupt
	}
	when, _, err := parseScheduleKey(key)
	return when, err
}

func saveHost(bucket *bolt.Bucket, state hostState) error {
	if state.Host == "" || len(state.Host) > maximumHostBytes || state.Generation == 0 {
		return ErrCorrupt
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(state.Host), raw)
}

func loadHost(bucket *bolt.Bucket, host string) (hostState, bool, error) {
	raw := bucket.Get([]byte(host))
	if raw == nil {
		return hostState{}, false, nil
	}
	var state hostState
	if err := json.Unmarshal(raw, &state); err != nil || state.Host != host || state.Generation == 0 || len(state.AttemptID) == 0 || len(state.AttemptID) > maximumAttemptBytes {
		return hostState{}, false, ErrCorrupt
	}
	return state, true, nil
}

func leaseFromState(state hostState) Lease {
	return Lease{Host: state.Host, AttemptID: state.AttemptID, Generation: state.Generation, ExpiresAt: time.Unix(0, state.LeaseExpiresNano).UTC()}
}

func validateLease(lease Lease) error {
	host, err := CanonicalHost(lease.Host)
	if err != nil || host != lease.Host || !attemptPattern.MatchString(lease.AttemptID) || lease.Generation == 0 || lease.ExpiresAt.IsZero() {
		return ErrInvalidOptions
	}
	return nil
}

func latestTime(values ...int64) time.Time {
	var maximum int64
	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}
	if maximum == 0 {
		return time.Time{}
	}
	return time.Unix(0, maximum).UTC()
}

func scheduleKey(nanos int64, host string) []byte {
	key := make([]byte, 8+len(host))
	binary.BigEndian.PutUint64(key[:8], uint64(nanos))
	copy(key[8:], host)
	return key
}

func parseScheduleKey(key []byte) (time.Time, string, error) {
	if len(key) <= 8 {
		return time.Time{}, "", ErrCorrupt
	}
	host, err := CanonicalHost(string(key[8:]))
	if err != nil || host != string(key[8:]) {
		return time.Time{}, "", ErrCorrupt
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(key[:8]))).UTC(), host, nil
}

func touchClock(meta *bolt.Bucket, now time.Time) error {
	previous, err := readInt64(meta.Get(lastClockKey))
	if err != nil {
		return ErrCorrupt
	}
	if now.UnixNano() < previous {
		return ErrClockRollback
	}
	return meta.Put(lastClockKey, int64Bytes(now.UnixNano()))
}

func uint64Bytes(value uint64) []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, value)
	return raw
}
func int64Bytes(value int64) []byte { return uint64Bytes(uint64(value)) }
func readUint64(raw []byte) (uint64, error) {
	if len(raw) != 8 {
		return 0, ErrCorrupt
	}
	return binary.BigEndian.Uint64(raw), nil
}
func readInt64(raw []byte) (int64, error) { value, err := readUint64(raw); return int64(value), err }
