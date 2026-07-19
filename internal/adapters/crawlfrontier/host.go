package crawlfrontier

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	maxAuthorityBytes = 512
	maxHostInterval   = time.Hour
)

var (
	ErrInvalidAuthority = errors.New("crawl frontier: invalid canonical authority")
	ErrUnknownAuthority = errors.New("crawl frontier: authority is not present in seed entries")
)

type hostState struct {
	Authority    string        `json:"authority"`
	LastDispatch time.Time     `json:"last_dispatch"`
	Interval     time.Duration `json:"interval"`
}

// ReserveHost atomically reserves one dispatch for a canonical authority. A
// blocked reservation returns the remaining wait without changing persisted
// state. Authorities must already be represented by a synchronized seed URL.
func (f *Frontier) ReserveHost(authority string, now time.Time, interval time.Duration) (time.Duration, error) {
	if err := validateCanonicalAuthority(authority); err != nil {
		return 0, err
	}
	if now.IsZero() || interval <= 0 || interval > maxHostInterval {
		return 0, ErrInvalidOptions
	}
	db, release, err := f.acquire()
	if err != nil {
		return 0, err
	}
	defer release()
	now = now.UTC()
	var wait time.Duration
	err = db.Update(func(tx *bolt.Tx) error {
		authorityRefs, err := requiredAuthorityRefsBucket(tx)
		if err != nil {
			return err
		}
		reference := authorityRefs.Get([]byte(authority))
		if reference == nil {
			return ErrUnknownAuthority
		}
		if len(reference) != 8 || binary.BigEndian.Uint64(reference) == 0 {
			return fmt.Errorf("%w: invalid authority reference %q", ErrCorrupt, authority)
		}
		hosts, err := requiredHostsBucket(tx)
		if err != nil {
			return err
		}
		raw := hosts.Get([]byte(authority))
		if raw == nil {
			return putHostState(hosts, hostState{Authority: authority, LastDispatch: now, Interval: interval})
		}
		state, err := decodeHostState([]byte(authority), raw)
		if err != nil {
			return err
		}
		requiredInterval := interval
		if state.Interval > requiredInterval {
			requiredInterval = state.Interval
		}
		next := state.LastDispatch.Add(requiredInterval)
		if now.Before(next) {
			wait = next.Sub(now)
			return nil
		}
		state.LastDispatch = now
		state.Interval = interval
		return putHostState(hosts, state)
	})
	return wait, err
}

func validateCanonicalAuthority(authority string) error {
	if strings.TrimSpace(authority) != authority || authority == "" || len(authority) > maxAuthorityBytes || strings.ContainsAny(authority, "/?#@") || strings.Contains(authority, "://") {
		return ErrInvalidAuthority
	}
	parsed, err := url.Parse("http://" + authority)
	if err != nil || parsed.User != nil || parsed.Host != authority || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ErrInvalidAuthority
	}
	canonical, err := canonicalAuthorityFromParts(parsed.Hostname(), parsed.Port())
	if err != nil || canonical != authority {
		return ErrInvalidAuthority
	}
	return nil
}

func canonicalAuthorityForURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", ErrInvalidAuthority
	}
	host := parsed.Hostname()
	if host == "" || strings.Contains(host, "%") {
		return "", ErrInvalidAuthority
	}
	port := parsed.Port()
	if port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 || strconv.Itoa(portNumber) != port {
			return "", ErrInvalidAuthority
		}
		if (parsed.Scheme == "http" && portNumber == 80) || (parsed.Scheme == "https" && portNumber == 443) {
			port = ""
		}
	}
	return canonicalAuthorityFromParts(host, port)
}

func canonicalAuthorityFromParts(host, port string) (string, error) {
	if port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 || strconv.Itoa(portNumber) != port {
			return "", ErrInvalidAuthority
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		host = strings.ToLower(ip.String())
		if strings.Contains(host, ":") {
			if port == "" {
				return "[" + host + "]", nil
			}
			return net.JoinHostPort(host, port), nil
		}
	} else {
		var err error
		host, err = canonicalDNSHost(host)
		if err != nil {
			return "", err
		}
	}
	if port != "" {
		return net.JoinHostPort(host, port), nil
	}
	return host, nil
}

func canonicalDNSHost(host string) (string, error) {
	for _, char := range host {
		if char > 127 {
			return "", ErrInvalidAuthority
		}
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "" || len(host) > 253 {
		return "", ErrInvalidAuthority
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", ErrInvalidAuthority
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return "", ErrInvalidAuthority
			}
		}
	}
	return host, nil
}

func pruneOrphanHosts(tx *bolt.Tx) error {
	if err := rebuildAuthorityRefs(tx); err != nil {
		return err
	}
	hosts, err := requiredHostsBucket(tx)
	if err != nil {
		return err
	}
	authorityRefs, err := requiredAuthorityRefsBucket(tx)
	if err != nil {
		return err
	}
	remove := make([][]byte, 0)
	if err := hosts.ForEach(func(key, raw []byte) error {
		if _, err := decodeHostState(key, raw); err != nil {
			return err
		}
		if authorityRefs.Get(key) == nil {
			remove = append(remove, append([]byte(nil), key...))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, key := range remove {
		if err := hosts.Delete(key); err != nil {
			return fmt.Errorf("crawl frontier: prune orphan host: %w", err)
		}
	}
	return nil
}

func validateHostStates(hosts *bolt.Bucket, authorities map[string]uint64, maxEntries int) error {
	count := 0
	if err := hosts.ForEach(func(key, raw []byte) error {
		if _, err := decodeHostState(key, raw); err != nil {
			return err
		}
		if _, found := authorities[string(key)]; !found {
			return fmt.Errorf("%w: host has no seed entry %q", ErrCorrupt, key)
		}
		count++
		return nil
	}); err != nil {
		return err
	}
	if count > maxEntries {
		return fmt.Errorf("%w: host state exceeds frontier bound", ErrCorrupt)
	}
	return nil
}

func rebuildAuthorityRefs(tx *bolt.Tx) error {
	entries, err := requiredEntriesBucket(tx)
	if err != nil {
		return err
	}
	refs, err := requiredAuthorityRefsBucket(tx)
	if err != nil {
		return err
	}
	counts := make(map[string]uint64)
	if err := entries.ForEach(func(key, raw []byte) error {
		entry, err := decodeEntry(key, raw)
		if err != nil {
			return err
		}
		authority, err := canonicalAuthorityForURL(entry.URL)
		if err != nil {
			return fmt.Errorf("%w: invalid entry authority %q", ErrCorrupt, key)
		}
		counts[authority]++
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
		authority, err := canonicalAuthorityForURL(source.URL)
		if err != nil {
			return fmt.Errorf("%w: invalid source authority %q", ErrCorrupt, key)
		}
		counts[authority]++
		return nil
	}); err != nil {
		return err
	}
	keys := make([][]byte, 0)
	if err := refs.ForEach(func(key, _ []byte) error {
		keys = append(keys, append([]byte(nil), key...))
		return nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		if err := refs.Delete(key); err != nil {
			return fmt.Errorf("crawl frontier: clear authority reference: %w", err)
		}
	}
	for authority, count := range counts {
		raw := make([]byte, 8)
		binary.BigEndian.PutUint64(raw, count)
		if err := refs.Put([]byte(authority), raw); err != nil {
			return fmt.Errorf("crawl frontier: store authority reference: %w", err)
		}
	}
	return nil
}

func validateAuthorityReferences(refs *bolt.Bucket, expected map[string]uint64) error {
	seen := 0
	if err := refs.ForEach(func(key, raw []byte) error {
		count, found := expected[string(key)]
		if !found || len(raw) != 8 || count == 0 || binary.BigEndian.Uint64(raw) != count {
			return fmt.Errorf("%w: invalid authority reference %q", ErrCorrupt, key)
		}
		seen++
		return nil
	}); err != nil {
		return err
	}
	if seen != len(expected) {
		return fmt.Errorf("%w: incomplete authority references", ErrCorrupt)
	}
	return nil
}

func requiredHostsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	bucket := tx.Bucket(hostsBucket)
	if bucket == nil {
		return nil, ErrSchemaMismatch
	}
	return bucket, nil
}

func requiredAuthorityRefsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	bucket := tx.Bucket(authorityRefsBucket)
	if bucket == nil {
		return nil, ErrSchemaMismatch
	}
	return bucket, nil
}

func putHostState(bucket *bolt.Bucket, state hostState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("%w: encode host state: %v", ErrCorrupt, err)
	}
	if err := bucket.Put([]byte(state.Authority), raw); err != nil {
		return fmt.Errorf("crawl frontier: store host state: %w", err)
	}
	return nil
}

func decodeHostState(key, raw []byte) (hostState, error) {
	var state hostState
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return hostState{}, fmt.Errorf("%w: decode host %q: %v", ErrCorrupt, key, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return hostState{}, fmt.Errorf("%w: trailing host data %q", ErrCorrupt, key)
	}
	if state.Authority != string(key) || validateCanonicalAuthority(state.Authority) != nil || state.LastDispatch.IsZero() || state.Interval <= 0 || state.Interval > maxHostInterval {
		return hostState{}, fmt.Errorf("%w: invalid host state %q", ErrCorrupt, key)
	}
	return state, nil
}
