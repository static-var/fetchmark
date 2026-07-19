// Package openpackregistry parses the operator-owned trust and artifact
// bindings used to activate signed open-index packs. It deliberately performs
// no filesystem or network access.
package openpackregistry

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

const (
	Version           = 1
	VersionWithDeltas = 2
	MaxRegistryBytes  = 1 << 20
	MaxPublishers     = 16
	MaxBindings       = 16

	maxIDBytes   = 128
	maxPathBytes = 4_096
)

var (
	ErrInvalidRegistry = errors.New("open pack registry: invalid registry")
	idPattern          = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	digestPattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Registry is an immutable, validated registry. Its key material is exposed
// only through defensive copies returned by TrustedKeys.
type Registry struct {
	bindings   map[string]Binding
	trustedKey map[string]ed25519.PublicKey
	document   registryJSON
}

// Binding binds one configured discovery source to one exact installed pack
// artifact and publisher key.
type Binding struct {
	SourceID             string
	PackID               string
	PublisherID          string
	Kind                 indexpack.Kind
	ManifestSHA256       string
	ParentManifestSHA256 string
	Revision             uint64
	ManifestRecordCount  uint64
	RecordCount          uint64
	CreatedAt            string
	ExpiresAt            string
	InstalledPath        string
	SigningKeyID         string
}

type registryJSON struct {
	Version    int             `json:"version"`
	Publishers []publisherJSON `json:"publishers"`
	Bindings   []bindingJSON   `json:"bindings"`
}

type publisherJSON struct {
	ID               string `json:"id"`
	KeyID            string `json:"key_id"`
	Ed25519PublicKey string `json:"ed25519_public_key"`
}

type bindingJSON struct {
	SourceID              string         `json:"source_id"`
	PackID                string         `json:"pack_id"`
	PublisherID           string         `json:"publisher_id"`
	ManifestSHA256        string         `json:"manifest_sha256"`
	Kind                  indexpack.Kind `json:"kind,omitempty"`
	ParentManifestSHA256  string         `json:"parent_manifest_sha256,omitempty"`
	Revision              uint64         `json:"revision"`
	RecordCount           uint64         `json:"record_count,omitempty"`
	ManifestRecordCount   uint64         `json:"manifest_record_count,omitempty"`
	ProjectionRecordCount uint64         `json:"projection_record_count,omitempty"`
	CreatedAt             string         `json:"created_at"`
	ExpiresAt             string         `json:"expires_at"`
	InstalledPath         string         `json:"installed_path"`
}

type resolvedPublisher struct {
	keyID     string
	publicKey ed25519.PublicKey
}

// Load parses and validates one registry document without consulting the
// filesystem. The input is bounded before JSON decoding.
func Load(reader io.Reader) (Registry, error) {
	if reader == nil {
		return Registry{}, invalid(errors.New("reader is required"))
	}
	raw, err := io.ReadAll(io.LimitReader(reader, MaxRegistryBytes+1))
	if err != nil {
		return Registry{}, invalid(fmt.Errorf("read: %w", err))
	}
	if len(raw) == 0 || len(raw) > MaxRegistryBytes {
		return Registry{}, invalid(fmt.Errorf("size must be 1..%d bytes", MaxRegistryBytes))
	}
	if !utf8.Valid(raw) {
		return Registry{}, invalid(errors.New("JSON must be valid UTF-8"))
	}
	if err := validateJSONShape(raw); err != nil {
		return Registry{}, invalid(err)
	}

	var document registryJSON
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return Registry{}, invalid(err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Registry{}, invalid(errors.New("must contain exactly one JSON value"))
	}
	if err := validateVersionedBindingFields(raw, document.Version); err != nil {
		return Registry{}, invalid(err)
	}
	return build(document)
}

func build(document registryJSON) (Registry, error) {
	if document.Version != Version && document.Version != VersionWithDeltas {
		return Registry{}, invalid(fmt.Errorf("version must be %d or %d", Version, VersionWithDeltas))
	}
	if len(document.Publishers) < 1 || len(document.Publishers) > MaxPublishers {
		return Registry{}, invalid(fmt.Errorf("publishers must contain 1..%d entries", MaxPublishers))
	}
	if len(document.Bindings) < 1 || len(document.Bindings) > MaxBindings {
		return Registry{}, invalid(fmt.Errorf("bindings must contain 1..%d entries", MaxBindings))
	}

	publishers := make(map[string]resolvedPublisher, len(document.Publishers))
	keyIDs := make(map[string]struct{}, len(document.Publishers))
	for index, publisher := range document.Publishers {
		if err := validateID("id", publisher.ID); err != nil {
			return Registry{}, invalid(fmt.Errorf("publishers[%d]: %w", index, err))
		}
		if _, duplicate := publishers[publisher.ID]; duplicate {
			return Registry{}, invalid(fmt.Errorf("publishers[%d]: duplicate id", index))
		}
		if !digestPattern.MatchString(publisher.KeyID) {
			return Registry{}, invalid(fmt.Errorf("publishers[%d]: key_id must be a lowercase SHA-256 digest", index))
		}
		if _, duplicate := keyIDs[publisher.KeyID]; duplicate {
			return Registry{}, invalid(fmt.Errorf("publishers[%d]: duplicate key_id", index))
		}
		publicKey, err := decodePublicKey(publisher.Ed25519PublicKey)
		if err != nil {
			return Registry{}, invalid(fmt.Errorf("publishers[%d]: %w", index, err))
		}
		if indexpack.KeyID(publicKey) != publisher.KeyID {
			return Registry{}, invalid(fmt.Errorf("publishers[%d]: key_id does not match public key", index))
		}
		publishers[publisher.ID] = resolvedPublisher{keyID: publisher.KeyID, publicKey: publicKey}
		keyIDs[publisher.KeyID] = struct{}{}
	}

	registry := Registry{
		bindings:   make(map[string]Binding, len(document.Bindings)),
		trustedKey: make(map[string]ed25519.PublicKey, len(document.Publishers)),
		document:   cloneRegistryDocument(document),
	}
	referenced := make(map[string]struct{}, len(document.Publishers))
	for index, sourceBinding := range document.Bindings {
		if err := validateID("source_id", sourceBinding.SourceID); err != nil {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: %w", index, err))
		}
		if _, duplicate := registry.bindings[sourceBinding.SourceID]; duplicate {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: duplicate source_id", index))
		}
		if err := validateID("pack_id", sourceBinding.PackID); err != nil {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: %w", index, err))
		}
		if err := validateID("publisher_id", sourceBinding.PublisherID); err != nil {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: %w", index, err))
		}
		publisher, exists := publishers[sourceBinding.PublisherID]
		if !exists {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: publisher_id is not defined", index))
		}
		if !digestPattern.MatchString(sourceBinding.ManifestSHA256) {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: manifest_sha256 must be a lowercase SHA-256 digest", index))
		}
		if sourceBinding.Revision == 0 {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: revision must be positive", index))
		}
		kind := sourceBinding.Kind
		manifestRecords := sourceBinding.ManifestRecordCount
		projectionRecords := sourceBinding.ProjectionRecordCount
		if document.Version == Version {
			kind = indexpack.KindSnapshot
			manifestRecords = sourceBinding.RecordCount
			projectionRecords = sourceBinding.RecordCount
		}
		if kind != indexpack.KindSnapshot && kind != indexpack.KindDelta {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: kind must be snapshot or delta", index))
		}
		if manifestRecords == 0 || manifestRecords > indexpack.RecommendedInstallRecordLimit {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: manifest record count must be 1..%d", index, indexpack.RecommendedInstallRecordLimit))
		}
		if projectionRecords == 0 || projectionRecords > indexpack.RecommendedInstallRecordLimit {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: projection record count must be 1..%d", index, indexpack.RecommendedInstallRecordLimit))
		}
		if kind == indexpack.KindSnapshot {
			if sourceBinding.ParentManifestSHA256 != "" || manifestRecords != projectionRecords {
				return Registry{}, invalid(fmt.Errorf("bindings[%d]: snapshot counts must match and parent digest must be absent", index))
			}
		} else {
			if sourceBinding.Revision < 2 || !digestPattern.MatchString(sourceBinding.ParentManifestSHA256) {
				return Registry{}, invalid(fmt.Errorf("bindings[%d]: delta requires revision >= 2 and a parent manifest digest", index))
			}
		}
		createdAt, err := parseTimestamp(sourceBinding.CreatedAt)
		if err != nil {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: created_at: %w", index, err))
		}
		expiresAt, err := parseTimestamp(sourceBinding.ExpiresAt)
		if err != nil {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: expires_at: %w", index, err))
		}
		if !expiresAt.After(createdAt) || expiresAt.Sub(createdAt) > 365*24*time.Hour {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: expires_at must be after created_at and no more than 365 days later", index))
		}
		if err := validateInstalledPath(sourceBinding.InstalledPath, sourceBinding.ManifestSHA256); err != nil {
			return Registry{}, invalid(fmt.Errorf("bindings[%d]: %w", index, err))
		}

		registry.bindings[sourceBinding.SourceID] = Binding{
			SourceID:             sourceBinding.SourceID,
			PackID:               sourceBinding.PackID,
			PublisherID:          sourceBinding.PublisherID,
			Kind:                 kind,
			ManifestSHA256:       sourceBinding.ManifestSHA256,
			ParentManifestSHA256: sourceBinding.ParentManifestSHA256,
			Revision:             sourceBinding.Revision,
			ManifestRecordCount:  manifestRecords,
			RecordCount:          projectionRecords,
			CreatedAt:            sourceBinding.CreatedAt,
			ExpiresAt:            sourceBinding.ExpiresAt,
			InstalledPath:        sourceBinding.InstalledPath,
			SigningKeyID:         publisher.keyID,
		}
		registry.trustedKey[publisher.keyID] = cloneKey(publisher.publicKey)
		referenced[sourceBinding.PublisherID] = struct{}{}
	}
	if len(referenced) != len(publishers) {
		return Registry{}, invalid(errors.New("every publisher must be referenced by a binding"))
	}
	return registry, nil
}

// Lookup returns a copy of the exact source binding.
func (registry Registry) Lookup(sourceID string) (Binding, bool) {
	binding, exists := registry.bindings[sourceID]
	return binding, exists
}

// TrustedKeys returns a new map containing copies of all referenced publisher
// public keys. Mutating it cannot alter the registry.
func (registry Registry) TrustedKeys() map[string]ed25519.PublicKey {
	keys := make(map[string]ed25519.PublicKey, len(registry.trustedKey))
	for keyID, publicKey := range registry.trustedKey {
		keys[keyID] = cloneKey(publicKey)
	}
	return keys
}

// Acceptance constructs the exact operator-selected verification policy for
// this binding. The publisher key was resolved and validated during Load.
func (binding Binding) Acceptance(now time.Time) indexpack.Acceptance {
	return indexpack.Acceptance{
		Now:                    now,
		ExpectedPackID:         binding.PackID,
		ExpectedManifestSHA256: binding.ManifestSHA256,
		ExpectedKeyID:          binding.SigningKeyID,
		MinimumRevision:        binding.Revision,
		ExpectedRevision:       binding.Revision,
		ExpectedRecordCount:    binding.ManifestRecordCount,
		ExpectedCreatedAt:      binding.CreatedAt,
		ExpectedExpiresAt:      binding.ExpiresAt,
	}
}

func validateVersionedBindingFields(raw []byte, version int) error {
	var envelope struct {
		Bindings []map[string]json.RawMessage `json:"bindings"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	for index, fields := range envelope.Bindings {
		_, legacyCount := fields["record_count"]
		_, kind := fields["kind"]
		_, parent := fields["parent_manifest_sha256"]
		_, manifestCount := fields["manifest_record_count"]
		_, projectionCount := fields["projection_record_count"]
		switch version {
		case Version:
			if !legacyCount || kind || parent || manifestCount || projectionCount {
				return fmt.Errorf("bindings[%d]: registry version 1 requires only record_count", index)
			}
		case VersionWithDeltas:
			if legacyCount || !kind || !manifestCount || !projectionCount {
				return fmt.Errorf("bindings[%d]: registry version 2 requires kind, manifest_record_count, and projection_record_count", index)
			}
			var parsedKind indexpack.Kind
			if err := json.Unmarshal(fields["kind"], &parsedKind); err != nil {
				return fmt.Errorf("bindings[%d]: kind is invalid", index)
			}
			if (parsedKind == indexpack.KindDelta) != parent {
				return fmt.Errorf("bindings[%d]: parent_manifest_sha256 must be present exactly for deltas", index)
			}
		}
	}
	return nil
}

func validateID(name, value string) error {
	if len(value) == 0 || len(value) > maxIDBytes || !idPattern.MatchString(value) {
		return fmt.Errorf("%s is invalid", name)
	}
	return nil
}

func parseTimestamp(value string) (time.Time, error) {
	if value == "" || len(value) > len("2006-01-02T15:04:05Z") {
		return time.Time{}, errors.New("must be second-precision RFC 3339 UTC")
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Format(time.RFC3339) != value || parsed.Location() != time.UTC || parsed.Year() < 1970 || parsed.Year() > 2200 {
		return time.Time{}, errors.New("must be second-precision RFC 3339 UTC with year 1970..2200")
	}
	return parsed, nil
}

func decodePublicKey(encoded string) (ed25519.PublicKey, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return nil, errors.New("ed25519_public_key must be canonical standard base64 for 32 bytes")
	}
	if base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New("ed25519_public_key must use canonical standard base64")
	}
	return ed25519.PublicKey(append([]byte(nil), decoded...)), nil
}

func validateInstalledPath(installedPath, digest string) error {
	if len(installedPath) == 0 || len(installedPath) > maxPathBytes || !utf8.ValidString(installedPath) {
		return errors.New("installed_path is invalid")
	}
	if strings.TrimSpace(installedPath) != installedPath || hasUnsafeRune(installedPath) {
		return errors.New("installed_path contains unsafe characters")
	}
	if !filepath.IsAbs(installedPath) || filepath.Clean(installedPath) != installedPath {
		return errors.New("installed_path must be absolute and clean")
	}
	if filepath.Base(installedPath) != digest || filepath.Base(filepath.Dir(installedPath)) != "objects" {
		return errors.New("installed_path must name its manifest object under an objects directory")
	}
	return nil
}

func hasUnsafeRune(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) || unicode.In(character, unicode.Categories["Cf"]) {
			return true
		}
	}
	return false
}

func cloneKey(publicKey ed25519.PublicKey) ed25519.PublicKey {
	return ed25519.PublicKey(append([]byte(nil), publicKey...))
}

func invalid(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidRegistry, err)
}

func validateJSONShape(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return err
	}
	if err := consumeJSONValue(decoder, first, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("must contain exactly one JSON value")
		}
		return err
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, token json.Token, depth int) error {
	if depth > 64 {
		return errors.New("JSON nesting exceeds 64 levels")
	}
	if token == nil {
		return errors.New("null values are forbidden")
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key must be a string")
			}
			if _, duplicate := keys[key]; duplicate {
				return fmt.Errorf("duplicate object key %q", key)
			}
			keys[key] = struct{}{}
			valueToken, err := decoder.Token()
			if err != nil {
				return err
			}
			if err := consumeJSONValue(decoder, valueToken, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return errors.New("object is not properly closed")
		}
	case '[':
		for decoder.More() {
			valueToken, err := decoder.Token()
			if err != nil {
				return err
			}
			if err := consumeJSONValue(decoder, valueToken, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return errors.New("array is not properly closed")
		}
	default:
		return errors.New("unexpected closing delimiter")
	}
	return nil
}
