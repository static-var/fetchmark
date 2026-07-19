package indexpack

import (
	"crypto/ed25519"
	"crypto/subtle"
	"errors"
	"fmt"
	"time"
)

var ErrManifestRejected = errors.New("index pack: manifest rejected")

// Acceptance binds a valid publisher signature to the exact artifact and
// operator-selected pack identity. Persisting the last accepted revision and
// constructing the next policy remains the registry adapter's responsibility.
type Acceptance struct {
	Now                    time.Time
	ExpectedPackID         string
	ExpectedManifestSHA256 string
	ExpectedKeyID          string
	MinimumRevision        uint64
	ExpectedRevision       uint64
	ExpectedRecordCount    uint64
	ExpectedCreatedAt      string
	ExpectedExpiresAt      string
}

func VerifyManifestFor(manifestBytes, signatureBytes []byte, trustedKeys map[string]ed25519.PublicKey, policy Acceptance) (VerifiedManifest, error) {
	if err := policy.validate(); err != nil {
		return VerifiedManifest{}, reject(err)
	}
	if len(manifestBytes) == 0 || len(manifestBytes) > MaxManifestBytes {
		return VerifiedManifest{}, invalidManifest(fmt.Errorf("manifest size must be 1..%d bytes", MaxManifestBytes))
	}
	actualDigest := ManifestDigest(manifestBytes)
	if !sameDigest(actualDigest, policy.ExpectedManifestSHA256) {
		return VerifiedManifest{}, reject(errors.New("manifest digest does not match operator expectation"))
	}
	verified, err := VerifyManifest(manifestBytes, signatureBytes, trustedKeys)
	if err != nil {
		return VerifiedManifest{}, err
	}
	if verified.Manifest.PackID != policy.ExpectedPackID {
		return VerifiedManifest{}, reject(errors.New("pack_id does not match operator expectation"))
	}
	if !sameDigest(verified.KeyID, policy.ExpectedKeyID) {
		return VerifiedManifest{}, reject(errors.New("key_id does not match operator expectation"))
	}
	if verified.Manifest.Revision < policy.MinimumRevision {
		return VerifiedManifest{}, reject(fmt.Errorf("revision %d is below minimum %d", verified.Manifest.Revision, policy.MinimumRevision))
	}
	if policy.ExpectedRevision != 0 && verified.Manifest.Revision != policy.ExpectedRevision {
		return VerifiedManifest{}, reject(fmt.Errorf("revision is %d, expected %d", verified.Manifest.Revision, policy.ExpectedRevision))
	}
	if verified.Manifest.RecordCount != policy.ExpectedRecordCount {
		return VerifiedManifest{}, reject(fmt.Errorf("record_count is %d, expected %d", verified.Manifest.RecordCount, policy.ExpectedRecordCount))
	}
	if verified.Manifest.CreatedAt != policy.ExpectedCreatedAt {
		return VerifiedManifest{}, reject(errors.New("created_at does not match operator expectation"))
	}
	if verified.Manifest.ExpiresAt != policy.ExpectedExpiresAt {
		return VerifiedManifest{}, reject(errors.New("expires_at does not match operator expectation"))
	}
	created, _ := time.Parse(time.RFC3339, verified.Manifest.CreatedAt)
	expires, _ := time.Parse(time.RFC3339, verified.Manifest.ExpiresAt)
	now := policy.Now.UTC()
	if now.Before(created) {
		return VerifiedManifest{}, reject(errors.New("manifest is not valid yet"))
	}
	if !now.Before(expires) {
		return VerifiedManifest{}, reject(errors.New("manifest has expired"))
	}
	return verified, nil
}

func (policy Acceptance) validate() error {
	if policy.Now.IsZero() {
		return errors.New("acceptance time is required")
	}
	if !packIDPattern.MatchString(policy.ExpectedPackID) {
		return errors.New("expected pack ID is invalid")
	}
	if err := validateDigest(policy.ExpectedManifestSHA256); err != nil {
		return fmt.Errorf("expected manifest digest: %w", err)
	}
	if err := validateDigest(policy.ExpectedKeyID); err != nil {
		return fmt.Errorf("expected key ID: %w", err)
	}
	if policy.ExpectedRevision != 0 && policy.ExpectedRevision < policy.MinimumRevision {
		return errors.New("expected revision must not be below minimum revision")
	}
	if policy.ExpectedRecordCount == 0 || policy.ExpectedRecordCount > MaxPackRecords {
		return fmt.Errorf("expected record count must be 1..%d", MaxPackRecords)
	}
	createdAtErr := validateTimestamp(policy.ExpectedCreatedAt)
	expiresAtErr := validateTimestamp(policy.ExpectedExpiresAt)
	if createdAtErr != nil || expiresAtErr != nil {
		return errors.New("expected created and expiry times must be valid manifest timestamps")
	}
	createdAt, _ := time.Parse(time.RFC3339, policy.ExpectedCreatedAt)
	expiresAt, _ := time.Parse(time.RFC3339, policy.ExpectedExpiresAt)
	if !expiresAt.After(createdAt) || expiresAt.Sub(createdAt) > 365*24*time.Hour {
		return errors.New("expected expiry must be after creation and no more than 365 days later")
	}
	return nil
}

func sameDigest(left, right string) bool {
	return len(left) == 64 && len(right) == 64 && subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func reject(err error) error {
	return fmt.Errorf("%w: %v", ErrManifestRejected, err)
}
