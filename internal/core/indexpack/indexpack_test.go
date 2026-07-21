package indexpack

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestManifestRoundTripAndExactByteSignature(t *testing.T) {
	if ManifestSignatureDomain != "fetchmark-open-index-pack-manifest-v1\n" {
		t.Fatalf("unexpected signature domain %q", ManifestSignatureDomain)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	manifest := validManifest()
	manifest.SigningKeyID = KeyID(publicKey)
	raw, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	if bytes.Contains(raw, []byte("\n")) {
		t.Fatalf("manifest encoding is not compact: %q", raw)
	}
	signature, err := SignManifest(raw, privateKey)
	if err != nil {
		t.Fatalf("SignManifest: %v", err)
	}
	signatureRaw, err := EncodeSignature(signature)
	if err != nil {
		t.Fatalf("EncodeSignature: %v", err)
	}
	trusted := map[string]ed25519.PublicKey{KeyID(publicKey): publicKey}
	verified, err := VerifyManifest(raw, signatureRaw, trusted)
	if err != nil {
		t.Fatalf("VerifyManifest: %v", err)
	}
	if verified.Digest != Digest(raw) || verified.Manifest.PackID != manifest.PackID {
		t.Fatalf("unexpected verification result: %#v", verified)
	}

	tampered := append([]byte(nil), raw...)
	tampered = append(tampered, ' ')
	if _, err := VerifyManifest(tampered, signatureRaw, trusted); err == nil {
		t.Fatal("VerifyManifest accepted bytes not covered by the signature")
	}
}

func TestVersionOneManifestGoldenBytesAndSignature(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = byte(index)
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	manifest := validManifest()
	manifest.SigningKeyID = KeyID(publicKey)
	raw, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	const expectedManifest = `{"version":1,"kind":"snapshot","pack_id":"developer-en","revision":1,"created_at":"2026-07-18T00:00:00Z","expires_at":"2026-08-18T00:00:00Z","signing_key_id":"56475aa75463474c0285df5dbf2bcab73da651358839e9b77481b2eab107708c","publisher":{"name":"Example Publisher","contact_uri":"mailto:operator@example.com","takedown_uri":"https://example.com/takedown","rights_notice":"CC0-1.0"},"policy":{"robots":"rfc9309","noindex":"exclude","body_distribution":"forbidden"},"languages":["en"],"record_count":1,"shards":[{"path":"shards/sha256/1111111111111111111111111111111111111111111111111111111111111111.ndjson.zst","compression":"zstd","sha256":"1111111111111111111111111111111111111111111111111111111111111111","compressed_size_bytes":80,"uncompressed_sha256":"5555555555555555555555555555555555555555555555555555555555555555","uncompressed_size_bytes":100,"record_count":1}],"build":{"generator":"fetchmark-pack-builder","generator_version":"1.0.0","analyzer":"unicode-lexical","analyzer_version":"1.0.0","policy_sha256":"6666666666666666666666666666666666666666666666666666666666666666","exclusions_sha256":"7777777777777777777777777777777777777777777777777777777777777777","inputs":[{"name":"example-input","uri":"https://example.com/input","retrieved_at":"2026-07-17T00:00:00Z","sha256":"2222222222222222222222222222222222222222222222222222222222222222","rights_notice":"source metadata used under CC0-1.0"}]}}`
	if string(raw) != expectedManifest {
		t.Fatalf("v1 manifest bytes changed:\n%s", raw)
	}
	signature, err := SignManifest(raw, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := EncodeSignature(signature)
	if err != nil {
		t.Fatal(err)
	}
	const expectedSignature = `{"version":1,"algorithm":"ed25519","key_id":"56475aa75463474c0285df5dbf2bcab73da651358839e9b77481b2eab107708c","signature":"OSg+fzYnxvX1YmHfDSQ/T2LcphTa8jSnAW+zHXiChGIDJZspUE25iea/w1BxX8A8WCBQ2DZam/rPEioBdDFQCA=="}`
	if string(signatureRaw) != expectedSignature {
		t.Fatalf("v1 signature bytes changed:\n%s", signatureRaw)
	}
}

func TestDecodeManifestRejectsDuplicateUnknownNullAndUnsafePath(t *testing.T) {
	raw, err := EncodeManifest(validManifest())
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}

	tests := map[string][]byte{
		"duplicate key": bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"unknown field": bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"surprise":true`), 1),
		"null":          bytes.Replace(raw, []byte(`"rights_notice":"CC0-1.0"`), []byte(`"rights_notice":null`), 1),
		"unsafe path":   bytes.Replace(raw, []byte(`shards/sha256/`), []byte(`../sha256/`), 1),
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeManifest(candidate); err == nil {
				t.Fatalf("DecodeManifest accepted %s", name)
			}
		})
	}
}

func TestDecodeManifestVersionOneRejectsPresentCandidateDigest(t *testing.T) {
	raw, err := EncodeManifest(validManifest())
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	raw = bytes.Replace(raw, []byte(`"inputs":`), []byte(`"candidate_sha256":"","inputs":`), 1)
	if _, err := DecodeManifest(raw); err == nil {
		t.Fatal("v1 manifest accepted an explicitly present candidate_sha256")
	}
}

func TestDecodeManifestVersionTwoRequiresPresentCandidateDigest(t *testing.T) {
	raw, err := EncodeManifest(validManifest())
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":2`), 1)
	if _, err := DecodeManifest(raw); err == nil || !strings.Contains(err.Error(), "candidate_sha256 is required") {
		t.Fatalf("v2 manifest without candidate_sha256 error = %v", err)
	}
}

func TestDeltaRequiresParentAndSnapshotForbidsIt(t *testing.T) {
	manifest := validManifest()
	manifest.Kind = KindDelta
	manifest.Revision = 2
	if _, err := EncodeManifest(manifest); err == nil {
		t.Fatal("delta without parent digest was accepted")
	}
	manifest.ParentManifestSHA256 = strings.Repeat("a", 64)
	if _, err := EncodeManifest(manifest); err != nil {
		t.Fatalf("valid delta rejected: %v", err)
	}
	manifest.Kind = KindSnapshot
	if _, err := EncodeManifest(manifest); err == nil {
		t.Fatal("snapshot with parent digest was accepted")
	}
}

func TestValidateDeltaTransitionRequiresExactSafeParentChain(t *testing.T) {
	parentManifest := validManifest()
	parent := VerifiedManifest{Manifest: parentManifest, Digest: strings.Repeat("a", 64), KeyID: parentManifest.SigningKeyID}
	deltaManifest := parentManifest
	deltaManifest.Kind = KindDelta
	deltaManifest.Revision = 2
	deltaManifest.CreatedAt = "2026-07-19T00:00:00Z"
	deltaManifest.ExpiresAt = "2026-08-17T00:00:00Z"
	deltaManifest.ParentManifestSHA256 = parent.Digest
	delta := VerifiedManifest{Manifest: deltaManifest, Digest: strings.Repeat("b", 64), KeyID: deltaManifest.SigningKeyID}
	if err := ValidateDeltaTransition(parent, delta); err != nil {
		t.Fatalf("valid transition rejected: %v", err)
	}

	tests := map[string]func(*VerifiedManifest, *VerifiedManifest){
		"target snapshot": func(_ *VerifiedManifest, candidate *VerifiedManifest) {
			candidate.Manifest.Kind = KindSnapshot
			candidate.Manifest.ParentManifestSHA256 = ""
		},
		"parent digest": func(_ *VerifiedManifest, candidate *VerifiedManifest) {
			candidate.Manifest.ParentManifestSHA256 = strings.Repeat("c", 64)
		},
		"revision gap": func(_ *VerifiedManifest, candidate *VerifiedManifest) { candidate.Manifest.Revision++ },
		"schema": func(_ *VerifiedManifest, candidate *VerifiedManifest) {
			candidate.Manifest.Version = VersionURLMetadata
		},
		"pack": func(_ *VerifiedManifest, candidate *VerifiedManifest) { candidate.Manifest.PackID = "other-pack" },
		"signing key": func(_ *VerifiedManifest, candidate *VerifiedManifest) {
			candidate.KeyID = strings.Repeat("d", 64)
			candidate.Manifest.SigningKeyID = candidate.KeyID
		},
		"languages": func(_ *VerifiedManifest, candidate *VerifiedManifest) { candidate.Manifest.Languages = []string{"fr"} },
		"publisher metadata": func(_ *VerifiedManifest, candidate *VerifiedManifest) {
			candidate.Manifest.Publisher.ContactURI = "mailto:other-publisher@example.org"
		},
		"policy": func(_ *VerifiedManifest, candidate *VerifiedManifest) { candidate.Manifest.Policy.NoIndex = "include" },
		"created before parent": func(_ *VerifiedManifest, candidate *VerifiedManifest) {
			candidate.Manifest.CreatedAt = "2026-07-17T00:00:00Z"
		},
		"created after parent expiry": func(parent *VerifiedManifest, candidate *VerifiedManifest) {
			candidate.Manifest.CreatedAt = parent.Manifest.ExpiresAt
		},
		"extends parent expiry": func(_ *VerifiedManifest, candidate *VerifiedManifest) {
			candidate.Manifest.ExpiresAt = "2026-08-19T00:00:00Z"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidateParent, candidateDelta := parent, delta
			candidateParent.Manifest.Languages = append([]string(nil), parent.Manifest.Languages...)
			candidateDelta.Manifest.Languages = append([]string(nil), delta.Manifest.Languages...)
			mutate(&candidateParent, &candidateDelta)
			if err := ValidateDeltaTransition(candidateParent, candidateDelta); !errors.Is(err, ErrInvalidDeltaTransition) {
				t.Fatalf("ValidateDeltaTransition() = %v, want ErrInvalidDeltaTransition", err)
			}
		})
	}
}

func TestVerifyShardChecksDigestCountDuplicatesAndOperations(t *testing.T) {
	first := validRecord("https://example.com/a")
	second := validRecord("https://example.com/b")
	raw := encodeRecords(t, first, second)
	shard, compressed := shardForRecords(raw, 2)
	if err := VerifyCompressedShard(bytes.NewReader(compressed), shard); err != nil {
		t.Fatalf("VerifyCompressedShard: %v", err)
	}
	corrupted := append([]byte(nil), compressed...)
	corrupted[0] ^= 1
	if err := VerifyCompressedShard(bytes.NewReader(corrupted), shard); err == nil {
		t.Fatal("VerifyCompressedShard accepted corrupted bytes")
	}
	records, err := VerifyRecordStream(bytes.NewReader(raw), KindSnapshot, shard)
	if err != nil {
		t.Fatalf("VerifyRecordStream: %v", err)
	}
	if len(records) != 2 || records[1].URL != second.URL {
		t.Fatalf("unexpected records: %#v", records)
	}

	duplicateRaw := encodeRecords(t, first, first)
	duplicateShard, _ := shardForRecords(duplicateRaw, 2)
	if _, err := VerifyRecordStream(bytes.NewReader(duplicateRaw), KindSnapshot, duplicateShard); err == nil {
		t.Fatal("duplicate URL was accepted")
	}

	tombstone := Record{Operation: OperationTombstone, URL: first.URL}
	tombstoneRaw := encodeRecords(t, tombstone)
	tombstoneShard, _ := shardForRecords(tombstoneRaw, 1)
	if _, err := VerifyRecordStream(bytes.NewReader(tombstoneRaw), KindSnapshot, tombstoneShard); err == nil {
		t.Fatal("snapshot tombstone was accepted")
	}
	if _, err := VerifyRecordStream(bytes.NewReader(tombstoneRaw), KindDelta, tombstoneShard); err != nil {
		t.Fatalf("delta tombstone rejected: %v", err)
	}
}

func TestURLMetadataVersionRequiresURLOnlyRecordsWithoutRelaxingV1(t *testing.T) {
	record := validRecord("https://docs.example.com/open/search")
	record.Title = ""
	record.Headings = nil
	record.AnchorTerms = nil
	record.SalientSketch = ""
	record.PublishedAt = ""
	record.ContentSHA256 = ""
	record.AuthorityScore = 0
	raw := encodeRecords(t, record)
	shard, _ := shardForRecords(raw, 1)
	if _, err := VerifyRecordStream(bytes.NewReader(raw), KindSnapshot, shard); err == nil {
		t.Fatal("v1 accepted URL-only record")
	}
	records, err := VerifyRecordStreamVersion(bytes.NewReader(raw), VersionURLMetadata, KindSnapshot, shard)
	if err != nil || len(records) != 1 || records[0].URL != record.URL {
		t.Fatalf("v2 URL-only record = %#v, %v", records, err)
	}
	enriched := record
	enriched.Title = "Publisher-supplied title"
	enrichedRaw := encodeRecords(t, enriched)
	enrichedShard, _ := shardForRecords(enrichedRaw, 1)
	if _, err := VerifyRecordStreamVersion(bytes.NewReader(enrichedRaw), VersionURLMetadata, KindSnapshot, enrichedShard); err == nil {
		t.Fatal("v2 accepted enriched record")
	}
	queried := record
	queried.URL = "https://docs.example.com/open/search?session=private"
	queriedRaw := encodeRecords(t, queried)
	queriedShard, _ := shardForRecords(queriedRaw, 1)
	if _, err := VerifyRecordStreamVersion(bytes.NewReader(queriedRaw), VersionURLMetadata, KindSnapshot, queriedShard); err == nil {
		t.Fatal("v2 accepted query-bearing URL")
	}
	manifest := validManifest()
	manifest.Version = VersionURLMetadata
	manifest.Build.CandidateSHA256 = strings.Repeat("8", 64)
	if _, err := EncodeManifest(manifest); err != nil {
		t.Fatalf("v2 manifest rejected: %v", err)
	}
	manifest.Version = VersionURLMetadata + 1
	if _, err := EncodeManifest(manifest); err == nil {
		t.Fatal("unknown manifest version accepted")
	}
}

func TestRecordRejectsNonCanonicalURLAndBodyField(t *testing.T) {
	record := validRecord("https://example.com/a?utm_source=test")
	raw := encodeRecords(t, record)
	shard, _ := shardForRecords(raw, 1)
	if _, err := VerifyRecordStream(bytes.NewReader(raw), KindSnapshot, shard); err == nil {
		t.Fatal("non-canonical URL was accepted")
	}

	raw = []byte(`{"operation":"upsert","url":"https://example.com/a","title":"A","language":"en","body":"page body"}` + "\n")
	shard, _ = shardForRecords(raw, 1)
	if _, err := VerifyRecordStream(bytes.NewReader(raw), KindSnapshot, shard); err == nil {
		t.Fatal("page body field was accepted")
	}
}

func TestDeltaTombstoneRejectsExplicitZeroValuedMetadata(t *testing.T) {
	raw := []byte(`{"operation":"tombstone","url":"https://example.com/a","title":"","authority_score":0}` + "\n")
	shard, _ := shardForRecords(raw, 1)
	if err := ScanRecordStream(context.Background(), bytes.NewReader(raw), KindDelta, shard, func(Record) error { return nil }); err == nil {
		t.Fatal("tombstone with explicitly present metadata fields was accepted")
	}
}

func TestTrustedKeyIDMustMatchKeyMaterial(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	manifest := validManifest()
	manifest.SigningKeyID = KeyID(publicKey)
	raw, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	signature, err := SignManifest(raw, privateKey)
	if err != nil {
		t.Fatalf("SignManifest: %v", err)
	}
	signatureRaw, err := EncodeSignature(signature)
	if err != nil {
		t.Fatalf("EncodeSignature: %v", err)
	}
	if _, err := VerifyManifest(raw, signatureRaw, map[string]ed25519.PublicKey{strings.Repeat("b", 64): publicKey}); err == nil {
		t.Fatal("mismatched trusted key ID was accepted")
	}
}

func TestSignatureEnvelopeIsStrictAndBounded(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	manifest := validManifest()
	manifest.SigningKeyID = KeyID(publicKey)
	raw, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	signature, err := SignManifest(raw, privateKey)
	if err != nil {
		t.Fatalf("SignManifest: %v", err)
	}
	signatureRaw, err := EncodeSignature(signature)
	if err != nil {
		t.Fatalf("EncodeSignature: %v", err)
	}
	trusted := map[string]ed25519.PublicKey{KeyID(publicKey): publicKey}

	duplicate := bytes.Replace(signatureRaw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
	if _, err := VerifyManifest(raw, duplicate, trusted); err == nil {
		t.Fatal("duplicate signature key was accepted")
	}
	nullValue := bytes.Replace(signatureRaw, []byte(`"algorithm":"ed25519"`), []byte(`"algorithm":null`), 1)
	if _, err := VerifyManifest(raw, nullValue, trusted); err == nil {
		t.Fatal("null signature value was accepted")
	}
	tooLarge := make([]byte, MaxManifestBytes+1)
	if _, err := VerifyManifest(tooLarge, signatureRaw, trusted); err == nil {
		t.Fatal("oversized manifest was accepted")
	}
}

func TestShardRejectsDuplicateJSONKeyAndDescriptorMismatch(t *testing.T) {
	raw := []byte(`{"operation":"upsert","operation":"upsert","url":"https://example.com/a","title":"A","language":"en","fetched_at":"2026-07-18T00:00:00Z","provenance":[{"source":"input","source_uri":"https://example.com/input","retrieved_at":"2026-07-18T00:00:00Z","rights_notice":"CC0-1.0"}]}` + "\n")
	shard, _ := shardForRecords(raw, 1)
	if _, err := VerifyRecordStream(bytes.NewReader(raw), KindSnapshot, shard); err == nil {
		t.Fatal("duplicate record key was accepted")
	}

	validRaw := encodeRecords(t, validRecord("https://example.com/a"))
	shard, _ = shardForRecords(validRaw, 1)
	shard.UncompressedSizeBytes++
	if _, err := VerifyRecordStream(bytes.NewReader(validRaw), KindSnapshot, shard); err == nil {
		t.Fatal("descriptor size mismatch was accepted")
	}
}

func TestManifestRequiresSortedUniqueMetadata(t *testing.T) {
	manifest := validManifest()
	manifest.Languages = []string{"fr", "en"}
	if _, err := EncodeManifest(manifest); err == nil {
		t.Fatal("unsorted languages were accepted")
	}
	manifest = validManifest()
	manifest.Build.Inputs = append(manifest.Build.Inputs, manifest.Build.Inputs[0])
	if _, err := EncodeManifest(manifest); err == nil {
		t.Fatal("duplicate build input was accepted")
	}
}

func TestVersion2ManifestSeparatesSourceAndCandidateDigests(t *testing.T) {
	manifest := validManifest()
	manifest.Build.CandidateSHA256 = strings.Repeat("8", 64)
	if _, err := EncodeManifest(manifest); err == nil {
		t.Fatal("version 1 accepted candidate_sha256")
	}
	manifest = validManifest()
	manifest.Version = VersionURLMetadata
	if _, err := EncodeManifest(manifest); err == nil {
		t.Fatal("version 2 accepted missing candidate_sha256")
	}
	manifest = validManifest()
	manifest.Version = VersionURLMetadata
	manifest.Build.CandidateSHA256 = strings.Repeat("8", 64)
	manifest.Build.Inputs[0].SHA256 = strings.Repeat("9", 64)
	raw, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Build.CandidateSHA256 != strings.Repeat("8", 64) || decoded.Build.Inputs[0].SHA256 != strings.Repeat("9", 64) {
		t.Fatalf("build provenance=%#v", decoded.Build)
	}
}

func TestVerifyManifestForEnforcesTimeAndOperatorBindings(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	manifest := validManifest()
	manifest.SigningKeyID = KeyID(publicKey)
	raw, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	signature, err := SignManifest(raw, privateKey)
	if err != nil {
		t.Fatalf("SignManifest: %v", err)
	}
	signatureRaw, err := EncodeSignature(signature)
	if err != nil {
		t.Fatalf("EncodeSignature: %v", err)
	}
	trusted := map[string]ed25519.PublicKey{KeyID(publicKey): publicKey}
	policy := Acceptance{
		Now:                    time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC),
		ExpectedPackID:         manifest.PackID,
		ExpectedManifestSHA256: ManifestDigest(raw),
		ExpectedKeyID:          KeyID(publicKey),
		MinimumRevision:        1,
		ExpectedRevision:       1,
		ExpectedRecordCount:    manifest.RecordCount,
		ExpectedCreatedAt:      manifest.CreatedAt,
		ExpectedExpiresAt:      manifest.ExpiresAt,
	}
	if _, err := VerifyManifestFor(raw, signatureRaw, trusted, policy); err != nil {
		t.Fatalf("VerifyManifestFor: %v", err)
	}

	tests := map[string]func(*Acceptance){
		"future created": func(candidate *Acceptance) { candidate.Now = time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC) },
		"expired":        func(candidate *Acceptance) { candidate.Now = time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC) },
		"digest":         func(candidate *Acceptance) { candidate.ExpectedManifestSHA256 = strings.Repeat("a", 64) },
		"pack":           func(candidate *Acceptance) { candidate.ExpectedPackID = "other-pack" },
		"key":            func(candidate *Acceptance) { candidate.ExpectedKeyID = strings.Repeat("b", 64) },
		"revision":       func(candidate *Acceptance) { candidate.ExpectedRevision = 2 },
		"minimum":        func(candidate *Acceptance) { candidate.MinimumRevision = 2; candidate.ExpectedRevision = 0 },
		"record count":   func(candidate *Acceptance) { candidate.ExpectedRecordCount++ },
		"created at":     func(candidate *Acceptance) { candidate.ExpectedCreatedAt = "2026-07-17T00:00:00Z" },
		"expires at":     func(candidate *Acceptance) { candidate.ExpectedExpiresAt = "2026-08-17T00:00:00Z" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := policy
			mutate(&candidate)
			if _, err := VerifyManifestFor(raw, signatureRaw, trusted, candidate); !errors.Is(err, ErrManifestRejected) {
				t.Fatalf("got %v, want ErrManifestRejected", err)
			}
		})
	}
}

func TestScanRecordStreamHonorsCancellation(t *testing.T) {
	raw := encodeRecords(t, validRecord("https://example.com/a"), validRecord("https://example.com/b"))
	shard, _ := shardForRecords(raw, 2)
	ctx, cancel := context.WithCancel(context.Background())
	visits := 0
	err := ScanRecordStream(ctx, bytes.NewReader(raw), KindSnapshot, shard, func(Record) error {
		visits++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || visits != 1 {
		t.Fatalf("ScanRecordStream = (%v, %d visits), want context cancellation after one", err, visits)
	}
}

func TestBufferedRecordHelperRejectsUnboundedPreallocation(t *testing.T) {
	raw := encodeRecords(t, validRecord("https://example.com/a"))
	shard, _ := shardForRecords(raw, MaxShardRecords+1)
	if _, err := VerifyRecordStream(bytes.NewReader(raw), KindSnapshot, shard); !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("VerifyRecordStream = %v, want ErrInvalidShard", err)
	}
}

func TestAcceptanceRejectsOversizedManifestBeforeVerification(t *testing.T) {
	oversized := make([]byte, MaxManifestBytes+1)
	policy := Acceptance{
		Now:                    time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC),
		ExpectedPackID:         "developer-en",
		ExpectedManifestSHA256: strings.Repeat("a", 64),
		ExpectedKeyID:          strings.Repeat("b", 64),
		ExpectedRecordCount:    1,
		ExpectedCreatedAt:      "2026-07-18T00:00:00Z",
		ExpectedExpiresAt:      "2026-08-18T00:00:00Z",
	}
	if _, err := VerifyManifestFor(oversized, nil, nil, policy); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("VerifyManifestFor = %v, want ErrInvalidManifest", err)
	}
}

func validManifest() Manifest {
	digest := strings.Repeat("1", 64)
	return Manifest{
		Version:      1,
		Kind:         KindSnapshot,
		PackID:       "developer-en",
		Revision:     1,
		CreatedAt:    "2026-07-18T00:00:00Z",
		ExpiresAt:    "2026-08-18T00:00:00Z",
		SigningKeyID: strings.Repeat("4", 64),
		Publisher: Publisher{
			Name:         "Example Publisher",
			ContactURI:   "mailto:operator@example.com",
			TakedownURI:  "https://example.com/takedown",
			RightsNotice: "CC0-1.0",
		},
		Policy:      Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages:   []string{"en"},
		RecordCount: 1,
		Shards: []Shard{{
			Path:                  ShardPath(digest),
			Compression:           "zstd",
			SHA256:                digest,
			CompressedSizeBytes:   80,
			UncompressedSHA256:    strings.Repeat("5", 64),
			UncompressedSizeBytes: 100,
			RecordCount:           1,
		}},
		Build: Build{
			Generator:        "fetchmark-pack-builder",
			GeneratorVersion: "1.0.0",
			Analyzer:         "unicode-lexical",
			AnalyzerVersion:  "1.0.0",
			PolicySHA256:     strings.Repeat("6", 64),
			ExclusionsSHA256: strings.Repeat("7", 64),
			Inputs: []BuildInput{{
				Name:         "example-input",
				URI:          "https://example.com/input",
				RetrievedAt:  "2026-07-17T00:00:00Z",
				SHA256:       strings.Repeat("2", 64),
				RightsNotice: "source metadata used under CC0-1.0",
			}},
		},
	}
}

func shardForRecords(raw []byte, count uint64) (Shard, []byte) {
	compressed := []byte("zstd-test-fixture:" + ShardDigest(raw))
	compressedDigest := ShardDigest(compressed)
	return Shard{
		Path:                  ShardPath(compressedDigest),
		Compression:           "zstd",
		SHA256:                compressedDigest,
		CompressedSizeBytes:   uint64(len(compressed)),
		UncompressedSHA256:    ShardDigest(raw),
		UncompressedSizeBytes: uint64(len(raw)),
		RecordCount:           count,
	}, compressed
}

func validRecord(rawURL string) Record {
	return Record{
		Operation:      OperationUpsert,
		URL:            rawURL,
		Title:          "Example",
		Headings:       []string{"Overview"},
		AnchorTerms:    []string{"example"},
		SalientSketch:  "A short discovery sketch, not a page body.",
		Language:       "en",
		PublishedAt:    "2026-07-17T00:00:00Z",
		FetchedAt:      "2026-07-18T00:00:00Z",
		ContentSHA256:  strings.Repeat("3", 64),
		AuthorityScore: 5000,
		FreshnessScore: 9000,
		Provenance: []Provenance{{
			Source:       "example-input",
			SourceURI:    "https://example.com/input",
			RetrievedAt:  "2026-07-18T00:00:00Z",
			RightsNotice: "source metadata used under CC0-1.0",
		}},
	}
}

func encodeRecords(t *testing.T, records ...Record) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			t.Fatalf("Encode record: %v", err)
		}
	}
	return buffer.Bytes()
}
