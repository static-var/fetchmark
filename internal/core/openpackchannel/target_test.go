package openpackchannel

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestDecodeCustomRejectsAmbiguousAndInvalidTargets(t *testing.T) {
	digest := strings.Repeat("a", 64)
	tests := []string{
		`{}`,
		`{"fetchmark_open_pack":{"schema":2,"pack_id":"developer-en","kind":"snapshot","revision":1,"manifest_sha256":"` + digest + `"}}`,
		`{"fetchmark_open_pack":{"schema":1,"schema":1,"pack_id":"developer-en","kind":"snapshot","revision":1,"manifest_sha256":"` + digest + `"}}`,
		`{"fetchmark_open_pack":{"schema":1,"pack_id":"developer-en","kind":"snapshot","revision":1,"manifest_sha256":"` + digest + `","unknown":true}}`,
		`{"fetchmark_open_pack":{"schema":1,"pack_id":"developer-en","kind":"delta","revision":2,"manifest_sha256":"` + digest + `"}}`,
	}
	for _, raw := range tests {
		value := json.RawMessage(raw)
		if _, err := DecodeCustom(&value); !errors.Is(err, ErrInvalidTarget) {
			t.Fatalf("DecodeCustom(%s) = %v, want ErrInvalidTarget", raw, err)
		}
	}
}

func TestEncodeCustomRoundTripsDeterministically(t *testing.T) {
	target := Target{
		Schema: Schema, PackID: "developer-en", Kind: indexpack.KindDelta, Revision: 4,
		ManifestSHA256: strings.Repeat("a", 64), ParentManifestSHA256: strings.Repeat("b", 64),
	}
	raw, err := EncodeCustom(target)
	if err != nil {
		t.Fatalf("EncodeCustom: %v", err)
	}
	decoded, err := DecodeCustom(&raw)
	if err != nil {
		t.Fatalf("DecodeCustom: %v", err)
	}
	if decoded != target {
		t.Fatalf("round trip = %#v, want %#v", decoded, target)
	}
	want := "{\"fetchmark_open_pack\":{\"schema\":1,\"pack_id\":\"developer-en\",\"kind\":\"delta\",\"revision\":4,\"manifest_sha256\":\"" + strings.Repeat("a", 64) + "\",\"parent_manifest_sha256\":\"" + strings.Repeat("b", 64) + "\"}}"
	if string(raw) != want {
		t.Fatalf("encoded custom = %s", raw)
	}
}

func TestTargetVerifyManifestRejectsIdentityTimeAndDigestMismatch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := validChannelManifest(now)
	raw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	target := Target{
		Schema: Schema, PackID: manifest.PackID, Kind: manifest.Kind, Revision: manifest.Revision,
		ManifestSHA256: indexpack.ManifestDigest(raw),
	}
	if _, err := target.VerifyManifest(raw, now); err != nil {
		t.Fatalf("VerifyManifest: %v", err)
	}

	wrong := target
	wrong.Revision++
	if _, err := wrong.VerifyManifest(raw, now); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("identity mismatch = %v", err)
	}
	wrong = target
	wrong.ManifestSHA256 = strings.Repeat("b", 64)
	if _, err := wrong.VerifyManifest(raw, now); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("digest mismatch = %v", err)
	}
	if _, err := target.VerifyManifest(raw, now.Add(-2*time.Hour)); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("not-yet-valid = %v", err)
	}
	if _, err := target.VerifyManifest(raw, now.Add(2*time.Hour)); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("expired = %v", err)
	}
}

func validChannelManifest(now time.Time) indexpack.Manifest {
	shardDigest := strings.Repeat("1", 64)
	return indexpack.Manifest{
		Version: indexpack.Version, Kind: indexpack.KindSnapshot, PackID: "developer-en", Revision: 1,
		CreatedAt: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		SigningKeyID: strings.Repeat("4", 64),
		Publisher: indexpack.Publisher{
			Name: "Example Publisher", ContactURI: "mailto:operator@example.com",
			TakedownURI: "https://example.com/takedown", RightsNotice: "CC0-1.0",
		},
		Policy:    indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages: []string{"en"}, RecordCount: 1,
		Shards: []indexpack.Shard{{
			Path: indexpack.ShardPath(shardDigest), Compression: "zstd", SHA256: shardDigest,
			CompressedSizeBytes: 80, UncompressedSHA256: strings.Repeat("5", 64),
			UncompressedSizeBytes: 100, RecordCount: 1,
		}},
		Build: indexpack.Build{
			Generator: "fetchmark-pack-builder", GeneratorVersion: "1.0.0",
			Analyzer: "unicode-lexical", AnalyzerVersion: "1.0.0",
			PolicySHA256: strings.Repeat("6", 64), ExclusionsSHA256: strings.Repeat("7", 64),
			Inputs: []indexpack.BuildInput{{
				Name: "example-input", URI: "https://example.com/input",
				RetrievedAt: now.Add(-2 * time.Hour).Format(time.RFC3339),
				SHA256:      strings.Repeat("2", 64), RightsNotice: "source metadata used under CC0-1.0",
			}},
		},
	}
}
