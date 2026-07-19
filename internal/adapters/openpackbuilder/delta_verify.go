package openpackbuilder

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

// VerifyDeltaBundle independently authenticates the exact parent snapshot and
// delta, checks publisher audit bindings and every operation, recomputes the
// final record count, and runs the ordinary consumer gate when applicable.
func VerifyDeltaBundle(
	ctx context.Context,
	parentBundle, deltaBundle string,
	publicIdentity PublicIdentity,
	publicKey ed25519.PublicKey,
) (result DeltaVerification, returnErr error) {
	if ctx == nil || !cleanAbsolute(parentBundle) || !cleanAbsolute(deltaBundle) || parentBundle == deltaBundle ||
		len(publicKey) != ed25519.PublicKeySize || publicIdentity.KeyID != indexpack.KeyID(publicKey) {
		return DeltaVerification{}, errors.New("open pack delta builder: valid context, distinct bundles, and public identity are required")
	}
	parentPath, err := secureconfigfile.ValidateDirectory(parentBundle)
	if err != nil {
		return DeltaVerification{}, fmt.Errorf("open pack delta builder: validate parent bundle: %w", err)
	}
	deltaPath, err := secureconfigfile.ValidateDirectory(deltaBundle)
	if err != nil {
		return DeltaVerification{}, fmt.Errorf("open pack delta builder: validate delta bundle: %w", err)
	}
	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return DeltaVerification{}, fmt.Errorf("open pack delta builder: anchor parent bundle: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, parentRoot.Close()) }()
	deltaRoot, err := os.OpenRoot(deltaPath)
	if err != nil {
		return DeltaVerification{}, fmt.Errorf("open pack delta builder: anchor delta bundle: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, deltaRoot.Close()) }()

	_, parent, parentReport, err := verifySnapshotBundleFromRoot(ctx, parentRoot, publicIdentity, publicKey)
	if err != nil {
		return DeltaVerification{}, fmt.Errorf("open pack delta builder: verify parent snapshot: %w", err)
	}
	manifestRaw, err := readBoundedRegular(deltaRoot, ManifestFilename, indexpack.MaxManifestBytes)
	if err != nil {
		return DeltaVerification{}, err
	}
	signatureRaw, err := readBoundedRegular(deltaRoot, SignatureFilename, indexpack.MaxSignatureBytes)
	if err != nil {
		return DeltaVerification{}, err
	}
	delta, err := indexpack.VerifyManifest(manifestRaw, signatureRaw, map[string]ed25519.PublicKey{publicIdentity.KeyID: publicKey})
	if err != nil {
		return DeltaVerification{}, err
	}
	if delta.Manifest.Version != indexpack.VersionURLMetadata || delta.Manifest.Kind != indexpack.KindDelta {
		return DeltaVerification{}, errors.New("open pack delta builder: delta verifier requires a v2 delta")
	}
	if err := validateBuilderManifestProvenance(delta.Manifest); err != nil {
		return DeltaVerification{}, err
	}
	if err := indexpack.ValidateDeltaTransition(parent, delta); err != nil {
		return DeltaVerification{}, err
	}
	reportRaw, err := readBoundedRegular(deltaRoot, BuildReportFilename, indexpack.MaxManifestBytes)
	if err != nil {
		return DeltaVerification{}, err
	}
	reportSignatureRaw, err := readBoundedRegular(deltaRoot, ReportSignatureFilename, indexpack.MaxSignatureBytes)
	if err != nil {
		return DeltaVerification{}, err
	}
	if err := VerifyDeltaBuildReport(reportRaw, reportSignatureRaw, publicKey); err != nil {
		return DeltaVerification{}, err
	}
	var report DeltaBuildReport
	if err := indexpack.DecodeStrictJSON(reportRaw, &report); err != nil {
		return DeltaVerification{}, err
	}
	if report.PublisherID != publicIdentity.PublisherID || report.KeyID != publicIdentity.KeyID ||
		report.ManifestSHA256 != delta.Digest || report.ParentManifestSHA256 != parent.Digest ||
		report.PackID != delta.Manifest.PackID || report.Revision != delta.Manifest.Revision || report.CreatedAt != delta.Manifest.CreatedAt ||
		report.Profile != parentReport.Profile || report.Operations.Total != delta.Manifest.RecordCount ||
		report.TargetSelection.PolicySHA256 != delta.Manifest.Build.PolicySHA256 ||
		report.TargetSelection.ExclusionsSHA256 != delta.Manifest.Build.ExclusionsSHA256 ||
		report.TargetSelection.CandidateSHA256 != delta.Manifest.Build.CandidateSHA256 || len(report.Shards) != len(delta.Manifest.Shards) {
		return DeltaVerification{}, errors.New("open pack delta builder: report does not bind the parent, manifest, and target selection")
	}

	temporary, err := os.MkdirTemp("", "fetchmark-delta-verify-state-")
	if err != nil {
		return DeltaVerification{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, os.RemoveAll(temporary)) }()
	temporaryRoot, err := os.OpenRoot(temporary)
	if err != nil {
		return DeltaVerification{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, temporaryRoot.Close()) }()
	store, err := openDeltaStore(temporaryRoot)
	if err != nil {
		return DeltaVerification{}, err
	}
	storeOpen := true
	defer func() {
		if storeOpen {
			returnErr = errors.Join(returnErr, store.Close())
		}
	}()
	if err := loadSnapshotRecords(ctx, parentRoot, parent.Manifest, store, parentBucket); err != nil {
		return DeltaVerification{}, err
	}

	manifestShards := make(map[string]indexpack.Shard, len(delta.Manifest.Shards))
	for _, descriptor := range delta.Manifest.Shards {
		manifestShards[descriptor.Path] = descriptor
	}
	operationWriter, err := newBucketWriter(store, targetBucket)
	if err != nil {
		return DeltaVerification{}, err
	}
	hasher := sha256.New()
	var upserts, tombstones, additions, operationCount uint64
	seenReportShards := make(map[string]struct{}, len(report.Shards))
	for _, evidence := range report.Shards {
		if err := ctx.Err(); err != nil {
			_ = operationWriter.Close()
			return DeltaVerification{}, err
		}
		descriptor, found := manifestShards[evidence.Path]
		if !found {
			_ = operationWriter.Close()
			return DeltaVerification{}, errors.New("open pack delta builder: report references unknown delta shard")
		}
		if _, duplicate := seenReportShards[evidence.Path]; duplicate {
			_ = operationWriter.Close()
			return DeltaVerification{}, errors.New("open pack delta builder: report repeats a delta shard")
		}
		seenReportShards[evidence.Path] = struct{}{}
		if !sameShardEvidence(evidence, descriptor) {
			_ = operationWriter.Close()
			return DeltaVerification{}, errors.New("open pack delta builder: report shard evidence differs from delta manifest")
		}
		err := scanDeltaShard(ctx, deltaRoot, descriptor, delta.Manifest.Build.Inputs[0].Name, hasher, func(record indexpack.Record) error {
			if err := operationWriter.Put(record); err != nil {
				return err
			}
			present, err := bucketContains(store, parentBucket, record.URL)
			if err != nil {
				return err
			}
			switch record.Operation {
			case indexpack.OperationTombstone:
				if !present {
					return fmt.Errorf("open pack delta builder: tombstone URL is absent from parent: %q", record.URL)
				}
				tombstones++
			case indexpack.OperationUpsert:
				upserts++
				if !present {
					additions++
				}
			}
			operationCount++
			return nil
		})
		if err != nil {
			_ = operationWriter.Close()
			return DeltaVerification{}, err
		}
	}
	if err := operationWriter.Close(); err != nil {
		return DeltaVerification{}, err
	}
	if operationCount != delta.Manifest.RecordCount || hex.EncodeToString(hasher.Sum(nil)) != report.OperationStreamSHA256 ||
		upserts != report.Operations.Upserts || tombstones != report.Operations.Tombstones {
		return DeltaVerification{}, errors.New("open pack delta builder: delta operation stream differs from report")
	}
	if parent.Manifest.RecordCount < tombstones {
		return DeltaVerification{}, errors.New("open pack delta builder: tombstones exceed parent records")
	}
	projectionCount := parent.Manifest.RecordCount - tombstones + additions
	if projectionCount != report.Operations.ProjectionRecordCount || projectionCount != report.TargetSelection.Accepted {
		return DeltaVerification{}, errors.New("open pack delta builder: recomputed projection count differs from report")
	}
	if err := store.Close(); err != nil {
		return DeltaVerification{}, err
	}
	storeOpen = false

	var projectionBytes uint64
	if report.OrdinaryInstallable {
		projectionBytes, err = verifyOrdinaryDelta(ctx, parentRoot, deltaRoot, parent, delta, projectionCount, publicKey)
		if err != nil {
			return DeltaVerification{}, fmt.Errorf("open pack delta builder: lightweight consumer gate: %w", err)
		}
	}
	if err := rootStillAtPath(parentRoot, parentPath); err != nil {
		return DeltaVerification{}, fmt.Errorf("open pack delta builder: parent bundle path changed: %w", err)
	}
	if err := rootStillAtPath(deltaRoot, deltaPath); err != nil {
		return DeltaVerification{}, fmt.Errorf("open pack delta builder: delta bundle path changed: %w", err)
	}
	return DeltaVerification{
		PackID: delta.Manifest.PackID, Revision: delta.Manifest.Revision,
		ParentManifestSHA256: parent.Digest, ManifestSHA256: delta.Digest, BuildReportSHA256: indexpack.Digest(reportRaw),
		OperationCount: operationCount, ProjectionRecordCount: projectionCount, ShardCount: len(delta.Manifest.Shards),
		OrdinaryInstallable: report.OrdinaryInstallable, ProjectionBytes: projectionBytes,
	}, nil
}

func scanDeltaShard(
	ctx context.Context,
	root *os.Root,
	descriptor indexpack.Shard,
	expectedSource string,
	stream hash.Hash,
	visit func(indexpack.Record) error,
) error {
	file, err := openRegularExact(root, filepath.FromSlash(descriptor.Path), descriptor.CompressedSizeBytes)
	if err != nil {
		return err
	}
	if err := indexpack.VerifyCompressedShard(file, descriptor); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return err
	}
	decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(indexpack.MaxUncompressedShardBytes))
	if err != nil {
		_ = file.Close()
		return err
	}
	err = indexpack.ScanRecordStreamVersion(ctx, io.TeeReader(decoder, stream), indexpack.VersionURLMetadata, indexpack.KindDelta, descriptor, func(record indexpack.Record) error {
		if record.Operation == indexpack.OperationUpsert {
			if err := validateBuilderRecordProvenance(record, expectedSource); err != nil {
				return err
			}
		}
		return visit(record)
	})
	decoder.Close()
	return errors.Join(err, file.Close())
}

func bucketContains(db *bolt.DB, bucket []byte, rawURL string) (bool, error) {
	var found bool
	err := db.View(func(tx *bolt.Tx) error {
		found = tx.Bucket(bucket).Get([]byte(rawURL)) != nil
		return nil
	})
	return found, err
}

func sameShardEvidence(evidence ShardEvidence, descriptor indexpack.Shard) bool {
	return evidence.Path == descriptor.Path && evidence.RecordCount == descriptor.RecordCount &&
		evidence.CompressedSizeBytes == descriptor.CompressedSizeBytes && evidence.UncompressedSizeBytes == descriptor.UncompressedSizeBytes &&
		evidence.SHA256 == descriptor.SHA256 && evidence.UncompressedSHA256 == descriptor.UncompressedSHA256
}
