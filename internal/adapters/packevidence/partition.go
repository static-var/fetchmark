package packevidence

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
)

const (
	PartitionManifestVersion         = 1
	PartitionManifestFilename        = "manifest.json"
	PartitionFormat                  = "normalized-admission-partitions-v1"
	PartitionAlgorithm               = "sequential-unique-host-v1"
	MaxPartitionInputRecords  uint64 = 5_000_000
	MaxPartitionInputBytes    int64  = 8 << 30
)

type PartitionOptions struct {
	Input         io.ReaderAt
	InputSize     int64
	ValidateInput func() error
	OutputDir     string
}

type PartitionManifest struct {
	Version                int                   `json:"version"`
	Format                 string                `json:"format"`
	Algorithm              string                `json:"algorithm"`
	MaxRecordsPerPartition uint64                `json:"max_records_per_partition"`
	Input                  ArtifactDescriptor    `json:"input"`
	Partitions             []PartitionDescriptor `json:"partitions"`
}

type PartitionDescriptor struct {
	ArtifactDescriptor
	FirstRow uint64 `json:"first_row"`
	LastRow  uint64 `json:"last_row"`
}

type PartitionResult struct {
	Path           string `json:"path"`
	ManifestSHA256 string `json:"manifest_sha256"`
	InputRecords   uint64 `json:"input_records"`
	Partitions     uint64 `json:"partitions"`
}

var ErrPartitionsCommitted = errors.New("pack evidence partitions committed but operation completion failed")

func PartitionsCommittedError(result PartitionResult, cause error) error {
	if cause == nil {
		return nil
	}
	return fmt.Errorf(
		"%w: output=%q manifest_sha256=%s: %w; inspect the existing partitions before retrying or removing them",
		ErrPartitionsCommitted, result.Path, result.ManifestSHA256, cause,
	)
}

// Partition splits one exact normalized Common Crawl selection into immutable
// collector-sized inputs. It requires globally unique canonical hosts to keep
// direct origins disjoint; robots redirect targets still require shared pacing
// before partitions may be collected concurrently.
func Partition(ctx context.Context, options PartitionOptions) (result PartitionResult, returnErr error) {
	return partitionWithHooks(ctx, options, defaultPartitionHooks())
}

type partitionHooks struct {
	activate      func(*os.Root, string, string) error
	checkParent   func(*os.Root, string) error
	syncParent    func(*os.Root) error
	removeStaging func(*os.Root, string) error
}

func defaultPartitionHooks() partitionHooks {
	return partitionHooks{
		activate: activateNoReplace, checkParent: rootStillAtPath, syncParent: syncRoot,
		removeStaging: func(root *os.Root, name string) error { return root.RemoveAll(name) },
	}
}

func partitionWithHooks(ctx context.Context, options PartitionOptions, hooks partitionHooks) (result PartitionResult, returnErr error) {
	if ctx == nil || options.Input == nil || options.ValidateInput == nil || options.InputSize <= 0 || options.InputSize > MaxPartitionInputBytes || !cleanAbsolute(options.OutputDir) ||
		hooks.activate == nil || hooks.checkParent == nil || hooks.syncParent == nil || hooks.removeStaging == nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: context, 1..%d-byte input, and clean absolute output are required", MaxPartitionInputBytes)
	}
	inputDigest, err := hashReaderAt(ctx, options.Input, options.InputSize)
	if err != nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: hash input: %w", err)
	}
	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.OutputDir))
	if err != nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: validate output parent: %w", err)
	}
	outputBase := filepath.Base(options.OutputDir)
	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: anchor output parent: %w", err)
	}
	committed := false
	defer func() { returnErr = joinPartitionError(returnErr, parentRoot.Close(), committed, result) }()
	nameDigest := sha256.Sum256([]byte(outputBase))
	suffix := hex.EncodeToString(nameDigest[:])
	lockName := ".fetchmark-pack-evidence-partition-lock-" + suffix
	lock, err := parentRoot.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: claim output lock: %w", err)
	}
	if err := errors.Join(lock.Sync(), lock.Close()); err != nil {
		_ = parentRoot.Remove(lockName)
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: persist output lock: %w", err)
	}
	defer func() {
		removeErr := parentRoot.Remove(lockName)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		returnErr = joinPartitionError(returnErr, errors.Join(removeErr, hooks.syncParent(parentRoot)), committed, result)
	}()
	if _, err := parentRoot.Lstat(outputBase); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return PartitionResult{}, errors.New("pack evidence partitioner: output already exists")
		}
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: inspect output: %w", err)
	}
	stagingName := ".fetchmark-pack-evidence-partition-" + suffix
	if err := parentRoot.Mkdir(stagingName, 0o700); err != nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: create staging directory: %w", err)
	}
	stagingCreated := true
	defer func() {
		if stagingCreated {
			returnErr = joinPartitionError(returnErr, hooks.removeStaging(parentRoot, stagingName), committed, result)
		}
	}()
	stagingRoot, err := parentRoot.OpenRoot(stagingName)
	if err != nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: anchor staging directory: %w", err)
	}
	defer func() { returnErr = joinPartitionError(returnErr, stagingRoot.Close(), committed, result) }()
	if err := stagingRoot.Mkdir("shards", 0o700); err != nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: create shards directory: %w", err)
	}

	manifest, err := partitionRows(ctx, options.Input, options.InputSize, inputDigest, stagingRoot)
	if err != nil {
		return PartitionResult{}, err
	}
	currentDigest, err := hashReaderAt(ctx, options.Input, options.InputSize)
	if err != nil || currentDigest != inputDigest {
		return PartitionResult{}, errors.New("pack evidence partitioner: input changed while partitioning")
	}
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		return PartitionResult{}, err
	}
	if err := writeExclusive(stagingRoot, PartitionManifestFilename, manifestRaw); err != nil {
		return PartitionResult{}, err
	}
	if err := errors.Join(syncDirectory(stagingRoot, "shards"), syncDirectory(stagingRoot, ".")); err != nil {
		return PartitionResult{}, err
	}
	currentDigest, err = hashReaderAt(ctx, options.Input, options.InputSize)
	if err != nil || currentDigest != inputDigest {
		return PartitionResult{}, errors.New("pack evidence partitioner: input changed before activation")
	}
	if err := options.ValidateInput(); err != nil {
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: revalidate input before activation: %w", err)
	}
	if err := hooks.checkParent(parentRoot, parentPath); err != nil {
		return PartitionResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return PartitionResult{}, err
	}
	if err := hooks.activate(parentRoot, stagingName, outputBase); err != nil {
		if errors.Is(err, os.ErrExist) {
			return PartitionResult{}, errors.New("pack evidence partitioner: output appeared during partitioning")
		}
		return PartitionResult{}, fmt.Errorf("pack evidence partitioner: activate output: %w", err)
	}
	stagingCreated = false
	committed = true
	result = PartitionResult{
		Path: options.OutputDir, ManifestSHA256: digest(manifestRaw), InputRecords: manifest.Input.Records,
		Partitions: uint64(len(manifest.Partitions)),
	}
	if err := errors.Join(hooks.syncParent(parentRoot), hooks.checkParent(parentRoot, parentPath)); err != nil {
		return result, PartitionsCommittedError(result, err)
	}
	return result, nil
}

func joinPartitionError(existing, next error, committed bool, result PartitionResult) error {
	if next == nil {
		return existing
	}
	if committed && !errors.Is(next, ErrPartitionsCommitted) {
		next = PartitionsCommittedError(result, next)
	}
	return errors.Join(existing, next)
}

func partitionRows(ctx context.Context, input io.ReaderAt, size int64, inputDigest string, root *os.Root) (PartitionManifest, error) {
	reader := bufio.NewReaderSize(io.NewSectionReader(input, 0, size), ccindex.MaxNormalizedLineBytes+2)
	seenHosts := make(map[string]struct{})
	partitions := make([]PartitionDescriptor, 0, 500)
	combined := sha256.New()
	var current *artifactWriter
	var records uint64
	abort := func() {
		if current != nil {
			_ = current.abort()
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			abort()
			return PartitionManifest{}, err
		}
		raw, err := reader.ReadSlice('\n')
		if errors.Is(err, io.EOF) && len(raw) == 0 {
			break
		}
		if err != nil {
			abort()
			if errors.Is(err, bufio.ErrBufferFull) {
				return PartitionManifest{}, fmt.Errorf("pack evidence partitioner: row %d exceeds %d bytes", records+1, ccindex.MaxNormalizedLineBytes)
			}
			if errors.Is(err, io.EOF) {
				return PartitionManifest{}, fmt.Errorf("pack evidence partitioner: row %d is not newline terminated", records+1)
			}
			return PartitionManifest{}, fmt.Errorf("pack evidence partitioner: read row %d: %w", records+1, err)
		}
		line := raw[:len(raw)-1]
		if len(line) == 0 || len(line) > ccindex.MaxNormalizedLineBytes || line[len(line)-1] == '\r' {
			abort()
			return PartitionManifest{}, fmt.Errorf("pack evidence partitioner: row %d has invalid framing", records+1)
		}
		candidate, err := ccindex.DecodeCandidate(line)
		if err != nil {
			abort()
			return PartitionManifest{}, fmt.Errorf("pack evidence partitioner: row %d: %w", records+1, err)
		}
		canonical, err := canonicalurl.V1(candidate.URL)
		parsed, parseErr := url.Parse(candidate.URL)
		if err != nil || canonical != candidate.URL || parseErr != nil || parsed.Hostname() == "" {
			abort()
			return PartitionManifest{}, fmt.Errorf("pack evidence partitioner: row %d URL must be canonical with a host", records+1)
		}
		host := strings.ToLower(parsed.Hostname())
		if _, duplicate := seenHosts[host]; duplicate {
			abort()
			return PartitionManifest{}, fmt.Errorf("pack evidence partitioner: row %d repeats host %q", records+1, host)
		}
		seenHosts[host] = struct{}{}
		records++
		if records > MaxPartitionInputRecords {
			abort()
			return PartitionManifest{}, fmt.Errorf("pack evidence partitioner: input exceeds %d rows", MaxPartitionInputRecords)
		}
		if current == nil {
			path := filepath.ToSlash(filepath.Join("shards", fmt.Sprintf("part-%06d.jsonl", len(partitions)+1)))
			current, err = newArtifactWriter(root, path)
			if err != nil {
				return PartitionManifest{}, err
			}
		}
		if err := current.writeRaw(raw); err != nil {
			abort()
			return PartitionManifest{}, err
		}
		_, _ = combined.Write(raw)
		if current.records == indexpackadmission.MaxCollectorRecords {
			if err := current.finish(); err != nil {
				return PartitionManifest{}, err
			}
			partitions = append(partitions, partitionDescriptor(current.descriptor(), records))
			current = nil
		}
	}
	if records == 0 {
		abort()
		return PartitionManifest{}, errors.New("pack evidence partitioner: input is empty")
	}
	if current != nil {
		if err := current.finish(); err != nil {
			return PartitionManifest{}, err
		}
		partitions = append(partitions, partitionDescriptor(current.descriptor(), records))
	}
	if hex.EncodeToString(combined.Sum(nil)) != inputDigest {
		return PartitionManifest{}, errors.New("pack evidence partitioner: partition bytes do not reproduce input")
	}
	return PartitionManifest{
		Version: PartitionManifestVersion, Format: PartitionFormat, Algorithm: PartitionAlgorithm,
		MaxRecordsPerPartition: indexpackadmission.MaxCollectorRecords,
		Input:                  ArtifactDescriptor{SHA256: inputDigest, Bytes: uint64(size), Records: records},
		Partitions:             partitions,
	}, nil
}

func partitionDescriptor(artifact ArtifactDescriptor, lastRow uint64) PartitionDescriptor {
	return PartitionDescriptor{
		ArtifactDescriptor: artifact,
		FirstRow:           lastRow - artifact.Records + 1,
		LastRow:            lastRow,
	}
}

func hashReaderAt(ctx context.Context, input io.ReaderAt, size int64) (string, error) {
	hasher := sha256.New()
	reader := io.NewSectionReader(input, 0, size)
	buffer := make([]byte, 1<<20)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		read, err := reader.Read(buffer)
		if read > 0 {
			_, _ = hasher.Write(buffer[:read])
			total += int64(read)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
	}
	if total != size {
		return "", io.ErrUnexpectedEOF
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
