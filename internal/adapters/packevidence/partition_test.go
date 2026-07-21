package packevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
)

func TestPartitionAdmissionInputPreservesExactCoverage(t *testing.T) {
	candidates := make([]ccindex.Candidate, indexpackadmission.MaxCollectorRecords+1)
	for index := range candidates {
		candidates[index] = normalizedCandidate(fmt.Sprintf("https://host-%05d.example.org/page", index))
	}
	input := encodeNormalized(t, candidates)
	output := filepath.Join(t.TempDir(), "partitions")

	result, err := Partition(context.Background(), PartitionOptions{
		Input: bytes.NewReader(input), InputSize: int64(len(input)), ValidateInput: validInput,
		OutputDir: output,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != output || result.InputRecords != uint64(len(candidates)) || result.Partitions != 2 || result.ManifestSHA256 == "" {
		t.Fatalf("result = %#v", result)
	}

	manifestRaw := readFile(t, filepath.Join(output, PartitionManifestFilename))
	if digest(manifestRaw) != result.ManifestSHA256 {
		t.Fatalf("manifest digest = %q", result.ManifestSHA256)
	}
	var manifest PartitionManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != PartitionManifestVersion || manifest.Format != PartitionFormat ||
		manifest.Algorithm != PartitionAlgorithm || manifest.Input.SHA256 != digest(input) ||
		manifest.Input.Bytes != uint64(len(input)) || manifest.Input.Records != uint64(len(candidates)) ||
		manifest.MaxRecordsPerPartition != indexpackadmission.MaxCollectorRecords || len(manifest.Partitions) != 2 ||
		manifest.Partitions[0].Records != indexpackadmission.MaxCollectorRecords || manifest.Partitions[0].FirstRow != 1 || manifest.Partitions[0].LastRow != indexpackadmission.MaxCollectorRecords ||
		manifest.Partitions[1].Records != 1 || manifest.Partitions[1].FirstRow != indexpackadmission.MaxCollectorRecords+1 || manifest.Partitions[1].LastRow != uint64(len(candidates)) {
		t.Fatalf("manifest = %#v", manifest)
	}

	var reconstructed bytes.Buffer
	for _, descriptor := range manifest.Partitions {
		raw := readFile(t, filepath.Join(output, filepath.FromSlash(descriptor.Path)))
		if descriptor.SHA256 != digest(raw) || descriptor.Bytes != uint64(len(raw)) {
			t.Fatalf("partition descriptor = %#v", descriptor)
		}
		reconstructed.Write(raw)
	}
	if !bytes.Equal(reconstructed.Bytes(), input) {
		t.Fatal("ordered partition bytes do not exactly reproduce input")
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatal(err)
	}

	repeatedOutput := filepath.Join(t.TempDir(), "partitions")
	repeated, err := Partition(context.Background(), PartitionOptions{
		Input: bytes.NewReader(input), InputSize: int64(len(input)), ValidateInput: validInput,
		OutputDir: repeatedOutput,
	})
	if err != nil {
		t.Fatal(err)
	}
	repeatedManifest := readFile(t, filepath.Join(repeatedOutput, PartitionManifestFilename))
	if repeated.ManifestSHA256 != result.ManifestSHA256 || !bytes.Equal(repeatedManifest, manifestRaw) {
		t.Fatal("repeat partition manifest differs")
	}
	for _, descriptor := range manifest.Partitions {
		first := readFile(t, filepath.Join(output, filepath.FromSlash(descriptor.Path)))
		second := readFile(t, filepath.Join(repeatedOutput, filepath.FromSlash(descriptor.Path)))
		if !bytes.Equal(first, second) {
			t.Fatalf("repeat partition %q differs", descriptor.Path)
		}
	}
}

func TestPartitionAdmissionInputRejectsCrossPartitionHostCollision(t *testing.T) {
	candidates := make([]ccindex.Candidate, indexpackadmission.MaxCollectorRecords+1)
	for index := range candidates {
		candidates[index] = normalizedCandidate(fmt.Sprintf("https://host-%05d.example.org/page", index))
	}
	candidates[len(candidates)-1] = normalizedCandidate("https://host-00000.example.org/other")
	input := encodeNormalized(t, candidates)
	output := filepath.Join(t.TempDir(), "partitions")

	_, err := Partition(context.Background(), PartitionOptions{Input: bytes.NewReader(input), InputSize: int64(len(input)), ValidateInput: validInput, OutputDir: output})
	if err == nil || !strings.Contains(err.Error(), "repeats host") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial output exists: %v", statErr)
	}
}

func TestPartitionAdmissionInputDetectsMutationBeforeActivation(t *testing.T) {
	input := encodeNormalized(t, []ccindex.Candidate{normalizedCandidate("https://example.org/page")})
	changed := bytes.Clone(input)
	changed[len(changed)-2] ^= 1
	reader := &changingReaderAt{original: input, changed: changed, changeOnPass: 3}
	output := filepath.Join(t.TempDir(), "partitions")

	_, err := Partition(context.Background(), PartitionOptions{Input: reader, InputSize: int64(len(input)), ValidateInput: validInput, OutputDir: output})
	if err == nil || !strings.Contains(err.Error(), "input changed") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial output exists: %v", statErr)
	}
}

func TestPartitionAdmissionInputRejectsCancellationFramingAndOverwrite(t *testing.T) {
	valid := encodeNormalized(t, []ccindex.Candidate{normalizedCandidate("https://example.org/page")})
	for name, setup := range map[string]func(string) (context.Context, PartitionOptions){
		"cancelled": func(output string) (context.Context, PartitionOptions) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, PartitionOptions{Input: bytes.NewReader(valid), InputSize: int64(len(valid)), ValidateInput: validInput, OutputDir: output}
		},
		"missing newline": func(output string) (context.Context, PartitionOptions) {
			raw := bytes.TrimSuffix(valid, []byte{'\n'})
			return context.Background(), PartitionOptions{Input: bytes.NewReader(raw), InputSize: int64(len(raw)), ValidateInput: validInput, OutputDir: output}
		},
		"existing output": func(output string) (context.Context, PartitionOptions) {
			if err := os.Mkdir(output, 0o700); err != nil {
				t.Fatal(err)
			}
			return context.Background(), PartitionOptions{Input: bytes.NewReader(valid), InputSize: int64(len(valid)), ValidateInput: validInput, OutputDir: output}
		},
	} {
		t.Run(name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "partitions")
			ctx, options := setup(output)
			if _, err := Partition(ctx, options); err == nil {
				t.Fatal("invalid partition request succeeded")
			}
		})
	}
}

func TestPartitionAdmissionInputRevalidationFailurePublishesNothing(t *testing.T) {
	input := encodeNormalized(t, []ccindex.Candidate{normalizedCandidate("https://example.org/page")})
	output := filepath.Join(t.TempDir(), "partitions")
	_, err := Partition(context.Background(), PartitionOptions{
		Input: bytes.NewReader(input), InputSize: int64(len(input)), OutputDir: output,
		ValidateInput: func() error { return errors.New("source path replaced") },
	})
	if err == nil || !strings.Contains(err.Error(), "revalidate input") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial output exists: %v", statErr)
	}
}

func TestPartitionAdmissionReportsPostCommitFailure(t *testing.T) {
	input := encodeNormalized(t, []ccindex.Candidate{normalizedCandidate("https://example.org/page")})
	for name, mutate := range map[string]func(*partitionHooks){
		"parent sync": func(hooks *partitionHooks) {
			hooks.syncParent = func(*os.Root) error { return errors.New("injected parent sync failure") }
		},
		"final parent identity": func(hooks *partitionHooks) {
			base := hooks.checkParent
			calls := 0
			hooks.checkParent = func(root *os.Root, path string) error {
				calls++
				if calls == 2 {
					return errors.New("injected final parent identity failure")
				}
				return base(root, path)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "partitions")
			hooks := defaultPartitionHooks()
			mutate(&hooks)
			result, err := partitionWithHooks(context.Background(), PartitionOptions{
				Input: bytes.NewReader(input), InputSize: int64(len(input)), ValidateInput: validInput, OutputDir: output,
			}, hooks)
			if !errors.Is(err, ErrPartitionsCommitted) || result.Path != output || result.ManifestSHA256 == "" {
				t.Fatalf("result=%#v error=%v", result, err)
			}
			if _, statErr := os.Stat(filepath.Join(output, PartitionManifestFilename)); statErr != nil {
				t.Fatalf("committed manifest missing: %v", statErr)
			}
		})
	}
}

func TestPartitionAdmissionResourceCapsAreExplicit(t *testing.T) {
	if MaxPartitionInputRecords != 5_000_000 || MaxPartitionInputBytes != 8<<30 || indexpackadmission.MaxCollectorRecords != 10_000 {
		t.Fatalf("partition caps records=%d bytes=%d collector=%d", MaxPartitionInputRecords, MaxPartitionInputBytes, indexpackadmission.MaxCollectorRecords)
	}
}

func validInput() error { return nil }

type changingReaderAt struct {
	original     []byte
	changed      []byte
	changeOnPass int
	passes       int
}

func (reader *changingReaderAt) ReadAt(buffer []byte, offset int64) (int, error) {
	if offset == 0 {
		reader.passes++
	}
	source := reader.original
	if reader.passes >= reader.changeOnPass {
		source = reader.changed
	}
	if offset >= int64(len(source)) {
		return 0, io.EOF
	}
	read := copy(buffer, source[offset:])
	if read != len(buffer) {
		return read, io.EOF
	}
	return read, nil
}
