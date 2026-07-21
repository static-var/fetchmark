package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/adapters/packevidence"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const commandConfig = `{"version":1,"product_token":"FetchmarkPackEvidence","contact_uri":"mailto:operator@example.org","evidence_validity_hours":12,"min_host_interval_ms":1000,"global_concurrency":2,"request_timeout_seconds":20,"rights":{"allowed_fields":["url_metadata"],"basis":"url_metadata_policy","evidence_uri":"https://commoncrawl.org/terms-of-use","rights_notice":"URL metadata only"}}`

func TestRunCollectWiresExactInputsAndWritesResult(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	rightsPath := filepath.Join(root, "rights.txt")
	inputPath := filepath.Join(root, "normalized.jsonl")
	outputPath := filepath.Join(root, "bundle")
	input := &recordingReadCloser{Reader: strings.NewReader("normalized\n")}
	var observedOptions packevidence.Options
	deps := dependencies{
		readSecure: func(path string, options secureconfigfile.Options) ([]byte, error) {
			switch path {
			case configPath:
				if options.MaxBytes != indexpackadmission.MaxConfigBytes {
					t.Fatalf("config options = %#v", options)
				}
				return []byte(commandConfig), nil
			case rightsPath:
				return []byte("rights evidence"), nil
			default:
				return nil, errors.New("unexpected path")
			}
		},
		openInput: func(path string) (io.ReadCloser, error) {
			if path != inputPath {
				t.Fatalf("input path = %q", path)
			}
			return input, nil
		},
		resolveVersions: func() (versions, error) {
			return versions{collector: "binary-sha256:" + strings.Repeat("a", 64), userAgent: "sha256-aaaaaaaaaaaaaaaa"}, nil
		},
		newObserver: func(config indexpackadmission.Config, version string) (packevidence.RowObserver, error) {
			if version != "sha256-aaaaaaaaaaaaaaaa" || config.GlobalConcurrency != 2 {
				t.Fatalf("observer config=%#v version=%q", config, version)
			}
			return noopObserver{}, nil
		},
		collect: func(_ context.Context, options packevidence.Options) (packevidence.CollectionResult, error) {
			observedOptions = options
			return packevidence.CollectionResult{Path: outputPath, ReportSHA256: strings.Repeat("b", 64), InputRecords: 1}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"collect", "-config", configPath, "-rights-evidence", rightsPath, "-input", inputPath, "-out", outputPath}, &stdout, &stderr, deps)
	if code != 0 || stderr.Len() != 0 || !input.closed {
		t.Fatalf("code=%d stderr=%q closed=%v", code, stderr.String(), input.closed)
	}
	if observedOptions.OutputDir != outputPath || observedOptions.CollectorVersion == "" || observedOptions.UserAgentVersion == "" || observedOptions.Observer == nil || string(observedOptions.ConfigRaw) != commandConfig || string(observedOptions.RightsEvidence) != "rights evidence" {
		t.Fatalf("options = %#v", observedOptions)
	}
	if !strings.Contains(stdout.String(), `"path":"`+outputPath+`"`) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunReportsCommittedBundleWhenStdoutFails(t *testing.T) {
	root := t.TempDir()
	paths := []string{filepath.Join(root, "config"), filepath.Join(root, "rights"), filepath.Join(root, "input"), filepath.Join(root, "bundle")}
	deps := dependencies{
		readSecure: func(path string, _ secureconfigfile.Options) ([]byte, error) {
			if path == paths[0] {
				return []byte(commandConfig), nil
			}
			return []byte("rights"), nil
		},
		openInput:       func(string) (io.ReadCloser, error) { return &recordingReadCloser{Reader: strings.NewReader("x")}, nil },
		resolveVersions: func() (versions, error) { return versions{collector: "1", userAgent: "1"}, nil },
		newObserver:     func(indexpackadmission.Config, string) (packevidence.RowObserver, error) { return noopObserver{}, nil },
		collect: func(context.Context, packevidence.Options) (packevidence.CollectionResult, error) {
			return packevidence.CollectionResult{Path: paths[3], ReportSHA256: strings.Repeat("c", 64)}, nil
		},
	}
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"collect", "-config", paths[0], "-rights-evidence", paths[1], "-input", paths[2], "-out", paths[3]}, failingWriter{}, &stderr, deps)
	if code != 1 || !strings.Contains(stderr.String(), packevidence.ErrBundleCommitted.Error()) || !strings.Contains(stderr.String(), paths[3]) {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestRunPartitionWiresStableReaderAndWritesResult(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "normalized.jsonl")
	outputPath := filepath.Join(root, "partitions")
	input := &recordingReaderAtCloser{Reader: bytes.NewReader([]byte("normalized\n"))}
	var observed packevidence.PartitionOptions
	deps := dependencies{
		openPartitionInput: func(path string) (partitionInput, int64, error) {
			if path != inputPath {
				t.Fatalf("input path = %q", path)
			}
			return input, int64(input.Len()), nil
		},
		partition: func(_ context.Context, options packevidence.PartitionOptions) (packevidence.PartitionResult, error) {
			observed = options
			return packevidence.PartitionResult{Path: outputPath, ManifestSHA256: strings.Repeat("d", 64), InputRecords: 1, Partitions: 1}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"partition", "-input", inputPath, "-out", outputPath}, &stdout, &stderr, deps)
	if code != 0 || stderr.Len() != 0 || !input.closed {
		t.Fatalf("code=%d stderr=%q closed=%v", code, stderr.String(), input.closed)
	}
	if observed.Input != input || observed.InputSize != int64(len("normalized\n")) || observed.ValidateInput == nil || observed.OutputDir != outputPath {
		t.Fatalf("options = %#v", observed)
	}
	if !strings.Contains(stdout.String(), `"partitions":1`) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunReportsCommittedPartitionsWhenStdoutFails(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "normalized.jsonl")
	outputPath := filepath.Join(root, "partitions")
	deps := dependencies{
		openPartitionInput: func(string) (partitionInput, int64, error) {
			return &recordingReaderAtCloser{Reader: bytes.NewReader([]byte("x"))}, 1, nil
		},
		partition: func(context.Context, packevidence.PartitionOptions) (packevidence.PartitionResult, error) {
			return packevidence.PartitionResult{Path: outputPath, ManifestSHA256: strings.Repeat("e", 64)}, nil
		},
	}
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"partition", "-input", inputPath, "-out", outputPath}, failingWriter{}, &stderr, deps)
	if code != 1 || !strings.Contains(stderr.String(), packevidence.ErrPartitionsCommitted.Error()) || !strings.Contains(stderr.String(), outputPath) {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestRunMergeWiresExactDirectoriesAndWritesResult(t *testing.T) {
	root := t.TempDir()
	partitions := filepath.Join(root, "partitions")
	bundleOne := filepath.Join(root, "bundle-one")
	bundleTwo := filepath.Join(root, "bundle-two")
	output := filepath.Join(root, "merged")
	var observed packevidence.MergeOptions
	deps := dependencies{
		resolveVersions: func() (versions, error) { return versions{collector: "binary-sha256:" + strings.Repeat("a", 64)}, nil },
		merge: func(_ context.Context, options packevidence.MergeOptions) (packevidence.MergeResult, error) {
			observed = options
			return packevidence.MergeResult{Path: output, ReportSHA256: strings.Repeat("b", 64), CandidateSHA256: strings.Repeat("c", 64), Bundles: 2}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"merge", "-partitions", partitions, "-bundle", bundleTwo, "-bundle", bundleOne, "-out", output,
	}, &stdout, &stderr, deps)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if observed.PartitionsDir != partitions || observed.OutputDir != output || observed.MergerVersion == "" ||
		len(observed.BundleDirs) != 2 || observed.BundleDirs[0] != bundleTwo || observed.BundleDirs[1] != bundleOne {
		t.Fatalf("options = %#v", observed)
	}
	if !strings.Contains(stdout.String(), `"bundles":2`) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunReportsCommittedMergeWhenStdoutFails(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "merged")
	deps := dependencies{
		resolveVersions: func() (versions, error) { return versions{collector: "1"}, nil },
		merge: func(context.Context, packevidence.MergeOptions) (packevidence.MergeResult, error) {
			return packevidence.MergeResult{Path: output, ReportSHA256: strings.Repeat("d", 64), CandidateSHA256: strings.Repeat("e", 64)}, nil
		},
	}
	var stderr bytes.Buffer
	code := run(context.Background(), []string{
		"merge", "-partitions", filepath.Join(root, "partitions"), "-bundle", filepath.Join(root, "bundle"), "-out", output,
	}, failingWriter{}, &stderr, deps)
	if code != 1 || !strings.Contains(stderr.String(), packevidence.ErrMergeCommitted.Error()) || !strings.Contains(stderr.String(), output) {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestParseRequestRejectsMissingDuplicateAndRelativePaths(t *testing.T) {
	var stderr bytes.Buffer
	for name, args := range map[string][]string{
		"missing command":           nil,
		"wrong command":             {"build"},
		"relative":                  {"collect", "-config", "relative", "-rights-evidence", "/tmp/r", "-input", "/tmp/i", "-out", "/tmp/o"},
		"duplicate":                 {"collect", "-config", "/tmp/c", "-config", "/tmp/c2", "-rights-evidence", "/tmp/r", "-input", "/tmp/i", "-out", "/tmp/o"},
		"partition missing output":  {"partition", "-input", "/tmp/i"},
		"partition duplicate input": {"partition", "-input", "/tmp/i", "-input", "/tmp/j", "-out", "/tmp/o"},
		"partition same path":       {"partition", "-input", "/tmp/i", "-out", "/tmp/i"},
		"merge missing bundle":      {"merge", "-partitions", "/tmp/p", "-out", "/tmp/o"},
		"merge duplicate bundle":    {"merge", "-partitions", "/tmp/p", "-bundle", "/tmp/b", "-bundle", "/tmp/b", "-out", "/tmp/o"},
		"merge relative bundle":     {"merge", "-partitions", "/tmp/p", "-bundle", "relative", "-out", "/tmp/o"},
		"merge output inside input": {"merge", "-partitions", "/tmp/p", "-bundle", "/tmp/b", "-out", "/tmp/p/out"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRequest(args, &stderr); err == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}

func TestResolvedVersionsUsesImmutableExecutableIdentity(t *testing.T) {
	previous := version
	version = "dev"
	t.Cleanup(func() { version = previous })
	resolved, err := resolvedVersions()
	if err != nil || !strings.HasPrefix(resolved.collector, "binary-sha256:") || !strings.HasPrefix(resolved.userAgent, "sha256-") || len(resolved.userAgent) != len("sha256-")+16 {
		t.Fatalf("versions=%#v error=%v", resolved, err)
	}
}

func TestStablePartitionInputDetectsAppendAndPathReplacement(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "append", mutate: func(t *testing.T, path string) {
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteString("changed"); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "path replacement", mutate: func(t *testing.T, path string) {
			if err := os.Rename(path, path+".original"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("original\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "input.jsonl")
			if err := os.WriteFile(path, []byte("original\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			input, _, err := openStablePartitionInput(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = input.Close() })
			test.mutate(t, path)
			if err := input.Validate(); err == nil {
				t.Fatal("changed input accepted")
			}
		})
	}
}

type noopObserver struct{}

func (noopObserver) Observe(context.Context, uint64, ccindex.Candidate, indexpackselection.RightsDecision) (packevidence.Result, error) {
	return packevidence.Result{}, nil
}

type recordingReadCloser struct {
	io.Reader
	closed bool
}

type recordingReaderAtCloser struct {
	*bytes.Reader
	closed bool
}

func (reader *recordingReaderAtCloser) Close() error {
	reader.closed = true
	return nil
}

func (reader *recordingReaderAtCloser) Validate() error { return nil }

func (reader *recordingReadCloser) Close() error {
	reader.closed = true
	return nil
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("injected stdout failure") }
