// Command fetchmark-pack-evidence is the separately operated, networked
// publisher evidence collector. It is not part of the ordinary Fetchmark
// server, crawler, or network-free pack-builder image.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/staticvar/fetchmark/internal/adapters/packevidence"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/buildidentity"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, defaultDependencies()))
}

type dependencies struct {
	readSecure         func(string, secureconfigfile.Options) ([]byte, error)
	openInput          func(string) (io.ReadCloser, error)
	openPartitionInput func(string) (partitionInput, int64, error)
	resolveVersions    func() (versions, error)
	newObserver        func(indexpackadmission.Config, string) (packevidence.RowObserver, error)
	collect            func(context.Context, packevidence.Options) (packevidence.CollectionResult, error)
	partition          func(context.Context, packevidence.PartitionOptions) (packevidence.PartitionResult, error)
	merge              func(context.Context, packevidence.MergeOptions) (packevidence.MergeResult, error)
}

func defaultDependencies() dependencies {
	return dependencies{
		readSecure: secureconfigfile.Read, openInput: openStableInput,
		openPartitionInput: openStablePartitionInput,
		resolveVersions:    resolvedVersions,
		newObserver: func(config indexpackadmission.Config, version string) (packevidence.RowObserver, error) {
			return packevidence.NewPublicObserver(config, version)
		},
		collect: packevidence.Collect, partition: packevidence.Partition, merge: packevidence.Merge,
	}
}

type versions struct {
	collector string
	userAgent string
}

type request struct {
	command        string
	config         string
	rightsEvidence string
	input          string
	partitions     string
	bundles        []string
	output         string
}

type partitionInput interface {
	io.ReaderAt
	io.Closer
	Validate() error
}

type stablePartitionFile struct {
	file     *os.File
	path     string
	original os.FileInfo
}

func (input *stablePartitionFile) ReadAt(buffer []byte, offset int64) (int, error) {
	return input.file.ReadAt(buffer, offset)
}

func (input *stablePartitionFile) Close() error { return input.file.Close() }

func (input *stablePartitionFile) Validate() error {
	opened, err := input.file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(input.original, opened) || opened.Size() != input.original.Size() || !opened.ModTime().Equal(input.original.ModTime()) {
		return errors.New("open input descriptor changed while partitioning")
	}
	current, err := os.Lstat(input.path)
	if err != nil || !current.Mode().IsRegular() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(input.original, current) || current.Size() != input.original.Size() || !current.ModTime().Equal(input.original.ModTime()) {
		return errors.New("input path changed while partitioning")
	}
	return nil
}

type singleStringFlag struct {
	name  string
	value string
	set   bool
}

type multiStringFlag struct {
	name   string
	values []string
}

func (value *multiStringFlag) String() string { return strings.Join(value.values, ",") }
func (value *multiStringFlag) Set(raw string) error {
	value.values = append(value.values, raw)
	return nil
}

func (value *singleStringFlag) String() string { return value.value }
func (value *singleStringFlag) Set(raw string) error {
	if value.set {
		return fmt.Errorf("-%s may only be specified once", value.name)
	}
	value.set = true
	value.value = raw
	return nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, deps dependencies) int {
	request, err := parseRequest(args, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-pack-evidence: %v\n", err)
		return 2
	}
	var result any
	switch request.command {
	case "collect":
		result, err = execute(ctx, request, deps)
	case "partition":
		result, err = executePartition(ctx, request, deps)
	case "merge":
		result, err = executeMerge(ctx, request, deps)
	}
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-pack-evidence: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		switch committed := result.(type) {
		case packevidence.CollectionResult:
			err = packevidence.CommittedError(committed, fmt.Errorf("write command result: %w", err))
		case packevidence.PartitionResult:
			err = packevidence.PartitionsCommittedError(committed, fmt.Errorf("write command result: %w", err))
		case packevidence.MergeResult:
			err = packevidence.MergeCommittedError(committed, fmt.Errorf("write command result: %w", err))
		}
		fmt.Fprintf(stderr, "fetchmark-pack-evidence: %v\n", err)
		return 1
	}
	return 0
}

func parseRequest(args []string, stderr io.Writer) (request, error) {
	if len(args) == 0 {
		return request{}, errors.New("expected exactly one supported subcommand")
	}
	switch args[0] {
	case "collect":
		return parseCollectRequest(args, stderr)
	case "partition":
		return parsePartitionRequest(args, stderr)
	case "merge":
		return parseMergeRequest(args, stderr)
	default:
		return request{}, fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func parseMergeRequest(args []string, stderr io.Writer) (request, error) {
	flags := flag.NewFlagSet("fetchmark-pack-evidence merge", flag.ContinueOnError)
	flags.SetOutput(stderr)
	partitions := &singleStringFlag{name: "partitions"}
	bundles := &multiStringFlag{name: "bundle"}
	output := &singleStringFlag{name: "out"}
	flags.Var(partitions, "partitions", "immutable admission-partition directory")
	flags.Var(bundles, "bundle", "collector bundle directory; repeat once per partition")
	flags.Var(output, "out", "new verified aggregate evidence directory")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	if !partitions.set || !cleanAbsolute(partitions.value) {
		return request{}, errors.New("-partitions must be a clean absolute path")
	}
	if !output.set || !cleanAbsolute(output.value) {
		return request{}, errors.New("-out must be a clean absolute path")
	}
	if len(bundles.values) == 0 {
		return request{}, errors.New("at least one -bundle is required")
	}
	seen := map[string]struct{}{partitions.value: {}}
	for _, bundle := range bundles.values {
		if !cleanAbsolute(bundle) {
			return request{}, errors.New("every -bundle must be a clean absolute path")
		}
		if _, duplicate := seen[bundle]; duplicate {
			return request{}, errors.New("input directories must be unique")
		}
		seen[bundle] = struct{}{}
	}
	for source := range seen {
		if pathWithin(output.value, source) {
			return request{}, errors.New("-out must not be inside an input directory")
		}
	}
	return request{command: "merge", partitions: partitions.value, bundles: append([]string(nil), bundles.values...), output: output.value}, nil
}

func pathWithin(candidate, parent string) bool {
	relative, err := filepath.Rel(parent, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func parseCollectRequest(args []string, stderr io.Writer) (request, error) {
	flags := flag.NewFlagSet("fetchmark-pack-evidence collect", flag.ContinueOnError)
	flags.SetOutput(stderr)
	config := &singleStringFlag{name: "config"}
	rights := &singleStringFlag{name: "rights-evidence"}
	input := &singleStringFlag{name: "input"}
	output := &singleStringFlag{name: "out"}
	flags.Var(config, "config", "strict collector policy JSON")
	flags.Var(rights, "rights-evidence", "exact local operator rights-evidence file")
	flags.Var(input, "input", "strict normalized Common Crawl metadata JSONL")
	flags.Var(output, "out", "new evidence bundle directory")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	for _, required := range []*singleStringFlag{config, rights, input, output} {
		if !required.set || !cleanAbsolute(required.value) {
			return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
		}
	}
	if input.value == output.value || config.value == output.value || rights.value == output.value {
		return request{}, errors.New("-out must differ from every input path")
	}
	return request{command: "collect", config: config.value, rightsEvidence: rights.value, input: input.value, output: output.value}, nil
}

func parsePartitionRequest(args []string, stderr io.Writer) (request, error) {
	flags := flag.NewFlagSet("fetchmark-pack-evidence partition", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := &singleStringFlag{name: "input"}
	output := &singleStringFlag{name: "out"}
	flags.Var(input, "input", "strict normalized Common Crawl metadata JSONL")
	flags.Var(output, "out", "new immutable collector-partition directory")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	if !input.set || !cleanAbsolute(input.value) {
		return request{}, errors.New("-input must be a clean absolute path")
	}
	if !output.set || !cleanAbsolute(output.value) {
		return request{}, errors.New("-out must be a clean absolute path")
	}
	if input.value == output.value {
		return request{}, errors.New("-out must differ from -input")
	}
	return request{command: "partition", input: input.value, output: output.value}, nil
}

func execute(ctx context.Context, request request, deps dependencies) (packevidence.CollectionResult, error) {
	if deps.readSecure == nil || deps.openInput == nil || deps.resolveVersions == nil || deps.newObserver == nil || deps.collect == nil {
		return packevidence.CollectionResult{}, errors.New("collector dependencies are unavailable")
	}
	configRaw, err := deps.readSecure(request.config, secureconfigfile.Options{MaxBytes: indexpackadmission.MaxConfigBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return packevidence.CollectionResult{}, fmt.Errorf("read collector config: %w", err)
	}
	config, _, err := indexpackadmission.DecodeConfig(configRaw)
	if err != nil {
		return packevidence.CollectionResult{}, err
	}
	rightsRaw, err := deps.readSecure(request.rightsEvidence, secureconfigfile.Options{MaxBytes: indexpackadmission.MaxRightsEvidenceBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return packevidence.CollectionResult{}, fmt.Errorf("read rights evidence: %w", err)
	}
	resolved, err := deps.resolveVersions()
	if err != nil {
		return packevidence.CollectionResult{}, fmt.Errorf("resolve collector version: %w", err)
	}
	observer, err := deps.newObserver(config, resolved.userAgent)
	if err != nil {
		return packevidence.CollectionResult{}, fmt.Errorf("create public evidence observer: %w", err)
	}
	input, err := deps.openInput(request.input)
	if err != nil {
		return packevidence.CollectionResult{}, fmt.Errorf("open normalized input: %w", err)
	}
	result, collectErr := deps.collect(ctx, packevidence.Options{
		ConfigRaw: configRaw, RightsEvidence: rightsRaw, Candidates: input, Observer: observer,
		CollectorVersion: resolved.collector, UserAgentVersion: resolved.userAgent, OutputDir: request.output,
	})
	closeErr := input.Close()
	if closeErr != nil && result.Path != "" {
		closeErr = packevidence.CommittedError(result, fmt.Errorf("close normalized input: %w", closeErr))
	}
	return result, errors.Join(collectErr, closeErr)
}

func executePartition(ctx context.Context, request request, deps dependencies) (packevidence.PartitionResult, error) {
	if deps.openPartitionInput == nil || deps.partition == nil {
		return packevidence.PartitionResult{}, errors.New("partition dependencies are unavailable")
	}
	input, size, err := deps.openPartitionInput(request.input)
	if err != nil {
		return packevidence.PartitionResult{}, fmt.Errorf("open normalized input: %w", err)
	}
	result, partitionErr := deps.partition(ctx, packevidence.PartitionOptions{Input: input, InputSize: size, ValidateInput: input.Validate, OutputDir: request.output})
	closeErr := input.Close()
	if closeErr != nil && result.Path != "" {
		closeErr = packevidence.PartitionsCommittedError(result, fmt.Errorf("close normalized input: %w", closeErr))
	}
	return result, errors.Join(partitionErr, closeErr)
}

func executeMerge(ctx context.Context, request request, deps dependencies) (packevidence.MergeResult, error) {
	if deps.resolveVersions == nil || deps.merge == nil {
		return packevidence.MergeResult{}, errors.New("merge dependencies are unavailable")
	}
	resolved, err := deps.resolveVersions()
	if err != nil {
		return packevidence.MergeResult{}, fmt.Errorf("resolve merger version: %w", err)
	}
	return deps.merge(ctx, packevidence.MergeOptions{
		PartitionsDir: request.partitions, BundleDirs: append([]string(nil), request.bundles...),
		MergerVersion: resolved.collector, OutputDir: request.output,
	})
}

func resolvedVersions() (versions, error) {
	if version != "dev" {
		if strings.TrimSpace(version) != version || version == "" || len(version) > 64 {
			return versions{}, errors.New("linked collector version is invalid")
		}
		return versions{collector: version, userAgent: version}, nil
	}
	digest, err := buildidentity.CurrentExecutableSHA256()
	if err != nil {
		return versions{}, err
	}
	canonical, ok := buildidentity.Parse(digest)
	if !ok {
		return versions{}, errors.New("collector executable SHA-256 is invalid")
	}
	return versions{collector: "binary-sha256:" + canonical, userAgent: "sha256-" + canonical[:16]}, nil
}

func openStableInput(path string) (io.ReadCloser, error) {
	if !cleanAbsolute(path) {
		return nil, errors.New("input path must be clean and absolute")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("input must be a regular non-symlink file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, errors.New("input changed while opening")
	}
	return file, nil
}

func openStablePartitionInput(path string) (partitionInput, int64, error) {
	if !cleanAbsolute(path) {
		return nil, 0, errors.New("input path must be clean and absolute")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, 0, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() <= 0 || before.Size() > packevidence.MaxPartitionInputBytes {
		return nil, 0, fmt.Errorf("input must be a regular non-symlink file of 1..%d bytes", packevidence.MaxPartitionInputBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || opened.Size() != before.Size() {
		_ = file.Close()
		return nil, 0, errors.New("input changed while opening")
	}
	return &stablePartitionFile{file: file, path: path, original: opened}, opened.Size(), nil
}

func cleanAbsolute(path string) bool {
	return path != "" && strings.TrimSpace(path) == path && filepath.IsAbs(path) && filepath.Clean(path) == path
}
