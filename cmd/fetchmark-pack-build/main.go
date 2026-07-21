// Command fetchmark-pack-build is the offline publisher-side companion for
// Fetchmark open-index packs. It performs no network access and is not shipped
// in the ordinary Fetchmark server image.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/adapters/openpackbuilder"
	"github.com/staticvar/fetchmark/internal/adapters/packevidence"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/adapters/tufrepository"
	"github.com/staticvar/fetchmark/internal/buildidentity"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, defaultDependencies()))
}

type dependencies struct {
	now                     func() time.Time
	random                  io.Reader
	createPrivate           func(string, []byte, int64) error
	readSecure              func(string, secureconfigfile.Options) ([]byte, error)
	openCandidate           func(string) (io.ReadCloser, error)
	normalize               func(context.Context, io.Reader, io.Writer, ccindex.Options) (ccindex.Report, error)
	normalizeParquet        func(context.Context, io.ReaderAt, int64, io.Writer) (ccindex.Report, error)
	selectParquet           func(context.Context, io.ReaderAt, int64, io.Writer, ccindex.ParquetSelectionOptions) (ccindex.Report, error)
	selectParquetParts      func(context.Context, []ccindex.ParquetSelectionInput, io.Writer, ccindex.ParquetSelectionOptions) (ccindex.Report, error)
	builderVersion          func() (string, error)
	build                   func(context.Context, openpackbuilder.Options) (openpackbuilder.Result, error)
	verify                  func(context.Context, string, openpackbuilder.PublicIdentity, ed25519.PublicKey) (openpackbuilder.Verification, error)
	buildDelta              func(context.Context, openpackbuilder.DeltaOptions) (openpackbuilder.DeltaResult, error)
	verifyDelta             func(context.Context, string, string, openpackbuilder.PublicIdentity, ed25519.PublicKey) (openpackbuilder.DeltaVerification, error)
	generateChannelIdentity func(string, time.Time, io.Reader) ([]byte, []byte, error)
	writeChannelIdentity    func(context.Context, string, []byte) (tufrepository.IdentityWriteResult, error)
	stageChannel            func(context.Context, tufrepository.StageOptions) (tufrepository.StageResult, error)
	activateMirror          func(context.Context, tufrepository.MirrorActivationOptions) (tufrepository.MirrorActivationResult, error)
	generateRootSigner      func(string, string, io.Reader) ([]byte, []byte, error)
	prepareRootRotation     func(string, []byte, []tufrepository.RootSignerPublic, time.Time, time.Time) ([]byte, tufrepository.RootRotationProposal, error)
	signRootRotation        func(string, []byte, []byte, string, string, tufrepository.RootSigner, time.Time) ([]byte, error)
	signLegacyRootRotation  func([]byte, []byte, string, string, tufrepository.Identity, time.Time) ([][]byte, error)
	assembleRootRotation    func(string, []byte, []byte, string, string, [][]byte, time.Time) ([]byte, tufrepository.RootRotationResult, error)
	applyRootRotation       func(tufrepository.Identity, []byte, string, string, time.Time) ([]byte, error)
	writeRootArtifact       func(context.Context, string, []byte, int64) (tufrepository.CeremonyArtifactWriteResult, error)
}

func defaultDependencies() dependencies {
	return dependencies{
		now:    func() time.Time { return time.Now().UTC() },
		random: rand.Reader, createPrivate: secureconfigfile.CreatePrivate,
		readSecure: secureconfigfile.Read, openCandidate: openStableCandidate,
		normalize: ccindex.Normalize, normalizeParquet: ccindex.NormalizeParquet, selectParquet: ccindex.SelectParquet, selectParquetParts: ccindex.SelectParquetParts,
		builderVersion: resolvedBuilderVersion,
		build:          openpackbuilder.Build, verify: openpackbuilder.VerifyBundle,
		buildDelta: openpackbuilder.BuildDelta, verifyDelta: openpackbuilder.VerifyDeltaBundle,
		generateChannelIdentity: tufrepository.GenerateIdentity, writeChannelIdentity: tufrepository.WriteIdentity,
		stageChannel: tufrepository.Stage, activateMirror: tufrepository.ActivateMirror,
		generateRootSigner: tufrepository.GenerateRootSigner, prepareRootRotation: tufrepository.PrepareRootRotation,
		signRootRotation: tufrepository.SignRootRotation, signLegacyRootRotation: tufrepository.SignRootRotationWithLegacyIdentity,
		assembleRootRotation: tufrepository.AssembleRootRotation, applyRootRotation: tufrepository.ApplyRootRotation,
		writeRootArtifact: tufrepository.WriteCeremonyArtifact,
	}
}

type request struct {
	command             string
	publisherID         string
	spec                string
	candidates          string
	exclusions          string
	evidenceReport      string
	identity            string
	publicIdentity      string
	parentBundle        string
	bundle              string
	output              string
	format              string
	input               string
	inputs              []string
	selectionProfile    string
	maxRecords          uint64
	languages           []string
	notAfter            time.Time
	repositoryID        string
	rootExpires         time.Time
	previous            string
	targetPath          string
	metadataVersion     int64
	timestampExpires    time.Time
	snapshotExpires     time.Time
	targetsExpires      time.Time
	withdraw            bool
	trustedRoot         string
	mirror              string
	candidate           string
	expectedCurrent     string
	expectedCandidate   string
	initializeEmpty     bool
	signerID            string
	currentRoot         string
	signer              string
	newSigners          []string
	contributions       []string
	legacyContributions string
	signedRoot          string
}

type singleStringFlag struct {
	name  string
	value string
	set   bool
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
		fmt.Fprintf(stderr, "fetchmark-pack-build: %v\n", err)
		return 2
	}
	var output any
	switch request.command {
	case "keygen":
		if deps.random == nil || deps.createPrivate == nil {
			err = errors.New("key generation dependencies are unavailable")
			break
		}
		var raw []byte
		raw, output, err = openpackbuilder.GenerateIdentity(request.publisherID, deps.random)
		if err == nil {
			err = deps.createPrivate(request.output, raw, openpackbuilder.MaxIdentityBytes)
		}
	case "build":
		output, err = executeBuild(ctx, request, deps)
	case "build-delta":
		output, err = executeBuildDelta(ctx, request, deps)
	case "normalize":
		output, err = executeNormalize(ctx, request, deps)
	case "select-parquet":
		output, err = executeSelectParquet(ctx, request, deps)
	case "select-parquet-parts":
		output, err = executeSelectParquetParts(ctx, request, deps)
	case "verify-run":
		output, err = executeVerify(ctx, request, deps)
	case "verify-delta-run":
		output, err = executeVerifyDelta(ctx, request, deps)
	case "channel-keygen":
		output, err = executeChannelKeygen(ctx, request, deps)
	case "channel-stage":
		output, err = executeChannelStage(ctx, request, deps)
	case "channel-mirror-activate":
		output, err = executeChannelMirrorActivate(ctx, request, deps)
	case "channel-root-keygen":
		output, err = executeChannelRootKeygen(ctx, request, deps)
	case "channel-root-public":
		output, err = executeChannelRootPublic(ctx, request, deps)
	case "channel-root-prepare":
		output, err = executeChannelRootPrepare(ctx, request, deps)
	case "channel-root-sign":
		output, err = executeChannelRootSign(ctx, request, deps)
	case "channel-root-sign-legacy":
		output, err = executeChannelRootSignLegacy(ctx, request, deps)
	case "channel-root-assemble":
		output, err = executeChannelRootAssemble(ctx, request, deps)
	case "channel-root-apply":
		output, err = executeChannelRootApply(ctx, request, deps)
	}
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-pack-build: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(output); err != nil {
		switch request.command {
		case "normalize", "select-parquet", "select-parquet-parts":
			if result, ok := output.(normalizeResult); ok && result.Path != "" {
				err = committedNormalizationError(result.Path, result.Report, fmt.Errorf("write normalization result: %w", err))
			}
		case "channel-stage":
			if result, ok := output.(tufrepository.StageResult); ok && result.Path != "" {
				err = &tufrepository.CommittedStageError{Result: result, Cause: fmt.Errorf("write stage result: %w", err)}
			}
		case "channel-keygen":
			if result, ok := output.(channelIdentityResult); ok && result.Path != "" {
				err = committedChannelIdentityError(result, fmt.Errorf("write key generation result: %w", err))
			}
		case "channel-mirror-activate":
			if result, ok := output.(tufrepository.MirrorActivationResult); ok && result.HeadCommitted {
				err = &tufrepository.CommittedMirrorActivationError{Result: result, Cause: fmt.Errorf("write mirror activation result: %w", err)}
			}
		case "channel-root-keygen":
			if result, ok := output.(rootKeygenResult); ok && result.Path != "" {
				err = committedRootArtifactError(result.Path, result.PrivateSHA256, fmt.Errorf("write root key generation result: %w", err))
			}
		case "channel-root-public", "channel-root-prepare", "channel-root-sign", "channel-root-sign-legacy", "channel-root-assemble":
			if result, ok := output.(rootArtifactResult); ok && result.Path != "" {
				err = committedRootArtifactError(result.Path, result.SHA256, fmt.Errorf("write root ceremony result: %w", err))
			}
		case "channel-root-apply":
			if result, ok := output.(tufrepository.IdentityWriteResult); ok && result.Path != "" {
				err = committedChannelIdentityError(result, fmt.Errorf("write operational identity result: %w", err))
			}
		}
		fmt.Fprintf(stderr, "fetchmark-pack-build: write result: %v\n", err)
		return 1
	}
	return 0
}

func parseRequest(args []string, stderr io.Writer) (request, error) {
	if len(args) == 0 {
		return request{}, errors.New("expected exactly one supported subcommand")
	}
	switch args[0] {
	case "keygen":
		flags := flag.NewFlagSet("fetchmark-pack-build keygen", flag.ContinueOnError)
		flags.SetOutput(stderr)
		publisher := &singleStringFlag{name: "publisher-id"}
		output := &singleStringFlag{name: "out"}
		flags.Var(publisher, "publisher-id", "stable publisher identity ID")
		flags.Var(output, "out", "clean absolute path for the new private identity")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		if !publisher.set || strings.TrimSpace(publisher.value) != publisher.value || publisher.value == "" {
			return request{}, errors.New("-publisher-id is required without surrounding whitespace")
		}
		if !output.set || !cleanAbsolute(output.value) {
			return request{}, errors.New("-out must be a clean absolute path")
		}
		return request{command: "keygen", publisherID: publisher.value, output: output.value}, nil
	case "normalize":
		flags := flag.NewFlagSet("fetchmark-pack-build normalize", flag.ContinueOnError)
		flags.SetOutput(stderr)
		format := &singleStringFlag{name: "format"}
		input := &singleStringFlag{name: "input"}
		output := &singleStringFlag{name: "out"}
		flags.Var(format, "format", "source format: cdx-api-json-v1, cdxj-header-v1, or url-index-parquet-flat-v1")
		flags.Var(input, "input", "exact Common Crawl CDX or flat URL Index Parquet export")
		flags.Var(output, "out", "new normalized metadata JSONL artifact")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		if !format.set || (format.value != ccindex.FormatCDXAPIJSON && format.value != ccindex.FormatCDXJHeader && format.value != ccindex.FormatURLIndexParquet) {
			return request{}, errors.New("-format must be cdx-api-json-v1, cdxj-header-v1, or url-index-parquet-flat-v1")
		}
		for _, required := range []*singleStringFlag{input, output} {
			if !required.set || !cleanAbsolute(required.value) {
				return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
			}
		}
		if input.value == output.value {
			return request{}, errors.New("-input and -out must differ")
		}
		return request{command: "normalize", format: format.value, input: input.value, output: output.value}, nil
	case "select-parquet":
		flags := flag.NewFlagSet("fetchmark-pack-build select-parquet", flag.ContinueOnError)
		flags.SetOutput(stderr)
		input := &singleStringFlag{name: "input"}
		output := &singleStringFlag{name: "out"}
		maxRecords := &singleStringFlag{name: "max-records"}
		profile := &singleStringFlag{name: "profile"}
		languages := &singleStringFlag{name: "languages"}
		notAfter := &singleStringFlag{name: "not-after"}
		flags.Var(input, "input", "exact local flat URL Index Parquet part")
		flags.Var(output, "out", "new bounded normalized metadata JSONL artifact")
		flags.Var(maxRecords, "max-records", "maximum selected rows")
		flags.Var(profile, "profile", "collector-v1 or explicit format-prototype-v1")
		flags.Var(languages, "languages", "sorted unique comma-separated language tags")
		flags.Var(notAfter, "not-after", "inclusive capture cutoff as an exact UTC RFC 3339 second")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		for _, required := range []*singleStringFlag{input, output} {
			if !required.set || !cleanAbsolute(required.value) {
				return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
			}
		}
		if input.value == output.value {
			return request{}, errors.New("-input and -out must differ")
		}
		selectedProfile, err := parseSelectionProfile(profile)
		if err != nil {
			return request{}, err
		}
		maximum, err := parseSelectionMaximum(maxRecords, selectedProfile)
		if err != nil {
			return request{}, err
		}
		selectedLanguages, err := parseSelectionLanguages(languages)
		if err != nil {
			return request{}, err
		}
		cutoff, err := parseSelectionCutoff(notAfter)
		if err != nil {
			return request{}, err
		}
		options, err := ccindex.ValidateParquetSelectionOptions(ccindex.ParquetSelectionOptions{
			Profile: selectedProfile, MaxRecords: maximum, Languages: selectedLanguages, NotAfter: cutoff,
		})
		if err != nil {
			return request{}, err
		}
		return request{
			command: "select-parquet", input: input.value, output: output.value,
			selectionProfile: options.Profile, maxRecords: options.MaxRecords, languages: options.Languages, notAfter: options.NotAfter,
		}, nil
	case "select-parquet-parts":
		flags := flag.NewFlagSet("fetchmark-pack-build select-parquet-parts", flag.ContinueOnError)
		flags.SetOutput(stderr)
		inputs := &repeatedStringFlag{name: "input"}
		output := &singleStringFlag{name: "out"}
		profile := &singleStringFlag{name: "profile"}
		maxRecords := &singleStringFlag{name: "max-records"}
		languages := &singleStringFlag{name: "languages"}
		notAfter := &singleStringFlag{name: "not-after"}
		flags.Var(inputs, "input", "exact local flat URL Index Parquet part; repeat for each part")
		flags.Var(output, "out", "new globally selected normalized metadata JSONL artifact")
		flags.Var(profile, "profile", "collector-v1 or explicit format-prototype-v1")
		flags.Var(maxRecords, "max-records", "maximum globally selected rows")
		flags.Var(languages, "languages", "sorted unique comma-separated language tags")
		flags.Var(notAfter, "not-after", "inclusive capture cutoff as an exact UTC RFC 3339 second")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		if len(inputs.values) < 2 || len(inputs.values) > ccindex.MaxParquetSelectionInputs {
			return request{}, fmt.Errorf("-input must be repeated 2..%d times", ccindex.MaxParquetSelectionInputs)
		}
		if !output.set || !cleanAbsolute(output.value) {
			return request{}, errors.New("-out must be a clean absolute path")
		}
		seenInputs := make(map[string]struct{}, len(inputs.values))
		for _, input := range inputs.values {
			if !cleanAbsolute(input) {
				return request{}, errors.New("every -input must be a clean absolute path")
			}
			if input == output.value {
				return request{}, errors.New("-out must differ from every -input")
			}
			if _, duplicate := seenInputs[input]; duplicate {
				return request{}, errors.New("-input paths must be unique")
			}
			seenInputs[input] = struct{}{}
		}
		selectedProfile, err := parseSelectionProfile(profile)
		if err != nil {
			return request{}, err
		}
		maximum, err := parseSelectionMaximum(maxRecords, selectedProfile)
		if err != nil {
			return request{}, err
		}
		selectedLanguages, err := parseSelectionLanguages(languages)
		if err != nil {
			return request{}, err
		}
		cutoff, err := parseSelectionCutoff(notAfter)
		if err != nil {
			return request{}, err
		}
		options, err := ccindex.ValidateParquetSelectionOptions(ccindex.ParquetSelectionOptions{
			Profile: selectedProfile, MaxRecords: maximum, Languages: selectedLanguages, NotAfter: cutoff,
		})
		if err != nil {
			return request{}, err
		}
		return request{
			command: "select-parquet-parts", inputs: append([]string(nil), inputs.values...), output: output.value,
			selectionProfile: options.Profile, maxRecords: options.MaxRecords, languages: options.Languages, notAfter: options.NotAfter,
		}, nil
	case "build":
		flags := flag.NewFlagSet("fetchmark-pack-build build", flag.ContinueOnError)
		flags.SetOutput(stderr)
		spec := &singleStringFlag{name: "spec"}
		candidates := &singleStringFlag{name: "candidates"}
		exclusions := &singleStringFlag{name: "exclusions"}
		evidenceReport := &singleStringFlag{name: "evidence-report"}
		identity := &singleStringFlag{name: "identity"}
		output := &singleStringFlag{name: "out"}
		flags.Var(spec, "spec", "exact build policy JSON")
		flags.Var(candidates, "candidates", "exact Common Crawl-derived candidate JSONL")
		flags.Var(exclusions, "exclusions", "exact opt-out and takedown JSON")
		flags.Var(evidenceReport, "evidence-report", "verified aggregate pack-evidence merge report")
		flags.Var(identity, "identity", "private publisher identity JSON")
		flags.Var(output, "out", "new output bundle directory")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		for _, required := range []*singleStringFlag{spec, candidates, exclusions, identity, output} {
			if !required.set || !cleanAbsolute(required.value) {
				return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
			}
		}
		if evidenceReport.set && !cleanAbsolute(evidenceReport.value) {
			return request{}, errors.New("-evidence-report must be a clean absolute path")
		}
		return request{
			command: "build", spec: spec.value, candidates: candidates.value,
			exclusions: exclusions.value, evidenceReport: evidenceReport.value, identity: identity.value, output: output.value,
		}, nil
	case "build-delta":
		flags := flag.NewFlagSet("fetchmark-pack-build build-delta", flag.ContinueOnError)
		flags.SetOutput(stderr)
		parent := &singleStringFlag{name: "parent-bundle"}
		spec := &singleStringFlag{name: "spec"}
		candidates := &singleStringFlag{name: "candidates"}
		exclusions := &singleStringFlag{name: "exclusions"}
		identity := &singleStringFlag{name: "identity"}
		output := &singleStringFlag{name: "out"}
		flags.Var(parent, "parent-bundle", "exact signed parent snapshot bundle")
		flags.Var(spec, "spec", "exact target build policy JSON")
		flags.Var(candidates, "candidates", "exact target Common Crawl-derived candidate JSONL")
		flags.Var(exclusions, "exclusions", "exact target opt-out and takedown JSON")
		flags.Var(identity, "identity", "private publisher identity JSON")
		flags.Var(output, "out", "new signed delta bundle directory")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		for _, required := range []*singleStringFlag{parent, spec, candidates, exclusions, identity, output} {
			if !required.set || !cleanAbsolute(required.value) {
				return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
			}
		}
		if parent.value == output.value {
			return request{}, errors.New("-parent-bundle and -out must differ")
		}
		return request{
			command: "build-delta", parentBundle: parent.value, spec: spec.value, candidates: candidates.value,
			exclusions: exclusions.value, identity: identity.value, output: output.value,
		}, nil
	case "verify-run":
		flags := flag.NewFlagSet("fetchmark-pack-build verify-run", flag.ContinueOnError)
		flags.SetOutput(stderr)
		bundle := &singleStringFlag{name: "bundle"}
		publicIdentity := &singleStringFlag{name: "public-identity"}
		flags.Var(bundle, "bundle", "built bundle directory")
		flags.Var(publicIdentity, "public-identity", "publisher public identity JSON")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		for _, required := range []*singleStringFlag{bundle, publicIdentity} {
			if !required.set || !cleanAbsolute(required.value) {
				return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
			}
		}
		return request{command: "verify-run", bundle: bundle.value, publicIdentity: publicIdentity.value}, nil
	case "verify-delta-run":
		flags := flag.NewFlagSet("fetchmark-pack-build verify-delta-run", flag.ContinueOnError)
		flags.SetOutput(stderr)
		parent := &singleStringFlag{name: "parent-bundle"}
		bundle := &singleStringFlag{name: "bundle"}
		publicIdentity := &singleStringFlag{name: "public-identity"}
		flags.Var(parent, "parent-bundle", "exact signed parent snapshot bundle")
		flags.Var(bundle, "bundle", "built signed delta bundle")
		flags.Var(publicIdentity, "public-identity", "publisher public identity JSON")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		for _, required := range []*singleStringFlag{parent, bundle, publicIdentity} {
			if !required.set || !cleanAbsolute(required.value) {
				return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
			}
		}
		if parent.value == bundle.value {
			return request{}, errors.New("-parent-bundle and -bundle must differ")
		}
		return request{command: "verify-delta-run", parentBundle: parent.value, bundle: bundle.value, publicIdentity: publicIdentity.value}, nil
	case "channel-keygen":
		flags := flag.NewFlagSet("fetchmark-pack-build channel-keygen", flag.ContinueOnError)
		flags.SetOutput(stderr)
		repositoryID := &singleStringFlag{name: "repository-id"}
		rootExpires := &singleStringFlag{name: "root-expires"}
		output := &singleStringFlag{name: "out"}
		flags.Var(repositoryID, "repository-id", "stable TUF repository identity ID")
		flags.Var(rootExpires, "root-expires", "bootstrap root expiry as an exact UTC RFC 3339 second")
		flags.Var(output, "out", "new private TUF repository identity JSON")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		if !repositoryID.set || repositoryID.value == "" || strings.TrimSpace(repositoryID.value) != repositoryID.value {
			return request{}, errors.New("-repository-id is required without surrounding whitespace")
		}
		expires, err := parseExactUTCTimeFlag(rootExpires, "root-expires")
		if err != nil {
			return request{}, err
		}
		if !output.set || !cleanAbsolute(output.value) {
			return request{}, errors.New("-out must be a clean absolute path")
		}
		return request{command: "channel-keygen", repositoryID: repositoryID.value, rootExpires: expires, output: output.value}, nil
	case "channel-root-keygen", "channel-root-public", "channel-root-prepare", "channel-root-sign", "channel-root-sign-legacy", "channel-root-assemble", "channel-root-apply":
		return parseChannelRootRequest(args, stderr)
	case "channel-stage":
		flags := flag.NewFlagSet("fetchmark-pack-build channel-stage", flag.ContinueOnError)
		flags.SetOutput(stderr)
		identity := &singleStringFlag{name: "identity"}
		bundle := &singleStringFlag{name: "bundle"}
		parent := &singleStringFlag{name: "parent-bundle"}
		publicIdentity := &singleStringFlag{name: "public-identity"}
		previous := &singleStringFlag{name: "previous"}
		target := &singleStringFlag{name: "target"}
		version := &singleStringFlag{name: "metadata-version"}
		timestampExpires := &singleStringFlag{name: "timestamp-expires"}
		snapshotExpires := &singleStringFlag{name: "snapshot-expires"}
		targetsExpires := &singleStringFlag{name: "targets-expires"}
		output := &singleStringFlag{name: "out"}
		withdraw := false
		flags.Var(identity, "identity", "private TUF repository identity JSON")
		flags.Var(bundle, "bundle", "exact signed snapshot or delta bundle")
		flags.Var(parent, "parent-bundle", "exact signed parent snapshot for a delta")
		flags.Var(publicIdentity, "public-identity", "pack publisher public identity JSON")
		flags.Var(previous, "previous", "previous immutable repository generation")
		flags.Var(target, "target", "logical packs/.../manifest.json TUF target path")
		flags.Var(version, "metadata-version", "next synchronized targets/snapshot/timestamp version")
		flags.Var(timestampExpires, "timestamp-expires", "timestamp expiry as an exact UTC RFC 3339 second")
		flags.Var(snapshotExpires, "snapshot-expires", "snapshot expiry as an exact UTC RFC 3339 second")
		flags.Var(targetsExpires, "targets-expires", "targets expiry as an exact UTC RFC 3339 second")
		flags.BoolVar(&withdraw, "withdraw", false, "publish authenticated target absence; requires -previous")
		flags.Var(output, "out", "new immutable mirror-stage directory")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		for _, required := range []*singleStringFlag{identity, target, version, timestampExpires, snapshotExpires, targetsExpires, output} {
			if !required.set {
				return request{}, fmt.Errorf("-%s is required", required.name)
			}
		}
		for _, file := range []*singleStringFlag{identity, output} {
			if !cleanAbsolute(file.value) {
				return request{}, fmt.Errorf("-%s must be a clean absolute path", file.name)
			}
		}
		if previous.set && !cleanAbsolute(previous.value) {
			return request{}, errors.New("-previous must be a clean absolute path")
		}
		if withdraw {
			if !previous.set {
				return request{}, errors.New("-withdraw requires -previous")
			}
			if bundle.set || parent.set || publicIdentity.set {
				return request{}, errors.New("-withdraw forbids -bundle, -parent-bundle, and -public-identity")
			}
		} else {
			for _, required := range []*singleStringFlag{bundle, publicIdentity} {
				if !required.set || !cleanAbsolute(required.value) {
					return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
				}
			}
			if parent.set && !cleanAbsolute(parent.value) {
				return request{}, errors.New("-parent-bundle must be a clean absolute path")
			}
		}
		parsedVersion, err := strconv.ParseInt(version.value, 10, 64)
		if err != nil || parsedVersion < 1 || strconv.FormatInt(parsedVersion, 10) != version.value {
			return request{}, errors.New("-metadata-version must be a canonical positive integer")
		}
		parsedTimestamp, err := parseExactUTCTimeFlag(timestampExpires, "timestamp-expires")
		if err != nil {
			return request{}, err
		}
		parsedSnapshot, err := parseExactUTCTimeFlag(snapshotExpires, "snapshot-expires")
		if err != nil {
			return request{}, err
		}
		parsedTargets, err := parseExactUTCTimeFlag(targetsExpires, "targets-expires")
		if err != nil {
			return request{}, err
		}
		return request{
			command: "channel-stage", identity: identity.value, bundle: bundle.value, parentBundle: parent.value,
			publicIdentity: publicIdentity.value, previous: previous.value, targetPath: target.value,
			metadataVersion: parsedVersion, timestampExpires: parsedTimestamp, snapshotExpires: parsedSnapshot,
			targetsExpires: parsedTargets, withdraw: withdraw, output: output.value,
		}, nil
	case "channel-mirror-activate":
		flags := flag.NewFlagSet("fetchmark-pack-build channel-mirror-activate", flag.ContinueOnError)
		flags.SetOutput(stderr)
		trustedRoot := &singleStringFlag{name: "trusted-root"}
		mirror := &singleStringFlag{name: "mirror"}
		candidate := &singleStringFlag{name: "candidate"}
		target := &singleStringFlag{name: "target"}
		publicIdentity := &singleStringFlag{name: "public-identity"}
		expectedCurrent := &singleStringFlag{name: "expected-current-timestamp-sha256"}
		expectedCandidate := &singleStringFlag{name: "expected-candidate-timestamp-sha256"}
		initializeEmpty := false
		flags.Var(trustedRoot, "trusted-root", "out-of-band pinned TUF root JSON")
		flags.Var(mirror, "mirror", "existing local mirror directory")
		flags.Var(candidate, "candidate", "verified immutable repository generation")
		flags.Var(target, "target", "logical packs/.../manifest.json TUF target path")
		flags.Var(publicIdentity, "public-identity", "pack publisher public identity JSON")
		flags.Var(expectedCurrent, "expected-current-timestamp-sha256", "exact live timestamp digest")
		flags.Var(expectedCandidate, "expected-candidate-timestamp-sha256", "exact candidate timestamp digest")
		flags.BoolVar(&initializeEmpty, "initialize-empty", false, "require an absent live timestamp head")
		if err := flags.Parse(args[1:]); err != nil {
			return request{}, err
		}
		if flags.NArg() != 0 {
			return request{}, errors.New("unexpected positional arguments")
		}
		for _, required := range []*singleStringFlag{trustedRoot, mirror, candidate, target, publicIdentity, expectedCandidate} {
			if !required.set {
				return request{}, fmt.Errorf("-%s is required", required.name)
			}
		}
		for _, required := range []*singleStringFlag{trustedRoot, mirror, candidate, publicIdentity} {
			if !cleanAbsolute(required.value) {
				return request{}, fmt.Errorf("-%s must be a clean absolute path", required.name)
			}
		}
		if !canonicalSHA256(expectedCandidate.value) {
			return request{}, errors.New("-expected-candidate-timestamp-sha256 must be 64 lowercase hexadecimal characters")
		}
		if initializeEmpty {
			if expectedCurrent.set {
				return request{}, errors.New("-initialize-empty forbids -expected-current-timestamp-sha256")
			}
		} else if !expectedCurrent.set || !canonicalSHA256(expectedCurrent.value) || expectedCurrent.value == expectedCandidate.value {
			return request{}, errors.New("normal activation requires distinct canonical -expected-current-timestamp-sha256 and -expected-candidate-timestamp-sha256")
		}
		return request{
			command: "channel-mirror-activate", trustedRoot: trustedRoot.value, mirror: mirror.value,
			candidate: candidate.value, targetPath: target.value, publicIdentity: publicIdentity.value,
			expectedCurrent: expectedCurrent.value, expectedCandidate: expectedCandidate.value, initializeEmpty: initializeEmpty,
		}, nil
	default:
		return request{}, fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func canonicalSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func parseExactUTCTimeFlag(value *singleStringFlag, name string) (time.Time, error) {
	if !value.set {
		return time.Time{}, fmt.Errorf("-%s is required", name)
	}
	parsed, err := time.Parse(time.RFC3339, value.value)
	if err != nil || parsed.Location() != time.UTC || parsed.Nanosecond() != 0 || parsed.Format(time.RFC3339) != value.value {
		return time.Time{}, fmt.Errorf("-%s must be an exact UTC RFC 3339 second", name)
	}
	return parsed, nil
}

func parseSelectionProfile(value *singleStringFlag) (string, error) {
	if !value.set {
		return ccindex.ParquetSelectionProfileCollector, nil
	}
	switch value.value {
	case ccindex.ParquetSelectionProfileCollector, ccindex.ParquetSelectionProfilePrototype:
		return value.value, nil
	default:
		return "", errors.New("-profile must be collector-v1 or format-prototype-v1")
	}
}

func parseSelectionMaximum(value *singleStringFlag, profile string) (uint64, error) {
	if !value.set || value.value == "" || strings.TrimSpace(value.value) != value.value {
		return 0, errors.New("-max-records is required as a canonical positive integer")
	}
	maximum := uint64(ccindex.MaxParquetSelectionRecords)
	if profile == ccindex.ParquetSelectionProfilePrototype {
		maximum = ccindex.MaxParquetPrototypeSelectionRecords
	}
	parsed, err := strconv.ParseUint(value.value, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != value.value || parsed == 0 || parsed > maximum {
		return 0, fmt.Errorf("-max-records must be a canonical integer from 1 through %d for profile %s", maximum, profile)
	}
	return parsed, nil
}

func parseSelectionLanguages(value *singleStringFlag) ([]string, error) {
	if !value.set || value.value == "" || strings.TrimSpace(value.value) != value.value {
		return nil, errors.New("-languages is required as sorted unique comma-separated language tags")
	}
	languages := strings.Split(value.value, ",")
	for index, language := range languages {
		if language == "" || strings.TrimSpace(language) != language || (index > 0 && languages[index-1] >= language) {
			return nil, errors.New("-languages must contain sorted unique comma-separated language tags")
		}
	}
	return languages, nil
}

func parseSelectionCutoff(value *singleStringFlag) (time.Time, error) {
	if !value.set {
		return time.Time{}, errors.New("-not-after is required as an exact UTC RFC 3339 second")
	}
	parsed, err := time.Parse(time.RFC3339, value.value)
	if err != nil || parsed.Location() != time.UTC || parsed.Nanosecond() != 0 || parsed.Format(time.RFC3339) != value.value {
		return time.Time{}, errors.New("-not-after must be an exact UTC RFC 3339 second")
	}
	return parsed, nil
}

type normalizeResult struct {
	Path   string         `json:"path"`
	Report ccindex.Report `json:"report"`
}

// ErrNormalizationCommitted means the final hard link was created, but a
// later cleanup, durability check, input close, or result write failed. The
// artifact must be inspected rather than blindly retried or removed.
var ErrNormalizationCommitted = errors.New("normalized artifact committed but command completion failed")

type normalizationStagingFile interface {
	io.Writer
	Sync() error
	Close() error
}

type normalizationHooks struct {
	removeStaging func(*os.Root, string) error
	syncParent    func(*os.Root) error
	checkParent   func(*os.Root, string) error
}

func defaultNormalizationHooks() normalizationHooks {
	return normalizationHooks{
		removeStaging: func(root *os.Root, name string) error { return root.Remove(name) },
		syncParent:    syncRoot,
		checkParent:   rootStillAtPath,
	}
}

func executeNormalize(ctx context.Context, request request, deps dependencies) (normalizeResult, error) {
	if deps.openCandidate == nil || (request.format == ccindex.FormatURLIndexParquet && deps.normalizeParquet == nil) || (request.format != ccindex.FormatURLIndexParquet && deps.normalize == nil) {
		return normalizeResult{}, errors.New("normalization dependencies are unavailable")
	}
	input, err := deps.openCandidate(request.input)
	if err != nil {
		return normalizeResult{}, fmt.Errorf("open normalization input: %w", err)
	}
	normalize := deps.normalize
	if request.format == ccindex.FormatURLIndexParquet {
		readerAt, ok := input.(io.ReaderAt)
		statter, statOK := input.(interface{ Stat() (os.FileInfo, error) })
		if !ok || !statOK {
			closeErr := input.Close()
			return normalizeResult{}, errors.Join(errors.New("Parquet normalization input must support stable random access"), closeErr)
		}
		info, statErr := statter.Stat()
		if statErr != nil {
			closeErr := input.Close()
			return normalizeResult{}, errors.Join(fmt.Errorf("stat Parquet normalization input: %w", statErr), closeErr)
		}
		normalize = func(ctx context.Context, _ io.Reader, output io.Writer, _ ccindex.Options) (ccindex.Report, error) {
			return deps.normalizeParquet(ctx, readerAt, info.Size(), output)
		}
	}
	result, normalizeErr := normalizeToFile(ctx, input, request.output, request.format, normalize)
	closeErr := input.Close()
	if closeErr != nil && result.Path != "" {
		closeErr = committedNormalizationError(result.Path, result.Report, fmt.Errorf("close normalization input: %w", closeErr))
	}
	return result, errors.Join(normalizeErr, closeErr)
}

func executeSelectParquet(ctx context.Context, request request, deps dependencies) (normalizeResult, error) {
	if deps.openCandidate == nil || deps.selectParquet == nil {
		return normalizeResult{}, errors.New("Parquet selection dependencies are unavailable")
	}
	input, readerAt, size, err := openParquetInput(deps.openCandidate, request.input, "selection")
	if err != nil {
		return normalizeResult{}, err
	}
	options := ccindex.ParquetSelectionOptions{
		Profile: request.selectionProfile, MaxRecords: request.maxRecords, Languages: request.languages, NotAfter: request.notAfter,
	}
	selectRows := func(ctx context.Context, _ io.Reader, output io.Writer, _ ccindex.Options) (ccindex.Report, error) {
		return deps.selectParquet(ctx, readerAt, size, output, options)
	}
	result, selectErr := normalizeToFile(ctx, input, request.output, ccindex.FormatURLIndexParquetSelection, selectRows)
	closeErr := input.Close()
	if closeErr != nil && result.Path != "" {
		closeErr = committedNormalizationError(result.Path, result.Report, fmt.Errorf("close Parquet selection input: %w", closeErr))
	}
	return result, errors.Join(selectErr, closeErr)
}

func executeSelectParquetParts(ctx context.Context, request request, deps dependencies) (normalizeResult, error) {
	if deps.openCandidate == nil || deps.selectParquetParts == nil {
		return normalizeResult{}, errors.New("multi-part Parquet selection dependencies are unavailable")
	}
	closers := make([]io.ReadCloser, 0, len(request.inputs))
	inputs := make([]ccindex.ParquetSelectionInput, 0, len(request.inputs))
	for index, path := range request.inputs {
		input, readerAt, size, err := openParquetInput(deps.openCandidate, path, fmt.Sprintf("multi-part selection input %d", index+1))
		if err != nil {
			return normalizeResult{}, errors.Join(err, closeSelectionInputs(closers))
		}
		closers = append(closers, input)
		inputs = append(inputs, ccindex.ParquetSelectionInput{Reader: readerAt, Size: size})
	}
	options := ccindex.ParquetSelectionOptions{
		Profile: request.selectionProfile, MaxRecords: request.maxRecords, Languages: request.languages, NotAfter: request.notAfter,
	}
	selectParts := func(ctx context.Context, _ io.Reader, output io.Writer, _ ccindex.Options) (ccindex.Report, error) {
		return deps.selectParquetParts(ctx, inputs, output, options)
	}
	result, selectErr := normalizeToFile(ctx, closers[0], request.output, ccindex.FormatURLIndexParquetPartsSelection, selectParts)
	closeErr := closeSelectionInputs(closers)
	if closeErr != nil && result.Path != "" {
		closeErr = committedNormalizationError(result.Path, result.Report, fmt.Errorf("close multi-part Parquet selection inputs: %w", closeErr))
	}
	return result, errors.Join(selectErr, closeErr)
}

func closeSelectionInputs(inputs []io.ReadCloser) error {
	var result error
	for _, input := range inputs {
		result = errors.Join(result, input.Close())
	}
	return result
}

func openParquetInput(open func(string) (io.ReadCloser, error), path, operation string) (io.ReadCloser, io.ReaderAt, int64, error) {
	input, err := open(path)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("open Parquet %s input: %w", operation, err)
	}
	readerAt, readerOK := input.(io.ReaderAt)
	statter, statOK := input.(interface{ Stat() (os.FileInfo, error) })
	if !readerOK || !statOK {
		closeErr := input.Close()
		return nil, nil, 0, errors.Join(fmt.Errorf("Parquet %s input must support stable random access", operation), closeErr)
	}
	info, statErr := statter.Stat()
	if statErr != nil {
		closeErr := input.Close()
		return nil, nil, 0, errors.Join(fmt.Errorf("stat Parquet %s input: %w", operation, statErr), closeErr)
	}
	return input, readerAt, info.Size(), nil
}

func normalizeToFile(
	ctx context.Context,
	input io.Reader,
	outputPath string,
	format string,
	normalize func(context.Context, io.Reader, io.Writer, ccindex.Options) (ccindex.Report, error),
) (result normalizeResult, returnErr error) {
	return normalizeToFileWithHooks(ctx, input, outputPath, format, normalize, defaultNormalizationHooks())
}

func normalizeToFileWithHooks(
	ctx context.Context,
	input io.Reader,
	outputPath string,
	format string,
	normalize func(context.Context, io.Reader, io.Writer, ccindex.Options) (ccindex.Report, error),
	hooks normalizationHooks,
) (result normalizeResult, returnErr error) {
	if ctx == nil || input == nil || normalize == nil || !cleanAbsolute(outputPath) {
		return normalizeResult{}, errors.New("normalization requires a context, input, implementation, and clean absolute output")
	}
	if hooks.removeStaging == nil || hooks.syncParent == nil || hooks.checkParent == nil {
		return normalizeResult{}, errors.New("normalization hooks are incomplete")
	}
	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(outputPath))
	if err != nil {
		return normalizeResult{}, fmt.Errorf("validate normalization output parent: %w", err)
	}
	outputBase := filepath.Base(outputPath)
	root, err := os.OpenRoot(parentPath)
	if err != nil {
		return normalizeResult{}, fmt.Errorf("anchor normalization output parent: %w", err)
	}
	committed := false
	defer func() {
		if err := root.Close(); err != nil {
			returnErr = joinNormalizationError(returnErr, err, committed, outputPath, result.Report)
		}
	}()
	nameDigest := sha256.Sum256([]byte(outputBase))
	temporary := ".fetchmark-normalize-" + hex.EncodeToString(nameDigest[:]) + ".tmp"
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return normalizeResult{}, fmt.Errorf("create private normalization staging file: %w", err)
	}
	temporaryExists := true
	defer func() {
		if temporaryExists {
			if err := root.Remove(temporary); err != nil && !errors.Is(err, os.ErrNotExist) {
				returnErr = joinNormalizationError(returnErr, fmt.Errorf("remove normalization staging file: %w", err), committed, outputPath, result.Report)
			}
		}
	}()
	report, normalizeErr := normalize(ctx, input, file, ccindex.Options{Format: format})
	if normalizeErr == nil {
		normalizeErr = ctx.Err()
	}
	if err := finishNormalizationFile(file, normalizeErr); err != nil {
		return normalizeResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return normalizeResult{}, err
	}
	if err := hooks.checkParent(root, parentPath); err != nil {
		return normalizeResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return normalizeResult{}, err
	}
	if err := root.Link(temporary, outputBase); err != nil {
		if errors.Is(err, os.ErrExist) {
			return normalizeResult{}, errors.New("normalization output already exists")
		}
		return normalizeResult{}, fmt.Errorf("atomically activate normalized artifact: %w", err)
	}
	committed = true
	result = normalizeResult{Path: outputPath, Report: report}
	if err := hooks.removeStaging(root, temporary); err != nil {
		return result, committedNormalizationError(outputPath, report, fmt.Errorf("remove activated normalization staging link: %w", err))
	}
	temporaryExists = false
	if err := hooks.syncParent(root); err != nil {
		return result, committedNormalizationError(outputPath, report, fmt.Errorf("sync normalization output parent: %w", err))
	}
	if err := hooks.checkParent(root, parentPath); err != nil {
		return result, committedNormalizationError(outputPath, report, err)
	}
	return result, nil
}

func finishNormalizationFile(file normalizationStagingFile, operationErr error) error {
	if operationErr != nil {
		return errors.Join(operationErr, file.Close())
	}
	return errors.Join(file.Sync(), file.Close())
}

func joinNormalizationError(existing, next error, committed bool, outputPath string, report ccindex.Report) error {
	if next == nil {
		return existing
	}
	if committed && !errors.Is(next, ErrNormalizationCommitted) {
		next = committedNormalizationError(outputPath, report, next)
	}
	return errors.Join(existing, next)
}

func committedNormalizationError(outputPath string, report ccindex.Report, cause error) error {
	return fmt.Errorf(
		"%w: output=%q sha256=%s: %w; inspect and hash any existing output against this digest before retrying or removing it",
		ErrNormalizationCommitted, outputPath, report.OutputSHA256, cause,
	)
}

func rootStillAtPath(root *os.Root, path string) error {
	anchored, err := root.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(path)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return errors.New("normalization output parent path changed during operation")
	}
	return nil
}

func syncRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

type channelIdentityResult = tufrepository.IdentityWriteResult

var ErrChannelIdentityCommitted = errors.New("TUF repository identity committed but command completion failed")

func committedChannelIdentityError(result channelIdentityResult, cause error) error {
	return fmt.Errorf(
		"%w: path=%q root_sha256=%s: %w; inspect the private identity and preserve this root digest before retrying or removing it",
		ErrChannelIdentityCommitted, result.Path, result.RootSHA256, cause,
	)
}

func executeChannelKeygen(ctx context.Context, request request, deps dependencies) (channelIdentityResult, error) {
	if deps.now == nil || deps.random == nil || deps.generateChannelIdentity == nil || deps.writeChannelIdentity == nil {
		return channelIdentityResult{}, errors.New("channel key generation dependencies are unavailable")
	}
	now := deps.now().UTC()
	if !request.rootExpires.After(now) {
		return channelIdentityResult{}, errors.New("root expiry must be in the future")
	}
	raw, _, err := deps.generateChannelIdentity(request.repositoryID, request.rootExpires, deps.random)
	if err != nil {
		return channelIdentityResult{}, err
	}
	return deps.writeChannelIdentity(ctx, request.output, raw)
}

func executeChannelStage(ctx context.Context, request request, deps dependencies) (tufrepository.StageResult, error) {
	if deps.now == nil || deps.readSecure == nil || deps.stageChannel == nil {
		return tufrepository.StageResult{}, errors.New("channel staging dependencies are unavailable")
	}
	identityRaw, err := deps.readSecure(request.identity, secureconfigfile.Options{MaxBytes: tufrepository.MaxIdentityBytes, Mode: secureconfigfile.PrivateIdentity})
	if err != nil {
		return tufrepository.StageResult{}, fmt.Errorf("read TUF repository identity: %w", err)
	}
	identity, err := tufrepository.DecodeIdentity(identityRaw)
	if err != nil {
		return tufrepository.StageResult{}, err
	}
	options := tufrepository.StageOptions{
		Identity: identity, BundleDir: request.bundle, PreviousRepositoryDir: request.previous,
		OutputDir: request.output, TargetPath: request.targetPath, Version: request.metadataVersion,
		Now: deps.now().UTC().Truncate(time.Second), TimestampExpires: request.timestampExpires,
		SnapshotExpires: request.snapshotExpires, TargetsExpires: request.targetsExpires, Withdraw: request.withdraw,
	}
	if request.withdraw {
		return deps.stageChannel(ctx, options)
	}
	if deps.verify == nil || deps.verifyDelta == nil {
		return tufrepository.StageResult{}, errors.New("pack verification dependencies are unavailable")
	}
	publicRaw, err := deps.readSecure(request.publicIdentity, secureconfigfile.Options{MaxBytes: openpackbuilder.MaxIdentityBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return tufrepository.StageResult{}, fmt.Errorf("read publisher public identity: %w", err)
	}
	publicIdentity, publicKey, err := openpackbuilder.DecodePublicIdentity(publicRaw)
	if err != nil {
		return tufrepository.StageResult{}, err
	}
	manifestRaw, err := deps.readSecure(filepath.Join(request.bundle, openpackbuilder.ManifestFilename), secureconfigfile.Options{MaxBytes: indexpack.MaxManifestBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return tufrepository.StageResult{}, fmt.Errorf("read bundle manifest: %w", err)
	}
	manifest, err := indexpack.DecodeManifest(manifestRaw)
	if err != nil {
		return tufrepository.StageResult{}, err
	}
	switch manifest.Kind {
	case indexpack.KindSnapshot:
		if request.parentBundle != "" {
			return tufrepository.StageResult{}, errors.New("snapshot channel stage forbids -parent-bundle")
		}
		verification, err := deps.verify(ctx, request.bundle, publicIdentity, publicKey)
		if err != nil {
			return tufrepository.StageResult{}, fmt.Errorf("verify snapshot before channel stage: %w", err)
		}
		options.ExpectedManifestSHA256 = verification.ManifestSHA256
	case indexpack.KindDelta:
		if request.parentBundle == "" {
			return tufrepository.StageResult{}, errors.New("delta channel stage requires -parent-bundle")
		}
		verification, err := deps.verifyDelta(ctx, request.parentBundle, request.bundle, publicIdentity, publicKey)
		if err != nil {
			return tufrepository.StageResult{}, fmt.Errorf("verify delta before channel stage: %w", err)
		}
		options.ExpectedManifestSHA256 = verification.ManifestSHA256
	default:
		return tufrepository.StageResult{}, fmt.Errorf("unsupported manifest kind %q", manifest.Kind)
	}
	options.PublisherKeys = map[string]ed25519.PublicKey{publicIdentity.KeyID: publicKey}
	return deps.stageChannel(ctx, options)
}

func executeChannelMirrorActivate(ctx context.Context, request request, deps dependencies) (tufrepository.MirrorActivationResult, error) {
	if deps.readSecure == nil || deps.activateMirror == nil {
		return tufrepository.MirrorActivationResult{}, errors.New("channel mirror activation dependencies are unavailable")
	}
	publicRaw, err := deps.readSecure(request.publicIdentity, secureconfigfile.Options{MaxBytes: openpackbuilder.MaxIdentityBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return tufrepository.MirrorActivationResult{}, fmt.Errorf("read publisher public identity: %w", err)
	}
	publicIdentity, publicKey, err := openpackbuilder.DecodePublicIdentity(publicRaw)
	if err != nil {
		return tufrepository.MirrorActivationResult{}, err
	}
	return deps.activateMirror(ctx, tufrepository.MirrorActivationOptions{
		TrustedRootPath: request.trustedRoot, MirrorDir: request.mirror, CandidateDir: request.candidate,
		TargetPath: request.targetPath, PublisherKeys: map[string]ed25519.PublicKey{publicIdentity.KeyID: publicKey},
		ExpectedCurrentTimestampSHA256:   request.expectedCurrent,
		ExpectedCandidateTimestampSHA256: request.expectedCandidate, InitializeEmpty: request.initializeEmpty,
	})
}

func executeVerify(ctx context.Context, request request, deps dependencies) (openpackbuilder.Verification, error) {
	if deps.readSecure == nil || deps.verify == nil {
		return openpackbuilder.Verification{}, errors.New("verification dependencies are unavailable")
	}
	raw, err := deps.readSecure(request.publicIdentity, secureconfigfile.Options{MaxBytes: openpackbuilder.MaxIdentityBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return openpackbuilder.Verification{}, fmt.Errorf("read public identity: %w", err)
	}
	identity, publicKey, err := openpackbuilder.DecodePublicIdentity(raw)
	if err != nil {
		return openpackbuilder.Verification{}, err
	}
	return deps.verify(ctx, request.bundle, identity, publicKey)
}

func executeVerifyDelta(ctx context.Context, request request, deps dependencies) (openpackbuilder.DeltaVerification, error) {
	if deps.readSecure == nil || deps.verifyDelta == nil {
		return openpackbuilder.DeltaVerification{}, errors.New("delta verification dependencies are unavailable")
	}
	raw, err := deps.readSecure(request.publicIdentity, secureconfigfile.Options{MaxBytes: openpackbuilder.MaxIdentityBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return openpackbuilder.DeltaVerification{}, fmt.Errorf("read public identity: %w", err)
	}
	identity, publicKey, err := openpackbuilder.DecodePublicIdentity(raw)
	if err != nil {
		return openpackbuilder.DeltaVerification{}, err
	}
	return deps.verifyDelta(ctx, request.parentBundle, request.bundle, identity, publicKey)
}

func executeBuild(ctx context.Context, request request, deps dependencies) (openpackbuilder.Result, error) {
	if deps.readSecure == nil || deps.openCandidate == nil || deps.builderVersion == nil || deps.build == nil {
		return openpackbuilder.Result{}, errors.New("build dependencies are unavailable")
	}
	builderVersion, err := deps.builderVersion()
	if err != nil {
		return openpackbuilder.Result{}, fmt.Errorf("resolve builder version: %w", err)
	}
	specRaw, err := deps.readSecure(request.spec, secureconfigfile.Options{MaxBytes: indexpackselection.MaxSpecBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return openpackbuilder.Result{}, fmt.Errorf("read spec: %w", err)
	}
	exclusionsRaw, err := deps.readSecure(request.exclusions, secureconfigfile.Options{MaxBytes: indexpackselection.MaxExclusionsBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return openpackbuilder.Result{}, fmt.Errorf("read exclusions: %w", err)
	}
	var evidenceRaw []byte
	if request.evidenceReport != "" {
		evidenceRaw, err = deps.readSecure(request.evidenceReport, secureconfigfile.Options{MaxBytes: packevidence.MaxMergeReportBytes, Mode: secureconfigfile.PublicConfig})
		if err != nil {
			return openpackbuilder.Result{}, fmt.Errorf("read evidence report: %w", err)
		}
	}
	identityRaw, err := deps.readSecure(request.identity, secureconfigfile.Options{MaxBytes: openpackbuilder.MaxIdentityBytes, Mode: secureconfigfile.PrivateIdentity})
	if err != nil {
		return openpackbuilder.Result{}, fmt.Errorf("read publisher identity: %w", err)
	}
	identity, err := openpackbuilder.DecodeIdentity(identityRaw)
	if err != nil {
		return openpackbuilder.Result{}, err
	}
	candidates, err := deps.openCandidate(request.candidates)
	if err != nil {
		return openpackbuilder.Result{}, fmt.Errorf("open candidates: %w", err)
	}
	result, buildErr := deps.build(ctx, openpackbuilder.Options{
		SpecRaw: specRaw, ExclusionsRaw: exclusionsRaw, Candidates: candidates, EvidenceReportRaw: evidenceRaw,
		Identity: identity, BuilderVersion: builderVersion, OutputDir: request.output,
	})
	closeErr := candidates.Close()
	return result, errors.Join(buildErr, closeErr)
}

func executeBuildDelta(ctx context.Context, request request, deps dependencies) (openpackbuilder.DeltaResult, error) {
	if deps.readSecure == nil || deps.openCandidate == nil || deps.builderVersion == nil || deps.buildDelta == nil {
		return openpackbuilder.DeltaResult{}, errors.New("delta build dependencies are unavailable")
	}
	builderVersion, err := deps.builderVersion()
	if err != nil {
		return openpackbuilder.DeltaResult{}, fmt.Errorf("resolve builder version: %w", err)
	}
	specRaw, err := deps.readSecure(request.spec, secureconfigfile.Options{MaxBytes: indexpackselection.MaxSpecBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return openpackbuilder.DeltaResult{}, fmt.Errorf("read spec: %w", err)
	}
	exclusionsRaw, err := deps.readSecure(request.exclusions, secureconfigfile.Options{MaxBytes: indexpackselection.MaxExclusionsBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return openpackbuilder.DeltaResult{}, fmt.Errorf("read exclusions: %w", err)
	}
	identityRaw, err := deps.readSecure(request.identity, secureconfigfile.Options{MaxBytes: openpackbuilder.MaxIdentityBytes, Mode: secureconfigfile.PrivateIdentity})
	if err != nil {
		return openpackbuilder.DeltaResult{}, fmt.Errorf("read publisher identity: %w", err)
	}
	identity, err := openpackbuilder.DecodeIdentity(identityRaw)
	if err != nil {
		return openpackbuilder.DeltaResult{}, err
	}
	candidates, err := deps.openCandidate(request.candidates)
	if err != nil {
		return openpackbuilder.DeltaResult{}, fmt.Errorf("open candidates: %w", err)
	}
	result, buildErr := deps.buildDelta(ctx, openpackbuilder.DeltaOptions{
		ParentBundleDir: request.parentBundle, SpecRaw: specRaw, ExclusionsRaw: exclusionsRaw,
		Candidates: candidates, Identity: identity, BuilderVersion: builderVersion, OutputDir: request.output,
	})
	closeErr := candidates.Close()
	if buildErr == nil && closeErr != nil && result.Path != "" {
		closeErr = openpackbuilder.DeltaCommittedError(result, fmt.Errorf("close candidate input after committed delta build: %w", closeErr))
	}
	return result, errors.Join(buildErr, closeErr)
}

func resolvedBuilderVersion() (string, error) {
	if version != "dev" {
		if version == "" || strings.TrimSpace(version) != version || len(version) > 128 {
			return "", errors.New("linked builder version is invalid")
		}
		return version, nil
	}
	digest, err := buildidentity.CurrentExecutableSHA256()
	if err != nil {
		return "", err
	}
	canonical, ok := buildidentity.Parse(digest)
	if !ok {
		return "", errors.New("executable SHA-256 is invalid")
	}
	return "binary-sha256:" + canonical, nil
}

func openStableCandidate(path string) (io.ReadCloser, error) {
	if !cleanAbsolute(path) {
		return nil, errors.New("candidate path must be clean and absolute")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("candidate input must be a regular non-symlink file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, errors.New("candidate input changed while opening")
	}
	return file, nil
}

func cleanAbsolute(path string) bool {
	return path != "" && strings.TrimSpace(path) == path && filepath.IsAbs(path) && filepath.Clean(path) == path
}
