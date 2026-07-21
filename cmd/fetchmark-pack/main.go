// Command fetchmark-pack selects, retrieves, verifies, installs, and prepares
// operator-trusted open-index pack registry candidates. Network access exists
// only behind the explicit channel-fetch subcommand and its required consent
// flag.
package main

import (
	"context"
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

	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/openpackregistryfile"
	"github.com/staticvar/fetchmark/internal/adapters/tufchannel"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackregistry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

type dependencies struct {
	now                    func() time.Time
	makeVerifyRoot         func() (string, error)
	removeVerifyRoot       func(string) error
	install                func(context.Context, openpackindex.InstallOptions) (openpackindex.Installed, error)
	applyDelta             func(context.Context, openpackindex.DeltaInstallOptions) (openpackindex.Installed, error)
	selectChannel          func(context.Context, tufchannel.Options) (tufchannel.Selection, error)
	retrieveChannel        func(context.Context, tufchannel.RetrieveOptions) (tufchannel.Retrieval, error)
	writeRegistryCandidate func(context.Context, string, openpackregistry.Registry) (string, error)
	activateRegistry       func(context.Context, openpackregistryfile.ActivationOptions) (openpackregistryfile.ActivationResult, error)
}

func defaultDependencies() dependencies {
	return dependencies{
		now: func() time.Time { return time.Now().UTC() },
		makeVerifyRoot: func() (string, error) {
			return os.MkdirTemp("", "fetchmark-pack-verify-")
		},
		removeVerifyRoot:       os.RemoveAll,
		install:                openpackindex.Install,
		applyDelta:             openpackindex.ApplyDelta,
		selectChannel:          tufchannel.Select,
		retrieveChannel:        tufchannel.Retrieve,
		writeRegistryCandidate: openpackregistryfile.WriteNew,
		activateRegistry:       openpackregistryfile.Activate,
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runWithDependencies(ctx, args, stdout, stderr, defaultDependencies())
}

func runWithDependencies(ctx context.Context, args []string, stdout, stderr io.Writer, deps dependencies) int {
	request, err := parseRequest(args, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-pack: %v\n", err)
		return 2
	}
	result, err := execute(ctx, request, deps)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-pack: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		if result.Status == string(openpackregistryfile.ActivationStatusActivated) ||
			result.Status == string(openpackregistryfile.ActivationStatusRolledBack) {
			err = &openpackregistryfile.CommittedActivationError{
				Result: openpackregistryfile.ActivationResult{
					Status: openpackregistryfile.ActivationStatus(result.Status), Mode: result.Mode,
					SourceID: result.SourceID, Path: result.Path, RollbackPath: result.RollbackPath,
					FromRegistrySHA256: result.FromRegistrySHA256, ToRegistrySHA256: result.ToRegistrySHA256,
					RollbackRegistrySHA256: result.RollbackRegistrySHA256,
					FromRevision:           result.FromRevision, ToRevision: result.ToRevision, RestartRequired: result.RestartRequired,
				},
				Cause: fmt.Errorf("write result: %w", err),
			}
		}
		fmt.Fprintf(stderr, "fetchmark-pack: write result: %v\n", err)
		return 1
	}
	return 0
}

type request struct {
	command                  string
	registry                 string
	sourceID                 string
	bundle                   string
	parentBundle             string
	maxProjectionBytes       uint64
	trustedRoot              string
	metadataDir              string
	stateDir                 string
	targetPath               string
	targetBaseURL            string
	output                   string
	maxDownloadBytes         uint64
	maxShards                int
	projectionRecordCount    uint64
	projectionRecordCountSet bool
	activationCandidate      string
	expectedCurrentSHA256    string
	expectedCandidateSHA256  string
	transitionMode           openpackregistry.TransitionMode
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

type singleUint64Flag struct {
	name    string
	value   uint64
	maximum uint64
	set     bool
}

type singleBoolFlag struct {
	name  string
	value bool
	set   bool
}

func (value *singleBoolFlag) String() string   { return strconv.FormatBool(value.value) }
func (value *singleBoolFlag) IsBoolFlag() bool { return true }

func (value *singleBoolFlag) Set(raw string) error {
	if value.set {
		return fmt.Errorf("-%s may only be specified once", value.name)
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fmt.Errorf("-%s must be true or false", value.name)
	}
	value.set = true
	value.value = parsed
	return nil
}

func (value *singleUint64Flag) String() string { return strconv.FormatUint(value.value, 10) }

func (value *singleUint64Flag) Set(raw string) error {
	if value.set {
		return fmt.Errorf("-%s may only be specified once", value.name)
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || parsed == 0 || parsed > value.maximum {
		return fmt.Errorf("-%s must be an integer from 1 through %d", value.name, value.maximum)
	}
	value.set = true
	value.value = parsed
	return nil
}

func parseRequest(args []string, stderr io.Writer) (request, error) {
	if len(args) == 0 {
		return request{}, errors.New("expected exactly one subcommand: verify, install, channel-select, channel-fetch, registry-candidate, registry-activate, or registry-rollback")
	}
	command := args[0]
	if command == "channel-select" {
		return parseChannelRequest(args, stderr)
	}
	if command == commandChannelFetch {
		return parseChannelFetchRequest(args, stderr)
	}
	if command == commandRegistryCandidate {
		return parseRegistryCandidateRequest(args, stderr)
	}
	if command == commandRegistryActivate || command == commandRegistryRollback {
		return parseRegistryActivationRequest(args, stderr)
	}
	if command != "verify" && command != "install" {
		return request{}, fmt.Errorf("unknown subcommand %q", command)
	}
	flags := flag.NewFlagSet("fetchmark-pack "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	registry := &singleStringFlag{name: "registry"}
	source := &singleStringFlag{name: "source"}
	bundle := &singleStringFlag{name: "bundle"}
	parentBundle := &singleStringFlag{name: "parent-bundle"}
	maxProjectionBytes := &singleUint64Flag{
		name: "max-projection-bytes", value: openpackindex.MaxProjectionBytes,
		maximum: openpackindex.MaxProjectionBytes,
	}
	flags.Var(registry, "registry", "absolute path to the operator trust registry")
	flags.Var(source, "source", "source binding ID in the registry")
	flags.Var(bundle, "bundle", "absolute path to the local pack bundle directory")
	flags.Var(parentBundle, "parent-bundle", "absolute path to the exact parent snapshot bundle for a delta")
	flags.Var(maxProjectionBytes, "max-projection-bytes", "maximum logical projection bytes allowed before activation")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	for _, required := range []*singleStringFlag{registry, source, bundle} {
		if !required.set || strings.TrimSpace(required.value) == "" {
			return request{}, fmt.Errorf("-%s is required", required.name)
		}
	}
	if !cleanAbsolute(registry.value) {
		return request{}, errors.New("-registry must be a clean absolute path")
	}
	if !cleanAbsolute(bundle.value) {
		return request{}, errors.New("-bundle must be a clean absolute path")
	}
	if parentBundle.set && !cleanAbsolute(parentBundle.value) {
		return request{}, errors.New("-parent-bundle must be a clean absolute path")
	}
	if strings.TrimSpace(source.value) != source.value {
		return request{}, errors.New("-source must not have leading or trailing whitespace")
	}
	return request{
		command: command, registry: registry.value, sourceID: source.value, bundle: bundle.value, parentBundle: parentBundle.value,
		maxProjectionBytes: maxProjectionBytes.value,
	}, nil
}

func parseChannelRequest(args []string, stderr io.Writer) (request, error) {
	flags := flag.NewFlagSet("fetchmark-pack channel-select", flag.ContinueOnError)
	flags.SetOutput(stderr)
	trustedRoot := &singleStringFlag{name: "trusted-root"}
	metadataDir := &singleStringFlag{name: "metadata-dir"}
	stateDir := &singleStringFlag{name: "state-dir"}
	target := &singleStringFlag{name: "target"}
	bundle := &singleStringFlag{name: "bundle"}
	flags.Var(trustedRoot, "trusted-root", "absolute path to the out-of-band pinned TUF root")
	flags.Var(metadataDir, "metadata-dir", "absolute path to the operator-supplied TUF metadata directory")
	flags.Var(stateDir, "state-dir", "absolute path to the durable trusted TUF state directory")
	flags.Var(target, "target", "top-level TUF target path packs/.../manifest.json")
	flags.Var(bundle, "bundle", "absolute path to the local pack bundle directory; optional for signed absence")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	for _, required := range []*singleStringFlag{trustedRoot, metadataDir, stateDir, target} {
		if !required.set || strings.TrimSpace(required.value) == "" {
			return request{}, fmt.Errorf("-%s is required", required.name)
		}
	}
	for _, pathFlag := range []*singleStringFlag{trustedRoot, metadataDir, stateDir} {
		if !cleanAbsolute(pathFlag.value) {
			return request{}, fmt.Errorf("-%s must be a clean absolute path", pathFlag.name)
		}
	}
	if bundle.set && !cleanAbsolute(bundle.value) {
		return request{}, errors.New("-bundle must be a clean absolute path")
	}
	if strings.TrimSpace(target.value) != target.value {
		return request{}, errors.New("-target must not have leading or trailing whitespace")
	}
	return request{
		command: commandChannelSelect, trustedRoot: trustedRoot.value, metadataDir: metadataDir.value,
		stateDir: stateDir.value, targetPath: target.value, bundle: bundle.value,
	}, nil
}

const commandChannelSelect = "channel-select"
const commandChannelFetch = "channel-fetch"
const commandRegistryCandidate = "registry-candidate"
const commandRegistryActivate = "registry-activate"
const commandRegistryRollback = "registry-rollback"

func parseRegistryActivationRequest(args []string, stderr io.Writer) (request, error) {
	command := args[0]
	mode := openpackregistry.TransitionActivate
	candidateFlagName := "candidate"
	candidateDigestFlagName := "expected-candidate-sha256"
	if command == commandRegistryRollback {
		mode = openpackregistry.TransitionRollback
		candidateFlagName = "rollback"
		candidateDigestFlagName = "expected-rollback-sha256"
	}
	flags := flag.NewFlagSet("fetchmark-pack "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	registry := &singleStringFlag{name: "registry"}
	source := &singleStringFlag{name: "source"}
	candidate := &singleStringFlag{name: candidateFlagName}
	expectedCurrent := &singleStringFlag{name: "expected-current-sha256"}
	expectedCandidate := &singleStringFlag{name: candidateDigestFlagName}
	rollbackOutput := &singleStringFlag{name: "rollback-out"}
	flags.Var(registry, "registry", "absolute path to the active operator registry")
	flags.Var(source, "source", "single existing source binding changed by this transition")
	flags.Var(candidate, candidateFlagName, "absolute path to the exact replacement registry")
	flags.Var(expectedCurrent, "expected-current-sha256", "exact SHA-256 of the active registry bytes")
	flags.Var(expectedCandidate, candidateDigestFlagName, "exact SHA-256 of the replacement registry bytes")
	flags.Var(rollbackOutput, "rollback-out", "same-parent path retaining the exact registry bytes being replaced")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	for _, required := range []*singleStringFlag{registry, source, candidate, expectedCurrent, expectedCandidate, rollbackOutput} {
		if !required.set || strings.TrimSpace(required.value) == "" {
			return request{}, fmt.Errorf("-%s is required", required.name)
		}
	}
	for _, pathFlag := range []*singleStringFlag{registry, candidate, rollbackOutput} {
		if !cleanAbsolute(pathFlag.value) {
			return request{}, fmt.Errorf("-%s must be a clean absolute path", pathFlag.name)
		}
	}
	if strings.TrimSpace(source.value) != source.value {
		return request{}, errors.New("-source must not have leading or trailing whitespace")
	}
	for _, digest := range []*singleStringFlag{expectedCurrent, expectedCandidate} {
		if !validSHA256(digest.value) {
			return request{}, fmt.Errorf("-%s must be a lowercase SHA-256 digest", digest.name)
		}
	}
	if expectedCurrent.value == expectedCandidate.value {
		return request{}, errors.New("current and replacement registry digests must differ")
	}
	return request{
		command: command, registry: registry.value, sourceID: source.value,
		activationCandidate: candidate.value, output: rollbackOutput.value,
		expectedCurrentSHA256: expectedCurrent.value, expectedCandidateSHA256: expectedCandidate.value,
		transitionMode: mode,
	}, nil
}

func parseChannelFetchRequest(args []string, stderr io.Writer) (request, error) {
	flags := flag.NewFlagSet("fetchmark-pack channel-fetch", flag.ContinueOnError)
	flags.SetOutput(stderr)
	allowNetwork := &singleBoolFlag{name: "allow-network"}
	trustedRoot := &singleStringFlag{name: "trusted-root"}
	metadataDir := &singleStringFlag{name: "metadata-dir"}
	stateDir := &singleStringFlag{name: "state-dir"}
	target := &singleStringFlag{name: "target"}
	targetBaseURL := &singleStringFlag{name: "target-base-url"}
	output := &singleStringFlag{name: "output"}
	maxDownloadBytes := &singleUint64Flag{
		name: "max-download-bytes", value: tufchannel.DefaultMaxDownloadBytes,
		maximum: tufchannel.MaximumDownloadBytes,
	}
	maxShards := &singleUint64Flag{
		name: "max-shards", value: tufchannel.DefaultMaxShards,
		maximum: tufchannel.MaximumDownloadShards,
	}
	flags.Var(allowNetwork, "allow-network", "explicitly permit bounded HTTPS artifact retrieval")
	flags.Var(trustedRoot, "trusted-root", "absolute path to the out-of-band pinned TUF root")
	flags.Var(metadataDir, "metadata-dir", "absolute path to the operator-supplied TUF metadata directory")
	flags.Var(stateDir, "state-dir", "absolute path to the durable trusted TUF state directory")
	flags.Var(target, "target", "top-level TUF target path packs/.../manifest.json")
	flags.Var(targetBaseURL, "target-base-url", "canonical HTTPS base URL for repository target artifacts")
	flags.Var(output, "output", "absolute path for a new atomically activated local bundle directory")
	flags.Var(maxDownloadBytes, "max-download-bytes", "maximum aggregate bytes permitted for manifest, signature, and shards")
	flags.Var(maxShards, "max-shards", "maximum manifest shard count permitted for retrieval")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	if !allowNetwork.set || !allowNetwork.value {
		return request{}, errors.New("-allow-network is required and must be true")
	}
	for _, required := range []*singleStringFlag{trustedRoot, metadataDir, stateDir, target, targetBaseURL, output} {
		if !required.set || strings.TrimSpace(required.value) == "" {
			return request{}, fmt.Errorf("-%s is required", required.name)
		}
	}
	for _, pathFlag := range []*singleStringFlag{trustedRoot, metadataDir, stateDir, output} {
		if !cleanAbsolute(pathFlag.value) {
			return request{}, fmt.Errorf("-%s must be a clean absolute path", pathFlag.name)
		}
	}
	for _, value := range []*singleStringFlag{target, targetBaseURL} {
		if strings.TrimSpace(value.value) != value.value {
			return request{}, fmt.Errorf("-%s must not have leading or trailing whitespace", value.name)
		}
	}
	return request{
		command: commandChannelFetch, trustedRoot: trustedRoot.value, metadataDir: metadataDir.value,
		stateDir: stateDir.value, targetPath: target.value, targetBaseURL: targetBaseURL.value,
		output: output.value, maxDownloadBytes: maxDownloadBytes.value, maxShards: int(maxShards.value),
	}, nil
}

func parseRegistryCandidateRequest(args []string, stderr io.Writer) (request, error) {
	flags := flag.NewFlagSet("fetchmark-pack registry-candidate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	registry := &singleStringFlag{name: "registry"}
	source := &singleStringFlag{name: "source"}
	trustedRoot := &singleStringFlag{name: "trusted-root"}
	metadataDir := &singleStringFlag{name: "metadata-dir"}
	stateDir := &singleStringFlag{name: "state-dir"}
	target := &singleStringFlag{name: "target"}
	bundle := &singleStringFlag{name: "bundle"}
	parentBundle := &singleStringFlag{name: "parent-bundle"}
	output := &singleStringFlag{name: "out"}
	projectionRecordCount := &singleUint64Flag{
		name: "projection-record-count", maximum: indexpack.RecommendedInstallRecordLimit,
	}
	maxProjectionBytes := &singleUint64Flag{
		name: "max-projection-bytes", value: openpackindex.MaxProjectionBytes,
		maximum: openpackindex.MaxProjectionBytes,
	}
	flags.Var(registry, "registry", "absolute path to the current operator trust registry")
	flags.Var(source, "source", "existing source binding ID to advance")
	flags.Var(trustedRoot, "trusted-root", "absolute path to the out-of-band pinned TUF root")
	flags.Var(metadataDir, "metadata-dir", "absolute path to the operator-supplied TUF metadata directory")
	flags.Var(stateDir, "state-dir", "absolute path to the durable trusted TUF state directory")
	flags.Var(target, "target", "top-level TUF target path packs/.../manifest.json")
	flags.Var(bundle, "bundle", "absolute path to the exact local bundle; optional only for signed absence")
	flags.Var(parentBundle, "parent-bundle", "absolute path to the exact parent snapshot bundle for a delta")
	flags.Var(output, "out", "absolute new path for the candidate registry")
	flags.Var(projectionRecordCount, "projection-record-count", "expected final projection count for a delta")
	flags.Var(maxProjectionBytes, "max-projection-bytes", "maximum logical projection bytes allowed during candidate verification")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	for _, required := range []*singleStringFlag{registry, source, trustedRoot, metadataDir, stateDir, target, output} {
		if !required.set || strings.TrimSpace(required.value) == "" {
			return request{}, fmt.Errorf("-%s is required", required.name)
		}
	}
	for _, pathFlag := range []*singleStringFlag{registry, trustedRoot, metadataDir, stateDir, output} {
		if !cleanAbsolute(pathFlag.value) {
			return request{}, fmt.Errorf("-%s must be a clean absolute path", pathFlag.name)
		}
	}
	for _, optionalPath := range []*singleStringFlag{bundle, parentBundle} {
		if optionalPath.set && !cleanAbsolute(optionalPath.value) {
			return request{}, fmt.Errorf("-%s must be a clean absolute path", optionalPath.name)
		}
	}
	for _, value := range []*singleStringFlag{source, target} {
		if strings.TrimSpace(value.value) != value.value {
			return request{}, fmt.Errorf("-%s must not have leading or trailing whitespace", value.name)
		}
	}
	return request{
		command: commandRegistryCandidate, registry: registry.value, sourceID: source.value,
		trustedRoot: trustedRoot.value, metadataDir: metadataDir.value, stateDir: stateDir.value,
		targetPath: target.value, bundle: bundle.value, parentBundle: parentBundle.value,
		output: output.value, projectionRecordCount: projectionRecordCount.value,
		projectionRecordCountSet: projectionRecordCount.set, maxProjectionBytes: maxProjectionBytes.value,
	}, nil
}

func cleanAbsolute(path string) bool {
	return path != "" && strings.TrimSpace(path) == path && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func validSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

type result struct {
	Status                  string                          `json:"status"`
	SourceID                string                          `json:"source_id,omitempty"`
	PackID                  string                          `json:"pack_id,omitempty"`
	Revision                uint64                          `json:"revision,omitempty"`
	ManifestSHA256          string                          `json:"manifest_sha256,omitempty"`
	RecordCount             uint64                          `json:"record_count,omitempty"`
	OperationCount          uint64                          `json:"operation_count,omitempty"`
	ProjectionBytes         uint64                          `json:"projection_bytes,omitempty"`
	Path                    string                          `json:"path,omitempty"`
	Kind                    indexpack.Kind                  `json:"kind,omitempty"`
	TargetPath              string                          `json:"target_path,omitempty"`
	TargetLength            int64                           `json:"target_length,omitempty"`
	RootVersion             int64                           `json:"root_version,omitempty"`
	TimestampVersion        int64                           `json:"timestamp_version,omitempty"`
	SnapshotVersion         int64                           `json:"snapshot_version,omitempty"`
	TargetsVersion          int64                           `json:"targets_version,omitempty"`
	TimestampExpiresAt      string                          `json:"timestamp_expires_at,omitempty"`
	SnapshotExpiresAt       string                          `json:"snapshot_expires_at,omitempty"`
	TargetsExpiresAt        string                          `json:"targets_expires_at,omitempty"`
	ParentManifestSHA256    string                          `json:"parent_manifest_sha256,omitempty"`
	ShardCount              int                             `json:"shard_count,omitempty"`
	FileCount               int                             `json:"file_count,omitempty"`
	DownloadBytes           uint64                          `json:"download_bytes,omitempty"`
	BaseRegistrySHA256      string                          `json:"base_registry_sha256,omitempty"`
	CandidateRegistrySHA256 string                          `json:"candidate_registry_sha256,omitempty"`
	Mode                    openpackregistry.TransitionMode `json:"mode,omitempty"`
	RollbackPath            string                          `json:"rollback_path,omitempty"`
	FromRegistrySHA256      string                          `json:"from_registry_sha256,omitempty"`
	ToRegistrySHA256        string                          `json:"to_registry_sha256,omitempty"`
	RollbackRegistrySHA256  string                          `json:"rollback_registry_sha256,omitempty"`
	FromRevision            uint64                          `json:"from_revision,omitempty"`
	ToRevision              uint64                          `json:"to_revision,omitempty"`
	RestartRequired         bool                            `json:"restart_required,omitempty"`
}

func execute(ctx context.Context, request request, deps dependencies) (result, error) {
	if request.command == commandRegistryActivate || request.command == commandRegistryRollback {
		if deps.activateRegistry == nil {
			return result{}, errors.New("registry activation dependencies are unavailable")
		}
		activated, err := deps.activateRegistry(ctx, openpackregistryfile.ActivationOptions{
			Mode: request.transitionMode, SourceID: request.sourceID, RegistryPath: request.registry,
			CandidatePath: request.activationCandidate, RollbackPath: request.output,
			ExpectedCurrentSHA256:   request.expectedCurrentSHA256,
			ExpectedCandidateSHA256: request.expectedCandidateSHA256,
		})
		if err != nil {
			return result{}, err
		}
		return result{
			Status: string(activated.Status), Mode: activated.Mode, SourceID: activated.SourceID,
			Path: activated.Path, RollbackPath: activated.RollbackPath,
			FromRegistrySHA256: activated.FromRegistrySHA256, ToRegistrySHA256: activated.ToRegistrySHA256,
			RollbackRegistrySHA256: activated.RollbackRegistrySHA256,
			FromRevision:           activated.FromRevision, ToRevision: activated.ToRevision,
			RestartRequired: activated.RestartRequired,
		}, nil
	}
	if request.command == commandRegistryCandidate {
		return executeRegistryCandidate(ctx, request, deps)
	}
	if request.command == commandChannelFetch {
		retrieved, err := deps.retrieveChannel(ctx, tufchannel.RetrieveOptions{
			TrustedRootPath: request.trustedRoot, MetadataDir: request.metadataDir, StateDir: request.stateDir,
			TargetPath: request.targetPath, TargetBaseURL: request.targetBaseURL, OutputDir: request.output,
			MaxDownloadBytes: request.maxDownloadBytes, MaxShards: request.maxShards,
		})
		if err != nil {
			return result{}, err
		}
		return result{
			Status: string(retrieved.Status), Path: retrieved.Path, TargetPath: retrieved.TargetPath,
			PackID: retrieved.PackID, Kind: retrieved.Kind, Revision: retrieved.Revision,
			ManifestSHA256: retrieved.ManifestSHA256, ParentManifestSHA256: retrieved.ParentManifestSHA256,
			ShardCount: retrieved.ShardCount, FileCount: retrieved.FileCount, DownloadBytes: retrieved.DownloadBytes,
			RootVersion: retrieved.RootVersion, TimestampVersion: retrieved.TimestampVersion,
			SnapshotVersion: retrieved.SnapshotVersion, TargetsVersion: retrieved.TargetsVersion,
		}, nil
	}
	if request.command == commandChannelSelect {
		selected, err := deps.selectChannel(ctx, tufchannel.Options{
			TrustedRootPath: request.trustedRoot, MetadataDir: request.metadataDir, StateDir: request.stateDir,
			BundleDir: request.bundle, TargetPath: request.targetPath,
		})
		if err != nil {
			return result{}, err
		}
		return result{
			Status: string(selected.Status), TargetPath: selected.TargetPath, PackID: selected.PackID,
			Kind: selected.Kind, Revision: selected.Revision, ManifestSHA256: selected.ManifestSHA256,
			ParentManifestSHA256: selected.ParentManifestSHA256, TargetLength: selected.TargetLength,
			RootVersion: selected.RootVersion, TimestampVersion: selected.TimestampVersion,
			SnapshotVersion: selected.SnapshotVersion, TargetsVersion: selected.TargetsVersion,
			TimestampExpiresAt: selected.TimestampExpiresAt, SnapshotExpiresAt: selected.SnapshotExpiresAt,
			TargetsExpiresAt: selected.TargetsExpiresAt,
		}, nil
	}
	registry, err := openpackregistryfile.Load(request.registry)
	if err != nil {
		return result{}, fmt.Errorf("open registry: %w", err)
	}
	binding, found := registry.Lookup(request.sourceID)
	if !found {
		return result{}, fmt.Errorf("source %q is not defined in registry", request.sourceID)
	}
	switch binding.Kind {
	case indexpack.KindSnapshot:
		if request.parentBundle != "" {
			return result{}, errors.New("snapshot binding forbids -parent-bundle")
		}
	case indexpack.KindDelta:
		if request.parentBundle == "" {
			return result{}, errors.New("delta binding requires -parent-bundle")
		}
	default:
		return result{}, fmt.Errorf("unsupported registry binding kind %q", binding.Kind)
	}
	trustedKeys := registry.TrustedKeys()
	acceptance := binding.Acceptance(deps.now().UTC())
	materialize := func(root string) (openpackindex.Installed, error) {
		if binding.Kind == indexpack.KindDelta {
			return deps.applyDelta(ctx, openpackindex.DeltaInstallOptions{
				ParentBundleDir: request.parentBundle, BundleDir: request.bundle, Root: root,
				TrustedKeys: trustedKeys, DeltaAcceptance: acceptance,
				ExpectedParentManifestSHA256:  binding.ParentManifestSHA256,
				ExpectedProjectionRecordCount: binding.RecordCount,
				MaxProjectionBytes:            request.maxProjectionBytes,
			})
		}
		return deps.install(ctx, openpackindex.InstallOptions{
			BundleDir: request.bundle, Root: root, TrustedKeys: trustedKeys,
			Acceptance: acceptance, MaxProjectionBytes: request.maxProjectionBytes,
		})
	}

	var installed openpackindex.Installed
	if request.command == "verify" {
		verifyRoot, err := deps.makeVerifyRoot()
		if err != nil {
			return result{}, fmt.Errorf("create private verification root: %w", err)
		}
		installed, err = materialize(verifyRoot)
		cleanupErr := deps.removeVerifyRoot(verifyRoot)
		if cleanupErr != nil {
			cleanupErr = fmt.Errorf("remove private verification root: %w", cleanupErr)
		}
		if err != nil || cleanupErr != nil {
			return result{}, errors.Join(err, cleanupErr)
		}
	} else {
		installed, err = materialize(filepath.Dir(filepath.Dir(binding.InstalledPath)))
		if err != nil {
			return result{}, err
		}
		if err := confirmInstalledPath(installed.Path, binding.InstalledPath); err != nil {
			return result{}, err
		}
	}
	if installed.RecordCount != binding.RecordCount || installed.OperationCount != binding.ManifestRecordCount {
		return result{}, fmt.Errorf(
			"materialized counts do not match registry: records=%d/%d operations=%d/%d",
			installed.RecordCount, binding.RecordCount, installed.OperationCount, binding.ManifestRecordCount,
		)
	}

	output := result{
		Status: "verified", SourceID: binding.SourceID, PackID: installed.PackID,
		Revision: installed.Revision, ManifestSHA256: installed.ManifestSHA256, RecordCount: installed.RecordCount,
		OperationCount: installed.OperationCount, ProjectionBytes: installed.ProjectionBytes,
	}
	if request.command == "install" {
		output.Status = "installed"
		output.Path = binding.InstalledPath
	}
	return output, nil
}

func executeRegistryCandidate(ctx context.Context, request request, deps dependencies) (result, error) {
	if err := validateRegistryCandidateOutput(request); err != nil {
		return result{}, err
	}
	registry, baseRegistryDigest, err := openpackregistryfile.LoadWithDigest(request.registry)
	if err != nil {
		return result{}, fmt.Errorf("open current registry: %w", err)
	}
	current, found := registry.Lookup(request.sourceID)
	if !found {
		return result{}, fmt.Errorf("source %q is not defined in current registry", request.sourceID)
	}
	if inside, err := pathInsideExistingDirectory(request.output, filepath.Dir(current.InstalledPath)); err != nil {
		return result{}, fmt.Errorf("inspect candidate/object isolation: %w", err)
	} else if inside {
		return result{}, errors.New("candidate output must not be inside the installed object directory")
	}
	selected, err := deps.selectChannel(ctx, tufchannel.Options{
		TrustedRootPath: request.trustedRoot, MetadataDir: request.metadataDir, StateDir: request.stateDir,
		BundleDir: request.bundle, TargetPath: request.targetPath,
	})
	if err != nil {
		return result{}, err
	}
	if selected.Status == tufchannel.StatusAbsent {
		return result{Status: string(selected.Status), TargetPath: selected.TargetPath}, nil
	}
	if selected.Status != tufchannel.StatusSelected {
		return result{}, fmt.Errorf("unsupported channel selection status %q", selected.Status)
	}
	projectionRecordCount := selected.ManifestRecordCount
	switch selected.Kind {
	case indexpack.KindSnapshot:
		if request.parentBundle != "" {
			return result{}, errors.New("snapshot forbids -parent-bundle")
		}
		if request.projectionRecordCountSet {
			return result{}, errors.New("snapshot forbids -projection-record-count")
		}
	case indexpack.KindDelta:
		if request.parentBundle == "" {
			return result{}, errors.New("delta requires -parent-bundle")
		}
		if !request.projectionRecordCountSet {
			return result{}, errors.New("delta requires -projection-record-count")
		}
		projectionRecordCount = request.projectionRecordCount
	default:
		return result{}, fmt.Errorf("unsupported selected manifest kind %q", selected.Kind)
	}

	installedPath := filepath.Join(filepath.Dir(current.InstalledPath), selected.ManifestSHA256)
	candidate, err := registry.Promote(request.sourceID, openpackregistry.Promotion{
		PackID: selected.PackID, SigningKeyID: selected.SigningKeyID, Kind: selected.Kind,
		ManifestSHA256: selected.ManifestSHA256, ParentManifestSHA256: selected.ParentManifestSHA256,
		Revision: selected.Revision, ManifestRecordCount: selected.ManifestRecordCount,
		ProjectionRecordCount: projectionRecordCount, CreatedAt: selected.CreatedAt,
		ExpiresAt: selected.ExpiresAt, InstalledPath: installedPath,
	})
	if err != nil {
		return result{}, err
	}
	candidateRaw, err := candidate.Encode()
	if err != nil {
		return result{}, err
	}
	candidateRegistryDigest := indexpack.Digest(candidateRaw)
	candidateBinding, _ := candidate.Lookup(request.sourceID)
	verifyRoot, err := deps.makeVerifyRoot()
	if err != nil {
		return result{}, fmt.Errorf("create private candidate verification root: %w", err)
	}
	var installed openpackindex.Installed
	if selected.Kind == indexpack.KindDelta {
		installed, err = deps.applyDelta(ctx, openpackindex.DeltaInstallOptions{
			ParentBundleDir: request.parentBundle, BundleDir: request.bundle, Root: verifyRoot,
			TrustedKeys: candidate.TrustedKeys(), DeltaAcceptance: candidateBinding.Acceptance(deps.now().UTC()),
			ExpectedParentManifestSHA256:  candidateBinding.ParentManifestSHA256,
			ExpectedProjectionRecordCount: candidateBinding.RecordCount,
			MaxProjectionBytes:            request.maxProjectionBytes,
		})
	} else {
		installed, err = deps.install(ctx, openpackindex.InstallOptions{
			BundleDir: request.bundle, Root: verifyRoot, TrustedKeys: candidate.TrustedKeys(),
			Acceptance: candidateBinding.Acceptance(deps.now().UTC()), MaxProjectionBytes: request.maxProjectionBytes,
		})
	}
	cleanupErr := deps.removeVerifyRoot(verifyRoot)
	if cleanupErr != nil {
		cleanupErr = fmt.Errorf("remove private candidate verification root: %w", cleanupErr)
	}
	if err != nil || cleanupErr != nil {
		return result{}, errors.Join(err, cleanupErr)
	}
	if installed.ManifestSHA256 != candidateBinding.ManifestSHA256 || installed.PackID != candidateBinding.PackID ||
		installed.Revision != candidateBinding.Revision || installed.RecordCount != candidateBinding.RecordCount ||
		installed.OperationCount != candidateBinding.ManifestRecordCount {
		return result{}, errors.New("candidate verification result does not match promoted registry binding")
	}
	writtenPath, err := deps.writeRegistryCandidate(ctx, request.output, candidate)
	if err != nil {
		var committed *openpackregistryfile.CommittedCandidateError
		if errors.As(err, &committed) {
			return result{}, fmt.Errorf(
				"registry candidate committed at %s; base_registry_sha256=%s candidate_registry_sha256=%s; verify the candidate before retrying: %w",
				committed.Path, baseRegistryDigest, candidateRegistryDigest, err,
			)
		}
		return result{}, err
	}
	return result{
		Status: "candidate", SourceID: request.sourceID, PackID: installed.PackID,
		Revision: installed.Revision, ManifestSHA256: installed.ManifestSHA256,
		ParentManifestSHA256: selected.ParentManifestSHA256, Kind: selected.Kind,
		RecordCount: installed.RecordCount, OperationCount: installed.OperationCount,
		ProjectionBytes: installed.ProjectionBytes, Path: writtenPath, TargetPath: selected.TargetPath,
		BaseRegistrySHA256: baseRegistryDigest, CandidateRegistrySHA256: candidateRegistryDigest,
	}, nil
}

func validateRegistryCandidateOutput(request request) error {
	output, err := canonicalNewPath(request.output)
	if err != nil {
		return fmt.Errorf("inspect candidate output: %w", err)
	}
	for label, file := range map[string]string{"current registry": request.registry, "trusted root": request.trustedRoot} {
		canonical, err := canonicalPathAllowMissing(file)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", label, err)
		}
		if output == canonical {
			return fmt.Errorf("candidate output must not replace the %s", label)
		}
	}
	for _, protected := range []struct {
		label string
		path  string
	}{
		{label: "metadata", path: request.metadataDir},
		{label: "state", path: request.stateDir},
		{label: "bundle", path: request.bundle},
		{label: "parent bundle", path: request.parentBundle},
	} {
		if protected.path == "" {
			continue
		}
		inside, err := pathInsideExistingDirectory(output, protected.path)
		if err != nil {
			return fmt.Errorf("inspect candidate/%s isolation: %w", protected.label, err)
		}
		if inside {
			return fmt.Errorf("candidate output must not be inside %s", protected.label)
		}
	}
	return nil
}

func canonicalNewPath(path string) (string, error) {
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(path)), nil
}

func pathInsideExistingDirectory(path, directory string) (bool, error) {
	canonicalPath, err := canonicalPathAllowMissing(path)
	if err != nil {
		return false, err
	}
	canonicalDirectory, err := canonicalPathAllowMissing(directory)
	if err != nil {
		return false, err
	}
	relative, err := filepath.Rel(canonicalDirectory, canonicalPath)
	if err != nil {
		return false, err
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))), nil
}

func canonicalPathAllowMissing(path string) (string, error) {
	current := path
	missing := make([]string, 0, 4)
	for {
		if _, err := os.Lstat(current); err == nil {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", err
			}
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return resolved, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", os.ErrNotExist
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func confirmInstalledPath(actual, expected string) error {
	actualInfo, err := os.Stat(actual)
	if err != nil {
		return fmt.Errorf("stat installed projection: %w", err)
	}
	expectedInfo, err := os.Stat(expected)
	if err != nil {
		return fmt.Errorf("stat registry projection path: %w", err)
	}
	if !actualInfo.IsDir() || !expectedInfo.IsDir() || !os.SameFile(actualInfo, expectedInfo) {
		return errors.New("installed projection does not match registry path")
	}
	return nil
}
