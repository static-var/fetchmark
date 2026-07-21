package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/adapters/tufrepository"
)

type repeatedStringFlag struct {
	name   string
	values []string
}

func (value *repeatedStringFlag) String() string { return strings.Join(value.values, ",") }
func (value *repeatedStringFlag) Set(raw string) error {
	value.values = append(value.values, raw)
	return nil
}

type rootKeygenResult struct {
	Path          string `json:"path"`
	PrivateSHA256 string `json:"private_sha256"`
	PublicKeyID   string `json:"public_key_id"`
}

type rootArtifactResult struct {
	Path             string                              `json:"path"`
	SHA256           string                              `json:"sha256"`
	Kind             string                              `json:"kind"`
	RotationProposal *tufrepository.RootRotationProposal `json:"rotation_proposal,omitempty"`
	RotationResult   *tufrepository.RootRotationResult   `json:"rotation_result,omitempty"`
}

var ErrRootArtifactCommitted = errors.New("TUF root ceremony artifact committed but command completion failed")

func committedRootArtifactError(path, digest string, cause error) error {
	return fmt.Errorf(
		"%w: path=%q sha256=%s: %w; inspect and hash the no-overwrite artifact before retrying or removing it",
		ErrRootArtifactCommitted, path, digest, cause,
	)
}

func parseChannelRootRequest(args []string, stderr io.Writer) (request, error) {
	flags := flag.NewFlagSet("fetchmark-pack-build "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	repositoryID := &singleStringFlag{name: "repository-id"}
	signerID := &singleStringFlag{name: "signer-id"}
	identity := &singleStringFlag{name: "identity"}
	currentRoot := &singleStringFlag{name: "current-root"}
	candidate := &singleStringFlag{name: "candidate"}
	signedRoot := &singleStringFlag{name: "signed-root"}
	signer := &singleStringFlag{name: "signer"}
	expires := &singleStringFlag{name: "root-expires"}
	expectedCurrent := &singleStringFlag{name: "expected-current-root-sha256"}
	expectedCandidate := &singleStringFlag{name: "expected-candidate-root-sha256"}
	expectedSigned := &singleStringFlag{name: "expected-signed-root-sha256"}
	legacyContributions := &singleStringFlag{name: "legacy-contributions"}
	output := &singleStringFlag{name: "out"}
	newSigners := &repeatedStringFlag{name: "new-signer"}
	contributions := &repeatedStringFlag{name: "contribution"}

	flags.Var(repositoryID, "repository-id", "stable TUF repository identity ID")
	flags.Var(signerID, "signer-id", "stable offline root custodian ID")
	flags.Var(identity, "identity", "private TUF repository identity JSON")
	flags.Var(currentRoot, "current-root", "exact current signed TUF root JSON")
	flags.Var(candidate, "candidate", "exact unsigned successor TUF root JSON")
	flags.Var(signedRoot, "signed-root", "exact assembled successor TUF root JSON")
	flags.Var(signer, "signer", "private one-key root custodian JSON")
	flags.Var(expires, "root-expires", "successor root expiry as an exact UTC RFC 3339 second")
	flags.Var(expectedCurrent, "expected-current-root-sha256", "exact current root digest")
	flags.Var(expectedCandidate, "expected-candidate-root-sha256", "exact candidate or signed successor digest")
	flags.Var(expectedSigned, "expected-signed-root-sha256", "exact assembled signed successor digest")
	flags.Var(legacyContributions, "legacy-contributions", "two-contribution bundle from the legacy identity")
	flags.Var(newSigners, "new-signer", "public successor root custodian JSON; specify exactly twice")
	flags.Var(contributions, "contribution", "custodian contribution JSON; specify twice with -legacy-contributions or four times without it")
	flags.Var(output, "out", "new no-overwrite artifact path")
	if err := flags.Parse(args[1:]); err != nil {
		return request{}, err
	}
	if flags.NArg() != 0 {
		return request{}, errors.New("unexpected positional arguments")
	}
	requireID := func(value *singleStringFlag, name string) error {
		if !value.set || value.value == "" || strings.TrimSpace(value.value) != value.value {
			return fmt.Errorf("-%s is required without surrounding whitespace", name)
		}
		return nil
	}
	requirePath := func(value *singleStringFlag, name string) error {
		if !value.set || !cleanAbsolute(value.value) {
			return fmt.Errorf("-%s must be a clean absolute path", name)
		}
		return nil
	}
	requireDigest := func(value *singleStringFlag, name string) error {
		if !value.set || !canonicalSHA256(value.value) {
			return fmt.Errorf("-%s must be 64 lowercase hexadecimal characters", name)
		}
		return nil
	}
	specified := map[string]bool{
		"repository-id": repositoryID.set, "signer-id": signerID.set, "identity": identity.set,
		"current-root": currentRoot.set, "candidate": candidate.set, "signed-root": signedRoot.set,
		"signer": signer.set, "root-expires": expires.set, "expected-current-root-sha256": expectedCurrent.set,
		"expected-candidate-root-sha256": expectedCandidate.set, "expected-signed-root-sha256": expectedSigned.set,
		"legacy-contributions": legacyContributions.set, "out": output.set,
		"new-signer": len(newSigners.values) > 0, "contribution": len(contributions.values) > 0,
	}
	allowedByCommand := map[string]map[string]struct{}{
		"channel-root-keygen":      setOfFlags("repository-id", "signer-id", "out"),
		"channel-root-public":      setOfFlags("signer", "out"),
		"channel-root-prepare":     setOfFlags("repository-id", "current-root", "new-signer", "root-expires", "out"),
		"channel-root-sign":        setOfFlags("repository-id", "current-root", "candidate", "signer", "expected-current-root-sha256", "expected-candidate-root-sha256", "out"),
		"channel-root-sign-legacy": setOfFlags("identity", "current-root", "candidate", "expected-current-root-sha256", "expected-candidate-root-sha256", "out"),
		"channel-root-assemble":    setOfFlags("repository-id", "current-root", "candidate", "legacy-contributions", "contribution", "expected-current-root-sha256", "expected-candidate-root-sha256", "out"),
		"channel-root-apply":       setOfFlags("identity", "signed-root", "expected-current-root-sha256", "expected-signed-root-sha256", "out"),
	}
	allowed, found := allowedByCommand[args[0]]
	if !found {
		return request{}, fmt.Errorf("unknown root subcommand %q", args[0])
	}
	for name, set := range specified {
		if set {
			if _, ok := allowed[name]; !ok {
				return request{}, fmt.Errorf("-%s is not valid for %s", name, args[0])
			}
		}
	}

	parsed := request{command: args[0]}
	switch args[0] {
	case "channel-root-keygen":
		if err := requireID(repositoryID, "repository-id"); err != nil {
			return request{}, err
		}
		if err := requireID(signerID, "signer-id"); err != nil {
			return request{}, err
		}
		if err := requirePath(output, "out"); err != nil {
			return request{}, err
		}
		parsed.repositoryID, parsed.signerID, parsed.output = repositoryID.value, signerID.value, output.value
	case "channel-root-public":
		if err := requirePath(signer, "signer"); err != nil {
			return request{}, err
		}
		if err := requirePath(output, "out"); err != nil {
			return request{}, err
		}
		parsed.signer, parsed.output = signer.value, output.value
	case "channel-root-prepare":
		if err := requireID(repositoryID, "repository-id"); err != nil {
			return request{}, err
		}
		if err := requirePath(currentRoot, "current-root"); err != nil {
			return request{}, err
		}
		if len(newSigners.values) != 2 {
			return request{}, errors.New("-new-signer must be specified exactly twice")
		}
		for _, path := range newSigners.values {
			if !cleanAbsolute(path) {
				return request{}, errors.New("every -new-signer must be a clean absolute path")
			}
		}
		rootExpires, err := parseExactUTCTimeFlag(expires, "root-expires")
		if err != nil {
			return request{}, err
		}
		if err := requirePath(output, "out"); err != nil {
			return request{}, err
		}
		parsed.repositoryID, parsed.currentRoot, parsed.newSigners = repositoryID.value, currentRoot.value, append([]string(nil), newSigners.values...)
		parsed.rootExpires, parsed.output = rootExpires, output.value
	case "channel-root-sign":
		if err := requireID(repositoryID, "repository-id"); err != nil {
			return request{}, err
		}
		for value, name := range map[*singleStringFlag]string{currentRoot: "current-root", candidate: "candidate", signer: "signer", output: "out"} {
			if err := requirePath(value, name); err != nil {
				return request{}, err
			}
		}
		if err := requireDigest(expectedCurrent, "expected-current-root-sha256"); err != nil {
			return request{}, err
		}
		if err := requireDigest(expectedCandidate, "expected-candidate-root-sha256"); err != nil {
			return request{}, err
		}
		parsed.repositoryID, parsed.currentRoot, parsed.candidate, parsed.signer, parsed.output = repositoryID.value, currentRoot.value, candidate.value, signer.value, output.value
		parsed.expectedCurrent, parsed.expectedCandidate = expectedCurrent.value, expectedCandidate.value
	case "channel-root-sign-legacy":
		for value, name := range map[*singleStringFlag]string{identity: "identity", currentRoot: "current-root", candidate: "candidate", output: "out"} {
			if err := requirePath(value, name); err != nil {
				return request{}, err
			}
		}
		if err := requireDigest(expectedCurrent, "expected-current-root-sha256"); err != nil {
			return request{}, err
		}
		if err := requireDigest(expectedCandidate, "expected-candidate-root-sha256"); err != nil {
			return request{}, err
		}
		parsed.identity, parsed.currentRoot, parsed.candidate, parsed.output = identity.value, currentRoot.value, candidate.value, output.value
		parsed.expectedCurrent, parsed.expectedCandidate = expectedCurrent.value, expectedCandidate.value
	case "channel-root-assemble":
		if err := requireID(repositoryID, "repository-id"); err != nil {
			return request{}, err
		}
		for value, name := range map[*singleStringFlag]string{currentRoot: "current-root", candidate: "candidate", output: "out"} {
			if err := requirePath(value, name); err != nil {
				return request{}, err
			}
		}
		if legacyContributions.set {
			if !cleanAbsolute(legacyContributions.value) {
				return request{}, errors.New("-legacy-contributions must be a clean absolute path")
			}
			if len(contributions.values) != 2 {
				return request{}, errors.New("legacy assembly requires exactly two -contribution flags")
			}
		} else if len(contributions.values) != 4 {
			return request{}, errors.New("split-custody assembly requires exactly four -contribution flags")
		}
		for _, path := range contributions.values {
			if !cleanAbsolute(path) {
				return request{}, errors.New("every -contribution must be a clean absolute path")
			}
		}
		if err := requireDigest(expectedCurrent, "expected-current-root-sha256"); err != nil {
			return request{}, err
		}
		if err := requireDigest(expectedCandidate, "expected-candidate-root-sha256"); err != nil {
			return request{}, err
		}
		parsed.repositoryID, parsed.currentRoot, parsed.candidate = repositoryID.value, currentRoot.value, candidate.value
		parsed.legacyContributions, parsed.contributions, parsed.output = legacyContributions.value, append([]string(nil), contributions.values...), output.value
		parsed.expectedCurrent, parsed.expectedCandidate = expectedCurrent.value, expectedCandidate.value
	case "channel-root-apply":
		for value, name := range map[*singleStringFlag]string{identity: "identity", signedRoot: "signed-root", output: "out"} {
			if err := requirePath(value, name); err != nil {
				return request{}, err
			}
		}
		if err := requireDigest(expectedCurrent, "expected-current-root-sha256"); err != nil {
			return request{}, err
		}
		if err := requireDigest(expectedSigned, "expected-signed-root-sha256"); err != nil {
			return request{}, err
		}
		parsed.identity, parsed.signedRoot, parsed.output = identity.value, signedRoot.value, output.value
		parsed.expectedCurrent, parsed.expectedCandidate = expectedCurrent.value, expectedSigned.value
	default:
		return request{}, fmt.Errorf("unknown root subcommand %q", args[0])
	}
	return parsed, nil
}

func setOfFlags(names ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(names))
	for _, name := range names {
		result[name] = struct{}{}
	}
	return result
}

func executeChannelRootKeygen(ctx context.Context, request request, deps dependencies) (rootKeygenResult, error) {
	if deps.random == nil || deps.writeRootArtifact == nil || deps.generateRootSigner == nil {
		return rootKeygenResult{}, errors.New("root custodian key generation dependencies are unavailable")
	}
	privateRaw, publicRaw, err := deps.generateRootSigner(request.repositoryID, request.signerID, deps.random)
	if err != nil {
		return rootKeygenResult{}, err
	}
	public, err := tufrepository.DecodeRootSignerPublic(publicRaw)
	if err != nil {
		return rootKeygenResult{}, err
	}
	written, err := deps.writeRootArtifact(ctx, request.output, privateRaw, tufrepository.MaxRootSignerBytes)
	if err != nil {
		return rootKeygenResult{}, err
	}
	return rootKeygenResult{Path: written.Path, PrivateSHA256: written.SHA256, PublicKeyID: public.KeyID}, nil
}

func executeChannelRootPublic(ctx context.Context, request request, deps dependencies) (rootArtifactResult, error) {
	if deps.readSecure == nil || deps.writeRootArtifact == nil {
		return rootArtifactResult{}, errors.New("root public export dependencies are unavailable")
	}
	privateRaw, err := readRootArtifact(deps, request.signer, tufrepository.MaxRootSignerBytes, secureconfigfile.PrivateIdentity)
	if err != nil {
		return rootArtifactResult{}, err
	}
	signer, err := tufrepository.DecodeRootSigner(privateRaw)
	if err != nil {
		return rootArtifactResult{}, err
	}
	publicRaw, err := json.Marshal(signer.Public())
	if err != nil {
		return rootArtifactResult{}, err
	}
	written, err := deps.writeRootArtifact(ctx, request.output, publicRaw, tufrepository.MaxRootSignerBytes)
	if err != nil {
		return rootArtifactResult{}, err
	}
	return rootArtifactResult{Path: written.Path, SHA256: written.SHA256, Kind: "root_signer_public"}, nil
}

func executeChannelRootPrepare(ctx context.Context, request request, deps dependencies) (rootArtifactResult, error) {
	if deps.now == nil || deps.readSecure == nil || deps.writeRootArtifact == nil || deps.prepareRootRotation == nil {
		return rootArtifactResult{}, errors.New("root rotation preparation dependencies are unavailable")
	}
	currentRaw, err := readRootArtifact(deps, request.currentRoot, 512<<10, secureconfigfile.PublicConfig)
	if err != nil {
		return rootArtifactResult{}, err
	}
	publicSigners := make([]tufrepository.RootSignerPublic, 0, 2)
	for _, signerPath := range request.newSigners {
		raw, err := readRootArtifact(deps, signerPath, tufrepository.MaxRootSignerBytes, secureconfigfile.PublicConfig)
		if err != nil {
			return rootArtifactResult{}, err
		}
		public, err := tufrepository.DecodeRootSignerPublic(raw)
		if err != nil {
			return rootArtifactResult{}, err
		}
		publicSigners = append(publicSigners, public)
	}
	candidateRaw, proposal, err := deps.prepareRootRotation(request.repositoryID, currentRaw, publicSigners, request.rootExpires, rootCommandNow(deps))
	if err != nil {
		return rootArtifactResult{}, err
	}
	written, err := deps.writeRootArtifact(ctx, request.output, candidateRaw, 512<<10)
	if err != nil {
		return rootArtifactResult{}, err
	}
	return rootArtifactResult{Path: written.Path, SHA256: written.SHA256, Kind: "unsigned_root_candidate", RotationProposal: &proposal}, nil
}

func executeChannelRootSign(ctx context.Context, request request, deps dependencies) (rootArtifactResult, error) {
	if deps.now == nil || deps.readSecure == nil || deps.writeRootArtifact == nil || deps.signRootRotation == nil {
		return rootArtifactResult{}, errors.New("root custodian signing dependencies are unavailable")
	}
	currentRaw, err := readRootArtifact(deps, request.currentRoot, 512<<10, secureconfigfile.PublicConfig)
	if err != nil {
		return rootArtifactResult{}, err
	}
	candidateRaw, err := readRootArtifact(deps, request.candidate, 512<<10, secureconfigfile.PublicConfig)
	if err != nil {
		return rootArtifactResult{}, err
	}
	signerRaw, err := readRootArtifact(deps, request.signer, tufrepository.MaxRootSignerBytes, secureconfigfile.PrivateIdentity)
	if err != nil {
		return rootArtifactResult{}, err
	}
	signer, err := tufrepository.DecodeRootSigner(signerRaw)
	if err != nil {
		return rootArtifactResult{}, err
	}
	contributionRaw, err := deps.signRootRotation(
		request.repositoryID, currentRaw, candidateRaw, request.expectedCurrent, request.expectedCandidate, signer, rootCommandNow(deps),
	)
	if err != nil {
		return rootArtifactResult{}, err
	}
	written, err := deps.writeRootArtifact(ctx, request.output, contributionRaw, tufrepository.MaxRootContributionBytes)
	if err != nil {
		return rootArtifactResult{}, err
	}
	return rootArtifactResult{Path: written.Path, SHA256: written.SHA256, Kind: "root_signature_contribution"}, nil
}

func executeChannelRootSignLegacy(ctx context.Context, request request, deps dependencies) (rootArtifactResult, error) {
	if deps.now == nil || deps.readSecure == nil || deps.writeRootArtifact == nil || deps.signLegacyRootRotation == nil {
		return rootArtifactResult{}, errors.New("legacy root signing dependencies are unavailable")
	}
	identityRaw, err := readRootArtifact(deps, request.identity, tufrepository.MaxIdentityBytes, secureconfigfile.PrivateIdentity)
	if err != nil {
		return rootArtifactResult{}, err
	}
	identity, err := tufrepository.DecodeIdentity(identityRaw)
	if err != nil {
		return rootArtifactResult{}, err
	}
	currentRaw, err := readRootArtifact(deps, request.currentRoot, 512<<10, secureconfigfile.PublicConfig)
	if err != nil {
		return rootArtifactResult{}, err
	}
	candidateRaw, err := readRootArtifact(deps, request.candidate, 512<<10, secureconfigfile.PublicConfig)
	if err != nil {
		return rootArtifactResult{}, err
	}
	contributions, err := deps.signLegacyRootRotation(
		currentRaw, candidateRaw, request.expectedCurrent, request.expectedCandidate, identity, rootCommandNow(deps),
	)
	if err != nil {
		return rootArtifactResult{}, err
	}
	bundleRaw, err := tufrepository.EncodeRootContributionBundle(contributions)
	if err != nil {
		return rootArtifactResult{}, err
	}
	written, err := deps.writeRootArtifact(ctx, request.output, bundleRaw, tufrepository.MaxRootContributionBundleBytes)
	if err != nil {
		return rootArtifactResult{}, err
	}
	return rootArtifactResult{Path: written.Path, SHA256: written.SHA256, Kind: "legacy_root_contribution_bundle"}, nil
}

func executeChannelRootAssemble(ctx context.Context, request request, deps dependencies) (rootArtifactResult, error) {
	if deps.now == nil || deps.readSecure == nil || deps.writeRootArtifact == nil || deps.assembleRootRotation == nil {
		return rootArtifactResult{}, errors.New("root rotation assembly dependencies are unavailable")
	}
	currentRaw, err := readRootArtifact(deps, request.currentRoot, 512<<10, secureconfigfile.PublicConfig)
	if err != nil {
		return rootArtifactResult{}, err
	}
	candidateRaw, err := readRootArtifact(deps, request.candidate, 512<<10, secureconfigfile.PublicConfig)
	if err != nil {
		return rootArtifactResult{}, err
	}
	contributions := make([][]byte, 0, 4)
	if request.legacyContributions != "" {
		bundleRaw, err := readRootArtifact(deps, request.legacyContributions, tufrepository.MaxRootContributionBundleBytes, secureconfigfile.PublicConfig)
		if err != nil {
			return rootArtifactResult{}, err
		}
		contributions, err = tufrepository.DecodeRootContributionBundle(bundleRaw)
		if err != nil {
			return rootArtifactResult{}, err
		}
	}
	for _, contributionPath := range request.contributions {
		raw, err := readRootArtifact(deps, contributionPath, tufrepository.MaxRootContributionBytes, secureconfigfile.PublicConfig)
		if err != nil {
			return rootArtifactResult{}, err
		}
		contributions = append(contributions, raw)
	}
	signedRaw, rotation, err := deps.assembleRootRotation(
		request.repositoryID, currentRaw, candidateRaw, request.expectedCurrent, request.expectedCandidate, contributions, rootCommandNow(deps),
	)
	if err != nil {
		return rootArtifactResult{}, err
	}
	written, err := deps.writeRootArtifact(ctx, request.output, signedRaw, 512<<10)
	if err != nil {
		return rootArtifactResult{}, err
	}
	return rootArtifactResult{Path: written.Path, SHA256: written.SHA256, Kind: "signed_root", RotationResult: &rotation}, nil
}

func executeChannelRootApply(ctx context.Context, request request, deps dependencies) (tufrepository.IdentityWriteResult, error) {
	if deps.now == nil || deps.readSecure == nil || deps.applyRootRotation == nil || deps.writeChannelIdentity == nil {
		return tufrepository.IdentityWriteResult{}, errors.New("root identity migration dependencies are unavailable")
	}
	identityRaw, err := readRootArtifact(deps, request.identity, tufrepository.MaxIdentityBytes, secureconfigfile.PrivateIdentity)
	if err != nil {
		return tufrepository.IdentityWriteResult{}, err
	}
	identity, err := tufrepository.DecodeIdentity(identityRaw)
	if err != nil {
		return tufrepository.IdentityWriteResult{}, err
	}
	signedRaw, err := readRootArtifact(deps, request.signedRoot, 512<<10, secureconfigfile.PublicConfig)
	if err != nil {
		return tufrepository.IdentityWriteResult{}, err
	}
	operationalRaw, err := deps.applyRootRotation(identity, signedRaw, request.expectedCurrent, request.expectedCandidate, rootCommandNow(deps))
	if err != nil {
		return tufrepository.IdentityWriteResult{}, err
	}
	return deps.writeChannelIdentity(ctx, request.output, operationalRaw)
}

func readRootArtifact(deps dependencies, path string, maxBytes int64, mode secureconfigfile.ModePolicy) ([]byte, error) {
	raw, err := deps.readSecure(path, secureconfigfile.Options{MaxBytes: maxBytes, Mode: mode})
	if err != nil {
		return nil, fmt.Errorf("read root ceremony artifact: %w", err)
	}
	return raw, nil
}

func rootCommandNow(deps dependencies) time.Time {
	return deps.now().UTC().Truncate(time.Second)
}
