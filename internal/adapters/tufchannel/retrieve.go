package tufchannel

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackchannel"
)

const (
	DefaultMaxDownloadBytes uint64 = 66 << 20
	MaximumDownloadBytes    uint64 = 1 << 30
	DefaultMaxShards               = 1
	MaximumDownloadShards          = 64
	artifactRequestTimeout         = 5 * time.Minute
	retrievalStagingPrefix         = ".fetchmark-pack-fetch-"
)

var (
	ErrRetrievalExists = errors.New("TUF channel retrieval: output already exists")
	ErrRetrievalLimit  = errors.New("TUF channel retrieval: download limit exceeded")
)

type RetrieveOptions struct {
	TrustedRootPath  string
	MetadataDir      string
	StateDir         string
	TargetPath       string
	TargetBaseURL    string
	OutputDir        string
	MaxDownloadBytes uint64
	MaxShards        int
}

type Retrieval struct {
	Status               Status         `json:"status"`
	Path                 string         `json:"path,omitempty"`
	TargetPath           string         `json:"target_path"`
	PackID               string         `json:"pack_id,omitempty"`
	Kind                 indexpack.Kind `json:"kind,omitempty"`
	Revision             uint64         `json:"revision,omitempty"`
	ManifestSHA256       string         `json:"manifest_sha256,omitempty"`
	ParentManifestSHA256 string         `json:"parent_manifest_sha256,omitempty"`
	ShardCount           int            `json:"shard_count,omitempty"`
	FileCount            int            `json:"file_count,omitempty"`
	DownloadBytes        uint64         `json:"download_bytes,omitempty"`
	RootVersion          int64          `json:"root_version"`
	TimestampVersion     int64          `json:"timestamp_version"`
	SnapshotVersion      int64          `json:"snapshot_version"`
	TargetsVersion       int64          `json:"targets_version"`
}

const StatusRetrieved Status = "retrieved"

type artifactFetcher func(context.Context, string, int64, io.Writer) (int64, error)

type retrievalDependencies struct {
	fetch                    artifactFetcher
	now                      func() time.Time
	beforeActivate           func()
	closeActivationDirectory func(*os.File) error
	closeParentRoot          func(*os.Root) error
}

func Retrieve(ctx context.Context, options RetrieveOptions) (Retrieval, error) {
	if ctx == nil {
		return Retrieval{}, invalidOptions(errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return Retrieval{}, err
	}
	baseURL, err := parseTargetBaseURL(options.TargetBaseURL)
	if err != nil {
		return Retrieval{}, err
	}
	fetcher, err := newHTTPArtifactFetcher(ctx, baseURL)
	if err != nil {
		return Retrieval{}, err
	}
	return retrieve(ctx, options, baseURL, defaultRetrievalDependencies(fetcher))
}

func retrieveWithFetcher(ctx context.Context, options RetrieveOptions, fetch artifactFetcher) (Retrieval, error) {
	return retrieveWithDependencies(ctx, options, defaultRetrievalDependencies(fetch))
}

func defaultRetrievalDependencies(fetch artifactFetcher) retrievalDependencies {
	return retrievalDependencies{
		fetch: fetch, now: time.Now,
		closeActivationDirectory: func(file *os.File) error { return file.Close() },
		closeParentRoot:          func(root *os.Root) error { return root.Close() },
	}
}

func retrieveWithDependencies(ctx context.Context, options RetrieveOptions, dependencies retrievalDependencies) (Retrieval, error) {
	baseURL, err := parseTargetBaseURL(options.TargetBaseURL)
	if err != nil {
		return Retrieval{}, err
	}
	return retrieve(ctx, options, baseURL, dependencies)
}

func retrieve(ctx context.Context, options RetrieveOptions, baseURL *url.URL, dependencies retrievalDependencies) (result Retrieval, returnErr error) {
	if ctx == nil || dependencies.fetch == nil || dependencies.now == nil ||
		dependencies.closeActivationDirectory == nil || dependencies.closeParentRoot == nil {
		return Retrieval{}, invalidOptions(errors.New("context and complete retrieval dependencies are required"))
	}
	if err := ctx.Err(); err != nil {
		return Retrieval{}, err
	}
	if err := validateTargetPath(options.TargetPath); err != nil {
		return Retrieval{}, invalidOptions(err)
	}
	maxDownloadBytes := options.MaxDownloadBytes
	if maxDownloadBytes == 0 {
		maxDownloadBytes = DefaultMaxDownloadBytes
	}
	if maxDownloadBytes > MaximumDownloadBytes {
		return Retrieval{}, invalidOptions(fmt.Errorf("max download bytes must not exceed %d", MaximumDownloadBytes))
	}
	maxShards := options.MaxShards
	if maxShards == 0 {
		maxShards = DefaultMaxShards
	}
	if maxShards < 1 || maxShards > MaximumDownloadShards {
		return Retrieval{}, invalidOptions(fmt.Errorf("max shards must be 1..%d", MaximumDownloadShards))
	}
	if !cleanAbsolutePath(options.OutputDir) {
		return Retrieval{}, invalidOptions(errors.New("output must be a clean absolute path"))
	}
	outputBase := filepath.Base(options.OutputDir)
	if outputBase == "." || outputBase == string(filepath.Separator) {
		return Retrieval{}, invalidOptions(errors.New("output must name a new bundle directory"))
	}
	outputParent, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.OutputDir))
	if err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: output parent: %w", err)
	}
	outputPath := filepath.Join(outputParent, outputBase)
	metadataDir, err := secureconfigfile.ValidateDirectory(options.MetadataDir)
	if err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: metadata directory: %w", err)
	}
	stateDir, err := secureconfigfile.ValidateDirectory(options.StateDir)
	if err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: state directory: %w", err)
	}
	for name, protectedDir := range map[string]string{"metadata": metadataDir, "state": stateDir} {
		contains, err := pathContains(protectedDir, outputParent)
		if err != nil {
			return Retrieval{}, fmt.Errorf("TUF channel retrieval: inspect %s/output isolation: %w", name, err)
		}
		if contains {
			return Retrieval{}, fmt.Errorf("%w: output must not be inside the %s directory", secureconfigfile.ErrUnsafePath, name)
		}
	}
	parentRoot, err := os.OpenRoot(outputParent)
	if err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: anchor output parent: %w", err)
	}
	committed := false
	defer func() {
		if closeErr := dependencies.closeParentRoot(parentRoot); closeErr != nil {
			if committed {
				closeErr = fmt.Errorf("TUF channel retrieval: bundle committed at %s but closing the output parent failed: %w", outputPath, closeErr)
			} else {
				closeErr = fmt.Errorf("TUF channel retrieval: close output parent: %w", closeErr)
			}
			returnErr = errors.Join(returnErr, closeErr)
		}
	}()
	if err := retrievalParentStillAtPath(parentRoot, outputParent); err != nil {
		return Retrieval{}, err
	}
	if _, err := parentRoot.Lstat(outputBase); err == nil {
		return Retrieval{}, ErrRetrievalExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: inspect output: %w", err)
	}

	var manifestRaw []byte
	var selectedTarget openpackchannel.Target
	var downloaded uint64
	selectionDeps := defaultSelectionDependencies()
	selectionDeps.loadManifest = func(ctx context.Context, target verifiedManifestTarget) ([]byte, error) {
		selectedTarget = target.Custom
		if target.Target.Length < 1 || uint64(target.Target.Length) > maxDownloadBytes {
			return nil, ErrRetrievalLimit
		}
		remotePath := remoteManifestPath(target)
		rawURL, err := artifactURL(baseURL, remotePath)
		if err != nil {
			return nil, err
		}
		var buffer bytes.Buffer
		count, err := dependencies.fetch(ctx, rawURL, target.Target.Length, &buffer)
		if err != nil {
			return nil, fmt.Errorf("TUF channel retrieval: fetch manifest: %w", err)
		}
		if count != int64(buffer.Len()) {
			return nil, errors.New("TUF channel retrieval: manifest fetch reported inconsistent byte count")
		}
		downloaded = uint64(buffer.Len())
		manifestRaw = bytes.Clone(buffer.Bytes())
		return manifestRaw, nil
	}
	selected, err := selectWithDependencies(ctx, Options{
		TrustedRootPath: options.TrustedRootPath, MetadataDir: metadataDir, StateDir: stateDir,
		TargetPath: options.TargetPath,
	}, selectionDeps)
	if err != nil {
		return Retrieval{}, err
	}
	result = retrievalFromSelection(selected)
	if selected.Status == StatusAbsent {
		return result, nil
	}
	manifest, err := indexpack.DecodeManifest(manifestRaw)
	if err != nil {
		return Retrieval{}, err
	}
	if len(manifest.Shards) > maxShards {
		return Retrieval{}, fmt.Errorf("%w: manifest declares %d shards, maximum is %d", ErrRetrievalLimit, len(manifest.Shards), maxShards)
	}

	bundlePrefix := path.Dir(options.TargetPath) + "/"
	signatureURL, err := artifactURL(baseURL, remoteSignaturePath(verifiedManifestTarget{
		Path:   options.TargetPath,
		Custom: selectedTarget,
	}))
	if err != nil {
		return Retrieval{}, err
	}
	remaining := maxDownloadBytes - downloaded
	if remaining == 0 {
		return Retrieval{}, ErrRetrievalLimit
	}
	signatureLimit := uint64(indexpack.MaxSignatureBytes)
	if remaining < signatureLimit {
		signatureLimit = remaining
	}
	var signatureBuffer bytes.Buffer
	signatureCount, err := dependencies.fetch(ctx, signatureURL, int64(signatureLimit), &signatureBuffer)
	if err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: fetch manifest signature: %w", err)
	}
	if signatureCount != int64(signatureBuffer.Len()) || signatureCount < 1 {
		return Retrieval{}, errors.New("TUF channel retrieval: signature fetch reported an invalid byte count")
	}
	downloaded += uint64(signatureCount)
	signature, err := indexpack.DecodeSignature(signatureBuffer.Bytes())
	if err != nil {
		return Retrieval{}, err
	}
	if signature.KeyID != manifest.SigningKeyID {
		return Retrieval{}, errors.New("TUF channel retrieval: signature key ID does not match manifest")
	}
	remaining = maxDownloadBytes - downloaded
	var declaredShardBytes uint64
	for _, shard := range manifest.Shards {
		if shard.CompressedSizeBytes > remaining-declaredShardBytes {
			return Retrieval{}, fmt.Errorf("%w: manifest shard bytes exceed remaining budget", ErrRetrievalLimit)
		}
		declaredShardBytes += shard.CompressedSizeBytes
	}
	if err := retrievalParentStillAtPath(parentRoot, outputParent); err != nil {
		return Retrieval{}, err
	}

	stagingName, err := createRetrievalStaging(parentRoot)
	if err != nil {
		return Retrieval{}, err
	}
	stagingCreated := true
	defer func() {
		if stagingCreated {
			if err := parentRoot.RemoveAll(stagingName); err != nil && !errors.Is(err, os.ErrNotExist) {
				returnErr = errors.Join(returnErr, fmt.Errorf("TUF channel retrieval: remove incomplete staging directory: %w", err))
			}
		}
	}()
	stagingRoot, err := parentRoot.OpenRoot(stagingName)
	if err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: anchor staging directory: %w", err)
	}
	stagingOpen := true
	defer func() {
		if stagingOpen {
			returnErr = errors.Join(returnErr, stagingRoot.Close())
		}
	}()
	if err := writeRetrievedFile(stagingRoot, "manifest.json", manifestRaw); err != nil {
		return Retrieval{}, err
	}
	if err := writeRetrievedFile(stagingRoot, "manifest.ed25519", signatureBuffer.Bytes()); err != nil {
		return Retrieval{}, err
	}
	directories := map[string]struct{}{".": {}}
	for _, shard := range manifest.Shards {
		if err := ctx.Err(); err != nil {
			return Retrieval{}, err
		}
		relative := filepath.FromSlash(shard.Path)
		directory := filepath.Dir(relative)
		if err := stagingRoot.MkdirAll(directory, 0o700); err != nil {
			return Retrieval{}, fmt.Errorf("TUF channel retrieval: create shard directory: %w", err)
		}
		for current := directory; current != "."; current = filepath.Dir(current) {
			directories[current] = struct{}{}
		}
		file, err := stagingRoot.OpenFile(relative, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return Retrieval{}, fmt.Errorf("TUF channel retrieval: create shard: %w", err)
		}
		hasher := sha256.New()
		shardURL, urlErr := artifactURL(baseURL, bundlePrefix+shard.Path)
		var count int64
		if urlErr == nil {
			count, err = dependencies.fetch(ctx, shardURL, int64(shard.CompressedSizeBytes), io.MultiWriter(file, hasher))
		} else {
			err = urlErr
		}
		fileErr := errors.Join(file.Sync(), file.Close())
		if err != nil || fileErr != nil {
			if err != nil {
				err = fmt.Errorf("TUF channel retrieval: fetch shard %s: %w", shard.Path, err)
			}
			if fileErr != nil {
				fileErr = fmt.Errorf("TUF channel retrieval: persist shard %s: %w", shard.Path, fileErr)
			}
			return Retrieval{}, errors.Join(err, fileErr)
		}
		if count != int64(shard.CompressedSizeBytes) {
			return Retrieval{}, fmt.Errorf("TUF channel retrieval: shard %s length is %d, expected %d", shard.Path, count, shard.CompressedSizeBytes)
		}
		if actual := hex.EncodeToString(hasher.Sum(nil)); actual != shard.SHA256 {
			return Retrieval{}, fmt.Errorf("TUF channel retrieval: shard SHA-256 mismatch for %s", shard.Path)
		}
		downloaded += uint64(count)
	}
	if err := syncRetrievalDirectories(stagingRoot, directories); err != nil {
		return Retrieval{}, err
	}
	if err := ctx.Err(); err != nil {
		return Retrieval{}, err
	}
	if err := retrievalParentStillAtPath(parentRoot, outputParent); err != nil {
		return Retrieval{}, err
	}
	if err := stagingRoot.Close(); err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: close staging directory: %w", err)
	}
	stagingOpen = false
	if dependencies.beforeActivate != nil {
		dependencies.beforeActivate()
	}
	if _, err := selectedTarget.VerifyManifest(manifestRaw, dependencies.now().UTC()); err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: revalidate manifest before activation: %w", err)
	}
	parentDirectory, err := parentRoot.Open(".")
	if err != nil {
		return Retrieval{}, fmt.Errorf("TUF channel retrieval: open output parent for activation: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Retrieval{}, errors.Join(err, dependencies.closeActivationDirectory(parentDirectory))
	}
	activationErr := renameNoReplaceAt(parentDirectory, stagingName, outputBase)
	if activationErr != nil {
		closeErr := dependencies.closeActivationDirectory(parentDirectory)
		if errors.Is(activationErr, os.ErrExist) {
			activationErr = ErrRetrievalExists
		}
		return Retrieval{}, errors.Join(activationErr, closeErr)
	}
	committed = true
	stagingCreated = false
	result.Status = StatusRetrieved
	result.Path = outputPath
	result.ShardCount = len(manifest.Shards)
	result.FileCount = len(manifest.Shards) + 2
	result.DownloadBytes = downloaded
	if closeErr := dependencies.closeActivationDirectory(parentDirectory); closeErr != nil {
		return result, fmt.Errorf("TUF channel retrieval: bundle committed at %s but closing the activation directory failed: %w", outputPath, closeErr)
	}
	if err := retrievalParentStillAtPath(parentRoot, outputParent); err != nil {
		return result, fmt.Errorf("TUF channel retrieval: bundle committed at %s but output parent identity changed: %w", outputPath, err)
	}
	if err := syncRetrievalParent(parentRoot); err != nil {
		return result, fmt.Errorf("TUF channel retrieval: bundle committed at %s but parent durability is uncertain: %w", outputPath, err)
	}
	if err := retrievalParentStillAtPath(parentRoot, outputParent); err != nil {
		return result, fmt.Errorf("TUF channel retrieval: bundle committed at %s but output parent identity changed: %w", outputPath, err)
	}
	return result, nil
}

func retrievalFromSelection(selection Selection) Retrieval {
	return Retrieval{
		Status: selection.Status, TargetPath: selection.TargetPath, PackID: selection.PackID,
		Kind: selection.Kind, Revision: selection.Revision, ManifestSHA256: selection.ManifestSHA256,
		ParentManifestSHA256: selection.ParentManifestSHA256, RootVersion: selection.RootVersion,
		TimestampVersion: selection.TimestampVersion, SnapshotVersion: selection.SnapshotVersion,
		TargetsVersion: selection.TargetsVersion,
	}
}

func parseTargetBaseURL(raw string) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, invalidOptions(errors.New("target base URL is required without surrounding whitespace"))
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Opaque != "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || !strings.HasSuffix(parsed.Path, "/") {
		return nil, invalidOptions(errors.New("target base URL must be an absolute clean HTTPS directory URL without credentials, query, or fragment"))
	}
	cleanPath := path.Clean(strings.TrimSuffix(parsed.Path, "/")) + "/"
	if cleanPath == "./" {
		cleanPath = "/"
	}
	if cleanPath != parsed.Path || parsed.String() != raw {
		return nil, invalidOptions(errors.New("target base URL must use a canonical clean path ending in slash"))
	}
	return parsed, nil
}

func artifactURL(base *url.URL, relative string) (string, error) {
	if relative == "" || path.IsAbs(relative) || path.Clean(relative) != relative || strings.Contains(relative, "\\") || strings.Contains(relative, "%") || strings.HasPrefix(relative, "../") {
		return "", errors.New("TUF channel retrieval: artifact path is not clean and relative")
	}
	joined := *base
	joined.Path = base.Path + relative
	return joined.String(), nil
}

func remoteManifestPath(target verifiedManifestTarget) string {
	if !target.ConsistentSnapshot {
		return target.Path
	}
	base := path.Base(target.Path)
	directory := path.Dir(target.Path)
	return path.Join(directory, target.Custom.ManifestSHA256+"."+base)
}

func remoteSignaturePath(target verifiedManifestTarget) string {
	directory := path.Dir(target.Path)
	return path.Join(directory, target.Custom.ManifestSHA256+".manifest.ed25519")
}

func newHTTPArtifactFetcher(ctx context.Context, baseURL *url.URL) (artifactFetcher, error) {
	policy := egress.DefaultExternal()
	policy.AllowedSchemes = []string{"https"}
	policy.HostAllowlist = []string{baseURL.Hostname()}
	policy.MaxRedirects = 0
	policy.ResponseHeaderTimeout = 15 * time.Second
	if err := policy.Validate(ctx, baseURL.String()); err != nil {
		return nil, fmt.Errorf("TUF channel retrieval: target base egress: %w", err)
	}
	client := retrievalHTTPClient(policy, artifactRequestTimeout)
	return httpArtifactFetcher(policy, client), nil
}

func retrievalHTTPClient(policy egress.Policy, timeout time.Duration) *http.Client {
	client := policy.HTTPClient(timeout)
	transport := policy.Transport()
	transport.DisableKeepAlives = true
	client.Transport = transport
	return client
}

func httpArtifactFetcher(policy egress.Policy, client *http.Client) artifactFetcher {
	return func(ctx context.Context, rawURL string, maximum int64, destination io.Writer) (count int64, returnErr error) {
		if maximum < 1 || uint64(maximum) > MaximumDownloadBytes {
			return 0, ErrRetrievalLimit
		}
		if err := policy.Validate(ctx, rawURL); err != nil {
			return 0, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return 0, err
		}
		request.Header.Set("Accept", "application/octet-stream")
		request.Header.Set("Accept-Encoding", "identity")
		request.Header.Set("User-Agent", "Fetchmark-Pack/1.0")
		response, err := client.Do(request)
		if err != nil {
			return 0, err
		}
		defer func() { returnErr = errors.Join(returnErr, response.Body.Close()) }()
		if response.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
		}
		if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
			return 0, fmt.Errorf("unsupported content encoding %q", encoding)
		}
		if response.ContentLength > maximum {
			return 0, ErrRetrievalLimit
		}
		count, err = io.Copy(destination, io.LimitReader(response.Body, maximum+1))
		if err != nil {
			return count, err
		}
		if count > maximum {
			return count, ErrRetrievalLimit
		}
		return count, nil
	}
}

func createRetrievalStaging(parent *os.Root) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", fmt.Errorf("TUF channel retrieval: generate staging name: %w", err)
		}
		name := retrievalStagingPrefix + hex.EncodeToString(random)
		if err := parent.Mkdir(name, 0o700); err == nil {
			return name, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("TUF channel retrieval: create staging directory: %w", err)
		}
	}
	return "", errors.New("TUF channel retrieval: could not allocate a unique staging directory")
}

func writeRetrievedFile(root *os.Root, relative string, raw []byte) error {
	file, err := root.OpenFile(relative, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("TUF channel retrieval: create %s: %w", relative, err)
	}
	written, err := file.Write(raw)
	if err != nil || written != len(raw) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return errors.Join(fmt.Errorf("TUF channel retrieval: write %s: %w", relative, err), file.Close())
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("TUF channel retrieval: sync %s: %w", relative, err)
	}
	return nil
}

func syncRetrievalDirectories(root *os.Root, directories map[string]struct{}) error {
	names := make([]string, 0, len(directories))
	for name := range directories {
		names = append(names, name)
	}
	sort.Slice(names, func(left, right int) bool {
		leftDepth := strings.Count(names[left], string(filepath.Separator))
		rightDepth := strings.Count(names[right], string(filepath.Separator))
		if leftDepth != rightDepth {
			return leftDepth > rightDepth
		}
		return names[left] < names[right]
	})
	for _, name := range names {
		directory, err := root.Open(name)
		if err != nil {
			return fmt.Errorf("TUF channel retrieval: open directory %s for sync: %w", name, err)
		}
		if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
			return fmt.Errorf("TUF channel retrieval: sync directory %s: %w", name, err)
		}
	}
	return nil
}

func syncRetrievalParent(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func retrievalParentStillAtPath(root *os.Root, parentPath string) error {
	anchored, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("TUF channel retrieval: inspect anchored output parent: %w", err)
	}
	current, err := os.Lstat(parentPath)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return errors.New("TUF channel retrieval: output parent path changed during retrieval")
	}
	return nil
}

func cleanAbsolutePath(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && filepath.IsAbs(value) && filepath.Clean(value) == value
}
