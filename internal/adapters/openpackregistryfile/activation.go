package openpackregistryfile

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackregistry"
)

const activationStagingPrefix = ".fetchmark-registry-activation-"

var (
	ErrInvalidActivation = errors.New("open pack registry file: invalid activation")
	ErrCASMismatch       = errors.New("open pack registry file: compare-and-swap mismatch")
	digestPattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type ActivationStatus string

const (
	ActivationStatusPrepared       ActivationStatus = "prepared"
	ActivationStatusActivated      ActivationStatus = "activated"
	ActivationStatusRolledBack     ActivationStatus = "rolled_back"
	ActivationStatusAlreadyApplied ActivationStatus = "already_applied"
)

type ActivationOptions struct {
	Mode                    openpackregistry.TransitionMode
	SourceID                string
	RegistryPath            string
	CandidatePath           string
	RollbackPath            string
	ExpectedCurrentSHA256   string
	ExpectedCandidateSHA256 string
}

type ActivationResult struct {
	Status                 ActivationStatus                `json:"status"`
	Mode                   openpackregistry.TransitionMode `json:"mode"`
	SourceID               string                          `json:"source_id"`
	Path                   string                          `json:"path"`
	RollbackPath           string                          `json:"rollback_path"`
	FromRegistrySHA256     string                          `json:"from_registry_sha256"`
	ToRegistrySHA256       string                          `json:"to_registry_sha256"`
	RollbackRegistrySHA256 string                          `json:"rollback_registry_sha256"`
	FromRevision           uint64                          `json:"from_revision"`
	ToRevision             uint64                          `json:"to_revision"`
	RestartRequired        bool                            `json:"restart_required"`
}

type PreparedActivationError struct {
	Result ActivationResult
	Cause  error
}

func (err *PreparedActivationError) Error() string {
	return fmt.Sprintf(
		"open pack registry file: rollback prepared at %s with sha256=%s but live registry is unchanged: %v; inspect the rollback file before retrying",
		err.Result.RollbackPath, err.Result.RollbackRegistrySHA256, err.Cause,
	)
}

func (err *PreparedActivationError) Unwrap() error { return err.Cause }

type CommittedActivationError struct {
	Result ActivationResult
	Cause  error
}

func (err *CommittedActivationError) Error() string {
	return fmt.Sprintf(
		"open pack registry file: %s committed at %s but completion failed: from_sha256=%s to_sha256=%s rollback_path=%s rollback_sha256=%s restart_required=true: %v; inspect the live and rollback registries before retrying",
		err.Result.Mode, err.Result.Path, err.Result.FromRegistrySHA256, err.Result.ToRegistrySHA256,
		err.Result.RollbackPath, err.Result.RollbackRegistrySHA256, err.Cause,
	)
}

func (err *CommittedActivationError) Unwrap() error { return err.Cause }

type activationDependencies struct {
	now                    func() time.Time
	validateProjection     func(context.Context, openpackregistry.Binding, time.Time) error
	beforeReplace          func()
	afterFinalRead         func()
	closeActivationFile    func(*os.File) error
	closeRollbackDirectory func(*os.File) error
	closeParent            func(*os.Root) error
	syncParentAfterArchive func(*os.Root) error
	syncParentAfterReplace func(*os.Root) error
}

func defaultActivationDependencies() activationDependencies {
	return activationDependencies{
		now:                    func() time.Time { return time.Now().UTC() },
		validateProjection:     validateInstalledProjection,
		closeActivationFile:    func(file *os.File) error { return file.Close() },
		closeRollbackDirectory: func(file *os.File) error { return file.Close() },
		closeParent:            func(root *os.Root) error { return root.Close() },
		syncParentAfterArchive: syncCandidateParent,
		syncParentAfterReplace: syncCandidateParent,
	}
}

func Activate(ctx context.Context, options ActivationOptions) (ActivationResult, error) {
	return activateWithDependencies(ctx, options, defaultActivationDependencies())
}

func activateWithDependencies(ctx context.Context, options ActivationOptions, dependencies activationDependencies) (result ActivationResult, returnErr error) {
	dependencies = dependencies.withDefaults()
	if ctx == nil {
		return ActivationResult{}, invalidActivation(errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return ActivationResult{}, err
	}
	validated, candidate, candidateRaw, err := validateActivationOptions(options)
	if err != nil {
		return ActivationResult{}, err
	}
	options = validated
	result = ActivationResult{
		Mode: options.Mode, SourceID: options.SourceID, Path: options.RegistryPath, RollbackPath: options.RollbackPath,
		FromRegistrySHA256: options.ExpectedCurrentSHA256, ToRegistrySHA256: options.ExpectedCandidateSHA256,
		RollbackRegistrySHA256: options.ExpectedCurrentSHA256,
	}

	parentPath := filepath.Dir(options.RegistryPath)
	registryName := filepath.Base(options.RegistryPath)
	rollbackName := filepath.Base(options.RollbackPath)
	parent, err := os.OpenRoot(parentPath)
	if err != nil {
		return ActivationResult{}, fmt.Errorf("open pack registry file: anchor active parent: %w", err)
	}
	prepared := false
	committed := false
	defer func() {
		if closeErr := dependencies.closeParent(parent); closeErr != nil {
			returnErr = errors.Join(returnErr, activationLifecycleError(result, prepared, committed, fmt.Errorf("close active parent: %w", closeErr)))
		}
	}()
	if err := activationParentStillAtPath(parent, parentPath); err != nil {
		return ActivationResult{}, err
	}
	lockName := activationLockName(registryName)
	lockFile, err := openActivationLock(ctx, parent, lockName)
	if err != nil {
		return ActivationResult{}, fmt.Errorf("open pack registry file: open activation lock: %w", err)
	}
	lockPathInfo, pathErr := parent.Lstat(lockName)
	lockInfo, statErr := lockFile.Stat()
	if pathErr != nil || statErr != nil || !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm() != 0o600 ||
		lockPathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(lockPathInfo, lockInfo) {
		_ = lockFile.Close()
		return ActivationResult{}, fmt.Errorf("%w: activation lock must be the private anchored regular file", ErrUnsafePath)
	}
	lockHeld := false
	defer func() {
		var lockErr error
		if lockHeld {
			lockErr = unlockRegistryFile(lockFile)
		}
		lockErr = errors.Join(lockErr, dependencies.closeActivationFile(lockFile))
		if lockErr != nil {
			returnErr = errors.Join(returnErr, activationLifecycleError(result, prepared, committed, fmt.Errorf("release activation lock: %w", lockErr)))
		}
	}()
	if err := waitForRegistryLock(ctx, lockFile); err != nil {
		return ActivationResult{}, err
	}
	lockHeld = true
	if err := activationParentStillAtPath(parent, parentPath); err != nil {
		return ActivationResult{}, err
	}

	current, currentRaw, currentDigest, err := readRegistryAt(parent, registryName)
	if err != nil {
		return ActivationResult{}, err
	}
	if currentDigest == options.ExpectedCandidateSHA256 {
		return recoverAlreadyApplied(ctx, options, result, parent, rollbackName, current, dependencies)
	}
	if currentDigest != options.ExpectedCurrentSHA256 {
		return ActivationResult{}, fmt.Errorf("%w: live registry sha256=%s, expected=%s", ErrCASMismatch, currentDigest, options.ExpectedCurrentSHA256)
	}
	transition, err := openpackregistry.ValidateTransition(current, candidate, options.SourceID, options.Mode)
	if err != nil {
		return ActivationResult{}, err
	}
	result.FromRevision, result.ToRevision = transition.From.Revision, transition.To.Revision
	validationTime, err := currentActivationTime(dependencies.now)
	if err != nil {
		return ActivationResult{}, err
	}
	if err := dependencies.validateProjection(ctx, transition.To, validationTime); err != nil {
		return ActivationResult{}, fmt.Errorf("open pack registry file: verify destination projection: %w", err)
	}
	rollbackPrepared, err := prepareRollbackFile(parent, rollbackName, currentRaw, currentDigest, dependencies.closeRollbackDirectory)
	if rollbackPrepared {
		prepared = true
		result.Status = ActivationStatusPrepared
	}
	if err != nil {
		if prepared {
			return result, &PreparedActivationError{Result: result, Cause: fmt.Errorf("prepare rollback archive: %w", err)}
		}
		return ActivationResult{}, err
	}
	if err := dependencies.syncParentAfterArchive(parent); err != nil {
		return result, &PreparedActivationError{Result: result, Cause: fmt.Errorf("sync rollback archive: %w", err)}
	}

	stagingName, err := createActivationStaging(parent)
	if err != nil {
		return result, &PreparedActivationError{Result: result, Cause: err}
	}
	stagingExists := true
	defer func() {
		if stagingExists {
			if removeErr := parent.Remove(stagingName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				returnErr = errors.Join(returnErr, activationLifecycleError(result, prepared, committed, fmt.Errorf("remove activation staging file: %w", removeErr)))
			}
		}
	}()
	if err := writeActivationFile(parent, stagingName, candidateRaw); err != nil {
		return result, &PreparedActivationError{Result: result, Cause: err}
	}
	if dependencies.beforeReplace != nil {
		dependencies.beforeReplace()
	}
	if err := ctx.Err(); err != nil {
		return result, &PreparedActivationError{Result: result, Cause: err}
	}
	_, finalRaw, finalDigest, err := readRegistryAt(parent, registryName)
	if err != nil {
		return result, &PreparedActivationError{Result: result, Cause: err}
	}
	if finalDigest != currentDigest || !bytes.Equal(finalRaw, currentRaw) {
		return result, &PreparedActivationError{Result: result, Cause: fmt.Errorf("%w: live registry changed before replacement", ErrCASMismatch)}
	}
	if dependencies.afterFinalRead != nil {
		dependencies.afterFinalRead()
	}
	if err := ctx.Err(); err != nil {
		return result, &PreparedActivationError{Result: result, Cause: err}
	}
	validationTime, err = currentActivationTime(dependencies.now)
	if err != nil {
		return result, &PreparedActivationError{Result: result, Cause: err}
	}
	if err := dependencies.validateProjection(ctx, transition.To, validationTime); err != nil {
		return result, &PreparedActivationError{Result: result, Cause: fmt.Errorf("revalidate destination projection: %w", err)}
	}
	directory, err := parent.Open(".")
	if err != nil {
		return result, &PreparedActivationError{Result: result, Cause: fmt.Errorf("open activation directory: %w", err)}
	}
	if err := ctx.Err(); err != nil {
		closeErr := dependencies.closeActivationFile(directory)
		return result, &PreparedActivationError{Result: result, Cause: errors.Join(err, closeErr)}
	}
	replaceErr := replaceRegistryAt(directory, stagingName, registryName)
	closeErr := dependencies.closeActivationFile(directory)
	if replaceErr != nil {
		return result, &PreparedActivationError{Result: result, Cause: errors.Join(replaceErr, closeErr)}
	}
	committed = true
	stagingExists = false
	result.RestartRequired = true
	if options.Mode == openpackregistry.TransitionRollback {
		result.Status = ActivationStatusRolledBack
	} else {
		result.Status = ActivationStatusActivated
	}
	if closeErr != nil {
		return result, &CommittedActivationError{Result: result, Cause: fmt.Errorf("close activation directory: %w", closeErr)}
	}
	if err := dependencies.syncParentAfterReplace(parent); err != nil {
		return result, &CommittedActivationError{Result: result, Cause: fmt.Errorf("sync active registry parent: %w", err)}
	}
	if err := activationParentStillAtPath(parent, parentPath); err != nil {
		return result, &CommittedActivationError{Result: result, Cause: err}
	}
	_, activeRaw, activeDigest, err := readRegistryAt(parent, registryName)
	if err != nil || activeDigest != options.ExpectedCandidateSHA256 || !bytes.Equal(activeRaw, candidateRaw) {
		return result, &CommittedActivationError{Result: result, Cause: errors.Join(errors.New("active registry readback differs from candidate"), err)}
	}
	return result, nil
}

func recoverAlreadyApplied(ctx context.Context, options ActivationOptions, result ActivationResult, parent *os.Root, rollbackName string, current openpackregistry.Registry, dependencies activationDependencies) (ActivationResult, error) {
	rollback, _, rollbackDigest, err := readRegistryAt(parent, rollbackName)
	if err != nil || rollbackDigest != options.ExpectedCurrentSHA256 {
		return ActivationResult{}, fmt.Errorf("%w: live registry already has candidate digest but exact rollback bytes are unavailable", ErrCASMismatch)
	}
	transition, err := openpackregistry.ValidateTransition(rollback, current, options.SourceID, options.Mode)
	if err != nil {
		return ActivationResult{}, err
	}
	validationTime, err := currentActivationTime(dependencies.now)
	if err != nil {
		return ActivationResult{}, err
	}
	if err := dependencies.validateProjection(ctx, transition.To, validationTime); err != nil {
		return ActivationResult{}, fmt.Errorf("open pack registry file: verify already-applied projection: %w", err)
	}
	result.Status = ActivationStatusAlreadyApplied
	result.RestartRequired = true
	result.FromRevision, result.ToRevision = transition.From.Revision, transition.To.Revision
	return result, nil
}

func validateActivationOptions(options ActivationOptions) (ActivationOptions, openpackregistry.Registry, []byte, error) {
	if options.Mode != openpackregistry.TransitionActivate && options.Mode != openpackregistry.TransitionRollback {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, invalidActivation(errors.New("mode must be activate or rollback"))
	}
	if strings.TrimSpace(options.SourceID) != options.SourceID || options.SourceID == "" ||
		!digestPattern.MatchString(options.ExpectedCurrentSHA256) || !digestPattern.MatchString(options.ExpectedCandidateSHA256) ||
		options.ExpectedCurrentSHA256 == options.ExpectedCandidateSHA256 {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, invalidActivation(errors.New("source and distinct exact digests are required"))
	}
	for _, path := range []string{options.RegistryPath, options.CandidatePath, options.RollbackPath} {
		if path == "" || strings.TrimSpace(path) != path || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return ActivationOptions{}, openpackregistry.Registry{}, nil, invalidActivation(errors.New("clean absolute registry, candidate, and rollback paths are required"))
		}
	}
	activeParent, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.RegistryPath))
	if err != nil {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, fmt.Errorf("open pack registry file: active parent: %w", err)
	}
	rollbackParent, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.RollbackPath))
	if err != nil || rollbackParent != activeParent {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, invalidActivation(errors.New("rollback output must share the active registry parent"))
	}
	options.RegistryPath = filepath.Join(activeParent, filepath.Base(options.RegistryPath))
	options.RollbackPath = filepath.Join(activeParent, filepath.Base(options.RollbackPath))
	if options.RegistryPath == options.RollbackPath {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, invalidActivation(errors.New("active registry and rollback paths must differ"))
	}
	candidateRaw, err := secureconfigfile.Read(options.CandidatePath, secureconfigfile.Options{MaxBytes: openpackregistry.MaxRegistryBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, fmt.Errorf("open pack registry file: candidate: %w", err)
	}
	if indexpack.Digest(candidateRaw) != options.ExpectedCandidateSHA256 {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, fmt.Errorf("%w: candidate registry digest differs", ErrCASMismatch)
	}
	candidate, err := openpackregistry.Load(bytes.NewReader(candidateRaw))
	if err != nil {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, err
	}
	canonicalCandidate, err := filepath.EvalSymlinks(options.CandidatePath)
	if err != nil {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, err
	}
	if canonicalCandidate == options.RegistryPath || canonicalCandidate == options.RollbackPath {
		return ActivationOptions{}, openpackregistry.Registry{}, nil, invalidActivation(errors.New("candidate must be distinct from active and rollback paths"))
	}
	options.CandidatePath = canonicalCandidate
	return options, candidate, candidateRaw, nil
}

func currentActivationTime(now func() time.Time) (time.Time, error) {
	if now == nil {
		return time.Time{}, errors.New("open pack registry file: activation clock is required")
	}
	current := now().UTC()
	if current.IsZero() {
		return time.Time{}, errors.New("open pack registry file: activation clock returned zero time")
	}
	return current, nil
}

func validateInstalledProjection(ctx context.Context, binding openpackregistry.Binding, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	projection, err := openpackindex.Open(openpackindex.OpenOptions{
		Path: binding.InstalledPath, ExpectedManifestSHA256: binding.ManifestSHA256,
		ExpectedPackID: binding.PackID, ExpectedRevision: binding.Revision,
		ExpectedRecordCount: binding.RecordCount, ExpectedKeyID: binding.SigningKeyID,
		ExpectedCreatedAt: binding.CreatedAt, ExpectedExpiresAt: binding.ExpiresAt,
		Now: now, ProviderID: binding.SourceID,
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, projection.Close())
	}
	return projection.Close()
}

func prepareRollbackFile(parent *os.Root, name string, raw []byte, digest string, closeDirectory func(*os.File) error) (bool, error) {
	if closeDirectory == nil {
		return false, errors.New("open pack registry file: rollback directory closer is required")
	}
	if _, err := parent.Lstat(name); err == nil {
		_, existing, existingDigest, readErr := readRegistryAt(parent, name)
		if readErr != nil || existingDigest != digest || !bytes.Equal(existing, raw) {
			return false, fmt.Errorf("open pack registry file: existing rollback output differs from exact current registry")
		}
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	staging, err := createActivationStaging(parent)
	if err != nil {
		return false, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = parent.Remove(staging)
		}
	}()
	if err := writeActivationFile(parent, staging, raw); err != nil {
		return false, err
	}
	directory, err := parent.Open(".")
	if err != nil {
		return false, err
	}
	renameErr := renameCandidateNoReplaceAt(directory, staging, name)
	closeErr := closeDirectory(directory)
	if renameErr != nil || closeErr != nil {
		return renameErr == nil, errors.Join(renameErr, closeErr)
	}
	cleanup = false
	return true, nil
}

func writeActivationFile(parent *os.Root, name string, raw []byte) error {
	file, err := parent.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := io.Copy(file, bytes.NewReader(raw))
	if writeErr == nil && written != int64(len(raw)) {
		writeErr = io.ErrShortWrite
	}
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func readRegistryAt(parent *os.Root, name string) (openpackregistry.Registry, []byte, string, error) {
	info, err := parent.Lstat(name)
	if err != nil {
		return openpackregistry.Registry{}, nil, "", fmt.Errorf("open pack registry file: inspect registry: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 ||
		info.Size() < 1 || info.Size() > openpackregistry.MaxRegistryBytes {
		return openpackregistry.Registry{}, nil, "", fmt.Errorf("%w: registry is not a bounded safe regular file", ErrUnsafePath)
	}
	file, err := parent.Open(name)
	if err != nil {
		return openpackregistry.Registry{}, nil, "", err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = file.Close()
		return openpackregistry.Registry{}, nil, "", fmt.Errorf("%w: registry changed while opening", ErrUnsafePath)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, openpackregistry.MaxRegistryBytes+1))
	if readErr == nil {
		_, readErr = file.Seek(0, io.SeekStart)
	}
	verification, verifyErr := io.ReadAll(io.LimitReader(file, openpackregistry.MaxRegistryBytes+1))
	closeErr := file.Close()
	if readErr != nil || verifyErr != nil || closeErr != nil || !bytes.Equal(raw, verification) || int64(len(raw)) != opened.Size() {
		return openpackregistry.Registry{}, nil, "", errors.Join(readErr, verifyErr, closeErr, fmt.Errorf("%w: registry changed while reading", ErrUnsafePath))
	}
	registry, err := openpackregistry.Load(bytes.NewReader(raw))
	if err != nil {
		return openpackregistry.Registry{}, nil, "", err
	}
	return registry, raw, indexpack.Digest(raw), nil
}

func waitForRegistryLock(ctx context.Context, lock *os.File) error {
	for {
		locked, err := tryLockRegistryFile(lock)
		if err != nil {
			return err
		}
		if locked {
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func activationLockName(registryName string) string {
	digest := sha256.Sum256([]byte(registryName))
	return ".fetchmark-registry-lock-" + hex.EncodeToString(digest[:])
}

func openActivationLock(ctx context.Context, parent *os.Root, name string) (*os.File, error) {
	for range 8 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file, err := parent.OpenFile(name, os.O_RDWR, 0)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		file, err = parent.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, os.ErrExist) && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return nil, errors.New("activation lock changed repeatedly while opening")
}

func createActivationStaging(parent *os.Root) (string, error) {
	for range 8 {
		randomBytes := make([]byte, 16)
		if _, err := rand.Read(randomBytes); err != nil {
			return "", err
		}
		name := activationStagingPrefix + hex.EncodeToString(randomBytes)
		if _, err := parent.Lstat(name); errors.Is(err, os.ErrNotExist) {
			return name, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("open pack registry file: could not allocate activation staging file")
}

func activationParentStillAtPath(parent *os.Root, parentPath string) error {
	anchored, err := parent.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(parentPath)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return fmt.Errorf("%w: active registry parent path changed", ErrUnsafePath)
	}
	return nil
}

func activationLifecycleError(result ActivationResult, prepared, committed bool, cause error) error {
	if committed {
		return &CommittedActivationError{Result: result, Cause: cause}
	}
	if prepared {
		return &PreparedActivationError{Result: result, Cause: cause}
	}
	return cause
}

func invalidActivation(cause error) error {
	return fmt.Errorf("%w: %v", ErrInvalidActivation, cause)
}

func (dependencies activationDependencies) withDefaults() activationDependencies {
	defaults := defaultActivationDependencies()
	if dependencies.now == nil {
		dependencies.now = defaults.now
	}
	if dependencies.validateProjection == nil {
		dependencies.validateProjection = defaults.validateProjection
	}
	if dependencies.closeActivationFile == nil {
		dependencies.closeActivationFile = defaults.closeActivationFile
	}
	if dependencies.closeRollbackDirectory == nil {
		dependencies.closeRollbackDirectory = defaults.closeRollbackDirectory
	}
	if dependencies.closeParent == nil {
		dependencies.closeParent = defaults.closeParent
	}
	if dependencies.syncParentAfterArchive == nil {
		dependencies.syncParentAfterArchive = defaults.syncParentAfterArchive
	}
	if dependencies.syncParentAfterReplace == nil {
		dependencies.syncParentAfterReplace = defaults.syncParentAfterReplace
	}
	return dependencies
}
