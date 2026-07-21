package tufrepository

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

var (
	ErrInvalidIdentityWrite = errors.New("TUF repository: invalid identity write")
	ErrIdentityExists       = errors.New("TUF repository: identity output already exists")
)

// IdentityWriteResult is non-secret recovery evidence for an atomically
// committed private repository identity.
type IdentityWriteResult struct {
	Path          string `json:"path"`
	RepositoryID  string `json:"repository_id"`
	RootSHA256    string `json:"root_sha256"`
	RootVersion   int64  `json:"root_version"`
	RootExpiresAt string `json:"root_expires_at"`
	RootThreshold int    `json:"root_threshold"`
}

// CommittedIdentityError means the private identity path was atomically
// claimed. Operators must inspect that exact file before any retry.
type CommittedIdentityError struct {
	Result IdentityWriteResult
	Cause  error
}

func (err *CommittedIdentityError) Error() string {
	return fmt.Sprintf(
		"TUF repository: identity committed at %s but completion failed: root_sha256=%s: %v; inspect the committed identity before retrying or removing it",
		err.Result.Path, err.Result.RootSHA256, err.Cause,
	)
}

func (err *CommittedIdentityError) Unwrap() error { return err.Cause }

type identityWriteDependencies struct {
	random                   io.Reader
	beforeActivate           func()
	closeStagingFile         func(*os.File) error
	closeActivationDirectory func(*os.File) error
	closeParent              func(*os.Root) error
	syncParent               func(*os.Root) error
}

func defaultIdentityWriteDependencies() identityWriteDependencies {
	return identityWriteDependencies{
		random:                   rand.Reader,
		closeStagingFile:         func(file *os.File) error { return file.Close() },
		closeActivationDirectory: func(file *os.File) error { return file.Close() },
		closeParent:              func(root *os.Root) error { return root.Close() },
		syncParent:               syncRoot,
	}
}

// WriteIdentity validates and atomically creates one owner-only private
// identity. It never overwrites an existing path and performs no network I/O.
func WriteIdentity(ctx context.Context, rawPath string, raw []byte) (IdentityWriteResult, error) {
	return writeIdentityWithDependencies(ctx, rawPath, raw, defaultIdentityWriteDependencies())
}

func writeIdentityWithDependencies(ctx context.Context, rawPath string, raw []byte, dependencies identityWriteDependencies) (result IdentityWriteResult, returnErr error) {
	if ctx == nil {
		return IdentityWriteResult{}, invalidIdentityWrite(errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return IdentityWriteResult{}, err
	}
	if dependencies.random == nil || dependencies.closeStagingFile == nil ||
		dependencies.closeActivationDirectory == nil || dependencies.closeParent == nil || dependencies.syncParent == nil {
		return IdentityWriteResult{}, invalidIdentityWrite(errors.New("identity write dependencies are incomplete"))
	}
	identity, err := DecodeIdentity(raw)
	if err != nil {
		return IdentityWriteResult{}, err
	}
	rootRaw, err := identity.ActiveRoot()
	if err != nil {
		return IdentityWriteResult{}, err
	}
	root, err := decodeCurrentRoot(rootRaw)
	if err != nil {
		return IdentityWriteResult{}, err
	}
	if !cleanAbsolute(rawPath) {
		return IdentityWriteResult{}, invalidIdentityWrite(errors.New("clean absolute output path is required"))
	}
	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(rawPath))
	if err != nil {
		return IdentityWriteResult{}, fmt.Errorf("TUF repository: identity output parent: %w", err)
	}
	outputBase := filepath.Base(rawPath)
	outputPath := filepath.Join(parentPath, outputBase)
	result = IdentityWriteResult{
		Path: outputPath, RepositoryID: identity.repositoryID, RootSHA256: indexpack.Digest(rootRaw),
		RootVersion: root.Signed.Version, RootExpiresAt: identity.rootExpires.Format(time.RFC3339), RootThreshold: 2,
	}

	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return IdentityWriteResult{}, fmt.Errorf("TUF repository: anchor identity output parent: %w", err)
	}
	committed := false
	defer func() {
		if closeErr := dependencies.closeParent(parentRoot); closeErr != nil {
			if committed {
				closeErr = &CommittedIdentityError{Result: result, Cause: fmt.Errorf("close identity output parent: %w", closeErr)}
			}
			returnErr = errors.Join(returnErr, closeErr)
		}
	}()
	if err := identityParentStillAtPath(parentRoot, parentPath); err != nil {
		return IdentityWriteResult{}, err
	}
	if _, err := parentRoot.Lstat(outputBase); err == nil {
		return IdentityWriteResult{}, ErrIdentityExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return IdentityWriteResult{}, fmt.Errorf("TUF repository: inspect identity output: %w", err)
	}
	stagingName, err := createIdentityStagingName(parentRoot, dependencies.random)
	if err != nil {
		return IdentityWriteResult{}, err
	}
	stagingFile, err := parentRoot.OpenFile(stagingName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return IdentityWriteResult{}, fmt.Errorf("TUF repository: create private identity staging file: %w", err)
	}
	stagingExists := true
	defer func() {
		if stagingExists {
			if removeErr := parentRoot.Remove(stagingName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				returnErr = errors.Join(returnErr, fmt.Errorf("TUF repository: remove incomplete identity: %w", removeErr))
			}
		}
	}()
	info, statErr := stagingFile.Stat()
	if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = stagingFile.Close()
		return IdentityWriteResult{}, invalidIdentityWrite(errors.New("new identity staging file is not a private regular file"))
	}
	written, writeErr := io.Copy(stagingFile, bytes.NewReader(raw))
	if writeErr == nil && written != int64(len(raw)) {
		writeErr = io.ErrShortWrite
	}
	finishErr := errors.Join(stagingFile.Sync(), dependencies.closeStagingFile(stagingFile))
	if writeErr != nil || finishErr != nil {
		return IdentityWriteResult{}, errors.Join(writeErr, finishErr)
	}
	verification, err := readStageFile(parentRoot, stagingName, MaxIdentityBytes)
	if err != nil || !bytes.Equal(verification, raw) {
		return IdentityWriteResult{}, invalidIdentityWrite(errors.Join(errors.New("private identity changed while staging"), err))
	}
	stagedInfo, err := parentRoot.Lstat(stagingName)
	if err != nil || !stagedInfo.Mode().IsRegular() || stagedInfo.Mode().Perm() != 0o600 || !os.SameFile(info, stagedInfo) {
		return IdentityWriteResult{}, invalidIdentityWrite(errors.New("private identity staging path changed"))
	}
	if dependencies.beforeActivate != nil {
		dependencies.beforeActivate()
	}
	if err := ctx.Err(); err != nil {
		return IdentityWriteResult{}, err
	}
	if err := identityParentStillAtPath(parentRoot, parentPath); err != nil {
		return IdentityWriteResult{}, err
	}
	directory, err := parentRoot.Open(".")
	if err != nil {
		return IdentityWriteResult{}, fmt.Errorf("TUF repository: open identity activation directory: %w", err)
	}
	renameErr := renameNoReplaceAt(directory, stagingName, outputBase)
	closeErr := dependencies.closeActivationDirectory(directory)
	if renameErr != nil {
		if errors.Is(renameErr, os.ErrExist) {
			renameErr = ErrIdentityExists
		}
		return IdentityWriteResult{}, errors.Join(renameErr, closeErr)
	}
	committed = true
	stagingExists = false
	if closeErr != nil {
		return result, &CommittedIdentityError{Result: result, Cause: fmt.Errorf("close identity activation directory: %w", closeErr)}
	}
	if err := identityParentStillAtPath(parentRoot, parentPath); err != nil {
		return result, &CommittedIdentityError{Result: result, Cause: err}
	}
	if err := dependencies.syncParent(parentRoot); err != nil {
		return result, &CommittedIdentityError{Result: result, Cause: fmt.Errorf("sync identity output parent: %w", err)}
	}
	if err := identityParentStillAtPath(parentRoot, parentPath); err != nil {
		return result, &CommittedIdentityError{Result: result, Cause: err}
	}
	return result, nil
}

func createIdentityStagingName(root *os.Root, random io.Reader) (string, error) {
	for range 8 {
		randomBytes := make([]byte, 16)
		if _, err := io.ReadFull(random, randomBytes); err != nil {
			return "", fmt.Errorf("TUF repository: allocate identity staging name: %w", err)
		}
		name := ".fetchmark-tuf-identity-" + hex.EncodeToString(randomBytes)
		if _, err := root.Lstat(name); errors.Is(err, os.ErrNotExist) {
			return name, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("TUF repository: could not allocate identity staging file")
}

func identityParentStillAtPath(root *os.Root, rootPath string) error {
	anchored, err := root.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(rootPath)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return invalidIdentityWrite(errors.New("identity output parent path changed"))
	}
	return nil
}

func invalidIdentityWrite(cause error) error {
	return fmt.Errorf("%w: %v", ErrInvalidIdentityWrite, cause)
}
