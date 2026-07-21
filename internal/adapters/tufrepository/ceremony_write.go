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

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

var (
	ErrInvalidCeremonyArtifactWrite = errors.New("TUF repository: invalid ceremony artifact write")
	ErrCeremonyArtifactExists       = errors.New("TUF repository: ceremony artifact output already exists")
)

type CeremonyArtifactWriteResult struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  uint64 `json:"bytes"`
}

type CommittedCeremonyArtifactError struct {
	Result CeremonyArtifactWriteResult
	Cause  error
}

func (err *CommittedCeremonyArtifactError) Error() string {
	return fmt.Sprintf(
		"TUF repository: ceremony artifact committed at %s but completion failed: sha256=%s bytes=%d: %v; inspect and hash the committed artifact before retrying or removing it",
		err.Result.Path, err.Result.SHA256, err.Result.Bytes, err.Cause,
	)
}

func (err *CommittedCeremonyArtifactError) Unwrap() error { return err.Cause }

type ceremonyArtifactWriteDependencies struct {
	random                   io.Reader
	beforeActivate           func()
	closeStagingFile         func(*os.File) error
	closeActivationDirectory func(*os.File) error
	closeParent              func(*os.Root) error
	syncParent               func(*os.Root) error
}

func defaultCeremonyArtifactWriteDependencies() ceremonyArtifactWriteDependencies {
	return ceremonyArtifactWriteDependencies{
		random: rand.Reader, closeStagingFile: func(file *os.File) error { return file.Close() },
		closeActivationDirectory: func(file *os.File) error { return file.Close() },
		closeParent:              func(root *os.Root) error { return root.Close() }, syncParent: syncRoot,
	}
}

// WriteCeremonyArtifact atomically creates one owner-only, no-overwrite root
// ceremony artifact. The final pathname is never visible with partial bytes.
func WriteCeremonyArtifact(ctx context.Context, rawPath string, raw []byte, maxBytes int64) (CeremonyArtifactWriteResult, error) {
	return writeCeremonyArtifactWithDependencies(ctx, rawPath, raw, maxBytes, defaultCeremonyArtifactWriteDependencies())
}

func writeCeremonyArtifactWithDependencies(
	ctx context.Context,
	rawPath string,
	raw []byte,
	maxBytes int64,
	dependencies ceremonyArtifactWriteDependencies,
) (result CeremonyArtifactWriteResult, returnErr error) {
	if ctx == nil {
		return result, invalidCeremonyArtifactWrite(errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(raw) == 0 || maxBytes < 1 || int64(len(raw)) > maxBytes || dependencies.random == nil ||
		dependencies.closeStagingFile == nil || dependencies.closeActivationDirectory == nil ||
		dependencies.closeParent == nil || dependencies.syncParent == nil {
		return result, invalidCeremonyArtifactWrite(errors.New("bounded artifact bytes and complete write dependencies are required"))
	}
	if !cleanAbsolute(rawPath) {
		return result, invalidCeremonyArtifactWrite(errors.New("clean absolute output path is required"))
	}
	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(rawPath))
	if err != nil {
		return result, fmt.Errorf("TUF repository: ceremony artifact output parent: %w", err)
	}
	outputBase := filepath.Base(rawPath)
	result = CeremonyArtifactWriteResult{
		Path: filepath.Join(parentPath, outputBase), SHA256: indexpack.Digest(raw), Bytes: uint64(len(raw)),
	}
	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return CeremonyArtifactWriteResult{}, fmt.Errorf("TUF repository: anchor ceremony artifact parent: %w", err)
	}
	committed := false
	defer func() {
		if closeErr := dependencies.closeParent(parentRoot); closeErr != nil {
			if committed {
				closeErr = &CommittedCeremonyArtifactError{Result: result, Cause: fmt.Errorf("close output parent: %w", closeErr)}
			}
			returnErr = errors.Join(returnErr, closeErr)
		}
	}()
	if err := ceremonyParentStillAtPath(parentRoot, parentPath); err != nil {
		return CeremonyArtifactWriteResult{}, err
	}
	if _, err := parentRoot.Lstat(outputBase); err == nil {
		return CeremonyArtifactWriteResult{}, ErrCeremonyArtifactExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return CeremonyArtifactWriteResult{}, fmt.Errorf("TUF repository: inspect ceremony artifact output: %w", err)
	}
	stagingName, err := createCeremonyArtifactStagingName(parentRoot, dependencies.random)
	if err != nil {
		return CeremonyArtifactWriteResult{}, err
	}
	stagingFile, err := parentRoot.OpenFile(stagingName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return CeremonyArtifactWriteResult{}, fmt.Errorf("TUF repository: create ceremony artifact staging file: %w", err)
	}
	stagingExists := true
	defer func() {
		if stagingExists {
			if removeErr := parentRoot.Remove(stagingName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				returnErr = errors.Join(returnErr, fmt.Errorf("TUF repository: remove incomplete ceremony artifact: %w", removeErr))
			}
		}
	}()
	info, statErr := stagingFile.Stat()
	if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = stagingFile.Close()
		return CeremonyArtifactWriteResult{}, invalidCeremonyArtifactWrite(errors.New("new staging file is not a private regular file"))
	}
	written, writeErr := io.Copy(stagingFile, bytes.NewReader(raw))
	if writeErr == nil && written != int64(len(raw)) {
		writeErr = io.ErrShortWrite
	}
	finishErr := errors.Join(stagingFile.Sync(), dependencies.closeStagingFile(stagingFile))
	if writeErr != nil || finishErr != nil {
		return CeremonyArtifactWriteResult{}, errors.Join(writeErr, finishErr)
	}
	verification, err := readStageFile(parentRoot, stagingName, maxBytes)
	if err != nil || !bytes.Equal(verification, raw) {
		return CeremonyArtifactWriteResult{}, invalidCeremonyArtifactWrite(errors.Join(errors.New("staged ceremony artifact changed"), err))
	}
	stagedInfo, err := parentRoot.Lstat(stagingName)
	if err != nil || !stagedInfo.Mode().IsRegular() || stagedInfo.Mode().Perm() != 0o600 || !os.SameFile(info, stagedInfo) {
		return CeremonyArtifactWriteResult{}, invalidCeremonyArtifactWrite(errors.New("ceremony artifact staging path changed"))
	}
	if dependencies.beforeActivate != nil {
		dependencies.beforeActivate()
	}
	if err := ctx.Err(); err != nil {
		return CeremonyArtifactWriteResult{}, err
	}
	if err := ceremonyParentStillAtPath(parentRoot, parentPath); err != nil {
		return CeremonyArtifactWriteResult{}, err
	}
	directory, err := parentRoot.Open(".")
	if err != nil {
		return CeremonyArtifactWriteResult{}, fmt.Errorf("TUF repository: open ceremony artifact activation directory: %w", err)
	}
	renameErr := renameNoReplaceAt(directory, stagingName, outputBase)
	closeErr := dependencies.closeActivationDirectory(directory)
	if renameErr != nil {
		if errors.Is(renameErr, os.ErrExist) {
			renameErr = ErrCeremonyArtifactExists
		}
		return CeremonyArtifactWriteResult{}, errors.Join(renameErr, closeErr)
	}
	committed = true
	stagingExists = false
	if closeErr != nil {
		return result, &CommittedCeremonyArtifactError{Result: result, Cause: fmt.Errorf("close activation directory: %w", closeErr)}
	}
	if err := ceremonyParentStillAtPath(parentRoot, parentPath); err != nil {
		return result, &CommittedCeremonyArtifactError{Result: result, Cause: err}
	}
	if err := dependencies.syncParent(parentRoot); err != nil {
		return result, &CommittedCeremonyArtifactError{Result: result, Cause: fmt.Errorf("sync output parent: %w", err)}
	}
	if err := ceremonyParentStillAtPath(parentRoot, parentPath); err != nil {
		return result, &CommittedCeremonyArtifactError{Result: result, Cause: err}
	}
	return result, nil
}

func createCeremonyArtifactStagingName(root *os.Root, random io.Reader) (string, error) {
	for range 8 {
		randomBytes := make([]byte, 16)
		if _, err := io.ReadFull(random, randomBytes); err != nil {
			return "", fmt.Errorf("TUF repository: allocate ceremony artifact staging name: %w", err)
		}
		name := ".fetchmark-tuf-ceremony-" + hex.EncodeToString(randomBytes)
		if _, err := root.Lstat(name); errors.Is(err, os.ErrNotExist) {
			return name, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("TUF repository: could not allocate ceremony artifact staging file")
}

func ceremonyParentStillAtPath(root *os.Root, rootPath string) error {
	anchored, err := root.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(rootPath)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return invalidCeremonyArtifactWrite(errors.New("output parent path changed"))
	}
	return nil
}

func invalidCeremonyArtifactWrite(cause error) error {
	return fmt.Errorf("%w: %v", ErrInvalidCeremonyArtifactWrite, cause)
}
