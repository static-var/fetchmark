package openpackregistryfile

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
	"strings"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/openpackregistry"
)

const candidateStagingPrefix = ".fetchmark-registry-candidate-"

var ErrCandidateExists = errors.New("open pack registry file: candidate already exists")

// CommittedCandidateError reports that the no-replace rename succeeded, so the
// candidate path must be inspected before an operator retries the command.
type CommittedCandidateError struct {
	Path  string
	Cause error
}

func (e *CommittedCandidateError) Error() string {
	return fmt.Sprintf("open pack registry file: candidate committed at %s but completion failed: %v", e.Path, e.Cause)
}

func (e *CommittedCandidateError) Unwrap() error { return e.Cause }

type candidateWriteDependencies struct {
	beforeActivate           func()
	closeActivationDirectory func(*os.File) error
	closeParent              func(*os.Root) error
	syncParent               func(*os.Root) error
}

// WriteNew durably publishes a validated registry at a new owner-only path.
// It never replaces an existing file or changes the active operator registry.
func WriteNew(ctx context.Context, rawPath string, registry openpackregistry.Registry) (writtenPath string, returnErr error) {
	return writeNew(ctx, rawPath, registry, defaultCandidateWriteDependencies())
}

func writeNew(ctx context.Context, rawPath string, registry openpackregistry.Registry, dependencies candidateWriteDependencies) (writtenPath string, returnErr error) {
	dependencies = dependencies.withDefaults()
	if ctx == nil {
		return "", errors.New("open pack registry file: context is required")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	raw, err := registry.Encode()
	if err != nil {
		return "", err
	}
	if rawPath == "" || strings.TrimSpace(rawPath) != rawPath || !filepath.IsAbs(rawPath) || filepath.Clean(rawPath) != rawPath {
		return "", fmt.Errorf("%w: candidate path must be clean and absolute", ErrUnsafePath)
	}
	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(rawPath))
	if err != nil {
		return "", fmt.Errorf("open pack registry file: candidate parent: %w", err)
	}
	outputName := filepath.Base(rawPath)
	if outputName == "." || outputName == string(filepath.Separator) {
		return "", fmt.Errorf("%w: candidate path must name a file", ErrUnsafePath)
	}
	outputPath := filepath.Join(parentPath, outputName)
	parent, err := os.OpenRoot(parentPath)
	if err != nil {
		return "", fmt.Errorf("open pack registry file: anchor candidate parent: %w", err)
	}
	committed := false
	defer func() {
		if closeErr := dependencies.closeParent(parent); closeErr != nil {
			if committed {
				closeErr = committedCandidateError(outputPath, fmt.Errorf("closing parent: %w", closeErr))
			}
			returnErr = errors.Join(returnErr, closeErr)
		}
	}()
	if err := candidateParentStillAtPath(parent, parentPath); err != nil {
		return "", err
	}
	if _, err := parent.Lstat(outputName); err == nil {
		return "", ErrCandidateExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("open pack registry file: inspect candidate output: %w", err)
	}

	stagingName, err := createCandidateStaging(parent)
	if err != nil {
		return "", err
	}
	stagingExists := true
	defer func() {
		if stagingExists {
			if err := parent.Remove(stagingName); err != nil && !errors.Is(err, os.ErrNotExist) {
				returnErr = errors.Join(returnErr, fmt.Errorf("open pack registry file: remove candidate staging file: %w", err))
			}
		}
	}()
	file, err := parent.OpenFile(stagingName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("open pack registry file: create candidate staging file: %w", err)
	}
	written, writeErr := io.Copy(file, bytes.NewReader(raw))
	fileErr := errors.Join(file.Sync(), file.Close())
	if writeErr != nil || written != int64(len(raw)) || fileErr != nil {
		if writeErr == nil && written != int64(len(raw)) {
			writeErr = io.ErrShortWrite
		}
		return "", errors.Join(writeErr, fileErr)
	}
	if err := candidateParentStillAtPath(parent, parentPath); err != nil {
		return "", err
	}
	if dependencies.beforeActivate != nil {
		dependencies.beforeActivate()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	directory, err := parent.Open(".")
	if err != nil {
		return "", fmt.Errorf("open pack registry file: open candidate parent for activation: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", errors.Join(err, dependencies.closeActivationDirectory(directory))
	}
	renameErr := renameCandidateNoReplaceAt(directory, stagingName, outputName)
	closeErr := dependencies.closeActivationDirectory(directory)
	if renameErr != nil {
		if errors.Is(renameErr, os.ErrExist) {
			renameErr = ErrCandidateExists
		}
		return "", errors.Join(renameErr, closeErr)
	}
	committed = true
	stagingExists = false
	if closeErr != nil {
		return outputPath, committedCandidateError(outputPath, fmt.Errorf("closing activation directory: %w", closeErr))
	}
	if err := candidateParentStillAtPath(parent, parentPath); err != nil {
		return outputPath, committedCandidateError(outputPath, fmt.Errorf("parent identity changed: %w", err))
	}
	if err := dependencies.syncParent(parent); err != nil {
		return outputPath, committedCandidateError(outputPath, fmt.Errorf("parent durability is uncertain: %w", err))
	}
	if err := candidateParentStillAtPath(parent, parentPath); err != nil {
		return outputPath, committedCandidateError(outputPath, fmt.Errorf("parent identity changed after sync: %w", err))
	}
	if _, err := Load(outputPath); err != nil {
		return outputPath, committedCandidateError(outputPath, fmt.Errorf("verification failed: %w", err))
	}
	return outputPath, nil
}

func committedCandidateError(path string, cause error) error {
	return &CommittedCandidateError{Path: path, Cause: cause}
}

func defaultCandidateWriteDependencies() candidateWriteDependencies {
	return candidateWriteDependencies{
		closeActivationDirectory: func(file *os.File) error { return file.Close() },
		closeParent:              func(parent *os.Root) error { return parent.Close() },
		syncParent:               syncCandidateParent,
	}
}

func (dependencies candidateWriteDependencies) withDefaults() candidateWriteDependencies {
	defaults := defaultCandidateWriteDependencies()
	if dependencies.closeActivationDirectory == nil {
		dependencies.closeActivationDirectory = defaults.closeActivationDirectory
	}
	if dependencies.closeParent == nil {
		dependencies.closeParent = defaults.closeParent
	}
	if dependencies.syncParent == nil {
		dependencies.syncParent = defaults.syncParent
	}
	return dependencies
}

func createCandidateStaging(parent *os.Root) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", fmt.Errorf("open pack registry file: generate staging name: %w", err)
		}
		name := candidateStagingPrefix + hex.EncodeToString(random)
		if _, err := parent.Lstat(name); errors.Is(err, os.ErrNotExist) {
			return name, nil
		} else if err != nil {
			return "", fmt.Errorf("open pack registry file: inspect staging path: %w", err)
		}
	}
	return "", errors.New("open pack registry file: could not allocate a unique staging name")
}

func candidateParentStillAtPath(parent *os.Root, parentPath string) error {
	anchored, err := parent.Stat(".")
	if err != nil {
		return fmt.Errorf("open pack registry file: inspect anchored parent: %w", err)
	}
	current, err := os.Lstat(parentPath)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return fmt.Errorf("%w: candidate parent path changed", ErrUnsafePath)
	}
	return nil
}

func syncCandidateParent(parent *os.Root) error {
	directory, err := parent.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
