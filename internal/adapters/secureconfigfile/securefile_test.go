package secureconfigfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadPublicConfig(t *testing.T) {
	for _, mode := range []os.FileMode{0o600, 0o640, 0o644, 0o400, 0o444} {
		t.Run(mode.String(), func(t *testing.T) {
			path := writeFixture(t, t.TempDir(), mode, "public configuration")
			raw, err := Read(path, Options{MaxBytes: 1024, Mode: PublicConfig})
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if got := string(raw); got != "public configuration" {
				t.Fatalf("Read = %q", got)
			}
		})
	}
}

func TestReadPrivateIdentityRequiresExactly0600(t *testing.T) {
	path := writeFixture(t, t.TempDir(), 0o600, "private identity")
	raw, err := Read(path, Options{MaxBytes: 1024, Mode: PrivateIdentity})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(raw) != "private identity" {
		t.Fatalf("Read = %q", raw)
	}

	for _, mode := range []os.FileMode{0o400, 0o640, 0o644, 0o660} {
		t.Run(mode.String(), func(t *testing.T) {
			path := writeFixture(t, t.TempDir(), mode, "private identity")
			if _, err := Read(path, Options{MaxBytes: 1024, Mode: PrivateIdentity}); !errors.Is(err, ErrUnsafePath) {
				t.Fatalf("Read = %v, want ErrUnsafePath", err)
			}
		})
	}
}

func TestReadRejectsUnsafePathsAndModes(t *testing.T) {
	t.Run("relative path", func(t *testing.T) {
		if _, err := Read("config.json", Options{MaxBytes: 1024, Mode: PublicConfig}); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Read = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("unclean path", func(t *testing.T) {
		path := t.TempDir() + string(filepath.Separator) + "nested" + string(filepath.Separator) + ".." + string(filepath.Separator) + "config.json"
		if _, err := Read(path, Options{MaxBytes: 1024, Mode: PublicConfig}); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Read = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("writable file", func(t *testing.T) {
		path := writeFixture(t, t.TempDir(), 0o666, "config")
		if _, err := Read(path, Options{MaxBytes: 1024, Mode: PublicConfig}); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Read = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("final symlink", func(t *testing.T) {
		root := t.TempDir()
		target := writeFixture(t, root, 0o600, "config")
		link := filepath.Join(root, "link.json")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(link, Options{MaxBytes: 1024, Mode: PublicConfig}); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Read = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("symlink directory component", func(t *testing.T) {
		root := t.TempDir()
		realDirectory := filepath.Join(root, "real")
		if err := os.Mkdir(realDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		_ = writeFixture(t, realDirectory, 0o600, "config")
		link := filepath.Join(root, "linked")
		if err := os.Symlink(realDirectory, link); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(filepath.Join(link, "fixture"), Options{MaxBytes: 1024, Mode: PublicConfig}); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Read = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("non-sticky writable parent", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "operator")
		if err := os.Mkdir(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		path := writeFixture(t, parent, 0o600, "config")
		if err := os.Chmod(parent, 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(path, Options{MaxBytes: 1024, Mode: PublicConfig}); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Read = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("arbitrary sticky writable parent", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "operator")
		if err := os.Mkdir(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		path := writeFixture(t, parent, 0o600, "config")
		if err := os.Chmod(parent, os.ModeSticky|0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(path, Options{MaxBytes: 1024, Mode: PublicConfig}); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Read = %v, want ErrUnsafePath", err)
		}
	})
}

func TestReadBoundsAndOptions(t *testing.T) {
	path := writeFixture(t, t.TempDir(), 0o600, "12345")
	if _, err := Read(path, Options{MaxBytes: 4, Mode: PublicConfig}); err == nil {
		t.Fatal("Read oversized file succeeded")
	}
	if _, err := Read(path, Options{MaxBytes: 0, Mode: PublicConfig}); err == nil {
		t.Fatal("Read with zero maximum succeeded")
	}
	if _, err := Read(path, Options{MaxBytes: 1024}); err == nil {
		t.Fatal("Read with zero mode policy succeeded")
	}

	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(empty, Options{MaxBytes: 1024, Mode: PublicConfig}); err == nil {
		t.Fatal("Read empty file succeeded")
	}
}

func TestReadErrorsDoNotExposeContents(t *testing.T) {
	const secret = "do-not-leak-this-private-key-material"
	path := writeFixture(t, t.TempDir(), 0o644, secret)
	_, err := Read(path, Options{MaxBytes: 1024, Mode: PrivateIdentity})
	if err == nil {
		t.Fatal("Read succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error exposed file contents: %v", err)
	}
}

func TestCreatePrivateWritesOwnerOnlyAndNeverOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	contents := []byte(`{"private":"key material"}`)
	if err := CreatePrivate(path, contents, 1024); err != nil {
		t.Fatalf("CreatePrivate: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("created mode = %v", info.Mode())
	}
	read, err := Read(path, Options{MaxBytes: 1024, Mode: PrivateIdentity})
	if err != nil || string(read) != string(contents) {
		t.Fatalf("Read created file = %q, %v", read, err)
	}
	if err := CreatePrivate(path, []byte("replacement"), 1024); err == nil {
		t.Fatal("CreatePrivate overwrote an existing file")
	}
	read, err = os.ReadFile(path)
	if err != nil || string(read) != string(contents) {
		t.Fatalf("existing contents changed = %q, %v", read, err)
	}
}

func TestCreatePrivateRejectsUnsafePathsAndBoundsWithoutLeavingFile(t *testing.T) {
	root := t.TempDir()
	unsafeParent := filepath.Join(root, "unsafe")
	if err := os.Mkdir(unsafeParent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafeParent, 0o777); err != nil {
		t.Fatal(err)
	}
	unsafePath := filepath.Join(unsafeParent, "identity.json")
	if err := CreatePrivate(unsafePath, []byte("secret"), 1024); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("CreatePrivate unsafe parent = %v, want ErrUnsafePath", err)
	}
	if _, err := os.Lstat(unsafePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe create left a file: %v", err)
	}

	for name, test := range map[string]struct {
		path    string
		raw     []byte
		maximum int64
	}{
		"relative": {path: "identity.json", raw: []byte("secret"), maximum: 1024},
		"empty":    {path: filepath.Join(root, "empty"), maximum: 1024},
		"oversize": {path: filepath.Join(root, "large"), raw: []byte("large"), maximum: 4},
		"zero max": {path: filepath.Join(root, "zero"), raw: []byte("secret")},
	} {
		t.Run(name, func(t *testing.T) {
			if err := CreatePrivate(test.path, test.raw, test.maximum); err == nil {
				t.Fatal("CreatePrivate succeeded")
			}
		})
	}
}

func writeFixture(t *testing.T, root string, mode os.FileMode, contents string) string {
	t.Helper()
	path := filepath.Join(root, "fixture")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
