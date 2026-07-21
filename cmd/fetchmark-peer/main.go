// Command fetchmark-peer manages the local cryptographic identity used by
// Fetchmark's optional private federation protocol. It performs no network
// access and never prints private key material.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/federation"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, defaultDependencies())) }

type dependencies struct {
	random        io.Reader
	createPrivate func(string, []byte, int64) error
}

func defaultDependencies() dependencies {
	return dependencies{random: rand.Reader, createPrivate: secureconfigfile.CreatePrivate}
}

type keygenRequest struct {
	identityID string
	outputPath string
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

func run(args []string, stdout, stderr io.Writer, deps dependencies) int {
	request, err := parseRequest(args, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-peer: %v\n", err)
		return 2
	}
	entry, err := keygen(request, deps)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-peer: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(entry); err != nil {
		fmt.Fprintf(stderr, "fetchmark-peer: write public identity: %v\n", err)
		return 1
	}
	return 0
}

func parseRequest(args []string, stderr io.Writer) (keygenRequest, error) {
	if len(args) == 0 {
		return keygenRequest{}, errors.New("expected exactly one subcommand: keygen")
	}
	if args[0] != "keygen" {
		return keygenRequest{}, fmt.Errorf("unknown subcommand %q", args[0])
	}
	flags := flag.NewFlagSet("fetchmark-peer keygen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	identityID := &singleStringFlag{name: "identity-id"}
	output := &singleStringFlag{name: "out"}
	flags.Var(identityID, "identity-id", "stable local federation identity ID")
	flags.Var(output, "out", "clean absolute path for the new private identity file")
	if err := flags.Parse(args[1:]); err != nil {
		return keygenRequest{}, err
	}
	if flags.NArg() != 0 {
		return keygenRequest{}, errors.New("unexpected positional arguments")
	}
	if !identityID.set || strings.TrimSpace(identityID.value) == "" {
		return keygenRequest{}, errors.New("-identity-id is required")
	}
	if strings.TrimSpace(identityID.value) != identityID.value {
		return keygenRequest{}, errors.New("-identity-id must not have leading or trailing whitespace")
	}
	if !output.set || !cleanAbsolute(output.value) {
		return keygenRequest{}, errors.New("-out must be a clean absolute path")
	}
	return keygenRequest{identityID: identityID.value, outputPath: output.value}, nil
}

func cleanAbsolute(path string) bool {
	return path != "" && strings.TrimSpace(path) == path && filepath.IsAbs(path) && filepath.Clean(path) == path
}

type identityDocument struct {
	Version           int    `json:"version"`
	IdentityID        string `json:"identity_id"`
	KeyID             string `json:"key_id"`
	Ed25519PrivateKey string `json:"ed25519_private_key"`
}

type publicIdentity struct {
	ID               string `json:"id"`
	KeyID            string `json:"key_id"`
	Ed25519PublicKey string `json:"ed25519_public_key"`
}

func keygen(request keygenRequest, deps dependencies) (publicIdentity, error) {
	if deps.random == nil || deps.createPrivate == nil {
		return publicIdentity{}, errors.New("key generation dependencies are unavailable")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(deps.random)
	if err != nil {
		return publicIdentity{}, errors.New("generate Ed25519 identity")
	}
	keyID := federation.KeyID(publicKey)
	document := identityDocument{
		Version: federation.IdentityVersion, IdentityID: request.identityID, KeyID: keyID,
		Ed25519PrivateKey: base64.StdEncoding.EncodeToString(privateKey),
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return publicIdentity{}, errors.New("encode private identity")
	}
	if _, err := federation.DecodeIdentity(raw); err != nil {
		return publicIdentity{}, fmt.Errorf("validate identity metadata: %w", err)
	}
	if err := deps.createPrivate(request.outputPath, raw, federation.MaxIdentityBytes); err != nil {
		return publicIdentity{}, fmt.Errorf("create private identity without overwrite: %w", err)
	}
	return publicIdentity{
		ID: request.identityID, KeyID: keyID,
		Ed25519PublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}, nil
}
