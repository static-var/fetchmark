package federation

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	TrustRegistryVersion  = 1
	MaxTrustRegistryBytes = 1 << 20
	MaxIdentities         = 16
	MaxPeerBindings       = 16
)

var ErrInvalidTrustRegistry = errors.New("federation: invalid trust registry")

// TrustRegistry is immutable after loading. Lookup methods return defensive
// copies, including copies of Ed25519 key bytes.
type TrustRegistry struct {
	peers      map[string]TrustedPeer
	byIdentity map[string]string
}

// TrustedPeer is one operator-approved peer identity and exact HTTPS origin.
// AllowPrivateNetwork is only an operator declaration; a future transport
// adapter must still resolve and enforce SSRF policy on every connection.
type TrustedPeer struct {
	PeerID              string
	IdentityID          string
	KeyID               string
	BaseOrigin          string
	AllowPrivateNetwork bool
	AllowInbound        bool
	publicKey           ed25519.PublicKey
}

func (peer TrustedPeer) PublicKey() ed25519.PublicKey { return cloneBytes(peer.publicKey) }

type trustRegistryJSON struct {
	Version    int            `json:"version"`
	Identities []identityJSON `json:"identities"`
	Peers      []peerJSON     `json:"peers"`
}

type identityJSON struct {
	ID               string `json:"id"`
	KeyID            string `json:"key_id"`
	Ed25519PublicKey string `json:"ed25519_public_key"`
}

type peerJSON struct {
	ID                  string `json:"id"`
	IdentityID          string `json:"identity_id"`
	BaseOrigin          string `json:"base_origin"`
	AllowPrivateNetwork *bool  `json:"allow_private_network"`
	AllowInbound        *bool  `json:"allow_inbound"`
}

type resolvedIdentity struct {
	keyID     string
	publicKey ed25519.PublicKey
}

// LoadTrustRegistry parses operator-owned trust data without consulting the
// filesystem, network, environment, or application configuration.
func LoadTrustRegistry(reader io.Reader) (TrustRegistry, error) {
	if reader == nil {
		return TrustRegistry{}, invalidRegistry(errors.New("reader is required"))
	}
	raw, err := io.ReadAll(io.LimitReader(reader, MaxTrustRegistryBytes+1))
	if err != nil {
		return TrustRegistry{}, invalidRegistry(fmt.Errorf("read: %w", err))
	}
	if len(raw) == 0 || len(raw) > MaxTrustRegistryBytes {
		return TrustRegistry{}, invalidRegistry(fmt.Errorf("size must be 1..%d bytes", MaxTrustRegistryBytes))
	}
	if !utf8.Valid(raw) {
		return TrustRegistry{}, invalidRegistry(errors.New("JSON must be valid UTF-8"))
	}
	if err := validateJSONShape(raw); err != nil {
		return TrustRegistry{}, invalidRegistry(err)
	}
	var document trustRegistryJSON
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return TrustRegistry{}, invalidRegistry(err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return TrustRegistry{}, invalidRegistry(errors.New("must contain exactly one JSON value"))
	}
	return buildTrustRegistry(document)
}

func buildTrustRegistry(document trustRegistryJSON) (TrustRegistry, error) {
	if document.Version != TrustRegistryVersion {
		return TrustRegistry{}, invalidRegistry(fmt.Errorf("version must be %d", TrustRegistryVersion))
	}
	if len(document.Identities) < 1 || len(document.Identities) > MaxIdentities {
		return TrustRegistry{}, invalidRegistry(fmt.Errorf("identities must contain 1..%d entries", MaxIdentities))
	}
	if len(document.Peers) < 1 || len(document.Peers) > MaxPeerBindings {
		return TrustRegistry{}, invalidRegistry(fmt.Errorf("peers must contain 1..%d entries", MaxPeerBindings))
	}
	identities := make(map[string]resolvedIdentity, len(document.Identities))
	keyIDs := make(map[string]struct{}, len(document.Identities))
	for index, identity := range document.Identities {
		if err := validateID("id", identity.ID); err != nil {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("identities[%d]: %w", index, err))
		}
		if _, duplicate := identities[identity.ID]; duplicate {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("identities[%d]: duplicate id", index))
		}
		if err := validateDigest("key_id", identity.KeyID); err != nil {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("identities[%d]: %w", index, err))
		}
		if _, duplicate := keyIDs[identity.KeyID]; duplicate {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("identities[%d]: duplicate key_id", index))
		}
		publicKey, err := decodePublicKey(identity.Ed25519PublicKey)
		if err != nil {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("identities[%d]: %w", index, err))
		}
		if KeyID(publicKey) != identity.KeyID {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("identities[%d]: key_id does not match public key", index))
		}
		identities[identity.ID] = resolvedIdentity{identity.KeyID, publicKey}
		keyIDs[identity.KeyID] = struct{}{}
	}

	registry := TrustRegistry{peers: make(map[string]TrustedPeer, len(document.Peers)), byIdentity: make(map[string]string, len(document.Peers))}
	for index, peer := range document.Peers {
		if err := validateID("id", peer.ID); err != nil {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("peers[%d]: %w", index, err))
		}
		if _, duplicate := registry.peers[peer.ID]; duplicate {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("peers[%d]: duplicate id", index))
		}
		if err := validateID("identity_id", peer.IdentityID); err != nil {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("peers[%d]: %w", index, err))
		}
		identity, exists := identities[peer.IdentityID]
		if !exists {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("peers[%d]: identity_id is not defined", index))
		}
		if _, duplicate := registry.byIdentity[peer.IdentityID]; duplicate {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("peers[%d]: duplicate identity binding", index))
		}
		if peer.AllowPrivateNetwork == nil || peer.AllowInbound == nil {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("peers[%d]: allow_private_network and allow_inbound are required", index))
		}
		if err := validateBaseOrigin(peer.BaseOrigin, *peer.AllowPrivateNetwork); err != nil {
			return TrustRegistry{}, invalidRegistry(fmt.Errorf("peers[%d]: %w", index, err))
		}
		registry.peers[peer.ID] = TrustedPeer{
			PeerID: peer.ID, IdentityID: peer.IdentityID, KeyID: identity.keyID, BaseOrigin: peer.BaseOrigin,
			AllowPrivateNetwork: *peer.AllowPrivateNetwork, AllowInbound: *peer.AllowInbound,
			publicKey: cloneBytes(identity.publicKey),
		}
		registry.byIdentity[peer.IdentityID] = peer.ID
	}
	if len(registry.byIdentity) != len(identities) {
		return TrustRegistry{}, invalidRegistry(errors.New("every identity must be referenced by exactly one peer"))
	}
	return registry, nil
}

func (registry TrustRegistry) Lookup(peerID string) (TrustedPeer, bool) {
	peer, exists := registry.peers[peerID]
	peer.publicKey = cloneBytes(peer.publicKey)
	return peer, exists
}

// LookupInbound returns a peer only when its identity is both trusted and
// explicitly allowed to initiate inbound requests.
func (registry TrustRegistry) LookupInbound(identityID string) (TrustedPeer, bool) {
	peerID, exists := registry.byIdentity[identityID]
	if !exists {
		return TrustedPeer{}, false
	}
	peer, exists := registry.Lookup(peerID)
	if !exists || !peer.AllowInbound {
		return TrustedPeer{}, false
	}
	return peer, true
}

func decodePublicKey(encoded string) (ed25519.PublicKey, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New("ed25519_public_key must be canonical padded base64 for 32 bytes")
	}
	return ed25519.PublicKey(cloneBytes(decoded)), nil
}

func validateBaseOrigin(raw string, allowPrivate bool) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Opaque != "" || parsed.User != nil || parsed.Host == "" ||
		parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery {
		return errors.New("base_origin must be an exact HTTPS origin without path, query, fragment, or user information")
	}
	hostname := parsed.Hostname()
	if hostname == "" || hostname != strings.ToLower(hostname) || !isASCII(hostname) || strings.HasSuffix(hostname, ".") || strings.Contains(hostname, "%") {
		return errors.New("base_origin hostname must be lowercase ASCII without a trailing dot or zone")
	}
	port := parsed.Port()
	if port != "" {
		numeric, err := strconv.Atoi(port)
		if err != nil || numeric < 1 || numeric > 65535 {
			return errors.New("base_origin port must be 1..65535")
		}
	}
	canonicalHost := hostname
	if ip := net.ParseIP(hostname); ip != nil {
		canonicalHost = ip.String()
		if ip.To4() == nil {
			canonicalHost = "[" + canonicalHost + "]"
		}
		if isPrivateAddress(ip) && !allowPrivate {
			return errors.New("private or special-use IP origin requires allow_private_network")
		}
	} else if hostname == "localhost" || strings.HasSuffix(hostname, ".localhost") {
		if !allowPrivate {
			return errors.New("localhost origin requires allow_private_network")
		}
	}
	if port != "" {
		canonicalHost = net.JoinHostPort(hostname, port)
	}
	canonical := "https://" + canonicalHost
	if canonical != raw {
		return errors.New("base_origin must use canonical exact-origin encoding")
	}
	return nil
}

func isPrivateAddress(ip net.IP) bool {
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}

func isASCII(value string) bool {
	for _, character := range value {
		if character > 127 {
			return false
		}
	}
	return true
}

func invalidRegistry(err error) error { return fmt.Errorf("%w: %v", ErrInvalidTrustRegistry, err) }
