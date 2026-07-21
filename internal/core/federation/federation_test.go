package federation

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

var testTime = time.Date(2026, 7, 18, 12, 34, 56, 0, time.UTC)

func testKey(offset byte) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = byte(index) + offset
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	return privateKey.Public().(ed25519.PublicKey), privateKey
}

func testNonce(offset byte) []byte {
	nonce := make([]byte, NonceBytes)
	for index := range nonce {
		nonce[index] = byte(index) + offset
	}
	return nonce
}

func TestProtocolConstants(t *testing.T) {
	if SearchPath != "/federation/v1/index/search" || SearchMethod != "POST" {
		t.Fatalf("unexpected route %s %s", SearchMethod, SearchPath)
	}
	if MaxRequestBytes != 32<<10 || MaxQueryBytes != 1024 || MaxResults != 20 || NonceBytes != 16 || DefaultClockSkew != 60*time.Second {
		t.Fatalf("unexpected v1 limits")
	}
}

func TestSignedRequestAndResponseGoldenRoundTrip(t *testing.T) {
	clientPublic, clientPrivate := testKey(1)
	serverPublic, serverPrivate := testKey(33)
	requestRaw, err := SignRequest(SearchRequest{Query: "open web discovery", Limit: 12}, "node.alpha", "node.beta", testTime, testNonce(2), clientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if got := digest(requestRaw); got != "0250ffb28a824b5f3940a1dee5722e7b062ce692871ad56df4031ff91ac8e2ed" {
		t.Fatalf("request golden digest changed: %s\n%s", got, requestRaw)
	}
	verifiedRequest, err := VerifyRequest(requestRaw, clientPublic, Verification{Sender: "node.alpha", Audience: "node.beta", Now: testTime.Add(59 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if verifiedRequest.Search.Query != "open web discovery" || !bytes.Equal(verifiedRequest.NonceBytes(), testNonce(2)) {
		t.Fatalf("unexpected verified request: %+v", verifiedRequest)
	}

	responseRaw, err := SignResponse(SearchResponse{Results: []SearchResult{{URL: "https://example.com/a?a=1&b=2"}, {URL: "http://wiby.me/"}}}, requestRaw, testTime.Add(time.Second), testNonce(90), serverPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if got := digest(responseRaw); got != "0f2cb52c7dea53c940cc14c92caac1bb5c3b6f8c8c0ebb3215c2fc8682938361" {
		t.Fatalf("response golden digest changed: %s\n%s", got, responseRaw)
	}
	verifiedResponse, err := VerifyResponse(responseRaw, requestRaw, serverPublic, Verification{Sender: "node.beta", Audience: "node.alpha", Now: testTime.Add(60 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if len(verifiedResponse.Search.Results) != 2 || verifiedResponse.Search.Results[0].URL != "https://example.com/a?a=1&b=2" {
		t.Fatalf("unexpected verified response: %+v", verifiedResponse)
	}
}

func TestRequestRejectsTamperingAndNonCanonicalValues(t *testing.T) {
	publicKey, privateKey := testKey(1)
	raw, err := SignRequest(SearchRequest{Query: "query", Limit: 5}, "alpha", "beta", testTime, testNonce(1), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	var envelope SignedRequest
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}

	mutations := map[string]func(*SignedRequest){
		"sender":      func(value *SignedRequest) { value.Sender = "mallory" },
		"audience":    func(value *SignedRequest) { value.Audience = "other" },
		"method":      func(value *SignedRequest) { value.Method = "GET" },
		"path":        func(value *SignedRequest) { value.Path = "/other" },
		"timestamp":   func(value *SignedRequest) { value.Timestamp = "2026-07-18T12:34:55Z" },
		"nonce":       func(value *SignedRequest) { value.Nonce = base64.RawURLEncoding.EncodeToString(testNonce(2)) },
		"body digest": func(value *SignedRequest) { value.BodySHA256 = strings.Repeat("0", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := envelope
			changed.Body = cloneBytes(envelope.Body)
			mutate(&changed)
			changedRaw, _ := json.Marshal(changed)
			if _, err := VerifyRequest(changedRaw, publicKey, Verification{Sender: changed.Sender, Audience: changed.Audience, Now: testTime}); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}

	bad := []string{
		`{"query":"q","query":"q","limit":1}`,
		`{"query":"q","limit":1,"extra":true}`,
		`{ "query":"q","limit":1}`,
		`{"query":"q","limit":0}`,
		`{"query":"q\n","limit":1}`,
	}
	for _, body := range bad {
		if _, err := DecodeSearchRequest([]byte(body)); err == nil {
			t.Fatalf("expected body rejection: %s", body)
		}
	}

	if _, _, err := DecodeRequest(bytes.Repeat([]byte{'x'}, MaxRequestBytes+1)); err == nil {
		t.Fatal("expected oversized request rejection")
	}
	if _, err := VerifyRequest(raw, publicKey, Verification{Sender: "alpha", Audience: "beta", Now: testTime.Add(61 * time.Second)}); err == nil {
		t.Fatal("expected stale request rejection")
	}

	malformedPrivate := append(ed25519.PrivateKey(nil), privateKey...)
	malformedPrivate[len(malformedPrivate)-1] ^= 0xff
	if _, err := SignRequest(SearchRequest{Query: "q", Limit: 1}, "alpha", "beta", testTime, testNonce(1), malformedPrivate); err == nil {
		t.Fatal("expected noncanonical private key rejection")
	}
}

func TestResponseRejectsWrongRequestAndURLMetadata(t *testing.T) {
	_, clientPrivate := testKey(1)
	serverPublic, serverPrivate := testKey(33)
	request, _ := SignRequest(SearchRequest{Query: "q", Limit: 2}, "alpha", "beta", testTime, testNonce(1), clientPrivate)
	otherRequest, _ := SignRequest(SearchRequest{Query: "q2", Limit: 2}, "alpha", "beta", testTime, testNonce(2), clientPrivate)
	response, err := SignResponse(SearchResponse{Results: []SearchResult{{URL: "https://example.com/"}}}, request, testTime, testNonce(3), serverPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyResponse(response, otherRequest, serverPublic, Verification{Sender: "beta", Audience: "alpha", Now: testTime}); err == nil {
		t.Fatal("expected exact-request binding failure")
	}

	badURLs := []string{
		"https://Example.com/",
		"https://example.com/?utm_source=x",
		"https://user@example.com/",
		"ftp://example.com/",
		"https://example.com/#fragment",
	}
	for _, rawURL := range badURLs {
		if _, err := EncodeSearchResponse(SearchResponse{Results: []SearchResult{{URL: rawURL}}}); err == nil {
			t.Fatalf("expected URL rejection: %s", rawURL)
		}
	}
	results := make([]SearchResult, MaxResults+1)
	for index := range results {
		results[index].URL = fmt.Sprintf("https://example.com/%d", index)
	}
	if _, err := EncodeSearchResponse(SearchResponse{Results: results}); err == nil {
		t.Fatal("expected result cap rejection")
	}
	if _, err := EncodeSearchResponse(SearchResponse{Results: []SearchResult{{URL: "https://example.com/"}, {URL: "https://example.com/"}}}); err == nil {
		t.Fatal("expected duplicate URL rejection")
	}
}

func TestTrustRegistryStrictImmutableAndInboundAware(t *testing.T) {
	publicKey, _ := testKey(1)
	document := registryDocument(publicKey, "https://peer.example", false, true)
	registry, err := LoadTrustRegistry(strings.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	peer, ok := registry.Lookup("peer-one")
	if !ok || peer.IdentityID != "node.beta" || peer.AllowPrivateNetwork || !peer.AllowInbound {
		t.Fatalf("unexpected peer: %+v", peer)
	}
	copyKey := peer.PublicKey()
	copyKey[0] ^= 0xff
	again, _ := registry.Lookup("peer-one")
	if bytes.Equal(copyKey, again.PublicKey()) {
		t.Fatal("registry key mutated through returned copy")
	}
	if _, ok := registry.LookupInbound("node.beta"); !ok {
		t.Fatal("expected inbound lookup")
	}

	notInbound, err := LoadTrustRegistry(strings.NewReader(registryDocument(publicKey, "https://peer.example", false, false)))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := notInbound.LookupInbound("node.beta"); ok {
		t.Fatal("inbound-disabled peer must not resolve")
	}
}

func TestTrustRegistryRejectsInvalidTrustAndOrigins(t *testing.T) {
	publicKey, _ := testKey(1)
	cases := map[string]string{
		"unknown field": strings.Replace(registryDocument(publicKey, "https://peer.example", false, true), `"version":1`, `"version":1,"extra":1`, 1),
		"duplicate key": strings.Replace(registryDocument(publicKey, "https://peer.example", false, true), `"version":1`, `"version":1,"version":1`, 1),
		"HTTP":          registryDocument(publicKey, "http://peer.example", false, true),
		"path":          registryDocument(publicKey, "https://peer.example/path", false, true),
		"uppercase":     registryDocument(publicKey, "https://Peer.example", false, true),
		"userinfo":      registryDocument(publicKey, "https://user@peer.example", false, true),
		"private":       registryDocument(publicKey, "https://127.0.0.1", false, true),
		"localhost":     registryDocument(publicKey, "https://localhost", false, true),
	}
	for name, document := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadTrustRegistry(strings.NewReader(document)); !errors.Is(err, ErrInvalidTrustRegistry) {
				t.Fatalf("expected invalid registry, got %v", err)
			}
		})
	}
	if _, err := LoadTrustRegistry(strings.NewReader(registryDocument(publicKey, "https://127.0.0.1", true, true))); err != nil {
		t.Fatalf("private opt-in should permit literal private origin: %v", err)
	}
	if _, err := LoadTrustRegistry(bytes.NewReader(bytes.Repeat([]byte{'x'}, MaxTrustRegistryBytes+1))); !errors.Is(err, ErrInvalidTrustRegistry) {
		t.Fatalf("expected size rejection, got %v", err)
	}
}

func registryDocument(publicKey ed25519.PublicKey, origin string, private, inbound bool) string {
	return fmt.Sprintf(`{"version":1,"identities":[{"id":"node.beta","key_id":"%s","ed25519_public_key":"%s"}],"peers":[{"id":"peer-one","identity_id":"node.beta","base_origin":"%s","allow_private_network":%t,"allow_inbound":%t}]}`,
		KeyID(publicKey), base64.StdEncoding.EncodeToString(publicKey), origin, private, inbound)
}
