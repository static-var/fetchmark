package federationapi_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/bleveindex"
	"github.com/staticvar/fetchmark/internal/adapters/federationpeer"
	"github.com/staticvar/fetchmark/internal/core/federation"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/federationapi"
)

func TestTrustedTLSClientQueriesOnlySafeCuratedIndexAndReceivesURLs(t *testing.T) {
	serverIdentity := integrationIdentity(t, "server-node")
	clientIdentity := integrationIdentity(t, "client-node")
	serverTrust := integrationTrustRegistry(t, "client-peer", clientIdentity, "https://client.example", false, true)

	index, err := bleveindex.Open(bleveindex.Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	for _, document := range []localcorpus.Document{
		{
			URL: "https://docs.example.org/safe", Title: "must not cross the federation boundary",
			Body: "private mesh lexical discovery", FetchedAt: now,
			SafetyClassification: localcorpus.SafetySafe, IndexingDisposition: localcorpus.DispositionPermitted,
		},
		{
			URL: "https://docs.example.org/unclassified", Title: "must remain private",
			Body: "private mesh lexical discovery", FetchedAt: now,
			SafetyClassification: localcorpus.SafetyUnclassified, IndexingDisposition: localcorpus.DispositionPermitted,
		},
	} {
		if err := index.Reconcile(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}
	replay, err := federationapi.NewReplayCache(128, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := federationapi.NewHandler(federationapi.HandlerOptions{
		Identity: serverIdentity, TrustRegistry: serverTrust, Searcher: index, ReplayCache: replay,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)

	clientTrust := integrationTrustRegistry(t, "server-peer", serverIdentity, server.URL, true, false)
	peer, found := clientTrust.Lookup("server-peer")
	if !found {
		t.Fatal("server peer binding is absent")
	}
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	client, err := federationpeer.New(federationpeer.Options{
		Identity: clientIdentity, Peer: peer, HTTPClient: httpClient, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	strict := 2
	batch, err := client.SearchBatch(context.Background(), search.Query{
		Q: "private mesh lexical discovery", MaxResults: 10, SafeSearch: &strict,
	})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchHealthy || len(batch.Hits) != 1 || batch.Hits[0].URL != "https://docs.example.org/safe" {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.Title != "" || hit.Snippet != "" || hit.PublishedAt != nil || len(hit.Metadata) != 2 ||
		hit.Metadata["source"] != "federation" || hit.Metadata["federation_rank"] != "1" {
		t.Fatalf("federation leaked retained fields: %+v", hit)
	}
	if _, leaked := hit.Metadata["federation_peer"]; leaked {
		t.Fatalf("federation leaked private peer binding: %+v", hit)
	}
}

func integrationIdentity(t *testing.T, id string) federation.Identity {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{
		"version": federation.IdentityVersion, "identity_id": id,
		"key_id": federation.KeyID(publicKey), "ed25519_private_key": base64.StdEncoding.EncodeToString(privateKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := federation.DecodeIdentity(raw)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func integrationTrustRegistry(t *testing.T, peerID string, identity federation.Identity, origin string, allowPrivate, allowInbound bool) federation.TrustRegistry {
	t.Helper()
	raw := fmt.Sprintf(`{"version":1,"identities":[{"id":%q,"key_id":%q,"ed25519_public_key":%q}],"peers":[{"id":%q,"identity_id":%q,"base_origin":%q,"allow_private_network":%t,"allow_inbound":%t}]}`,
		identity.IdentityID(), identity.KeyID(), base64.StdEncoding.EncodeToString(identity.PublicKey()), peerID,
		identity.IdentityID(), origin, allowPrivate, allowInbound)
	registry, err := federation.LoadTrustRegistry(bytes.NewBufferString(raw))
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
