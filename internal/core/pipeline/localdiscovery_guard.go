package pipeline

import (
	"context"
	"strings"

	"github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const localArtifactMismatchReason = "artifact_mismatch"

type artifactVerifiedLocalSearcher struct {
	base      search.Searcher
	artifacts localartifact.Store
}

func (p *Pipeline) availableLocalSearcher() search.Searcher {
	if p == nil || p.LocalSearcher == nil || p.localCorpusQuarantined.Load() {
		return nil
	}
	if p.LocalArtifacts == nil {
		return p.LocalSearcher
	}
	return artifactVerifiedLocalSearcher{base: p.LocalSearcher, artifacts: p.LocalArtifacts}
}

func (verified artifactVerifiedLocalSearcher) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := verified.SearchBatch(ctx, query)
	return batch.Hits, err
}

func (verified artifactVerifiedLocalSearcher) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	var (
		batch search.SearchBatch
		err   error
	)
	if rich, ok := verified.base.(search.BatchSearcher); ok {
		batch, err = rich.SearchBatch(ctx, query)
	} else {
		batch.Hits, err = verified.base.Search(ctx, query)
		batch.Provider = "local-index"
		batch.Instance = "local-index"
		batch.Status = search.BatchHealthy
	}
	if err != nil || len(batch.Hits) == 0 {
		return batch, err
	}
	retained := make([]search.Hit, 0, len(batch.Hits))
	dropped := false
	for _, hit := range batch.Hits {
		version, ok, currentErr := verified.artifacts.Current(ctx, hit.URL)
		if currentErr != nil || !ok || !localHitMatchesArtifact(hit, version) {
			dropped = true
			continue
		}
		retained = append(retained, hit)
	}
	batch.Hits = retained
	if dropped {
		batch.Diagnostics = append(batch.Diagnostics, search.ProviderDiagnostic{
			Provider: "local-index", Instance: "local-index", Reason: localArtifactMismatchReason,
		})
		if len(retained) == 0 {
			batch.Status = search.BatchDegradedEmpty
		} else {
			batch.Status = search.BatchPartial
		}
	}
	return batch, nil
}

func localHitMatchesArtifact(hit search.Hit, version localartifact.Version) bool {
	if hit.Metadata == nil || !strings.EqualFold(strings.TrimSpace(hit.Metadata["content_hash"]), strings.TrimSpace(version.ContentHash)) {
		return false
	}
	hitSafety, hitErr := localcorpus.ParseSafetyClassification(hit.Metadata["safety_classification"])
	artifactSafety, artifactErr := localcorpus.ParseSafetyClassification(string(version.SafetyClassification))
	return hitErr == nil && artifactErr == nil && hitSafety == artifactSafety
}
