package api

import (
	"net/http"
	"strconv"

	"github.com/staticvar/fetchmark/internal/core/model"
)

// wibyAttributionLink preserves Wiby's required source link without adding
// fields to vendor-compatible JSON envelopes. The registered "via" relation
// identifies an information source for the response context.
const wibyAttributionLink = `<https://wiby.me/>; rel="via"; title="Wiby"`

// arXiv asks free independent API consumers to include this acknowledgment.
// Metadata licensing stays in each native result's explicitly scoped metadata;
// an unanchored Link license relation would apply to the mixed response itself.
const arxivAttributionLink = `<https://arxiv.org/>; rel="via"; title="Thank you to arXiv for use of its open access interoperability."`

// NLM requires clear source acknowledgment plus an evident disclaimer and
// copyright notice for software using E-utilities. The PubMed adapter returns
// bibliographic metadata only; these relations must not be read as licensing
// abstracts or linked articles.
const pubMedAttributionLink = `<https://pubmed.ncbi.nlm.nih.gov/>; rel="via"; title="PubMed, National Library of Medicine", <https://www.nlm.nih.gov/databases/download.html>; rel="terms-of-service"; title="NLM data terms, disclaimer, and copyright notice"`

// Stack Exchange's API terms require applications to visibly identify the
// network as the source. Result URLs and native metadata retain that identity;
// this header carries it through vendor-compatible JSON envelopes as well.
const stackExchangeAttributionLink = `<https://stackoverflow.com/>; rel="via"; title="Stack Overflow", <https://stackoverflow.com/help/licensing>; rel="license"; title="CC BY-SA"`

var stackExchangeLicenseLinks = map[string]string{
	"CC BY-SA 2.5": `<https://creativecommons.org/licenses/by-sa/2.5/>; rel="license"; title="CC BY-SA 2.5"`,
	"CC BY-SA 3.0": `<https://creativecommons.org/licenses/by-sa/3.0/>; rel="license"; title="CC BY-SA 3.0"`,
	"CC BY-SA 4.0": `<https://creativecommons.org/licenses/by-sa/4.0/>; rel="license"; title="CC BY-SA 4.0"`,
}

func addDiscoveryAttributionHeaders(header http.Header, results []model.SearchResult) {
	seenProviders := make(map[string]struct{}, 2)
	seenLinks := make(map[string]struct{}, 8)
	addLink := func(value string) {
		if value == "" {
			return
		}
		if _, duplicate := seenLinks[value]; duplicate {
			return
		}
		seenLinks[value] = struct{}{}
		header.Add("Link", value)
	}
	for _, result := range results {
		stackExchangeResult := false
		for _, provenance := range result.Provenance {
			switch provenance.Provider {
			case "wiby":
				if _, duplicate := seenProviders[provenance.Provider]; !duplicate {
					addLink(wibyAttributionLink)
					seenProviders[provenance.Provider] = struct{}{}
				}
			case "stackexchange":
				stackExchangeResult = true
				if _, duplicate := seenProviders[provenance.Provider]; !duplicate {
					addLink(stackExchangeAttributionLink)
					seenProviders[provenance.Provider] = struct{}{}
				}
			case "arxiv":
				if _, duplicate := seenProviders[provenance.Provider]; !duplicate {
					addLink(arxivAttributionLink)
					seenProviders[provenance.Provider] = struct{}{}
				}
			case "pubmed":
				if _, duplicate := seenProviders[provenance.Provider]; !duplicate {
					addLink(pubMedAttributionLink)
					seenProviders[provenance.Provider] = struct{}{}
				}
			}
		}
		if !stackExchangeResult {
			continue
		}
		addLink(stackExchangeLicenseLinks[result.Metadata["license"]])
		if ownerID, err := strconv.ParseInt(result.Metadata["owner_id"], 10, 64); err == nil && ownerID > 0 {
			addLink(`<https://stackoverflow.com/users/` + strconv.FormatInt(ownerID, 10) + `>; rel="author"`)
		}
	}
}
