package discovery

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	// MaxSpecBytes bounds operator-supplied registry files before decoding.
	MaxSpecBytes = 1 << 20
	maxSources   = 32
	maxPacks     = 32
	maxLanes     = 128
	maxListItems = 64
)

// RegistrySpec is the versioned declarative source-pack file.
type RegistrySpec struct {
	Version int          `json:"version"`
	Sources []SourceSpec `json:"sources"`
	Packs   []Pack       `json:"packs"`
}

// SourceSpec describes global provider budgets. Adapters enforce rate and
// concurrency across requests; the planner enforces result and timeout caps.
type SourceSpec struct {
	ID             string  `json:"id"`
	Kind           string  `json:"kind"`
	Weight         float64 `json:"weight"`
	MaxResults     int     `json:"max_results"`
	TimeoutMS      int     `json:"timeout_ms"`
	MaxConcurrency int     `json:"max_concurrency"`
	RatePerSecond  float64 `json:"rate_per_second"`
	Burst          int     `json:"burst"`
}

// NewRegistryFromSpec applies operator allowlists to a validated declarative
// spec and binds only explicitly enabled compiled adapters. The primary source
// must remain enabled while it is the compatibility fallback.
func NewRegistryFromSpec(spec RegistrySpec, adapters map[string]search.Searcher, primary string, enabledPacks, enabledSources []string) (*Registry, error) {
	specSources := make(map[string]SourceSpec, len(spec.Sources))
	for _, source := range spec.Sources {
		specSources[source.ID] = source
	}
	enabledSourceSet := make(map[string]struct{}, len(enabledSources))
	sources := make([]Source, 0, len(enabledSources))
	for _, id := range enabledSources {
		if _, duplicate := enabledSourceSet[id]; duplicate {
			continue
		}
		sourceSpec, exists := specSources[id]
		if !exists {
			return nil, fmt.Errorf("discovery: enabled source %q is not defined", id)
		}
		adapter, exists := adapters[id]
		if !exists || adapter == nil {
			return nil, fmt.Errorf("discovery: enabled source %q has no adapter", id)
		}
		enabledSourceSet[id] = struct{}{}
		sources = append(sources, sourceSpec.Bind(adapter))
	}
	if _, enabled := enabledSourceSet[primary]; !enabled {
		return nil, fmt.Errorf("discovery: primary source %q must be enabled", primary)
	}
	engineSource := ""
	if _, enabled := enabledSourceSet["searxng"]; enabled {
		engineSource = "searxng"
	}

	filteredPacks := make([]Pack, 0, len(spec.Packs))
	for _, pack := range spec.Packs {
		filtered := clonePack(pack)
		filtered.Sources = filtered.Sources[:0]
		for _, ref := range pack.Sources {
			if _, enabled := enabledSourceSet[ref.ID]; enabled {
				filtered.Sources = append(filtered.Sources, ref)
			}
		}
		filteredPacks = append(filteredPacks, filtered)
	}
	return NewRegistry(RegistryOptions{
		PrimarySource: primary,
		EngineSource:  engineSource,
		Sources:       sources,
		Packs:         filteredPacks,
		EnabledPacks:  enabledPacks,
	})
}

// Bind attaches a compiled adapter to validated declarative source budgets.
func (s SourceSpec) Bind(searcher search.Searcher) Source {
	return Source{
		ID:           s.ID,
		ProviderID:   s.ID,
		ProviderKind: s.Kind,
		Searcher:     searcher,
		Weight:       s.Weight,
		MaxResults:   s.MaxResults,
		Timeout:      time.Duration(s.TimeoutMS) * time.Millisecond,
	}
}

//go:embed default_packs.json
var defaultSpecJSON []byte

// DefaultSpec parses the embedded registry through the same strict path used
// for operator overrides.
func DefaultSpec() (RegistrySpec, error) {
	return LoadSpec(bytes.NewReader(defaultSpecJSON))
}

// LoadSpec strictly decodes and validates a bounded v1 registry.
func LoadSpec(reader io.Reader) (RegistrySpec, error) {
	if reader == nil {
		return RegistrySpec{}, errors.New("discovery: registry reader is required")
	}
	limited := io.LimitReader(reader, MaxSpecBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return RegistrySpec{}, fmt.Errorf("discovery: read registry: %w", err)
	}
	if len(raw) > MaxSpecBytes {
		return RegistrySpec{}, fmt.Errorf("discovery: registry exceeds %d bytes", MaxSpecBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var spec RegistrySpec
	if err := decoder.Decode(&spec); err != nil {
		return RegistrySpec{}, fmt.Errorf("discovery: decode registry: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return RegistrySpec{}, errors.New("discovery: registry contains trailing JSON")
		}
		return RegistrySpec{}, fmt.Errorf("discovery: decode trailing registry data: %w", err)
	}
	if err := validateSpec(spec); err != nil {
		return RegistrySpec{}, err
	}
	return spec, nil
}

func validateSpec(spec RegistrySpec) error {
	if spec.Version != 1 {
		return fmt.Errorf("discovery: unsupported registry version %d", spec.Version)
	}
	if len(spec.Sources) == 0 || len(spec.Sources) > maxSources {
		return fmt.Errorf("discovery: registry sources must contain 1..%d entries", maxSources)
	}
	if len(spec.Packs) == 0 || len(spec.Packs) > maxPacks {
		return fmt.Errorf("discovery: registry packs must contain 1..%d entries", maxPacks)
	}
	sources := make(map[string]SourceSpec, len(spec.Sources))
	for _, source := range spec.Sources {
		if !validID(source.ID) {
			return fmt.Errorf("discovery: invalid source id %q", source.ID)
		}
		if _, duplicate := sources[source.ID]; duplicate {
			return fmt.Errorf("discovery: duplicate source id %q", source.ID)
		}
		switch source.Kind {
		case "searxng", "wikipedia", "crossref", "arxiv", "mwmbl", "wiby", "stackexchange", "github", "pubmed", "yacy", "scrapling", "feedindex":
			if source.ID != source.Kind {
				return fmt.Errorf("discovery: %s source must use id %q", source.Kind, source.Kind)
			}
		case "openpack", "federation":
		default:
			return fmt.Errorf("discovery: unsupported source kind %q", source.Kind)
		}
		if source.Weight <= 0 || source.Weight > 10 || source.MaxResults < 1 || source.MaxResults > 100 ||
			source.TimeoutMS < 100 || source.TimeoutMS > 120000 || source.MaxConcurrency < 1 || source.MaxConcurrency > 16 ||
			source.RatePerSecond <= 0 || source.RatePerSecond > 100 || source.Burst < 1 || source.Burst > 100 {
			return fmt.Errorf("discovery: invalid budgets for source %q", source.ID)
		}
		sources[source.ID] = source
	}
	packIDs := make(map[string]struct{}, len(spec.Packs))
	laneIDs := make(map[string]struct{})
	laneCount := 0
	for _, pack := range spec.Packs {
		if !validID(pack.ID) {
			return fmt.Errorf("discovery: invalid pack id %q", pack.ID)
		}
		if _, duplicate := packIDs[pack.ID]; duplicate {
			return fmt.Errorf("discovery: duplicate pack id %q", pack.ID)
		}
		packIDs[pack.ID] = struct{}{}
		if !pack.Always && len(pack.Intents) == 0 {
			return fmt.Errorf("discovery: pack %q requires always or intents", pack.ID)
		}
		for _, intent := range pack.Intents {
			if !validIntent(intent) {
				return fmt.Errorf("discovery: pack %q has invalid intent %q", pack.ID, intent)
			}
		}
		if len(pack.Sources) == 0 || len(pack.Sources) > maxListItems {
			return fmt.Errorf("discovery: pack %q sources must contain 1..%d entries", pack.ID, maxListItems)
		}
		for _, lane := range pack.Sources {
			laneCount++
			if laneCount > maxLanes {
				return fmt.Errorf("discovery: registry exceeds %d lanes", maxLanes)
			}
			if !validID(lane.LaneID) {
				return fmt.Errorf("discovery: pack %q has invalid lane id %q", pack.ID, lane.LaneID)
			}
			if _, duplicate := laneIDs[lane.LaneID]; duplicate {
				return fmt.Errorf("discovery: duplicate lane id %q", lane.LaneID)
			}
			laneIDs[lane.LaneID] = struct{}{}
			source, exists := sources[lane.ID]
			if !exists {
				return fmt.Errorf("discovery: pack %q references unknown source %q", pack.ID, lane.ID)
			}
			if lane.Weight <= 0 || lane.Weight > 10 || len(lane.Variants) == 0 || len(lane.Variants) > 4 ||
				lane.MaxResults < 0 || lane.MaxResults > 100 || lane.TimeoutMS < 0 || lane.TimeoutMS > 120000 {
				return fmt.Errorf("discovery: invalid budgets for lane %q", lane.LaneID)
			}
			if len(lane.Engines) > maxListItems || len(lane.Categories) > maxListItems {
				return fmt.Errorf("discovery: lane %q has too many controls", lane.LaneID)
			}
			if lane.MaxResults > 0 && source.MaxResults <= 0 {
				return fmt.Errorf("discovery: lane %q has no source result budget", lane.LaneID)
			}
			if err := validateSourceRef(pack.ID, lane); err != nil {
				return err
			}
			for _, value := range append(append([]string(nil), lane.Engines...), lane.Categories...) {
				if value == "" || len(value) > 64 {
					return fmt.Errorf("discovery: lane %q has invalid control", lane.LaneID)
				}
			}
		}
	}
	return nil
}

func validIntent(intent Intent) bool {
	switch intent {
	case IntentGeneral, IntentFresh, IntentDeveloper, IntentResearch, IntentKnowledge, IntentExplore:
		return true
	default:
		return false
	}
}
