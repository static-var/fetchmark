// Package discovery owns provider-agnostic source registration and declarative
// source-pack routing. It depends only on the search port so adapters and the
// pipeline can share the plan without importing one another.
package discovery

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

// Intent is a deterministic, coarse query class used only for source-pack
// selection. General is always present; additional intents can overlap.
type Intent string

const (
	IntentGeneral   Intent = "general"
	IntentFresh     Intent = "fresh"
	IntentDeveloper Intent = "developer"
	IntentResearch  Intent = "research"
	IntentKnowledge Intent = "knowledge"
	IntentExplore   Intent = "explore"
)

// QueryProfile is the shared deterministic interpretation of query text and
// controls. Discovery routing and query expansion both consume this profile so
// a term recognized as fresh or developer-facing cannot be lost at the next
// planning stage.
type QueryProfile struct {
	Fresh     bool
	Developer bool
	Research  bool
	Knowledge bool
	Explore   bool
}

// Source is a trusted process-registered discovery provider. Fields after
// Weight are optional lane controls populated by a source pack.
type Source struct {
	ID         string
	ProviderID string
	// ProviderKind is the bounded public adapter identity. ProviderID remains
	// the private binding key used for planning and lifecycle management.
	ProviderKind string
	Searcher     search.Searcher
	Weight       float64
	Variants     []string
	Engines      []string
	Categories   []string
	MaxResults   int
	Timeout      time.Duration
}

// SourceRef is the declarative lane entry inside a pack. Weight multiplies the
// provider's base weight; zero means 1. Controls are applied only when the
// caller did not explicitly choose engines/categories.
type SourceRef struct {
	ID         string   `json:"source"`
	LaneID     string   `json:"id"`
	Weight     float64  `json:"weight,omitempty"`
	Variants   []string `json:"variants,omitempty"`
	Engines    []string `json:"engines,omitempty"`
	Categories []string `json:"categories,omitempty"`
	MaxResults int      `json:"max_results,omitempty"`
	TimeoutMS  int      `json:"timeout_ms,omitempty"`
}

// Pack declares which ordered source lanes apply to one or more intents.
type Pack struct {
	ID      string      `json:"id"`
	Always  bool        `json:"always,omitempty"`
	Intents []Intent    `json:"intents,omitempty"`
	Sources []SourceRef `json:"sources"`
}

// BuiltinOptions omits unavailable optional providers from the built-in pack
// graph instead of leaving dead lanes in every query plan.
type BuiltinOptions struct {
	PrimarySource   string
	EnableWikipedia bool
	EnableCrossref  bool
	EnableArxiv     bool
}

// BuiltinPacks returns stable named packs. Developer and fresh are explicit
// extension points even before they gain independent native providers.
func BuiltinPacks(options BuiltinOptions) []Pack {
	primary := options.PrimarySource
	general := Pack{ID: "general-open", Always: true, Sources: []SourceRef{{ID: primary}}}
	developer := Pack{ID: "developer", Intents: []Intent{IntentDeveloper}, Sources: []SourceRef{{ID: primary}}}
	research := Pack{ID: "research", Intents: []Intent{IntentResearch}, Sources: []SourceRef{{ID: primary}}}
	knowledge := Pack{ID: "knowledge", Intents: []Intent{IntentKnowledge, IntentExplore}, Sources: []SourceRef{{ID: primary}}}
	fresh := Pack{ID: "fresh", Intents: []Intent{IntentFresh}, Sources: []SourceRef{{ID: primary}}}
	if options.EnableCrossref {
		research.Sources = append(research.Sources, SourceRef{ID: "crossref", Variants: []string{"original", "exact", "freshness"}})
	}
	if options.EnableArxiv {
		research.Sources = append(research.Sources, SourceRef{ID: "arxiv", Variants: []string{"original", "exact", "freshness"}})
	}
	if options.EnableWikipedia {
		knowledge.Sources = append(knowledge.Sources, SourceRef{ID: "wikipedia", Variants: []string{"original"}})
	}
	return []Pack{general, developer, research, knowledge, fresh}
}

// RegistryOptions binds declarative packs to live adapter instances.
type RegistryOptions struct {
	PrimarySource string
	// EngineSource names the optional provider that understands engine names.
	// It is intentionally independent of PrimarySource so a native provider can
	// be the ordinary fallback while explicit engine requests still use SearXNG.
	EngineSource string
	Sources      []Source
	Packs        []Pack
	EnabledPacks []string
}

// Registry is immutable after construction and safe for concurrent planning.
type Registry struct {
	primary      Source
	engineSource *Source
	sources      map[string]Source
	packs        []Pack
}

// Planner selects trusted source lanes for a query.
type Planner interface {
	Sources(search.Query) []Source
}

// QueryPlanner is the richer planning contract used when request controls can
// be unsupported by the configured provider set.
type QueryPlanner interface {
	Planner
	Plan(search.Query) ([]Source, error)
}

var _ Planner = (*Registry)(nil)
var _ QueryPlanner = (*Registry)(nil)

// NewRegistry validates all identifiers and references up front so request
// planning cannot silently accept misspelled or attacker-controlled labels.
func NewRegistry(options RegistryOptions) (*Registry, error) {
	if !validID(options.PrimarySource) {
		return nil, errors.New("discovery: valid primary source is required")
	}
	sources := make(map[string]Source, len(options.Sources))
	for _, source := range options.Sources {
		if !validID(source.ID) || source.Searcher == nil {
			return nil, fmt.Errorf("discovery: invalid source %q", source.ID)
		}
		if _, exists := sources[source.ID]; exists {
			return nil, fmt.Errorf("discovery: duplicate source %q", source.ID)
		}
		if source.Weight <= 0 {
			source.Weight = 1
		}
		source.ProviderID = source.ID
		sources[source.ID] = cloneSource(source)
	}
	primary, ok := sources[options.PrimarySource]
	if !ok {
		return nil, fmt.Errorf("discovery: primary source %q is not registered", options.PrimarySource)
	}
	var engineSource *Source
	if options.EngineSource != "" {
		configured, exists := sources[options.EngineSource]
		if !exists {
			return nil, fmt.Errorf("discovery: engine source %q is not registered", options.EngineSource)
		}
		cloned := cloneSource(configured)
		engineSource = &cloned
	}

	packByID := make(map[string]Pack, len(options.Packs))
	for _, pack := range options.Packs {
		if !validID(pack.ID) {
			return nil, fmt.Errorf("discovery: invalid pack %q", pack.ID)
		}
		if _, exists := packByID[pack.ID]; exists {
			return nil, fmt.Errorf("discovery: duplicate pack %q", pack.ID)
		}
		for _, ref := range pack.Sources {
			if _, exists := sources[ref.ID]; !exists {
				return nil, fmt.Errorf("discovery: pack %q references unknown source %q", pack.ID, ref.ID)
			}
			if err := validateSourceRef(pack.ID, ref); err != nil {
				return nil, err
			}
		}
		packByID[pack.ID] = clonePack(pack)
	}

	enabled := make([]Pack, 0, len(options.EnabledPacks))
	seenEnabled := make(map[string]struct{}, len(options.EnabledPacks))
	for _, id := range options.EnabledPacks {
		if _, duplicate := seenEnabled[id]; duplicate {
			continue
		}
		pack, exists := packByID[id]
		if !exists {
			return nil, fmt.Errorf("discovery: enabled pack %q is not defined", id)
		}
		seenEnabled[id] = struct{}{}
		enabled = append(enabled, pack)
	}
	return &Registry{primary: primary, engineSource: engineSource, sources: sources, packs: enabled}, nil
}

// Sources preserves the original Planner interface. Callers that need typed
// control errors should use Plan through QueryPlanner.
func (r *Registry) Sources(q search.Query) []Source {
	sources, _ := r.Plan(q)
	return sources
}

// PrimarySource returns the globally bounded configured primary without
// source-pack lane controls. Basic search uses it only when the selected packs
// contain advanced-only variants and still require one unchanged query.
func (r *Registry) PrimarySource() Source {
	return cloneSource(r.primary)
}

// Plan returns an ordered plan. Explicit user engines pin the query to the
// configured SearXNG engine source so native lanes cannot reinterpret them.
func (r *Registry) Plan(q search.Query) ([]Source, error) {
	if len(q.Engines) > 0 {
		if r.engineSource == nil {
			return nil, &search.UnsupportedControlError{Control: "engines", Reason: "no enabled SearXNG discovery source"}
		}
		return []Source{cloneSource(*r.engineSource)}, nil
	}
	intents := ClassifyIntents(q)
	intentSet := make(map[Intent]struct{}, len(intents))
	for _, intent := range intents {
		intentSet[intent] = struct{}{}
	}
	planned := make([]Source, 0, len(r.sources))
	positions := make(map[string]int, len(r.sources))
	for _, pack := range r.packs {
		if !pack.Always && !packMatches(pack, intentSet) {
			continue
		}
		for _, ref := range pack.Sources {
			source := cloneSource(r.sources[ref.ID])
			applyRef(&source, ref)
			key := sourcePlanKey(source)
			if position, exists := positions[key]; exists {
				if source.Weight > planned[position].Weight {
					planned[position].Weight = source.Weight
				}
				continue
			}
			positions[key] = len(planned)
			planned = append(planned, source)
		}
	}
	return r.primaryFirst(planned), nil
}

// primaryFirst makes the operator's primary choice an actual discovery lane,
// not merely an empty-plan fallback. Existing pack-specific controls are kept
// when the primary already appears; otherwise its globally bounded source is
// prepended. Other enabled providers remain ordered opportunistic lanes.
func (r *Registry) primaryFirst(planned []Source) []Source {
	primaryProvider := r.primary.ProviderID
	if len(planned) > 0 && planned[0].ProviderID == primaryProvider {
		return planned
	}
	for index, source := range planned {
		if source.ProviderID == primaryProvider {
			out := make([]Source, 0, len(planned))
			out = append(out, source)
			out = append(out, planned[:index]...)
			out = append(out, planned[index+1:]...)
			return out
		}
	}
	return append([]Source{cloneSource(r.primary)}, planned...)
}

// ProfileQuery extracts overlapping specialty signals, then applies routing
// precedence. Advanced depth is an exploration hint only when no stronger
// fresh, developer, research, or knowledge signal is present.
func ProfileQuery(q search.Query) QueryProfile {
	text := strings.ToLower(strings.Join(strings.Fields(q.Q), " "))
	profile := QueryProfile{
		Fresh: strings.TrimSpace(q.TimeRange) != "" || containsAnyWord(text, "latest", "recent", "news", "today", "current") || yearPattern.MatchString(text),
		Developer: containsAnyWord(text,
			"api", "sdk", "docs", "documentation", "error", "install", "configure", "config", "golang", "python", "kotlin", "java", "javascript", "typescript", "node", "react", "cli",
			"android", "jetpack", "rust", "abortcontroller", "docker", "buildkit", "kubernetes", "git", "sqlite", "postgresql", "github", "opentelemetry", "grpc", "gradle", "wasi", "webassembly",
		) || containsPhrase(text, "swift actor"),
		Research: containsAnyWord(text,
			"doi", "paper", "papers", "journal", "citation", "citations", "research", "study", "studies", "preprint", "peer-reviewed",
			"dataset", "datasets", "experiment", "experiments", "methodology", "benchmark", "benchmarks", "uncertainty",
			"meta-analysis", "meta-analyses",
		) || containsPhrase(text,
			"systematic review", "meta analysis", "peer reviewed", "evidence links", "evidence supports", "evidence connects", "how accurately",
		) || hasCategory(q.Categories, "science", "research"),
		Knowledge: containsPhrase(text, "who is", "who was", "what is", "what was", "define ", "meaning of", "history of", "biography of"),
	}
	profile.Explore = strings.EqualFold(strings.TrimSpace(q.SearchDepth), "advanced") &&
		!profile.Fresh && !profile.Developer && !profile.Research && !profile.Knowledge
	return profile
}

// ClassifyIntents returns stable order independent of map iteration. Strong
// specialty intents suppress the broad knowledge/exploration lanes so an
// advanced technical, fresh, or research request is not diluted by Wikipedia.
func ClassifyIntents(q search.Query) []Intent {
	profile := ProfileQuery(q)
	intents := []Intent{IntentGeneral}
	if profile.Fresh {
		intents = append(intents, IntentFresh)
	}
	if profile.Developer {
		intents = append(intents, IntentDeveloper)
	}
	if profile.Research {
		intents = append(intents, IntentResearch)
	}
	if profile.Knowledge && !profile.Fresh && !profile.Developer && !profile.Research {
		intents = append(intents, IntentKnowledge)
	}
	if profile.Explore {
		intents = append(intents, IntentExplore)
	}
	return intents
}

var yearPattern = regexp.MustCompile(`\b20[0-9]{2}\b`)

func containsAnyWord(text string, words ...string) bool {
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '-'
	})
	set := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		set[field] = struct{}{}
	}
	for _, word := range words {
		if _, ok := set[word]; ok {
			return true
		}
	}
	return false
}

func containsPhrase(text string, phrases ...string) bool {
	for _, phrase := range phrases {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}

func hasCategory(categories []string, wanted ...string) bool {
	for _, category := range categories {
		category = strings.ToLower(strings.TrimSpace(category))
		for _, value := range wanted {
			if category == value {
				return true
			}
		}
	}
	return false
}

func packMatches(pack Pack, intents map[Intent]struct{}) bool {
	for _, intent := range pack.Intents {
		if _, ok := intents[intent]; ok {
			return true
		}
	}
	return false
}

func applyRef(source *Source, ref SourceRef) {
	weight := ref.Weight
	if weight <= 0 {
		weight = 1
	}
	source.Weight *= weight
	if ref.LaneID != "" {
		source.ID = ref.LaneID
	}
	source.Variants = append([]string(nil), ref.Variants...)
	source.Engines = append([]string(nil), ref.Engines...)
	source.Categories = append([]string(nil), ref.Categories...)
	if ref.MaxResults > 0 && (source.MaxResults <= 0 || ref.MaxResults < source.MaxResults) {
		source.MaxResults = ref.MaxResults
	}
	if ref.TimeoutMS > 0 {
		laneTimeout := time.Duration(ref.TimeoutMS) * time.Millisecond
		if source.Timeout <= 0 || laneTimeout < source.Timeout {
			source.Timeout = laneTimeout
		}
	}
}

func sourcePlanKey(source Source) string {
	return strings.Join([]string{
		source.ID,
		strings.Join(source.Variants, ","),
		strings.Join(source.Engines, ","),
		strings.Join(source.Categories, ","),
		fmt.Sprint(source.MaxResults),
		source.Timeout.String(),
	}, "\x00")
}

func validateSourceRef(pack string, ref SourceRef) error {
	if ref.LaneID != "" && !validID(ref.LaneID) {
		return fmt.Errorf("discovery: pack %q has invalid lane id %q", pack, ref.LaneID)
	}
	if ref.Weight < 0 || ref.MaxResults < 0 || ref.TimeoutMS < 0 {
		return fmt.Errorf("discovery: pack %q has invalid source limits", pack)
	}
	for _, variant := range ref.Variants {
		switch variant {
		case "original", "exact", "freshness", "docs", "concept":
		default:
			return fmt.Errorf("discovery: pack %q has invalid variant %q", pack, variant)
		}
	}
	return nil
}

func cloneSource(source Source) Source {
	source.Variants = append([]string(nil), source.Variants...)
	source.Engines = append([]string(nil), source.Engines...)
	source.Categories = append([]string(nil), source.Categories...)
	return source
}

func clonePack(pack Pack) Pack {
	pack.Intents = append([]Intent(nil), pack.Intents...)
	pack.Sources = append([]SourceRef(nil), pack.Sources...)
	for i := range pack.Sources {
		pack.Sources[i].Variants = append([]string(nil), pack.Sources[i].Variants...)
		pack.Sources[i].Engines = append([]string(nil), pack.Sources[i].Engines...)
		pack.Sources[i].Categories = append([]string(nil), pack.Sources[i].Categories...)
	}
	return pack
}

var idPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,47}$`)

func validID(id string) bool { return idPattern.MatchString(id) }

// SortSourcesByID is useful only for deterministic diagnostics and tests; the
// query planner itself preserves declared pack order.
func SortSourcesByID(sources []Source) {
	sort.SliceStable(sources, func(i, j int) bool { return sources[i].ID < sources[j].ID })
}
