package summarizer

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Factory builds a Provider from a validated ProviderConfig. Injected
// for tests.
type Factory func(ProviderConfig, *http.Client) Provider

// DefaultFactory dispatches on Kind to produce the right provider.
func DefaultFactory(cfg ProviderConfig, httpClient *http.Client) Provider {
	switch cfg.Kind {
	case KindOpenAI:
		return NewOpenAIProvider(cfg, httpClient)
	case KindAnthropic:
		return NewAnthropicProvider(cfg, httpClient)
	default:
		return nil
	}
}

// Registry is a process-local, in-memory catalog of configured
// providers. It is intentionally not backed by Redis in v1: the
// summarize surface treats env vars as the bootstrap source of truth,
// and admin PUTs only live for the lifetime of the process. Operators
// who run multi-replica deployments should promote stable settings to
// env vars. This is called out in docs/tasks.md under "v2 R4".
type Registry struct {
	mu         sync.RWMutex
	factory    Factory
	httpClient *http.Client
	providers  map[string]Provider
	configs    map[string]ProviderConfig
	def        string
}

// NewRegistry builds an empty registry. Use Set to populate.
func NewRegistry(factory Factory, httpClient *http.Client) *Registry {
	if factory == nil {
		factory = DefaultFactory
	}
	return &Registry{
		factory:    factory,
		httpClient: httpClient,
		providers:  map[string]Provider{},
		configs:    map[string]ProviderConfig{},
	}
}

// Set registers or replaces a provider by name. The cfg is validated
// first; invalid configs leave the registry untouched.
func (r *Registry) Set(cfg ProviderConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	prov := r.factory(cfg, r.httpClient)
	if prov == nil {
		return fmt.Errorf("summarizer: no factory for kind %q", cfg.Kind)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[cfg.Name] = prov
	r.configs[cfg.Name] = cfg
	if r.def == "" {
		r.def = cfg.Name
	}
	return nil
}

// Delete removes a provider by name. No-op if the name is unknown.
// Clears the default if the default matched.
func (r *Registry) Delete(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.providers, name)
	delete(r.configs, name)
	if r.def == name {
		r.def = ""
		for k := range r.providers {
			r.def = k
			break
		}
	}
}

// SetDefault selects the default provider used when a request does
// not explicitly name one.
func (r *Registry) SetDefault(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.providers[name]; !ok {
		return fmt.Errorf("%w: %q", ErrProviderNotFound, name)
	}
	r.def = name
	return nil
}

// Default returns the default provider or nil.
func (r *Registry) Default() Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.def == "" {
		return nil
	}
	return r.providers[r.def]
}

// DefaultName returns the current default provider name, or "".
func (r *Registry) DefaultName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.def
}

// Get returns the provider registered under name, or nil.
func (r *Registry) Get(name string) Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.providers[name]
}

// Resolve picks a provider by preferred name, falling back to the
// default when name is empty.
func (r *Registry) Resolve(name string) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if name == "" {
		if r.def == "" {
			return nil, fmt.Errorf("%w: no default configured", ErrProviderNotFound)
		}
		return r.providers[r.def], nil
	}
	p, ok := r.providers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrProviderNotFound, name)
	}
	return p, nil
}

// Config returns a redacted copy of the named provider's config.
func (r *Registry) Config(name string) (ProviderConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cfg, ok := r.configs[name]
	if !ok {
		return ProviderConfig{}, false
	}
	return cfg.Redact(), true
}

// Snapshot returns redacted copies of every configured provider,
// sorted by name for stable output.
func (r *Registry) Snapshot() (configs []ProviderConfig, def string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	configs = make([]ProviderConfig, 0, len(r.configs))
	for _, cfg := range r.configs {
		configs = append(configs, cfg.Redact())
	}
	sort.Slice(configs, func(i, j int) bool { return configs[i].Name < configs[j].Name })
	return configs, r.def
}

// Empty reports whether any providers are configured.
func (r *Registry) Empty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.providers) == 0
}

// MergeWithExisting applies an admin overlay on top of a stored
// config, preserving the original APIKey when the overlay omits it.
// This lets admins tweak Model or Thinking without re-typing the key
// every time.
func (r *Registry) MergeWithExisting(overlay ProviderConfig) (ProviderConfig, error) {
	r.mu.RLock()
	existing, ok := r.configs[overlay.Name]
	r.mu.RUnlock()
	if !ok {
		return overlay, overlay.Validate()
	}
	if strings.TrimSpace(overlay.APIKey) == "" {
		overlay.APIKey = existing.APIKey
	}
	if overlay.Kind == "" {
		overlay.Kind = existing.Kind
	}
	if overlay.BaseURL == "" {
		overlay.BaseURL = existing.BaseURL
	}
	if overlay.Model == "" {
		overlay.Model = existing.Model
	}
	if overlay.Timeout == 0 {
		overlay.Timeout = existing.Timeout
	}
	if err := overlay.Validate(); err != nil {
		return ProviderConfig{}, err
	}
	return overlay, nil
}
