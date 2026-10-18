// Package adapter connects the scheduler to concrete providers.
//
// An adapter answers capacity and, optionally, sandbox claim operations for
// one kind of provider. Placement needs to compare a Kubernetes cluster
// running agent-sandbox against a hosted API with no cluster at all, and the
// only way that comparison stays honest is if every provider is reduced to the
// same small contract (pkg/provider) before policy sees it.
//
// Capacity polling runs off the placement path, driven by pkg/registry. A slow
// adapter costs freshness, never placement latency.
package adapter

import (
	"fmt"
	"sort"
	"sync"

	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
)

// Config is what a SandboxProvider spec becomes once its credentials are
// resolved.
type Config struct {
	// ProviderID is the SandboxProvider's name, and the identity recorded on
	// placed sandboxes.
	ProviderID string

	// Endpoint is adapter-specific: a gateway URL, an API base.
	Endpoint string

	// Credentials are the resolved contents of the referenced Secret. Held in
	// memory only; adapters must not log them.
	Credentials map[string][]byte

	// Options carries adapter-specific settings from the provider spec.
	Options map[string]string

	// Templates maps provider-neutral template names to provider-native
	// references (for example, a SandboxWarmPool name on agent-sandbox or a
	// template ID on a hosted API).
	Templates map[string]string
}

// Credential returns a credential value as a string, or "" if absent.
func (c *Config) Credential(key string) string {
	if c.Credentials == nil {
		return ""
	}
	return string(c.Credentials[key])
}

// Factory builds a provider.Provider for one provider.
type Factory func(cfg Config) (provider.Provider, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register makes an adapter available by name. Intended for init() in adapter
// packages. Registering the same name twice panics: two adapters answering to
// one name would make behaviour depend on package import order, which is not
// something an operator could ever debug from the outside.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := factories[name]; exists {
		panic(fmt.Sprintf("adapter %q registered twice", name))
	}
	factories[name] = f
}

// New builds a provider.Provider for the named adapter.
//
// An unknown adapter is an error naming what is available, not a silent
// fallback: a provider configured with an adapter this build does not have must
// show up as broken rather than quietly never being polled.
func New(name string, cfg Config) (provider.Provider, error) {
	mu.RLock()
	f, ok := factories[name]
	mu.RUnlock()
	if !ok {
		return provider.Provider{}, fmt.Errorf("unknown adapter %q (available: %v)", name, Names())
	}
	if cfg.ProviderID == "" {
		return provider.Provider{}, fmt.Errorf("adapter %q: provider id is required", name)
	}
	p, err := f(cfg)
	if err != nil {
		return provider.Provider{}, err
	}
	if p.Source == nil {
		return provider.Provider{}, fmt.Errorf("adapter %q returned a Provider with nil Source", name)
	}
	return p, nil
}

// Names lists the registered adapters, sorted.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for k := range factories {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Has reports whether an adapter is registered. Used by the controller to fail
// a provider's validation early rather than at first poll.
func Has(name string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := factories[name]
	return ok
}
