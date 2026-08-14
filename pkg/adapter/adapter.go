// Package adapter connects the scheduler to concrete providers.
//
// An adapter answers one question — "how much warm capacity do you have?" — for
// one kind of provider. That narrowness is deliberate. Placement needs to
// compare a Kubernetes cluster running agent-sandbox against a hosted API with
// no cluster at all, and the only way that comparison stays honest is if every
// provider is reduced to the same small set of facts before policy sees it.
//
// Adapters run off the placement path, driven by pkg/registry. A slow adapter
// costs freshness, never placement latency.
package adapter

import (
	"fmt"
	"sort"
	"sync"

	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
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
}

// Credential returns a credential value as a string, or "" if absent.
func (c *Config) Credential(key string) string {
	if c.Credentials == nil {
		return ""
	}
	return string(c.Credentials[key])
}

// Factory builds a Source for one provider.
type Factory func(cfg Config) (registry.Source, error)

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

// New builds a Source for the named adapter.
//
// An unknown adapter is an error naming what is available, not a silent
// fallback: a provider configured with an adapter this build does not have must
// show up as broken rather than quietly never being polled.
func New(name string, cfg Config) (registry.Source, error) {
	mu.RLock()
	f, ok := factories[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown adapter %q (available: %v)", name, Names())
	}
	if cfg.ProviderID == "" {
		return nil, fmt.Errorf("adapter %q: provider id is required", name)
	}
	return f(cfg)
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
