package adapter_test

import (
	"context"
	"strings"
	"testing"

	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

type stubSource struct{ id string }

func (s stubSource) ProviderID() string { return s.id }
func (s stubSource) Fetch(context.Context) (registry.Report, error) {
	return registry.Report{}, nil
}

func init() {
	adapter.Register("stub", func(cfg adapter.Config) (registry.Source, error) {
		return stubSource{id: cfg.ProviderID}, nil
	})
}

func TestNewBuildsARegisteredAdapter(t *testing.T) {
	src, err := adapter.New("stub", adapter.Config{ProviderID: "civo"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src.ProviderID() != "civo" {
		t.Fatalf("provider id %q, want civo", src.ProviderID())
	}
}

func TestUnknownAdapterErrorsAndListsWhatExists(t *testing.T) {
	// A provider configured with an adapter this build lacks must surface as
	// broken, not be silently skipped forever.
	_, err := adapter.New("nope", adapter.Config{ProviderID: "civo"})
	if err == nil {
		t.Fatal("expected an error for an unknown adapter")
	}
	if !strings.Contains(err.Error(), "stub") {
		t.Fatalf("error should list available adapters, got %q", err)
	}
}

func TestProviderIDIsRequired(t *testing.T) {
	// Without it the registry would key capacity under an empty string and
	// every unnamed provider would overwrite the last.
	if _, err := adapter.New("stub", adapter.Config{}); err == nil {
		t.Fatal("expected an error when provider id is empty")
	}
}

func TestHasReportsRegistration(t *testing.T) {
	if !adapter.Has("stub") {
		t.Fatal("stub should be registered")
	}
	if adapter.Has("absent") {
		t.Fatal("unregistered adapter reported as present")
	}
}

func TestNamesAreSorted(t *testing.T) {
	names := adapter.Names()
	for i := 1; i < len(names); i++ {
		if names[i-1] > names[i] {
			t.Fatalf("names not sorted: %v", names)
		}
	}
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	// Two adapters answering to one name would make behaviour depend on import
	// order — not something an operator could debug from the outside.
	defer func() {
		if recover() == nil {
			t.Fatal("registering a duplicate adapter name should panic")
		}
	}()
	adapter.Register("stub", func(adapter.Config) (registry.Source, error) { return nil, nil })
}

func TestCredentialAccessorHandlesAbsentKeys(t *testing.T) {
	cfg := adapter.Config{Credentials: map[string][]byte{"token": []byte("abc")}}
	if got := cfg.Credential("token"); got != "abc" {
		t.Fatalf("credential %q, want abc", got)
	}
	if got := cfg.Credential("missing"); got != "" {
		t.Fatalf("absent credential should be empty, got %q", got)
	}
	var empty adapter.Config
	if got := empty.Credential("token"); got != "" {
		t.Fatalf("nil credentials should be empty, got %q", got)
	}
}
