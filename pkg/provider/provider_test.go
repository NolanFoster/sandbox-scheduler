package provider_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
)

func TestKindOfDefaultsToUnknownOutcome(t *testing.T) {
	// Any unclassified error, or a ClaimError value rather than pointer, or
	// an unknown enum number must map to KindUnknownOutcome so a bug in an
	// adapter never spills a claim that may have succeeded.
	cases := []struct {
		name string
		err  error
		want provider.Kind
	}{
		{"nil", nil, provider.KindUnknownOutcome},
		{"plain error", errors.New("boom"), provider.KindUnknownOutcome},
		{"wrapped plain error", fmt.Errorf("wrap: %w", errors.New("boom")), provider.KindUnknownOutcome},
		{"out of range kind", &provider.ClaimError{Kind: provider.Kind(99), Err: errors.New("future")}, provider.KindUnknownOutcome},
		{"no capacity", provider.NoCapacity(errors.New("full")), provider.KindNoCapacity},
		{"unavailable", provider.Unavailable(errors.New("dial")), provider.KindUnavailable},
		{"rejected", provider.Rejected(errors.New("401")), provider.KindRejected},
		{"busy", provider.Busy(2*time.Second, errors.New("429")), provider.KindBusy},
		{"key conflict", provider.KeyConflict(errors.New("spec mismatch")), provider.KindKeyConflict},
		{"key busy", provider.KeyBusy(time.Second, errors.New("deleting")), provider.KindKeyBusy},
		{"wrapped claim error", fmt.Errorf("adapter: %w", provider.NoCapacity(errors.New("empty"))), provider.KindNoCapacity},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := provider.KindOf(tc.err); got != tc.want {
				t.Fatalf("KindOf(%v) = %v (%s), want %v (%s)", tc.err, got, got.String(), tc.want, tc.want.String())
			}
		})
	}

	// KIND_UNSPECIFIED (0) and any unrecognised proto enum value must map to
	// KindUnknownOutcome, never to a spilling kind.
	for _, unknown := range []int32{0, -1, 8, 99, 1000} {
		if got := provider.KindFromProto(unknown); got != provider.KindUnknownOutcome {
			t.Fatalf("KindFromProto(%d) = %v, want KindUnknownOutcome", unknown, got)
		}
	}
	for _, k := range []provider.Kind{
		provider.KindUnknownOutcome,
		provider.KindNoCapacity,
		provider.KindUnavailable,
		provider.KindRejected,
		provider.KindBusy,
		provider.KindKeyConflict,
		provider.KindKeyBusy,
	} {
		if got := provider.KindFromProto(provider.ToProtoKind(k)); got != k {
			t.Fatalf("round-trip KindFromProto(ToProtoKind(%v)) = %v", k, got)
		}
	}
}

func TestRetryAfterOf(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", provider.Busy(1500*time.Millisecond, errors.New("rate limit")))
	if got := provider.RetryAfterOf(err); got != 1500*time.Millisecond {
		t.Fatalf("RetryAfterOf = %v, want 1.5s", got)
	}
	if got := provider.RetryAfterOf(provider.Busy(-time.Second, errors.New("neg"))); got != 0 {
		t.Fatalf("negative RetryAfter should clamp to 0, got %v", got)
	}
	if got := provider.RetryAfterOf(errors.New("plain")); got != 0 {
		t.Fatalf("plain error RetryAfter = %v, want 0", got)
	}
}

func TestValidateClaimRequest(t *testing.T) {
	valid := provider.ClaimRequest{
		Template: "python-3.12",
		Key:      "req-12345_abc:01",
		TTL:      10 * time.Minute,
		Labels: map[string]string{
			"sandbox.users.io/tenant":     "acme",
			" sub.sandbox.users.io/stage": "prod",
		},
	}
	// Fix leading space in test case to check rejection first.
	if err := valid.Validate(24 * time.Hour); err == nil {
		t.Fatal("expected label prefix with leading space to be rejected")
	}

	valid.Labels = map[string]string{
		"sandbox.users.io/tenant":    "acme",
		"sub.sandbox.users.io/stage": "prod",
	}
	if err := valid.Validate(24 * time.Hour); err != nil {
		t.Fatalf("expected valid request to pass, got: %v", err)
	}

	bad := []struct {
		name   string
		mutate func(*provider.ClaimRequest)
	}{
		{"empty key", func(r *provider.ClaimRequest) { r.Key = "" }},
		{"key starts with dash", func(r *provider.ClaimRequest) { r.Key = "-bad" }},
		{"key ends with dot", func(r *provider.ClaimRequest) { r.Key = "bad." }},
		{"key too long", func(r *provider.ClaimRequest) { r.Key = strings.Repeat("a", 129) }},
		{"key has slash", func(r *provider.ClaimRequest) { r.Key = "a/b" }},
		{"zero ttl", func(r *provider.ClaimRequest) { r.TTL = 0 }},
		{"negative ttl", func(r *provider.ClaimRequest) { r.TTL = -time.Second }},
		{"ttl exceeds max", func(r *provider.ClaimRequest) { r.TTL = 25 * time.Hour }},
		{"label without domain", func(r *provider.ClaimRequest) {
			r.Labels = map[string]string{"tenant": "acme"}
		}},
		{"label wrong domain", func(r *provider.ClaimRequest) {
			r.Labels = map[string]string{"kubernetes.io/os": "linux"}
		}},
		{"label spoofed suffix domain", func(r *provider.ClaimRequest) {
			r.Labels = map[string]string{"evilsandbox.users.io/tenant": "acme"}
		}},
		{"label invalid value", func(r *provider.ClaimRequest) {
			r.Labels = map[string]string{"sandbox.users.io/tenant": "bad/value"}
		}},
	}

	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			cp := valid
			tc.mutate(&cp)
			if err := cp.Validate(24 * time.Hour); err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			}
		})
	}
}

func TestSpecHashEquivalenceAndCollisions(t *testing.T) {
	// Nil and empty labels must hash identically, and key order must not
	// matter.
	h1 := provider.SpecHashOf("py", nil, 60*time.Second)
	h2 := provider.SpecHashOf("py", map[string]string{}, 60*time.Second)
	if h1 != h2 {
		t.Fatalf("nil and empty labels produced different hashes: %s vs %s", h1, h2)
	}

	h3 := provider.SpecHashOf("py", map[string]string{"a": "1", "b": "2"}, time.Minute)
	h4 := provider.SpecHashOf("py", map[string]string{"b": "2", "a": "1"}, time.Minute)
	if h3 != h4 {
		t.Fatalf("label ordering changed hash: %s vs %s", h3, h4)
	}

	// Length prefixing must separate ("ab", "") from ("a", "b").
	if provider.SpecHashOf("ab", map[string]string{"k": ""}, time.Minute) ==
		provider.SpecHashOf("a", map[string]string{"bk": ""}, time.Minute) {
		t.Fatal("distinct specs collided")
	}

	// Different cluster IDs must produce distinct OwnerIDs even for identical
	// release names and namespaces.
	o1 := provider.OwnerID("sandbox-scheduler", "sandbox-scheduler", "uid-cluster-1")
	o2 := provider.OwnerID("sandbox-scheduler", "sandbox-scheduler", "uid-cluster-2")
	if o1 == o2 {
		t.Fatalf("distinct clusters produced identical owner IDs: %s", o1)
	}
}

func FuzzValidateKey(f *testing.F) {
	for _, seed := range []string{"", "a", "abc-123_def.456:789", "-bad", "bad-", "a/b", strings.Repeat("x", 130)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		req := provider.ClaimRequest{
			Template: "py",
			Key:      key,
			TTL:      time.Minute,
		}
		err := req.Validate(time.Hour)
		if err == nil {
			if key == "" || len(key) > 128 {
				t.Fatalf("accepted out-of-bounds key %q", key)
			}
		}
	})
}

func FuzzLabels(f *testing.F) {
	f.Add("sandbox.users.io/team", "ml")
	f.Add("sub.sandbox.users.io/env", "staging")
	f.Add("other.io/team", "ml")
	f.Add("sandbox.users.io/", "ml")
	f.Add("/team", "ml")
	f.Fuzz(func(t *testing.T, k, v string) {
		req := provider.ClaimRequest{
			Template: "py",
			Key:      "valid-key",
			TTL:      time.Minute,
			Labels:   map[string]string{k: v},
		}
		err := req.Validate(time.Hour)
		if err == nil {
			if !strings.Contains(k, "/") {
				t.Fatalf("accepted label without domain: %q=%q", k, v)
			}
		}
	})
}
