package adaptertest_test

import (
	"context"
	"testing"

	"github.com/NolanFoster/sandbox-scheduler/pkg/provider/adaptertest"
)

func TestFakeBinderPassesConformanceSuite(t *testing.T) {
	adaptertest.TestBinder(t, func(t *testing.T) adaptertest.Harness {
		f := adaptertest.NewFake("fake-cluster", 5, false)
		return adaptertest.Harness{
			Provider:     f.Provider(true),
			InjectFault:  f.InjectFault,
			MarkDeleting: f.MarkDeleting,
			CallCount:    f.CallCount,
		}
	})
}

func TestFakeListOwnedPagination(t *testing.T) {
	f := adaptertest.NewFake("fake-cluster", 5, false)
	ctx := context.Background()
	for _, k := range []string{"k1", "k2", "k3", "k4", "k5"} {
		req := f.SampleRequestForTest(k)
		if _, err := f.Claim(ctx, req); err != nil {
			t.Fatalf("Claim %s: %v", k, err)
		}
	}

	page1, next, err := f.ListOwned(ctx, f.SampleRequestForTest("k1").Owner, "", 2)
	if err != nil || len(page1) != 2 || next == "" {
		t.Fatalf("page1 len=%d next=%q err=%v", len(page1), next, err)
	}
	page2, next, err := f.ListOwned(ctx, f.SampleRequestForTest("k1").Owner, next, 2)
	if err != nil || len(page2) != 2 || next == "" {
		t.Fatalf("page2 len=%d next=%q err=%v", len(page2), next, err)
	}
	page3, next, err := f.ListOwned(ctx, f.SampleRequestForTest("k1").Owner, next, 2)
	if err != nil || len(page3) != 1 || next != "" {
		t.Fatalf("page3 len=%d next=%q err=%v", len(page3), next, err)
	}
}
