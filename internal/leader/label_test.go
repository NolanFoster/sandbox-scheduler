package leader_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/NolanFoster/sandbox-scheduler/internal/leader"
)

const ns, name = "sandbox-scheduler", "scheduler-0"

func pod(labels map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: labels}}
}

func labelOf(t *testing.T, c client.Client) (string, bool) {
	t.Helper()
	var p corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &p); err != nil {
		t.Fatalf("reading pod: %v", err)
	}
	v, ok := p.Labels[leader.ActiveLabel]
	return v, ok
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStaleLabelIsClearedBeforeReady(t *testing.T) {
	// A leader that crashed restarts in the same pod, still labeled. Until the
	// label is gone it must not report Ready, or it draws placement traffic as
	// a follower.
	c := fake.NewClientBuilder().WithObjects(pod(map[string]string{
		leader.ActiveLabel: "true",
		"app":              "kept",
	})).Build()
	l := &leader.PodLabeler{Client: c, Namespace: ns, Name: name, Elected: make(chan struct{}), Log: logr.Discard()}

	if l.Ready(nil) == nil {
		t.Fatal("ready before the stale label was cleared")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Start(ctx) //nolint:errcheck

	eventually(t, "readiness", func() bool { return l.Ready(nil) == nil })
	if _, ok := labelOf(t, c); ok {
		t.Fatal("stale active label survived startup")
	}
	var p corev1.Pod
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &p)
	if p.Labels["app"] != "kept" {
		t.Fatalf("other labels must be left alone: %v", p.Labels)
	}
}

func TestLabelFollowsLeadership(t *testing.T) {
	c := fake.NewClientBuilder().WithObjects(pod(nil)).Build()
	elected := make(chan struct{})
	l := &leader.PodLabeler{Client: c, Namespace: ns, Name: name, Elected: elected, Log: logr.Discard()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = l.Start(ctx); close(done) }()

	eventually(t, "readiness", func() bool { return l.Ready(nil) == nil })
	if _, ok := labelOf(t, c); ok {
		t.Fatal("a standby must not carry the active label")
	}

	close(elected)
	eventually(t, "the active label", func() bool { v, _ := labelOf(t, c); return v == "true" })

	// Shutdown, or losing the lease, which also stops the manager.
	cancel()
	<-done
	if _, ok := labelOf(t, c); ok {
		t.Fatal("the active label should be removed on the way out")
	}
}

func TestStartReturnsWhenStoppedBeforeElection(t *testing.T) {
	c := fake.NewClientBuilder().WithObjects(pod(nil)).Build()
	l := &leader.PodLabeler{Client: c, Namespace: ns, Name: name, Elected: make(chan struct{}), Log: logr.Discard()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = l.Start(ctx); close(done) }()
	eventually(t, "readiness", func() bool { return l.Ready(nil) == nil })

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after its context ended")
	}
	if _, ok := labelOf(t, c); ok {
		t.Fatal("a replica that never led must not end up labeled")
	}
}
