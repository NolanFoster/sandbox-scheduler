// Package leader routes placement traffic to the elected replica.
//
// Placement reserves warm capacity in the deciding replica's memory, so it is
// only exact while every placement goes through one replica. Every replica
// stays Ready, so rollouts and metrics scraping work as usual, but only the
// leader carries ActiveLabel, and the placement Service selects on it.
package leader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ActiveLabel marks the pod currently serving placement.
const ActiveLabel = "placement.agents.x-k8s.io/active"

// retryInterval paces label patches that fail, typically on an API server
// blip. Failing the manager instead would restart the pod and, for the
// leader, hand leadership away over a transient error.
const retryInterval = 2 * time.Second

// PodLabeler keeps ActiveLabel on its own pod while this replica leads, and
// off it otherwise.
type PodLabeler struct {
	Client    client.Client
	Namespace string
	Name      string
	// Elected closes when this replica becomes leader.
	Elected <-chan struct{}
	Log     logr.Logger

	cleared atomic.Bool
}

// NeedLeaderElection reports false: the label has to be cleared on every
// replica, not only the leader.
func (l *PodLabeler) NeedLeaderElection() bool { return false }

// Start clears the label, waits to be elected, then sets it.
//
// The clear comes first because labels outlive containers: a leader that
// crashed or lost its lease restarts in the same pod, still labeled, and would
// otherwise draw placement traffic as a follower once Ready again. Ready
// therefore fails until the clear has landed.
//
// Losing leadership needs no handling here. The manager exits the process,
// which cancels ctx; the label is removed on the way out where possible, and
// on restart otherwise.
func (l *PodLabeler) Start(ctx context.Context) error {
	if !l.patch(ctx, nil) {
		return nil
	}
	l.cleared.Store(true)

	select {
	case <-ctx.Done():
		return nil
	case <-l.Elected:
	}
	active := "true"
	if !l.patch(ctx, &active) {
		return nil
	}
	l.Log.Info("serving placement as leader", "pod", l.Name)

	<-ctx.Done()
	// Best effort, and bounded: shutdown must not hang on an unreachable API
	// server, and the next start clears the label anyway.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = l.patchOnce(shutdownCtx, nil)
	return nil
}

// Ready is a healthz.Checker that fails until any stale label is cleared.
func (l *PodLabeler) Ready(_ *http.Request) error {
	if !l.cleared.Load() {
		return errors.New("clearing a stale " + ActiveLabel + " label from a previous run")
	}
	return nil
}

// patch sets the label to value, or removes it when value is nil, retrying
// until it succeeds. It reports false if ctx ended first.
func (l *PodLabeler) patch(ctx context.Context, value *string) bool {
	for {
		err := l.patchOnce(ctx, value)
		if err == nil {
			return true
		}
		l.Log.Error(err, "patching pod label; retrying", "label", ActiveLabel, "pod", l.Name)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(retryInterval):
		}
	}
}

func (l *PodLabeler) patchOnce(ctx context.Context, value *string) error {
	// A merge patch with null deletes the key, and is a no-op when it is
	// already absent, so clearing needs no read first.
	var v any
	if value != nil {
		v = *value
	}
	body := map[string]any{"metadata": map[string]any{"labels": map[string]any{ActiveLabel: v}}}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: l.Namespace, Name: l.Name}}
	if err := l.Client.Patch(ctx, pod, client.RawPatch(types.MergePatchType, raw)); err != nil {
		return fmt.Errorf("pod %s/%s: %w", l.Namespace, l.Name, err)
	}
	return nil
}
