// Command sandbox-scheduler places agent sandboxes across clusters and
// providers.
//
// It runs two things in one process: a controller-runtime manager reconciling
// the placement API, and an HTTP server answering placement questions.
//
// They share state through the manager's informer cache rather than through
// copies. A List against that cache is a memory read, so the HTTP path stays
// off the API server entirely — which is the whole reason decisions are served
// over HTTP rather than through a CRD write per placement.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/internal/controller"
	_ "github.com/NolanFoster/sandbox-scheduler/pkg/adapter/agentsandbox"
	schedmetrics "github.com/NolanFoster/sandbox-scheduler/pkg/metrics"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
	"github.com/NolanFoster/sandbox-scheduler/pkg/scheduler"
)

var scheme = runtime.NewScheme()

func init() {
	utilRuntimeMust(clientgoscheme.AddToScheme(scheme))
	utilRuntimeMust(v1alpha1.AddToScheme(scheme))
}

func utilRuntimeMust(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	var (
		metricsAddr     string
		probeAddr       string
		schedulerAddr   string
		secretNamespace string
		refreshInterval time.Duration
		staleAfter      time.Duration
		leaderElection  bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address for the metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address for health and readiness probes.")
	flag.StringVar(&schedulerAddr, "scheduler-bind-address", ":8082", "Address for the placement API.")
	flag.StringVar(&secretNamespace, "secret-namespace", envOr("POD_NAMESPACE", "sandbox-scheduler"),
		"Namespace read for provider credentials. Confining this to the scheduler's own namespace "+
			"stops a cluster-scoped SandboxProvider referencing a Secret anywhere in the cluster.")
	flag.DurationVar(&refreshInterval, "refresh-interval", 10*time.Second,
		"How often provider capacity is polled. Off the placement path, so this trades freshness "+
			"against load on providers, never placement latency.")
	flag.DurationVar(&staleAfter, "stale-after", 30*time.Second,
		"How long a capacity report stays trusted before its provider is treated as unreachable.")
	flag.BoolVar(&leaderElection, "leader-elect", false,
		"Elect a leader for the controllers. Note the placement API is served by every replica: "+
			"it is a read over local cache, so serving it from a single leader would add a hop for no gain.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElection,
		LeaderElectionID:       "sandbox-scheduler.placement.agents.x-k8s.io",
	})
	if err != nil {
		log.Error(err, "unable to start manager")
		os.Exit(1)
	}

	reg := registry.New(registry.Options{StaleAfter: staleAfter})

	// Provider gauges are collected at scrape time, so they always reflect what
	// the scheduler believes right now rather than what a timer last copied.
	if err := schedmetrics.RegisterRegistryCollector(reg); err != nil {
		log.Error(err, "unable to register provider metrics")
		os.Exit(1)
	}

	if err := (&controller.SandboxProviderReconciler{
		Client:          mgr.GetClient(),
		Registry:        reg,
		SecretNamespace: secretNamespace,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to create controller", "controller", "SandboxProvider")
		os.Exit(1)
	}
	if err := (&controller.SandboxPlacementPolicyReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to create controller", "controller", "SandboxPlacementPolicy")
		os.Exit(1)
	}

	// Capacity refresh, on its own schedule and independent of reconcile timing.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		reg.Run(ctx, refreshInterval)
		return nil
	})); err != nil {
		log.Error(err, "unable to add the capacity refresher")
		os.Exit(1)
	}

	svc := &scheduler.Service{
		Registry:  reg,
		Policies:  &cachedPolicies{client: mgr.GetClient()},
		Endpoints: endpointLookup(mgr.GetClient()),
	}
	if err := mgr.Add(&httpServer{addr: schedulerAddr, handler: svc.Handler(), log: ctrl.Log.WithName("placement-api")}); err != nil {
		log.Error(err, "unable to add the placement API server")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	log.Info("starting sandbox-scheduler",
		"placementAPI", schedulerAddr, "refreshInterval", refreshInterval, "staleAfter", staleAfter)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}

// cachedPolicies reads policies from the manager's informer cache.
//
// A List here does not reach the API server, so the placement path stays a
// memory read. Invalid policies are left in: the service reports them as 422
// against the specific request rather than silently applying a different
// policy, which would be much harder to notice.
type cachedPolicies struct{ client client.Client }

func (c *cachedPolicies) Policies() []v1alpha1.SandboxPlacementPolicy {
	var list v1alpha1.SandboxPlacementPolicyList
	if err := c.client.List(context.Background(), &list); err != nil {
		// Returning nothing makes the service fall back to the default policy,
		// which schedules sensibly. Failing the request instead would turn a
		// cache hiccup into an outage.
		ctrl.Log.WithName("placement-api").Error(err, "listing policies; falling back to the default policy")
		return nil
	}
	return list.Items
}

// endpointLookup resolves a provider name to its endpoint, so a caller can act
// on a decision without keeping its own copy of the provider registry.
func endpointLookup(c client.Client) scheduler.EndpointLookup {
	return func(provider string) string {
		var p v1alpha1.SandboxProvider
		if err := c.Get(context.Background(), types.NamespacedName{Name: provider}, &p); err != nil {
			return ""
		}
		return p.Spec.Endpoint
	}
}

// httpServer runs the placement API under the manager's lifecycle so it shuts
// down with everything else rather than being killed mid-request.
type httpServer struct {
	addr    string
	handler http.Handler
	log     logrLogger
}

type logrLogger interface {
	Info(msg string, keysAndValues ...any)
	Error(err error, msg string, keysAndValues ...any)
}

func (s *httpServer) Start(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("serving placement API", "addr", s.addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("placement API: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

// NeedLeaderElection reports false: the placement API is a read over local
// cache, so every replica can serve it. Restricting it to the leader would add
// a hop and a single point of failure for no benefit.
func (s *httpServer) NeedLeaderElection() bool { return false }

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
