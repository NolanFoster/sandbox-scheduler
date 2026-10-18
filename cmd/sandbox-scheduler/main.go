// Command sandbox-scheduler places agent sandboxes across clusters and
// providers.
//
// It runs two things in one process: a controller-runtime manager reconciling
// the placement API, and an HTTP server answering placement and claim
// requests.
//
// With --leader-elect, every replica keeps a warm registry but only the leader
// serves placement and claims: reservations and per-key singleflight state
// live in the deciding replica's memory, so they are only exact while one
// replica makes every decision. See internal/leader.
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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/internal/controller"
	"github.com/NolanFoster/sandbox-scheduler/internal/leader"
	_ "github.com/NolanFoster/sandbox-scheduler/pkg/adapter/agentsandbox"
	"github.com/NolanFoster/sandbox-scheduler/pkg/binder"
	schedmetrics "github.com/NolanFoster/sandbox-scheduler/pkg/metrics"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
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
		metricsAddr       string
		probeAddr         string
		schedulerAddr     string
		secretNamespace   string
		refreshInterval   time.Duration
		staleAfter        time.Duration
		reservationGrace  time.Duration
		coldStartWindow   time.Duration
		leaderElection    bool
		enableClaims      bool
		claimTimeout      time.Duration
		claimMaxInFlight  int
		claimQueueTimeout time.Duration
		claimMaxTTL       time.Duration
		maxSpill          int
		maxJoinersPerKey  int
		refusalTTL        time.Duration
		clusterID         string
		releaseName       string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address for the metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address for health and readiness probes.")
	flag.StringVar(&schedulerAddr, "scheduler-bind-address", ":8082", "Address for the placement API.")
	flag.StringVar(&secretNamespace, "secret-namespace", envOr("POD_NAMESPACE", "sandbox-scheduler"),
		"Namespace read for provider credentials. Confining this to the scheduler's own namespace "+
			"stops a cluster-scoped SandboxProvider referencing a Secret anywhere in the cluster.")
	flag.DurationVar(&refreshInterval, "refresh-interval", 10*time.Second,
		"How often provider capacity is polled, for providers that set no spec.refreshInterval of "+
			"their own. Off the placement path, so this trades freshness against load on providers, "+
			"never placement latency.")
	flag.DurationVar(&staleAfter, "stale-after", 30*time.Second,
		"How long a capacity report stays trusted before its provider is treated as unreachable.")
	flag.DurationVar(&reservationGrace, "reservation-grace", 5*time.Second,
		"How long a claim may take to appear in a provider's capacity report. Placements are "+
			"subtracted from a provider's warm capacity until a poll started this long after them succeeds.")
	flag.DurationVar(&coldStartWindow, "cold-start-window", 30*time.Second,
		"How long a cold placement counts as in flight. Ties between providers with no warm "+
			"capacity go to the one with fewer cold starts in flight.")
	flag.BoolVar(&leaderElection, "leader-elect", false,
		"Elect a leader. Required with more than one replica: only the leader serves placement and "+
			"writes status, while the others keep a warm registry to take over from. Set POD_NAME so "+
			"the leader can label its own pod for the placement Service to select.")
	flag.BoolVar(&enableClaims, "enable-claims", false,
		"Enable scheduler-managed sandbox claims on POST/DELETE /api/v1alpha1/sandboxes.")
	flag.DurationVar(&claimTimeout, "claim-timeout", binder.DefaultClaimTimeout,
		"Default end-to-end request deadline for a sandbox claim.")
	flag.IntVar(&claimMaxInFlight, "claim-max-inflight", binder.DefaultGlobalMaxInFlight,
		"Global maximum concurrent sandbox claim requests (including joiners).")
	flag.DurationVar(&claimQueueTimeout, "claim-queue-timeout", binder.DefaultQueueTimeout,
		"How long to wait for a provider in-flight slot to open when all viable providers are full.")
	flag.DurationVar(&claimMaxTTL, "claim-max-ttl", binder.DefaultMaxTTL,
		"Maximum sandbox TTL permitted on a claim request.")
	flag.IntVar(&maxSpill, "max-spill", binder.DefaultMaxSpill,
		"Maximum number of providers tried per claim request.")
	flag.IntVar(&maxJoinersPerKey, "max-joiners-per-key", binder.DefaultMaxJoinersPerKey,
		"Maximum concurrent requests that may join an in-flight claim for the same key.")
	flag.DurationVar(&refusalTTL, "refusal-ttl", binder.DefaultRefusalTTL,
		"How long a provider's NoCapacity refusal for a template suppresses further claims for that template.")
	flag.StringVar(&clusterID, "cluster-id", "",
		"Cluster identifier used to scope sandbox owner labels; defaults to the kube-system Namespace UID when unset.")
	flag.StringVar(&releaseName, "release-name", envOr("RELEASE_NAME", "sandbox-scheduler"),
		"Helm release / deployment fullname used to scope sandbox owner labels.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: metricsAddr},
		// Credentials are only ever read from the secret namespace, so only
		// that namespace's Secrets are cached. Unscoped, the provider
		// controller's Secret watch would hold every Secret in the cluster in
		// this process's memory.
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}: {Namespaces: map[string]cache.Config{secretNamespace: {}}},
		}},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElection,
		LeaderElectionID:       "sandbox-scheduler.placement.agents.x-k8s.io",
		// Hand over on a graceful shutdown rather than leaving placement
		// unserved until the lease expires. Safe because main exits as soon as
		// the manager stops.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		log.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// True from election onward; always true when leader election is off.
	elected := func() bool {
		select {
		case <-mgr.Elected():
			return true
		default:
			return false
		}
	}

	reg := registry.New(registry.Options{
		StaleAfter:       staleAfter,
		ReservationGrace: reservationGrace,
		ColdStartWindow:  coldStartWindow,
	})

	var ownerID string
	if enableClaims {
		resolvedClusterID, err := resolveClusterID(mgr.GetAPIReader(), clusterID)
		if err != nil {
			log.Error(err, "unable to resolve cluster ID for sandbox claims; set --cluster-id or grant get on namespace kube-system")
			os.Exit(1)
		}
		ownerID = provider.OwnerID(releaseName, envOr("POD_NAMESPACE", secretNamespace), resolvedClusterID)
	} else {
		ownerID = provider.OwnerID(releaseName, envOr("POD_NAMESPACE", secretNamespace), clusterID)
	}

	claimBinder := binder.New(binder.Options{
		Registry:          reg,
		Owner:             ownerID,
		ClaimTimeout:      claimTimeout,
		QueueTimeout:      claimQueueTimeout,
		RefusalTTL:        refusalTTL,
		MaxTTL:            claimMaxTTL,
		MaxSpill:          maxSpill,
		GlobalMaxInFlight: claimMaxInFlight,
		MaxJoinersPerKey:  maxJoinersPerKey,
	})

	// Provider gauges are collected at scrape time, so they always reflect what
	// the scheduler believes right now rather than what a timer last copied.
	if err := schedmetrics.RegisterRegistryCollector(reg); err != nil {
		log.Error(err, "unable to register provider metrics")
		os.Exit(1)
	}

	if err := (&controller.SandboxProviderReconciler{
		Client:          mgr.GetClient(),
		Registry:        reg,
		Binder:          claimBinder,
		SecretNamespace: secretNamespace,
		WriteStatus:     elected,
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
	// On every replica, so a standby takes over with current capacity.
	if err := mgr.Add(everyReplica{manager.RunnableFunc(func(ctx context.Context) error {
		reg.Run(ctx, refreshInterval)
		return nil
	})}); err != nil {
		log.Error(err, "unable to add the capacity refresher")
		os.Exit(1)
	}

	// Tokens come from the environment rather than flags: a flag lands in the
	// process table and in `kubectl describe pod` output for anyone with read
	// access to the namespace.
	apiToken := os.Getenv("PLACEMENT_API_TOKEN")
	claimToken := os.Getenv("PLACEMENT_CLAIM_TOKEN")
	if apiToken == "" {
		log.Info("placement API is UNAUTHENTICATED; set PLACEMENT_API_TOKEN before " +
			"exposing it outside the cluster — GET /providers discloses provider " +
			"endpoints and live capacity")
	}

	svc := &scheduler.Service{
		Registry:      reg,
		Policies:      &cachedPolicies{client: mgr.GetClient()},
		Endpoints:     endpointLookup(mgr.GetClient()),
		Binder:        claimBinder,
		ClaimsEnabled: enableClaims,
		Token:         apiToken,
		ClaimToken:    claimToken,
	}
	var labeler *leader.PodLabeler
	if leaderElection {
		svc.Active = elected
		if err := mgr.Add(everyReplica{manager.RunnableFunc(func(ctx context.Context) error {
			select {
			case <-mgr.Elected():
				claimBinder.NotifyElected()
			case <-ctx.Done():
			}
			return nil
		})}); err != nil {
			log.Error(err, "unable to add leader election hold-off hook")
			os.Exit(1)
		}
		if podName := os.Getenv("POD_NAME"); podName != "" {
			labeler = &leader.PodLabeler{
				Client:    mgr.GetClient(),
				Namespace: envOr("POD_NAMESPACE", secretNamespace),
				Name:      podName,
				Elected:   mgr.Elected(),
				Log:       ctrl.Log.WithName("leader"),
			}
			if err := mgr.Add(labeler); err != nil {
				log.Error(err, "unable to add the leader labeler")
				os.Exit(1)
			}
		} else {
			log.Info("POD_NAME is unset, so the leader cannot label its pod; standbys will " +
				"refuse placement requests routed to them")
		}
	}

	shutdownTimeout := 10 * time.Second
	if enableClaims && claimTimeout+5*time.Second > shutdownTimeout {
		shutdownTimeout = claimTimeout + 5*time.Second
	}
	if err := mgr.Add(&httpServer{
		addr:            schedulerAddr,
		handler:         svc.Handler(),
		binder:          claimBinder,
		shutdownTimeout: shutdownTimeout,
		log:             ctrl.Log.WithName("placement-api"),
	}); err != nil {
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
	if labeler != nil {
		if err := mgr.AddReadyzCheck("active-label", labeler.Ready); err != nil {
			log.Error(err, "unable to set up ready check")
			os.Exit(1)
		}
	}

	log.Info("starting sandbox-scheduler",
		"placementAPI", schedulerAddr, "enableClaims", enableClaims,
		"refreshInterval", refreshInterval, "staleAfter", staleAfter,
		"reservationGrace", reservationGrace, "coldStartWindow", coldStartWindow)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}

func resolveClusterID(reader client.Reader, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if reader == nil {
		return "", errors.New("API reader is nil and --cluster-id is unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var ns corev1.Namespace
	if err := reader.Get(ctx, types.NamespacedName{Name: "kube-system"}, &ns); err != nil {
		return "", fmt.Errorf("getting kube-system namespace UID: %w", err)
	}
	if string(ns.UID) == "" {
		return "", errors.New("kube-system namespace has empty UID")
	}
	return string(ns.UID), nil
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
	return func(providerName string) string {
		var p v1alpha1.SandboxProvider
		if err := c.Get(context.Background(), types.NamespacedName{Name: providerName}, &p); err != nil {
			return ""
		}
		return p.Spec.Endpoint
	}
}

// httpServer runs the placement API under the manager's lifecycle so it shuts
// down with everything else rather than being killed mid-request.
type httpServer struct {
	addr            string
	handler         http.Handler
	binder          *binder.Binder
	shutdownTimeout time.Duration
	log             logrLogger
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
		timeout := s.shutdownTimeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		srvErr := srv.Shutdown(shutdownCtx)
		if s.binder != nil {
			_ = s.binder.Shutdown(shutdownCtx)
		}
		return srvErr
	}
}

// NeedLeaderElection reports false: every replica listens, so a new leader
// serves the moment it is elected. Standbys answer GET /providers and refuse
// placement; see scheduler.Service.Active.
func (s *httpServer) NeedLeaderElection() bool { return false }

// everyReplica runs a runnable on every replica rather than only the leader.
type everyReplica struct{ manager.Runnable }

func (everyReplica) NeedLeaderElection() bool { return false }

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
