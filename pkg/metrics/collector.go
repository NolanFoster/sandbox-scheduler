package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// StatusSource is what the collector reads. Satisfied by *registry.Registry.
type StatusSource interface {
	Statuses() []registry.Status
}

// registryCollector exposes provider state as gauges.
//
// Implemented as a Collector rather than gauges updated on a timer, so the
// values are read at scrape time and are always exactly what the scheduler
// believes right now. A timer-updated gauge drifts from reality between ticks,
// and that drift shows up precisely during the incidents these metrics exist to
// explain — a provider that went unreachable thirty seconds ago should not
// still be reporting healthy to a dashboard.
//
// It also means a provider that is removed simply stops being reported, with no
// stale series left behind claiming capacity that no longer exists.
type registryCollector struct {
	source StatusSource

	warmCapacity *prometheus.Desc
	reachable    *prometheus.Desc
	staleness    *prometheus.Desc
	cost         *prometheus.Desc
}

// NewRegistryCollector builds a collector over a capacity registry.
func NewRegistryCollector(source StatusSource) prometheus.Collector {
	return &registryCollector{
		source: source,
		warmCapacity: prometheus.NewDesc(
			namespace+"_provider_warm_capacity",
			"Pre-warmed sandboxes a provider reported at its last successful poll.",
			[]string{"provider"}, nil,
		),
		reachable: prometheus.NewDesc(
			namespace+"_provider_reachable",
			"1 when a provider's capacity report is recent enough to trust, 0 otherwise. "+
				"A provider at 0 is still a placement candidate — it is demoted, not excluded.",
			[]string{"provider"}, nil,
		),
		staleness: prometheus.NewDesc(
			namespace+"_provider_report_age_seconds",
			"Age of a provider's last successful capacity report. Grows without bound "+
				"while a provider is unreachable, which is what makes it alertable.",
			[]string{"provider"}, nil,
		),
		cost: prometheus.NewDesc(
			namespace+"_provider_cost_per_hour",
			"Operator-declared relative cost of a sandbox-hour. Units are the operator's own; "+
				"only the ordering between providers is meaningful.",
			[]string{"provider"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *registryCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.warmCapacity
	ch <- c.reachable
	ch <- c.staleness
	ch <- c.cost
}

// Collect implements prometheus.Collector.
func (c *registryCollector) Collect(ch chan<- prometheus.Metric) {
	for _, st := range c.source.Statuses() {
		ch <- prometheus.MustNewConstMetric(
			c.warmCapacity, prometheus.GaugeValue, float64(st.Report.WarmCapacity), st.Provider)
		ch <- prometheus.MustNewConstMetric(
			c.reachable, prometheus.GaugeValue, boolValue(st.Reachable), st.Provider)
		ch <- prometheus.MustNewConstMetric(
			c.staleness, prometheus.GaugeValue, st.Age.Seconds(), st.Provider)
		ch <- prometheus.MustNewConstMetric(
			c.cost, prometheus.GaugeValue, st.Config.CostPerHour, st.Provider)
	}
}

// RegisterRegistryCollector adds provider gauges to the controller-runtime
// registry, so they appear on the same metrics endpoint as everything else.
func RegisterRegistryCollector(source StatusSource) error {
	return ctrlmetrics.Registry.Register(NewRegistryCollector(source))
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
