package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Queue depth per priority bucket and tenant.
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sluice_queue_depth",
		Help: "Jobs waiting in each Redis priority queue per tenant.",
	}, []string{"priority", "tenant_id"})

	// Job lifecycle counters.
	JobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_jobs_total",
		Help: "Total jobs that reached a terminal state.",
	}, []string{"type", "state", "tenant_id"})

	JobRetriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_job_retries_total",
		Help: "Total job retry attempts.",
	}, []string{"type", "tenant_id"})

	// Execution latency histogram.
	JobDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sluice_job_duration_seconds",
		Help:    "Job execution wall-clock duration.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"type", "state", "tenant_id"})

	// Claims skipped because a tenant was at its max_concurrency.
	TenantThrottledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sluice_tenant_throttled_total",
		Help: "Jobs put back because their tenant was at its concurrency limit.",
	}, []string{"tenant_id"})

	// Worker in-flight gauge.
	WorkerInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sluice_worker_jobs_in_flight",
		Help: "Number of jobs currently executing on this worker.",
	})

	// Scheduler leader status (1 = leader, 0 = standby).
	SchedulerIsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sluice_scheduler_is_leader",
		Help: "1 if this scheduler instance currently holds the leader lease.",
	})

	// HTTP request duration (API role).
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sluice_http_request_duration_seconds",
		Help:    "API HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path", "status"})
)
