package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sluice/internal/api"
	"github.com/sluice/internal/leader"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/ratelimit"
	"github.com/sluice/internal/scheduler"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/telemetry"
	"github.com/sluice/internal/worker"
)

type config struct {
	role            string
	postgresURL     string
	redisAddr       string
	etcdEndpoints   string
	etcd            leader.ClientConfig
	otlpEndpoint    string
	httpPort        int
	metricsPort     int
	shutdownTimeout time.Duration
	webhookPrivate  bool
	concurrency     int
}

func loadConfig() config {
	var c config
	flag.StringVar(&c.role, "role", env("SLUICE_ROLE", ""), "role to run: api | scheduler | worker")
	flag.StringVar(&c.postgresURL, "postgres-url", env("SLUICE_POSTGRES_URL", "postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable"), "postgres connection string")
	flag.StringVar(&c.redisAddr, "redis-addr", env("SLUICE_REDIS_ADDR", "localhost:6379"), "redis host:port, or redis[s]://[user:pass@]host:port[/db] for auth, database and TLS")
	flag.StringVar(&c.etcdEndpoints, "etcd-endpoints", env("SLUICE_ETCD_ENDPOINTS", "localhost:2379"), "comma-separated etcd endpoints")
	flag.StringVar(&c.etcd.Username, "etcd-username", env("SLUICE_ETCD_USERNAME", ""), "etcd username")
	flag.StringVar(&c.etcd.Password, "etcd-password", env("SLUICE_ETCD_PASSWORD", ""), "etcd password (prefer the env var)")
	flag.StringVar(&c.etcd.CAFile, "etcd-ca-file", env("SLUICE_ETCD_CA_FILE", ""), "CA certificate that signs the etcd server certificate; enables TLS")
	flag.StringVar(&c.etcd.CertFile, "etcd-cert-file", env("SLUICE_ETCD_CERT_FILE", ""), "client certificate for etcd mTLS")
	flag.StringVar(&c.etcd.KeyFile, "etcd-key-file", env("SLUICE_ETCD_KEY_FILE", ""), "client key for etcd mTLS")
	flag.StringVar(&c.otlpEndpoint, "otlp-endpoint", env("SLUICE_OTLP_ENDPOINT", "localhost:4318"), "OTLP HTTP trace endpoint")
	flag.IntVar(&c.httpPort, "port", envInt("SLUICE_PORT", 8080), "http port (api role only)")
	flag.IntVar(&c.metricsPort, "metrics-port", envInt("SLUICE_METRICS_PORT", 0), "prometheus metrics port (scheduler=9091, worker=9092 by default)")
	flag.DurationVar(&c.shutdownTimeout, "shutdown-timeout", 30*time.Second, "graceful shutdown timeout")
	flag.IntVar(&c.concurrency, "concurrency", envInt("SLUICE_WORKER_CONCURRENCY", worker.DefaultConcurrency), "jobs a worker runs at once (worker role only)")
	flag.BoolVar(&c.webhookPrivate, "webhook-allow-private", env("SLUICE_WEBHOOK_ALLOW_PRIVATE", "") == "true", "let webhook jobs call loopback/private addresses (local dev only)")
	flag.Parse()
	return c
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	c := loadConfig()
	if c.concurrency < 1 {
		fmt.Fprintln(os.Stderr, "--concurrency must be at least 1")
		os.Exit(1)
	}
	switch c.role {
	case "api", "scheduler", "worker":
	case "":
		fmt.Fprintln(os.Stderr, "usage: sluice --role <api|scheduler|worker>")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q — must be api, scheduler, or worker\n", c.role)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Initialise OTel tracing. Non-fatal if Jaeger is not available.
	otelShutdown, err := telemetry.Init(ctx, "sluice-"+c.role, c.otlpEndpoint)
	if err != nil {
		slog.Warn("OTel init failed — tracing disabled", "err", err)
	} else {
		defer otelShutdown(context.Background()) //nolint:errcheck
	}

	var minConns int32
	if c.role == "worker" {
		// One connection per concurrent job, plus headroom for heartbeats and tenant reloads.
		minConns = int32(c.concurrency) + 4
	}
	db, err := storage.NewPool(ctx, c.postgresURL, minConns)
	if err != nil {
		slog.Error("connect to postgres", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	rdb, err := queue.NewClient(c.redisAddr)
	if err != nil {
		slog.Error("configure redis", "err", err)
		os.Exit(1)
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("connect to redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	q := queue.New(rdb)
	limiter := ratelimit.New(rdb)

	switch c.role {
	case "api":
		runAPI(ctx, c, db, q, limiter)
	case "scheduler":
		startMetricsServer(c.metricsPort, 9091)
		runScheduler(ctx, c, db, q)
	case "worker":
		startMetricsServer(c.metricsPort, 9092)
		runWorker(ctx, c, db, q)
	}
}

// startMetricsServer launches a tiny HTTP server serving only /metrics and /healthz.
// port overrides the default if non-zero.
func startMetricsServer(port, defaultPort int) {
	if port == 0 {
		port = defaultPort
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	addr := fmt.Sprintf(":%d", port)
	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("metrics server error", "err", err)
		}
	}()
}

func runAPI(ctx context.Context, c config, db *pgxpool.Pool, q *queue.Queue, limiter *ratelimit.Limiter) {
	srv := api.New(db, q, limiter, c.httpPort)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		slog.Error("api server error", "err", err)
		return
	}

	slog.Info("shutting down api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), c.shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("api shutdown error", "err", err)
	}
}

func runScheduler(ctx context.Context, c config, db *pgxpool.Pool, q *queue.Queue) {
	c.etcd.Endpoints = strings.Split(c.etcdEndpoints, ",")
	etcdClient, err := leader.NewClient(c.etcd)
	if err != nil {
		slog.Error("connect to etcd", "err", err)
		os.Exit(1)
	}
	defer etcdClient.Close()

	elect := leader.New(etcdClient, "/sluice/scheduler/leader", leader.DefaultTTLSeconds)
	s := scheduler.New(db, q)
	s.Run(ctx, elect)
}

func runWorker(ctx context.Context, c config, db *pgxpool.Pool, q *queue.Queue) {
	w := worker.New(db, q, worker.Options{Concurrency: c.concurrency, AllowPrivateWebhooks: c.webhookPrivate})
	if c.webhookPrivate {
		slog.Warn("webhook jobs may call private and loopback addresses — do not use in production")
	}
	go w.Run(ctx)

	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
	go func() {
		for range sighupCh {
			slog.Info("SIGHUP received — reloading tenant weights")
			w.Reload()
		}
	}()

	<-ctx.Done()
	w.Shutdown(c.shutdownTimeout)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		fmt.Sscanf(v, "%d", &n)
		return n
	}
	return fallback
}
