package api

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/ratelimit"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const maxBodyBytes = 1 << 20

// openAPISpec documents every route; TestOpenAPICoversAllRoutes keeps it in sync.
//
//go:embed openapi.yaml
var openAPISpec []byte

type Server struct {
	db      *pgxpool.Pool
	queue   *queue.Queue
	limiter *ratelimit.Limiter
	tenants *tenantCache
	server  *http.Server
	router  chi.Router

	adminTokenHash []byte // set by EnableAdmin; nil disables /admin/v1
}

func New(db *pgxpool.Pool, q *queue.Queue, limiter *ratelimit.Limiter, port int) *Server {
	s := &Server{db: db, queue: q, limiter: limiter, tenants: newTenantCache(db)}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(metricsMiddleware)
	r.Use(corsMiddleware)
	r.Use(correlationMiddleware)

	// Health + observability (no auth)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r.Get("/readyz", s.handleReady)
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/ws", s.handleWebSocket)
	r.Get("/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write(openAPISpec) //nolint:errcheck
	})

	r.Route("/v1", func(r chi.Router) {
		r.Use(authMiddleware(s.tenants))

		r.Post("/jobs", s.handleSubmitJob)
		r.Get("/jobs", s.handleListJobs)
		r.Get("/jobs/{id}", s.handleGetJob)
		r.Get("/jobs/{id}/runs", s.handleListJobRuns)
		r.Post("/jobs/{id}/cancel", s.handleCancelJob)
		r.Post("/jobs/{id}/replay", s.handleReplayJob)

		r.Get("/webhook-secret", s.handleWebhookSecret)

		r.Post("/schedules", s.handleCreateSchedule)
		r.Get("/schedules", s.handleListSchedules)
		r.Get("/schedules/{id}", s.handleGetSchedule)
		r.Delete("/schedules/{id}", s.handleDeleteSchedule)
		r.Patch("/schedules/{id}", s.handleUpdateSchedule)

		// Dashboard data endpoints, scoped to the calling tenant.
		r.Get("/stats", s.handleStats)
		r.Get("/stats/runs", s.handleRecentRuns)
		r.Get("/stats/dead-letter", s.handleDeadLetter)
	})

	r.Route("/admin/v1", func(r chi.Router) {
		r.Use(s.adminAuth)
		r.Post("/tenants", s.handleAdminCreateTenant)
		r.Get("/tenants", s.handleAdminListTenants)
		r.Get("/tenants/{id}", s.handleAdminGetTenant)
		r.Patch("/tenants/{id}", s.handleAdminUpdateTenant)
		r.Post("/tenants/{id}/rotate-key", s.handleAdminRotateKey)
		r.Post("/tenants/{id}/rotate-webhook-secret", s.handleAdminRotateWebhookSecret)
	})

	s.router = r
	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      s.Handler(r),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// Handler wraps the router with tracing. Exposed separately so tests can
// serve the API from httptest without binding a port.
func (s *Server) Handler(r http.Handler) http.Handler {
	return otelhttp.NewHandler(r, "sluice-api")
}

// Routes returns the fully wrapped HTTP handler.
func (s *Server) Routes() http.Handler {
	return s.server.Handler
}

func (s *Server) Start() error {
	slog.Info("api listening", "addr", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
