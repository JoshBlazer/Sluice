package api

import (
	"context"
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

type Server struct {
	db      *pgxpool.Pool
	queue   *queue.Queue
	limiter *ratelimit.Limiter
	server  *http.Server
}

func New(db *pgxpool.Pool, q *queue.Queue, limiter *ratelimit.Limiter, port int) *Server {
	s := &Server{db: db, queue: q, limiter: limiter}

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
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/ws", s.handleWebSocket)

	r.Route("/v1", func(r chi.Router) {
		r.Use(authMiddleware(db))

		r.Post("/jobs", s.handleSubmitJob)
		r.Get("/jobs", s.handleListJobs)
		r.Get("/jobs/{id}", s.handleGetJob)
		r.Get("/jobs/{id}/runs", s.handleListJobRuns)
		r.Post("/jobs/{id}/cancel", s.handleCancelJob)
		r.Post("/jobs/{id}/replay", s.handleReplayJob)

		r.Post("/schedules", s.handleCreateSchedule)
		r.Get("/schedules", s.handleListSchedules)
		r.Get("/schedules/{id}", s.handleGetSchedule)
		r.Delete("/schedules/{id}", s.handleDeleteSchedule)

		// Dashboard data endpoints, scoped to the calling tenant.
		r.Get("/stats", s.handleStats)
		r.Get("/stats/runs", s.handleRecentRuns)
		r.Get("/stats/dead-letter", s.handleDeadLetter)
	})

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
