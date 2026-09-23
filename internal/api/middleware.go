package api

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sluice/internal/metrics"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/telemetry"
	"github.com/sluice/internal/tenant"
)

// authMiddleware reads Authorization: Bearer <api_key>, looks up the tenant,
// and injects it into the request context. Rejects with 401 if missing or invalid.
func authMiddleware(db *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractBearerToken(r)
			if key == "" {
				writeError(w, http.StatusUnauthorized, "missing api key")
				return
			}
			t, err := storage.GetTenantByAPIKey(r.Context(), db, key)
			if err != nil {
				if err != storage.ErrNotFound {
					slog.Error("tenant lookup", "err", err)
				}
				writeError(w, http.StatusUnauthorized, "invalid api key")
				return
			}
			ctx := tenant.WithTenant(r.Context(), t)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// correlationMiddleware injects a correlation ID into every request context.
// It honours an incoming X-Request-ID header; if absent it generates a new UUID.
func correlationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		ctx := telemetry.WithCorrelationID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func extractBearerToken(r *http.Request) string {
	hdr := r.Header.Get("Authorization")
	if hdr == "" {
		return ""
	}
	parts := strings.SplitN(hdr, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

// metricsMiddleware records sluice_http_request_duration_seconds, labelled by the
// matched route pattern (not the raw path) to keep label cardinality bounded.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		pattern := chi.RouteContext(r.Context()).RoutePattern()
		if pattern == "" {
			pattern = "unmatched"
		}
		metrics.HTTPRequestDuration.
			WithLabelValues(r.Method, pattern, strconv.Itoa(ww.Status())).
			Observe(time.Since(start).Seconds())
	})
}
