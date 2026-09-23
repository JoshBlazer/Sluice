package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/tenant"
)

// Dashboard data endpoints. Everything is scoped to the calling tenant; the
// operator-wide view lives in sluice-cli dump-scheduler.

type queueDepth struct {
	TenantID uuid.UUID `json:"tenant_id"`
	Tenant   string    `json:"tenant"`
	Priority string    `json:"priority"`
	Depth    int64     `json:"depth"`
}

type statsSnapshot struct {
	Queues      []queueDepth     `json:"queues"`
	JobsByState map[string]int64 `json:"jobs_by_state"`
	Timestamp   time.Time        `json:"timestamp"`
}

func (s *Server) snapshot(ctx context.Context, t *tenant.Tenant) (*statsSnapshot, error) {
	depths, err := s.queue.Depths(ctx, []uuid.UUID{t.ID})
	if err != nil {
		return nil, err
	}
	counts, err := storage.CountJobsByState(ctx, s.db, t.ID)
	if err != nil {
		return nil, err
	}
	snap := &statsSnapshot{JobsByState: counts, Timestamp: time.Now().UTC()}
	for _, d := range depths {
		snap.Queues = append(snap.Queues, queueDepth{TenantID: d.TenantID, Tenant: t.Name, Priority: d.Priority, Depth: d.Depth})
	}
	return snap, nil
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	snap, err := s.snapshot(r.Context(), t)
	if err != nil {
		slog.Error("stats snapshot", "tenant_id", t.ID, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to read stats")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleRecentRuns(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	runs, err := storage.ListRecentRuns(r.Context(), s.db, t.ID, limitParam(r))
	if err != nil {
		slog.Error("list recent runs", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs, "count": len(runs)})
}

func (s *Server) handleDeadLetter(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	entries, err := storage.ListDeadLetter(r.Context(), s.db, t.ID, limitParam(r))
	if err != nil {
		slog.Error("list dead letter", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list dead letter")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "count": len(entries)})
}

func limitParam(r *http.Request) int {
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			return n
		}
	}
	return 50
}
