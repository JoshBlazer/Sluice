package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/telemetry"
	"github.com/sluice/internal/tenant"
)

const maxIdempotencyKeyLen = 255

type submitJobRequest struct {
	job.Template
	RunAt          *time.Time `json:"run_at,omitempty"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())

	var req submitJobRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.IdempotencyKey != nil && (*req.IdempotencyKey == "" || len(*req.IdempotencyKey) > maxIdempotencyKeyLen) {
		writeError(w, http.StatusBadRequest, "idempotency_key must be 1-255 characters")
		return
	}

	now := time.Now()
	j, err := req.Template.Build(t.ID, now)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	j.IdempotencyKey = req.IdempotencyKey
	if tp := telemetry.TraceParent(r.Context()); tp != "" {
		j.TraceParent = &tp
	}
	if req.RunAt != nil && req.RunAt.After(now) {
		j.RunAt = *req.RunAt
		j.State = job.StateScheduled
	}

	if s.limiter != nil {
		ok, err := s.limiter.Allow(r.Context(), t.ID, t.RateLimit)
		if err != nil {
			// Fail open: the job is still durably stored, and rejecting all traffic
			// because Redis blipped is worse than briefly exceeding a quota.
			slog.Error("rate limit check", "tenant_id", t.ID, "err", err)
		} else if !ok {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
	}

	err = storage.InsertJob(r.Context(), s.db, j)
	if errors.Is(err, storage.ErrDuplicate) {
		// Idempotency key conflict — return the original job with 200.
		if req.IdempotencyKey != nil {
			existing, fetchErr := storage.GetJobByIdempotencyKey(r.Context(), s.db, t.ID, *req.IdempotencyKey)
			if fetchErr == nil {
				writeJSON(w, http.StatusOK, existing)
				return
			}
		}
		writeError(w, http.StatusConflict, "duplicate idempotency key")
		return
	}
	if err != nil {
		slog.Error("insert job", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	if j.State == job.StatePending {
		if err := s.queue.Enqueue(r.Context(), j.TenantID, j.ID, j.Priority); err != nil {
			slog.Error("enqueue job", "job_id", j.ID, "err", err)
		}
	}

	writeJSON(w, http.StatusCreated, j)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	j, err := storage.GetJobForTenant(r.Context(), s.db, id, t.ID)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		slog.Error("get job", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// handleWebhookSecret returns the caller's webhook signing secret, which their
// receivers need to verify Sluice's requests.
func (s *Server) handleWebhookSecret(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	writeJSON(w, http.StatusOK, map[string]string{"secret": t.WebhookSecret, "scheme": "standard-webhooks-v1"})
}

// handleListJobRuns returns a job's attempt history, oldest first.
func (s *Server) handleListJobRuns(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	if _, err := storage.GetJobForTenant(r.Context(), s.db, id, t.ID); errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	} else if err != nil {
		slog.Error("get job", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	runs, err := storage.ListJobRuns(r.Context(), s.db, id, t.ID)
	if err != nil {
		slog.Error("list job runs", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs, "count": len(runs)})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	filter := storage.ListFilter{TenantID: &t.ID, Limit: 50}

	if v := r.URL.Query().Get("state"); v != "" {
		st := job.State(v)
		if !st.Valid() {
			writeError(w, http.StatusBadRequest, "unknown state "+strconv.Quote(v))
			return
		}
		filter.State = &st
	}
	if v := r.URL.Query().Get("retried"); v != "" {
		retried, err := strconv.ParseBool(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "retried must be true or false")
			return
		}
		filter.Retried = retried
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			filter.Limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			filter.Offset = n
		}
	}

	jobs, err := storage.ListJobs(r.Context(), s.db, filter)
	if err != nil {
		slog.Error("list jobs", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "count": len(jobs)})
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	if err := storage.CancelJob(r.Context(), s.db, id, t.ID); errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found or not cancellable")
		return
	} else if err != nil {
		slog.Error("cancel job", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleReplayJob(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	j, err := storage.ReplayJob(r.Context(), s.db, id, t.ID)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found or not in dead state")
		return
	}
	if err != nil {
		slog.Error("replay job", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to replay job")
		return
	}
	if err := s.queue.Enqueue(r.Context(), t.ID, id, j.Priority); err != nil {
		slog.Error("enqueue replayed job", "job_id", id, "err", err)
	}
	writeJSON(w, http.StatusOK, j)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
