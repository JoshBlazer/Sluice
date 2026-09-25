package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/tenant"
)

type createScheduleRequest struct {
	Name        string          `json:"name"`
	Cron        string          `json:"cron"`
	Timezone    string          `json:"timezone,omitempty"`
	JobTemplate json.RawMessage `json:"job_template"`
}

func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())

	var req createScheduleRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Cron == "" {
		writeError(w, http.StatusBadRequest, "cron is required")
		return
	}
	if len(req.JobTemplate) == 0 {
		writeError(w, http.StatusBadRequest, "job_template is required")
		return
	}
	if err := validateTemplate(t.ID, req.JobTemplate); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	tz := req.Timezone
	if tz == "" {
		tz = "UTC"
	}
	nextRunAt, err := nextRun(req.Cron, tz)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	sched := &storage.Schedule{
		ID:          uuid.New(),
		TenantID:    t.ID,
		Name:        req.Name,
		Cron:        req.Cron,
		Timezone:    tz,
		JobTemplate: req.JobTemplate,
		Enabled:     true,
		NextRunAt:   nextRunAt,
	}

	if err := storage.InsertSchedule(r.Context(), s.db, sched); err != nil {
		if errors.Is(err, storage.ErrDuplicate) {
			writeError(w, http.StatusConflict, "schedule name already exists")
			return
		}
		slog.Error("insert schedule", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to create schedule")
		return
	}

	writeJSON(w, http.StatusCreated, sched)
}

func (s *Server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	schedules, err := storage.ListSchedules(r.Context(), s.db, t.ID)
	if err != nil {
		slog.Error("list schedules", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list schedules")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": schedules, "count": len(schedules)})
}

func (s *Server) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid schedule id")
		return
	}
	sched, err := storage.GetSchedule(r.Context(), s.db, id, t.ID)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	if err != nil {
		slog.Error("get schedule", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get schedule")
		return
	}
	writeJSON(w, http.StatusOK, sched)
}

func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid schedule id")
		return
	}
	if err := storage.DeleteSchedule(r.Context(), s.db, id, t.ID); errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	} else if err != nil {
		slog.Error("delete schedule", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to delete schedule")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validateTemplate(tenantID uuid.UUID, raw json.RawMessage) error {
	var tmpl job.Template
	if err := json.Unmarshal(raw, &tmpl); err != nil {
		return errors.New("invalid job_template")
	}
	if _, err := tmpl.Build(tenantID, time.Now()); err != nil {
		return errors.New("invalid job_template: " + err.Error())
	}
	return nil
}

// nextRun is the schedule's next occurrence after now, in its timezone.
func nextRun(cronExpr, tz string) (time.Time, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Time{}, errors.New("invalid timezone")
	}
	expr, err := cronParser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, errors.New("invalid cron expression: " + err.Error())
	}
	return expr.Next(time.Now().In(loc)), nil
}

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type updateScheduleRequest struct {
	Enabled     *bool           `json:"enabled,omitempty"`
	Cron        *string         `json:"cron,omitempty"`
	Timezone    *string         `json:"timezone,omitempty"`
	JobTemplate json.RawMessage `json:"job_template,omitempty"`
}

// handleUpdateSchedule pauses, resumes or edits a schedule. Changing the cron or
// timezone, or resuming a paused schedule, recomputes the next run from now, so
// a schedule resumed after a pause doesn't fire a burst of missed occurrences.
func (s *Server) handleUpdateSchedule(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid schedule id")
		return
	}
	var req updateScheduleRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	sched, err := storage.GetSchedule(r.Context(), s.db, id, t.ID)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	if err != nil {
		slog.Error("get schedule", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to update schedule")
		return
	}

	recompute := false
	if req.Cron != nil && *req.Cron != sched.Cron {
		sched.Cron, recompute = *req.Cron, true
	}
	if req.Timezone != nil && *req.Timezone != sched.Timezone {
		sched.Timezone, recompute = *req.Timezone, true
	}
	if req.Enabled != nil {
		if *req.Enabled && !sched.Enabled {
			recompute = true
		}
		sched.Enabled = *req.Enabled
	}
	if len(req.JobTemplate) > 0 {
		if err := validateTemplate(t.ID, req.JobTemplate); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		sched.JobTemplate = req.JobTemplate
	}
	if recompute {
		next, err := nextRun(sched.Cron, sched.Timezone)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		sched.NextRunAt = next
	}

	if err := storage.UpdateSchedule(r.Context(), s.db, sched); err != nil {
		slog.Error("update schedule", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to update schedule")
		return
	}
	writeJSON(w, http.StatusOK, sched)
}
