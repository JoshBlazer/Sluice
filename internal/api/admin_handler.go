package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sluice/internal/storage"
)

// MinAdminTokenLen guards against guessable admin tokens.
const MinAdminTokenLen = 32

// EnableAdmin turns on the /admin/v1 tenant-management API, authenticated by
// token. Without it those routes answer 404. Call before Start.
func (s *Server) EnableAdmin(token string) error {
	if len(token) < MinAdminTokenLen {
		return fmt.Errorf("admin token must be at least %d characters", MinAdminTokenLen)
	}
	sum := sha256.Sum256([]byte(token))
	s.adminTokenHash = sum[:]
	return nil
}

func (s *Server) adminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.adminTokenHash == nil {
			writeError(w, http.StatusNotFound, "admin API is not enabled")
			return
		}
		// Hashing both sides first makes the comparison constant-time regardless of length.
		got := sha256.Sum256([]byte(extractBearerToken(r)))
		if subtle.ConstantTimeCompare(got[:], s.adminTokenHash) != 1 {
			writeError(w, http.StatusUnauthorized, "invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type adminTenant struct {
	ID             uuid.UUID `json:"id"`
	Name           string    `json:"name"`
	Status         string    `json:"status"`
	RateLimit      int       `json:"rate_limit"`
	Weight         int       `json:"weight"`
	MaxConcurrency int       `json:"max_concurrency"`
}

func toAdminTenant(t *storage.Tenant) adminTenant {
	return adminTenant{ID: t.ID, Name: t.Name, Status: t.Status, RateLimit: t.RateLimit, Weight: t.Weight, MaxConcurrency: t.MaxConcurrency}
}

type tenantLimitsRequest struct {
	RateLimit      *int `json:"rate_limit,omitempty"`
	Weight         *int `json:"weight,omitempty"`
	MaxConcurrency *int `json:"max_concurrency,omitempty"`
}

func (l tenantLimitsRequest) validate() error {
	if l.RateLimit != nil && *l.RateLimit < 0 {
		return errors.New("rate_limit must be 0 (unlimited) or positive")
	}
	if l.Weight != nil && *l.Weight <= 0 {
		return errors.New("weight must be positive")
	}
	if l.MaxConcurrency != nil && *l.MaxConcurrency < 0 {
		return errors.New("max_concurrency must be 0 (unlimited) or positive")
	}
	return nil
}

func (s *Server) handleAdminCreateTenant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		tenantLimitsRequest
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if err := req.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rate, weight, maxConc := 100, 100, 0
	if req.RateLimit != nil {
		rate = *req.RateLimit
	}
	if req.Weight != nil {
		weight = *req.Weight
	}
	if req.MaxConcurrency != nil {
		maxConc = *req.MaxConcurrency
	}
	t, key, err := storage.InsertTenant(r.Context(), s.db, req.Name, rate, weight, maxConc)
	if err != nil {
		slog.Error("admin create tenant", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to create tenant")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"tenant":         toAdminTenant(t),
		"api_key":        key,
		"webhook_secret": t.WebhookSecret,
	})
}

func (s *Server) handleAdminListTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := storage.ListAllTenants(r.Context(), s.db)
	if err != nil {
		slog.Error("admin list tenants", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list tenants")
		return
	}
	out := make([]adminTenant, len(tenants))
	for i, t := range tenants {
		out[i] = toAdminTenant(t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": out, "count": len(out)})
}

func (s *Server) adminTenantID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid tenant id")
		return uuid.Nil, false
	}
	return id, true
}

func (s *Server) writeAdminTenant(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	t, err := storage.GetTenantAnyStatus(r.Context(), s.db, id)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if err != nil {
		slog.Error("admin get tenant", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get tenant")
		return
	}
	writeJSON(w, http.StatusOK, toAdminTenant(t))
}

func (s *Server) handleAdminGetTenant(w http.ResponseWriter, r *http.Request) {
	if id, ok := s.adminTenantID(w, r); ok {
		s.writeAdminTenant(w, r, id)
	}
}

// handleAdminUpdateTenant changes limits and/or status. API replicas and
// workers apply changes within seconds (their tenant caches refresh).
func (s *Server) handleAdminUpdateTenant(w http.ResponseWriter, r *http.Request) {
	id, ok := s.adminTenantID(w, r)
	if !ok {
		return
	}
	var req struct {
		tenantLimitsRequest
		Status *string `json:"status,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := req.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Status != nil && *req.Status != "active" && *req.Status != "disabled" {
		writeError(w, http.StatusBadRequest, `status must be "active" or "disabled"`)
		return
	}

	if req.RateLimit != nil || req.Weight != nil || req.MaxConcurrency != nil {
		_, err := storage.UpdateTenantLimits(r.Context(), s.db, id, storage.TenantLimits{
			RateLimit: req.RateLimit, Weight: req.Weight, MaxConcurrency: req.MaxConcurrency,
		})
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "tenant not found")
			return
		}
		if err != nil {
			slog.Error("admin update tenant limits", "err", err)
			writeError(w, http.StatusInternalServerError, "failed to update tenant")
			return
		}
	}
	if req.Status != nil {
		if err := storage.SetTenantStatus(r.Context(), s.db, id, *req.Status); errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "tenant not found")
			return
		} else if err != nil {
			slog.Error("admin set tenant status", "err", err)
			writeError(w, http.StatusInternalServerError, "failed to update tenant")
			return
		}
	}
	s.writeAdminTenant(w, r, id)
}

func (s *Server) handleAdminRotateKey(w http.ResponseWriter, r *http.Request) {
	id, ok := s.adminTenantID(w, r)
	if !ok {
		return
	}
	key, err := storage.RotateAPIKey(r.Context(), s.db, id)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if err != nil {
		slog.Error("admin rotate key", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to rotate key")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"api_key": key})
}

func (s *Server) handleAdminRotateWebhookSecret(w http.ResponseWriter, r *http.Request) {
	id, ok := s.adminTenantID(w, r)
	if !ok {
		return
	}
	secret, err := storage.RotateWebhookSecret(r.Context(), s.db, id)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if err != nil {
		slog.Error("admin rotate webhook secret", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to rotate webhook secret")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"webhook_secret": secret})
}
