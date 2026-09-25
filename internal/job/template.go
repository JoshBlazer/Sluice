package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultMaxRetries     = 3
	DefaultBackoffSeconds = 30
	MaxMaxRetries         = 100
	MaxBackoffSeconds     = 86400
)

// Template is the client-supplied description of a job. Direct submissions and
// cron schedule templates share it so both get the same defaults and validation.
type Template struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int16          `json:"priority,omitempty"`
	MaxRetries     *int            `json:"max_retries,omitempty"`
	BackoffSeconds *int            `json:"backoff_seconds,omitempty"`
}

// Build validates t and returns a new pending job for tenantID with defaults applied.
func (t Template) Build(tenantID uuid.UUID, now time.Time) (*Job, error) {
	j := &Job{
		ID:             uuid.New(),
		TenantID:       tenantID,
		Type:           t.Type,
		Payload:        t.Payload,
		Priority:       PriorityNormal,
		State:          StatePending,
		RunAt:          now,
		MaxRetries:     DefaultMaxRetries,
		BackoffSeconds: DefaultBackoffSeconds,
		CreatedAt:      now,
	}
	if t.Priority != nil {
		j.Priority = *t.Priority
	}
	if t.MaxRetries != nil {
		j.MaxRetries = *t.MaxRetries
	}
	if t.BackoffSeconds != nil {
		j.BackoffSeconds = *t.BackoffSeconds
	}
	if err := j.validate(); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *Job) validate() error {
	if j.Priority < PriorityHigh || j.Priority > PriorityLow {
		return fmt.Errorf("priority must be between %d and %d", PriorityHigh, PriorityLow)
	}
	if j.MaxRetries < 0 || j.MaxRetries > MaxMaxRetries {
		return fmt.Errorf("max_retries must be between 0 and %d", MaxMaxRetries)
	}
	if j.BackoffSeconds < 1 || j.BackoffSeconds > MaxBackoffSeconds {
		return fmt.Errorf("backoff_seconds must be between 1 and %d", MaxBackoffSeconds)
	}
	switch j.Type {
	case "":
		return errors.New("type is required")
	case "webhook":
		return validateWebhookPayload(j.Payload)
	default:
		return fmt.Errorf("unsupported job type %q", j.Type)
	}
}

func validateWebhookPayload(raw json.RawMessage) error {
	if len(raw) == 0 {
		return errors.New("payload is required")
	}
	var p WebhookPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("invalid webhook payload: %w", err)
	}
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("payload.url must be an absolute http or https URL")
	}
	if p.TimeoutSeconds < 0 || time.Duration(p.TimeoutSeconds)*time.Second > MaxWebhookTimeout {
		return fmt.Errorf("payload.timeout_seconds must be between 1 and %d", int(MaxWebhookTimeout.Seconds()))
	}
	switch p.Method {
	case "", http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return fmt.Errorf("payload.method %q is not supported", p.Method)
	}
	return nil
}
