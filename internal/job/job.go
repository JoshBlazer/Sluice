package job

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type State string

const (
	StatePending   State = "pending"
	StateScheduled State = "scheduled"
	StateClaimed   State = "claimed"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

func (s State) Valid() bool {
	switch s {
	case StatePending, StateScheduled, StateClaimed, StateRunning,
		StateSucceeded, StateFailed, StateDead, StateCancelled:
		return true
	}
	return false
}

// Priority constants — lower number = higher priority.
const (
	PriorityHigh   int16 = 1
	PriorityNormal int16 = 5
	PriorityLow    int16 = 10
)

type Job struct {
	ID             uuid.UUID       `json:"id"`
	TenantID       uuid.UUID       `json:"tenant_id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int16           `json:"priority"`
	State          State           `json:"state"`
	RunAt          time.Time       `json:"run_at"`
	ClaimedAt      *time.Time      `json:"claimed_at,omitempty"`
	ClaimedBy      *string         `json:"claimed_by,omitempty"`
	ClaimToken     *uuid.UUID      `json:"-"`
	Deadline       *time.Time      `json:"deadline,omitempty"`
	Attempt        int             `json:"attempt"`
	MaxRetries     int             `json:"max_retries"`
	BackoffSeconds int             `json:"backoff_seconds"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

func (j *Job) IsTerminal() bool {
	return j.State == StateSucceeded || j.State == StateDead || j.State == StateCancelled
}

// ShouldRetry reports whether a failure of the current attempt earns another one.
// Attempt counts prior failed attempts, so max_retries=3 allows 4 executions in total.
func (j *Job) ShouldRetry() bool {
	return j.Attempt < j.MaxRetries
}

// NextRetryAt calculates the next attempt time using exponential backoff with ±20% jitter.
// Pure function — no I/O.
func NextRetryAt(attempt, backoffSeconds int, now time.Time) time.Time {
	delay := backoffSeconds
	for i := 0; i < attempt && delay < 3600; i++ {
		delay *= 2
	}
	if delay > 3600 {
		delay = 3600
	}
	// Add ±20% jitter to spread out retries from many concurrent jobs.
	jitter := int(float64(delay) * 0.2)
	if jitter > 0 {
		delay += rand.Intn(2*jitter+1) - jitter
	}
	return now.Add(time.Duration(delay) * time.Second)
}

// WebhookPayload is the payload shape for type="webhook" jobs.
type WebhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}
