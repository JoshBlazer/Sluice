package job

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func ptr[T any](v T) *T { return &v }

func TestTemplateBuild_Defaults(t *testing.T) {
	now := time.Now()
	j, err := Template{Type: "webhook", Payload: []byte(`{"url":"https://example.com/hook"}`)}.Build(uuid.New(), now)
	if err != nil {
		t.Fatal(err)
	}
	if j.Priority != PriorityNormal || j.MaxRetries != DefaultMaxRetries || j.BackoffSeconds != DefaultBackoffSeconds {
		t.Fatalf("defaults not applied: %+v", j)
	}
	if j.State != StatePending || !j.RunAt.Equal(now) {
		t.Fatalf("state/run_at = %s/%v", j.State, j.RunAt)
	}
}

func TestTemplateBuild_Validation(t *testing.T) {
	good := []byte(`{"url":"https://example.com/hook"}`)
	cases := []struct {
		name string
		tmpl Template
		want string
	}{
		{"missing type", Template{Payload: good}, "type is required"},
		{"unknown type", Template{Type: "wasm", Payload: good}, "unsupported job type"},
		{"missing payload", Template{Type: "webhook"}, "payload is required"},
		{"bad json", Template{Type: "webhook", Payload: []byte(`{`)}, "invalid webhook payload"},
		{"relative url", Template{Type: "webhook", Payload: []byte(`{"url":"/hook"}`)}, "absolute http"},
		{"ftp url", Template{Type: "webhook", Payload: []byte(`{"url":"ftp://x/y"}`)}, "absolute http"},
		{"bad method", Template{Type: "webhook", Payload: []byte(`{"url":"https://x","method":"TRACE"}`)}, "not supported"},
		{"priority zero", Template{Type: "webhook", Payload: good, Priority: ptr[int16](0)}, "priority"},
		{"priority high", Template{Type: "webhook", Payload: good, Priority: ptr[int16](11)}, "priority"},
		{"negative retries", Template{Type: "webhook", Payload: good, MaxRetries: ptr(-1)}, "max_retries"},
		{"huge retries", Template{Type: "webhook", Payload: good, MaxRetries: ptr(1000)}, "max_retries"},
		{"zero backoff", Template{Type: "webhook", Payload: good, BackoffSeconds: ptr(0)}, "backoff_seconds"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.tmpl.Build(uuid.New(), time.Now())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want containing %q", err, c.want)
			}
		})
	}
}

func TestTemplateBuild_ZeroRetriesAllowed(t *testing.T) {
	j, err := Template{Type: "webhook", Payload: []byte(`{"url":"http://example.com"}`), MaxRetries: ptr(0)}.Build(uuid.New(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if j.ShouldRetry() {
		t.Fatal("max_retries=0 must not retry")
	}
}

func TestStateValid(t *testing.T) {
	if !StateCancelled.Valid() || !StatePending.Valid() {
		t.Fatal("known states reported invalid")
	}
	if State("bogus").Valid() {
		t.Fatal("unknown state reported valid")
	}
}
