//go:build integration

package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/testutil"
	"github.com/sluice/internal/worker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The job's execution must join the trace of the API request that submitted it.
func TestTracePropagatesFromSubmissionToExecution(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	e := newEnv(t)
	db := testutil.DB(t)
	_, key := testutil.Tenant(t, db, 0, 100)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer hook.Close()

	w := worker.New(db, e.q, worker.Options{AllowPrivateWebhooks: true})
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	defer func() { cancel(); w.Shutdown(5 * time.Second) }()
	time.Sleep(300 * time.Millisecond)

	code, body := e.do("POST", "/v1/jobs", key, map[string]any{
		"type": "webhook", "payload": map[string]any{"url": hook.URL, "method": "GET"},
	})
	if code != http.StatusCreated {
		t.Fatalf("submit: %d %v", code, body)
	}
	jobID := uuid.MustParse(body["id"].(string))
	testutil.Eventually(t, 10*time.Second, "job succeeded", func() bool {
		j, err := storage.GetJob(context.Background(), db, jobID)
		return err == nil && j.State == job.StateSucceeded
	})
	time.Sleep(200 * time.Millisecond) // let the worker span end

	var apiSpan, workerSpan sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch {
		case s.SpanKind() == trace.SpanKindServer && apiSpan == nil:
			apiSpan = s
		case s.Name() == "worker.execute":
			for _, a := range s.Attributes() {
				if a.Key == "job.id" && a.Value.AsString() == jobID.String() {
					workerSpan = s
				}
			}
		}
	}
	if apiSpan == nil || workerSpan == nil {
		t.Fatalf("missing spans: api=%v worker=%v (recorded %d)", apiSpan != nil, workerSpan != nil, len(rec.Ended()))
	}
	if workerSpan.SpanContext().TraceID() != apiSpan.SpanContext().TraceID() {
		t.Fatalf("worker trace %s != submission trace %s", workerSpan.SpanContext().TraceID(), apiSpan.SpanContext().TraceID())
	}
	if workerSpan.Parent().SpanID() != apiSpan.SpanContext().SpanID() {
		t.Fatalf("worker span's parent is %s, want the submission span %s", workerSpan.Parent().SpanID(), apiSpan.SpanContext().SpanID())
	}
}
