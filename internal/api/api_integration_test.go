//go:build integration

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sluice/internal/api"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/ratelimit"
	"github.com/sluice/internal/testutil"
)

type env struct {
	t   *testing.T
	srv *httptest.Server
	q   *queue.Queue
}

func newEnv(t *testing.T) *env {
	db := testutil.DB(t)
	rdb := testutil.Redis(t)
	q := queue.New(rdb)
	s := api.New(db, q, ratelimit.New(rdb), 0)
	srv := httptest.NewServer(s.Routes())
	t.Cleanup(srv.Close)
	return &env{t: t, srv: srv, q: q}
}

func (e *env) do(method, path, key string, body any) (int, map[string]any) {
	e.t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, r)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out) //nolint:errcheck // some responses have no body
	return resp.StatusCode, out
}

func webhookJob() map[string]any {
	return map[string]any{"type": "webhook", "payload": map[string]any{"url": "https://example.com/hook"}}
}

func TestAuth(t *testing.T) {
	e := newEnv(t)
	if code, _ := e.do("GET", "/v1/jobs", "", nil); code != http.StatusUnauthorized {
		t.Fatalf("no key: %d, want 401", code)
	}
	if code, _ := e.do("GET", "/v1/jobs", "sk_wrong", nil); code != http.StatusUnauthorized {
		t.Fatalf("wrong key: %d, want 401", code)
	}
	_, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	if code, _ := e.do("GET", "/v1/jobs", key, nil); code != http.StatusOK {
		t.Fatalf("valid key: %d, want 200", code)
	}
}

func TestSubmitJob(t *testing.T) {
	e := newEnv(t)
	tn, key := testutil.Tenant(t, testutil.DB(t), 0, 100)

	code, body := e.do("POST", "/v1/jobs", key, webhookJob())
	if code != http.StatusCreated {
		t.Fatalf("submit: %d %v", code, body)
	}
	if body["state"] != "pending" || body["id"] == nil || body["tenant_id"] != tn.ID.String() {
		t.Fatalf("unexpected response: %v", body)
	}
	if _, leaked := body["claim_token"]; leaked {
		t.Fatal("claim_token must not be exposed")
	}
	if _, pascal := body["ID"]; pascal {
		t.Fatal("response should use snake_case keys")
	}

	depths, _ := e.q.Depths(context.Background(), []uuid.UUID{tn.ID})
	var total int64
	for _, d := range depths {
		total += d.Depth
	}
	if total != 1 {
		t.Fatalf("queued jobs = %d, want 1", total)
	}
}

func TestSubmitJob_Scheduled(t *testing.T) {
	e := newEnv(t)
	_, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	j := webhookJob()
	j["run_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	code, body := e.do("POST", "/v1/jobs", key, j)
	if code != http.StatusCreated || body["state"] != "scheduled" {
		t.Fatalf("scheduled submit: %d %v", code, body)
	}
}

func TestSubmitJob_Validation(t *testing.T) {
	e := newEnv(t)
	_, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	bad := []map[string]any{
		{"type": "webhook"},
		{"type": "nope", "payload": map[string]any{"url": "https://x"}},
		{"type": "webhook", "payload": map[string]any{"url": "not a url"}},
		{"type": "webhook", "payload": map[string]any{"url": "https://x"}, "priority": 0},
		{"type": "webhook", "payload": map[string]any{"url": "https://x"}, "max_retries": -1},
		{"type": "webhook", "payload": map[string]any{"url": "https://x"}, "idempotency_key": ""},
	}
	for _, b := range bad {
		if code, body := e.do("POST", "/v1/jobs", key, b); code != http.StatusBadRequest {
			t.Errorf("%v: %d %v, want 400", b, code, body)
		}
	}
}

func TestSubmitJob_Idempotent(t *testing.T) {
	e := newEnv(t)
	_, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	j := webhookJob()
	j["idempotency_key"] = "order-1234"

	code1, first := e.do("POST", "/v1/jobs", key, j)
	code2, second := e.do("POST", "/v1/jobs", key, j)
	if code1 != http.StatusCreated || code2 != http.StatusOK {
		t.Fatalf("codes = %d, %d, want 201, 200", code1, code2)
	}
	if first["id"] != second["id"] {
		t.Fatalf("duplicate returned a different job: %v vs %v", first["id"], second["id"])
	}
}

func TestSubmitJob_RateLimited(t *testing.T) {
	e := newEnv(t)
	_, key := testutil.Tenant(t, testutil.DB(t), 2, 100)
	var codes []int
	for i := 0; i < 4; i++ {
		code, _ := e.do("POST", "/v1/jobs", key, webhookJob())
		codes = append(codes, code)
	}
	if codes[0] != 201 || codes[1] != 201 || codes[2] != 429 || codes[3] != 429 {
		t.Fatalf("codes = %v, want [201 201 429 429]", codes)
	}
}

func TestTenantIsolation(t *testing.T) {
	e := newEnv(t)
	db := testutil.DB(t)
	_, keyA := testutil.Tenant(t, db, 0, 100)
	_, keyB := testutil.Tenant(t, db, 0, 100)

	_, j := e.do("POST", "/v1/jobs", keyA, webhookJob())
	id := j["id"].(string)

	if code, _ := e.do("GET", "/v1/jobs/"+id, keyB, nil); code != http.StatusNotFound {
		t.Fatalf("B reading A's job: %d, want 404", code)
	}
	if code, _ := e.do("POST", "/v1/jobs/"+id+"/cancel", keyB, nil); code != http.StatusNotFound {
		t.Fatalf("B cancelling A's job: %d, want 404", code)
	}
	_, list := e.do("GET", "/v1/jobs", keyB, nil)
	if list["count"].(float64) != 0 {
		t.Fatalf("B's job list includes other tenants' jobs: %v", list)
	}
	_, stats := e.do("GET", "/v1/stats", keyB, nil)
	if states := stats["jobs_by_state"].(map[string]any); len(states) != 0 {
		t.Fatalf("B's stats include other tenants' jobs: %v", states)
	}

	_, sched := e.do("POST", "/v1/schedules", keyA, map[string]any{
		"name": "nightly", "cron": "0 2 * * *", "job_template": webhookJob(),
	})
	if code, _ := e.do("GET", "/v1/schedules/"+sched["id"].(string), keyB, nil); code != http.StatusNotFound {
		t.Fatalf("B reading A's schedule: %d, want 404", code)
	}
}

func TestCancelJob(t *testing.T) {
	e := newEnv(t)
	_, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	_, j := e.do("POST", "/v1/jobs", key, webhookJob())
	id := j["id"].(string)

	if code, _ := e.do("POST", "/v1/jobs/"+id+"/cancel", key, nil); code != http.StatusNoContent {
		t.Fatalf("cancel: %d, want 204", code)
	}
	if _, got := e.do("GET", "/v1/jobs/"+id, key, nil); got["state"] != "cancelled" {
		t.Fatalf("state after cancel = %v", got["state"])
	}
	if _, list := e.do("GET", "/v1/jobs?state=cancelled", key, nil); list["count"].(float64) != 1 {
		t.Fatalf("state filter: %v", list)
	}
	if code, _ := e.do("GET", "/v1/jobs?state=bogus", key, nil); code != http.StatusBadRequest {
		t.Fatalf("unknown state filter: %d, want 400", code)
	}
}

func TestSchedules_ValidateTemplate(t *testing.T) {
	e := newEnv(t)
	_, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	code, _ := e.do("POST", "/v1/schedules", key, map[string]any{
		"name": "bad", "cron": "* * * * *", "job_template": map[string]any{"type": "webhook"},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("invalid template: %d, want 400", code)
	}
	code, body := e.do("POST", "/v1/schedules", key, map[string]any{
		"name": "ok", "cron": "0 9 * * *", "timezone": "America/New_York", "job_template": webhookJob(),
	})
	if code != http.StatusCreated || body["next_run_at"] == nil {
		t.Fatalf("valid schedule: %d %v", code, body)
	}
}

func TestWebSocket(t *testing.T) {
	e := newEnv(t)
	_, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	wsURL := "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/ws"

	if _, resp, err := websocket.DefaultDialer.Dial(wsURL, nil); err == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated ws: err=%v resp=%v, want 401", err, resp)
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL+"?token="+key, nil)
	if err != nil {
		t.Fatalf("authenticated ws: %v", err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var snap struct {
		Queues      []map[string]any `json:"queues"`
		JobsByState map[string]int64 `json:"jobs_by_state"`
	}
	if err := conn.ReadJSON(&snap); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if len(snap.Queues) != 3 {
		t.Fatalf("snapshot has %d queue rows, want 3 (one per priority for this tenant)", len(snap.Queues))
	}
}

func TestMetricsEndpoint(t *testing.T) {
	e := newEnv(t)
	e.do("GET", "/healthz", "", nil)
	resp, err := http.Get(e.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), `sluice_http_request_duration_seconds_count{method="GET",path="/healthz",status="200"}`) {
		t.Fatal("request duration metric not recorded for /healthz")
	}
}
