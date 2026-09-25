//go:build integration

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/sluice/internal/storage"
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

func TestRetryHistory(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	db := testutil.DB(t)
	tn, key := testutil.Tenant(t, db, 0, 100)
	_, otherKey := testutil.Tenant(t, db, 0, 100)

	retried := testutil.InsertJob(t, db, tn.ID, "https://example.com", nil)
	testutil.InsertJob(t, db, tn.ID, "https://example.com", nil) // never failed

	// Two failed attempts, each promoted back to pending for the next one.
	for i := 0; i < 2; i++ {
		token := uuid.New()
		claimed, runID, err := storage.TryClaim(ctx, db, retried.ID, "w", token, time.Now().Add(time.Minute))
		if err != nil || claimed == nil {
			t.Fatalf("claim %d: claimed=%v err=%v", i, claimed != nil, err)
		}
		next := time.Now()
		if err := storage.FailJob(ctx, db, retried.ID, runID, token, fmt.Sprintf("boom %d", i), &next); err != nil {
			t.Fatal(err)
		}
		if err := storage.PromoteFailedToPending(ctx, db, retried.ID); err != nil {
			t.Fatal(err)
		}
	}

	_, list := e.do("GET", "/v1/jobs?retried=true", key, nil)
	jobs := list["jobs"].([]any)
	if len(jobs) != 1 || jobs[0].(map[string]any)["id"] != retried.ID.String() {
		t.Fatalf("retried=true returned %v, want only the retried job", jobs)
	}
	if code, _ := e.do("GET", "/v1/jobs?retried=maybe", key, nil); code != http.StatusBadRequest {
		t.Fatalf("bad retried value: %d, want 400", code)
	}

	code, body := e.do("GET", "/v1/jobs/"+retried.ID.String()+"/runs", key, nil)
	if code != http.StatusOK {
		t.Fatalf("runs: %d %v", code, body)
	}
	runs := body["runs"].([]any)
	if len(runs) != 2 {
		t.Fatalf("runs = %d, want 2", len(runs))
	}
	for i, r := range runs {
		run := r.(map[string]any)
		if int(run["attempt"].(float64)) != i || run["state"] != "failed" || run["error"] != fmt.Sprintf("boom %d", i) {
			t.Fatalf("run %d = %v, want attempt %d failed with 'boom %d'", i, run, i, i)
		}
	}

	if code, _ := e.do("GET", "/v1/jobs/"+retried.ID.String()+"/runs", otherKey, nil); code != http.StatusNotFound {
		t.Fatalf("other tenant reading runs: %d, want 404", code)
	}
}

func TestWebhookSecretEndpoint(t *testing.T) {
	e := newEnv(t)
	tn, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	code, body := e.do("GET", "/v1/webhook-secret", key, nil)
	if code != http.StatusOK || body["secret"] != tn.WebhookSecret {
		t.Fatalf("got %d %v, want the tenant's secret", code, body)
	}
	if !strings.HasPrefix(tn.WebhookSecret, "whsec_") {
		t.Fatalf("secret %q is not in Standard Webhooks format", tn.WebhookSecret)
	}
	if code, _ := e.do("GET", "/v1/webhook-secret", "", nil); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated: %d, want 401", code)
	}
}

func TestUpdateSchedule(t *testing.T) {
	e := newEnv(t)
	_, key := testutil.Tenant(t, testutil.DB(t), 0, 100)
	_, otherKey := testutil.Tenant(t, testutil.DB(t), 0, 100)
	_, created := e.do("POST", "/v1/schedules", key, map[string]any{
		"name": "report", "cron": "0 9 * * *", "timezone": "UTC", "job_template": webhookJob(),
	})
	id := created["id"].(string)
	path := "/v1/schedules/" + id

	if code, body := e.do("PATCH", path, key, map[string]any{"enabled": false}); code != http.StatusOK || body["enabled"] != false {
		t.Fatalf("pause: %d %v", code, body)
	}
	code, body := e.do("PATCH", path, key, map[string]any{"cron": "30 17 * * *", "timezone": "Asia/Tokyo"})
	if code != http.StatusOK {
		t.Fatalf("edit: %d %v", code, body)
	}
	next, _ := time.Parse(time.RFC3339Nano, body["next_run_at"].(string))
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	if n := next.In(tokyo); n.Hour() != 17 || n.Minute() != 30 {
		t.Fatalf("next_run_at = %v in Tokyo, want 17:30", n)
	}
	if code, body := e.do("PATCH", path, key, map[string]any{"enabled": true}); code != http.StatusOK || body["enabled"] != true {
		t.Fatalf("resume: %d %v", code, body)
	}
	if code, _ := e.do("PATCH", path, key, map[string]any{"cron": "not a cron"}); code != http.StatusBadRequest {
		t.Fatalf("bad cron: %d, want 400", code)
	}
	if code, _ := e.do("PATCH", path, key, map[string]any{"job_template": map[string]any{"type": "webhook"}}); code != http.StatusBadRequest {
		t.Fatalf("bad template: %d, want 400", code)
	}
	if code, _ := e.do("PATCH", path, otherKey, map[string]any{"enabled": false}); code != http.StatusNotFound {
		t.Fatalf("other tenant: %d, want 404", code)
	}
}

func TestAdminAPI(t *testing.T) {
	db := testutil.DB(t)
	rdb := testutil.Redis(t)
	q := queue.New(rdb)
	s := api.New(db, q, ratelimit.New(rdb), 0)
	srv := httptest.NewServer(s.Routes())
	defer srv.Close()
	e := &env{t: t, srv: srv, q: q}

	if code, _ := e.do("GET", "/admin/v1/tenants", "anything", nil); code != http.StatusNotFound {
		t.Fatalf("admin API before EnableAdmin: %d, want 404", code)
	}
	if err := s.EnableAdmin("short"); err == nil {
		t.Fatal("a short admin token must be rejected")
	}
	admin := "admin-" + uuid.NewString() + uuid.NewString()
	if err := s.EnableAdmin(admin); err != nil {
		t.Fatal(err)
	}
	if code, _ := e.do("GET", "/admin/v1/tenants", "wrong-token", nil); code != http.StatusUnauthorized {
		t.Fatalf("wrong admin token: %d, want 401", code)
	}
	_, tenantKey := testutil.Tenant(t, db, 0, 100)
	if code, _ := e.do("GET", "/admin/v1/tenants", tenantKey, nil); code != http.StatusUnauthorized {
		t.Fatalf("a tenant key on the admin API: %d, want 401", code)
	}

	code, created := e.do("POST", "/admin/v1/tenants", admin, map[string]any{"name": "acme-" + uuid.NewString(), "max_concurrency": 5})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, created)
	}
	tn := created["tenant"].(map[string]any)
	id := tn["id"].(string)
	t.Cleanup(func() { db.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, id) })
	if tn["max_concurrency"].(float64) != 5 || tn["status"] != "active" {
		t.Fatalf("created tenant = %v", tn)
	}

	code, updated := e.do("PATCH", "/admin/v1/tenants/"+id, admin, map[string]any{"rate_limit": 50, "status": "disabled"})
	if code != http.StatusOK || updated["rate_limit"].(float64) != 50 || updated["status"] != "disabled" {
		t.Fatalf("update: %d %v", code, updated)
	}
	if code, _ := e.do("GET", "/v1/jobs", created["api_key"].(string), nil); code != http.StatusUnauthorized {
		t.Fatalf("disabled tenant's key: %d, want 401", code)
	}
	e.do("PATCH", "/admin/v1/tenants/"+id, admin, map[string]any{"status": "active"})

	_, rotated := e.do("POST", "/admin/v1/tenants/"+id+"/rotate-key", admin, nil)
	if code, _ := e.do("GET", "/v1/jobs", rotated["api_key"].(string), nil); code != http.StatusOK {
		t.Fatalf("rotated key: %d, want 200", code)
	}
	_, secret := e.do("POST", "/admin/v1/tenants/"+id+"/rotate-webhook-secret", admin, nil)
	if !strings.HasPrefix(secret["webhook_secret"].(string), "whsec_") {
		t.Fatalf("rotated secret = %v", secret)
	}
	if code, _ := e.do("PATCH", "/admin/v1/tenants/"+uuid.NewString(), admin, map[string]any{"weight": 10}); code != http.StatusNotFound {
		t.Fatalf("unknown tenant: %d, want 404", code)
	}
	if code, _ := e.do("PATCH", "/admin/v1/tenants/"+id, admin, map[string]any{"weight": 0}); code != http.StatusBadRequest {
		t.Fatalf("zero weight: %d, want 400", code)
	}
}
