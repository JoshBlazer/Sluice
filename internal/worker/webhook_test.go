package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sluice/internal/job"
)

func TestIsPublicAddr(t *testing.T) {
	cases := map[string]bool{
		"8.8.8.8":          true,
		"1.1.1.1":          true,
		"2606:4700::1111":  true,
		"127.0.0.1":        false,
		"10.1.2.3":         false,
		"172.16.0.1":       false,
		"192.168.1.1":      false,
		"169.254.169.254":  false, // cloud metadata
		"100.64.0.1":       false, // CGNAT
		"0.0.0.0":          false,
		"::1":              false,
		"fe80::1":          false,
		"fd00::1":          false,
		"::ffff:127.0.0.1": false, // IPv4-mapped loopback
		"::ffff:10.0.0.1":  false,
	}
	for addr, want := range cases {
		if got := isPublicAddr(netip.MustParseAddr(addr)); got != want {
			t.Errorf("isPublicAddr(%s) = %v, want %v", addr, got, want)
		}
	}
}

const testSecret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"

var testTenant = uuid.MustParse("00000000-0000-0000-0000-00000000000a")

func testWorker(allowPrivate bool) *Worker {
	return &Worker{
		http:    newWebhookClient(allowPrivate),
		secrets: map[uuid.UUID]string{testTenant: testSecret},
	}
}

func webhookJob(url string) *job.Job {
	return &job.Job{ID: uuid.New(), TenantID: testTenant, Type: "webhook",
		Payload: []byte(`{"url":"` + url + `","method":"GET"}`)}
}

// Test vector from the Standard Webhooks specification, so any of its receiver
// libraries can verify Sluice's signatures.
func TestSignRequest_StandardWebhooksVector(t *testing.T) {
	h := http.Header{}
	err := signRequest(h, testSecret, "msg_p5jXN8AQM9LWM0D4loKWxJek", time.Unix(1614265330, 0), []byte(`{"test": 2432232314}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := h.Get("webhook-signature"), "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="; got != want {
		t.Fatalf("signature = %s, want %s", got, want)
	}
	if h.Get("webhook-id") != "msg_p5jXN8AQM9LWM0D4loKWxJek" || h.Get("webhook-timestamp") != "1614265330" {
		t.Fatalf("id/timestamp headers = %q/%q", h.Get("webhook-id"), h.Get("webhook-timestamp"))
	}
}

func TestWebhook_SignsRequestsAndProtectsSignature(t *testing.T) {
	var got http.Header
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		body, _ = io.ReadAll(r.Body)
	}))
	defer srv.Close()

	j := webhookJob(srv.URL)
	j.Payload = []byte(`{"url":"` + srv.URL + `","body":{"order":42},"headers":{"webhook-signature":"forged"}}`)
	if err := testWorker(true).executeWebhook(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	ts, _ := strconv.ParseInt(got.Get("webhook-timestamp"), 10, 64)
	want := http.Header{}
	signRequest(want, testSecret, j.ID.String(), time.Unix(ts, 0), body)
	if got.Get("webhook-signature") != want.Get("webhook-signature") {
		t.Fatalf("receiver could not verify: got %q, want %q", got.Get("webhook-signature"), want.Get("webhook-signature"))
	}
	if got.Get("webhook-id") != j.ID.String() {
		t.Fatalf("webhook-id = %q, want the job ID", got.Get("webhook-id"))
	}
}

func TestWebhook_PerJobTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()

	j := webhookJob(srv.URL)
	j.Payload = []byte(`{"url":"` + srv.URL + `","timeout_seconds":1}`)
	start := time.Now()
	err := testWorker(true).executeWebhook(context.Background(), j)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("took %v; the 1s job timeout was not applied", took)
	}
}

func TestWebhook_BlocksLoopbackByDefault(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer srv.Close()

	w := testWorker(false)
	err := w.executeWebhook(context.Background(), webhookJob(srv.URL))
	if err == nil || !strings.Contains(err.Error(), "non-public address") {
		t.Fatalf("err = %v, want non-public address refusal", err)
	}
	if called {
		t.Fatal("request reached the loopback server")
	}
}

func TestWebhook_AllowPrivateForDev(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
	}))
	defer srv.Close()

	w := testWorker(true)
	if err := w.executeWebhook(context.Background(), webhookJob(srv.URL)); err != nil {
		t.Fatalf("err = %v", err)
	}
}

func TestWebhook_ErrorStatusFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	w := testWorker(true)
	err := w.executeWebhook(context.Background(), webhookJob(srv.URL))
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err = %v, want 503 failure", err)
	}
}
