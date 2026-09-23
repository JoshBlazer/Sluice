package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

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

func webhookJob(url string) *job.Job {
	return &job.Job{Type: "webhook", Payload: []byte(`{"url":"` + url + `","method":"GET"}`)}
}

func TestWebhook_BlocksLoopbackByDefault(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer srv.Close()

	w := &Worker{http: newWebhookClient(false)}
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

	w := &Worker{http: newWebhookClient(true)}
	if err := w.executeWebhook(context.Background(), webhookJob(srv.URL)); err != nil {
		t.Fatalf("err = %v", err)
	}
}

func TestWebhook_ErrorStatusFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	w := &Worker{http: newWebhookClient(true)}
	err := w.executeWebhook(context.Background(), webhookJob(srv.URL))
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err = %v, want 503 failure", err)
	}
}
