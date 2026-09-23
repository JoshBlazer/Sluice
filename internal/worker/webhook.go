package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"

	"github.com/sluice/internal/job"
)

const (
	httpTimeout      = 25 * time.Second
	maxResponseDrain = 64 << 10
)

// Ranges that are not publicly routable but aren't covered by netip's
// IsPrivate/IsLoopback/IsLinkLocal* helpers.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
}

func isPublicAddr(a netip.Addr) bool {
	a = a.Unmap()
	if a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() ||
		a.IsInterfaceLocalMulticast() || a.IsMulticast() || a.IsUnspecified() {
		return false
	}
	for _, p := range blockedPrefixes {
		if p.Contains(a) {
			return false
		}
	}
	return true
}

// newWebhookClient returns the HTTP client used for webhook jobs. Unless
// allowPrivate is set, it refuses to connect to loopback, private, link-local
// (e.g. cloud metadata) and other non-public addresses. The check runs on the
// resolved IP at dial time, so it also covers redirects and DNS rebinding.
func newWebhookClient(allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	if !allowPrivate {
		dialer.Control = func(network, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("webhook: unparseable address %q: %w", address, err)
			}
			if !isPublicAddr(ap.Addr()) {
				return fmt.Errorf("webhook: refusing to connect to non-public address %s", ap.Addr())
			}
			return nil
		}
	}
	return &http.Client{
		Timeout: httpTimeout,
		Transport: &http.Transport{
			// No proxy: a proxy would dial on our behalf and bypass the address check.
			Proxy:                 nil,
			DialContext:           dialer.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
}

func (w *Worker) executeWebhook(ctx context.Context, j *job.Job) error {
	var p job.WebhookPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return fmt.Errorf("invalid webhook payload: %w", err)
	}

	method := p.Method
	if method == "" {
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(ctx, method, p.URL, bytes.NewReader(p.Body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" && len(p.Body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := w.http.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseDrain)) //nolint:errcheck // draining lets the connection be reused

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}
