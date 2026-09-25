package api

import (
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// Every route the server registers must be documented in openapi.yaml, and the
// spec must not describe routes that don't exist.
func TestOpenAPICoversAllRoutes(t *testing.T) {
	s := New(nil, nil, nil, 0)

	registered := map[string]bool{}
	err := chi.Walk(s.router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if method == http.MethodOptions || method == "CONNECT" || method == "TRACE" || method == "HEAD" {
			return nil
		}
		registered[strings.ToLower(method)+" "+strings.TrimSuffix(route, "/")] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// /metrics is mounted with Handle, which chi reports under every method.
	for k := range registered {
		if strings.HasSuffix(k, " /metrics") && !strings.HasPrefix(k, "get ") {
			delete(registered, k)
		}
	}

	documented := map[string]bool{}
	pathRe := regexp.MustCompile(`^  (/\S*):\s*$`)
	methodRe := regexp.MustCompile(`^    (get|post|put|patch|delete):`)
	var current string
	for _, line := range strings.Split(strings.ReplaceAll(string(openAPISpec), "\r\n", "\n"), "\n") {
		if m := pathRe.FindStringSubmatch(line); m != nil {
			current = m[1]
		} else if m := methodRe.FindStringSubmatch(line); m != nil && current != "" {
			documented[m[1]+" "+current] = true
		} else if line != "" && !strings.HasPrefix(line, " ") {
			current = ""
		}
	}

	for r := range registered {
		if !documented[r] {
			t.Errorf("route %q is not documented in openapi.yaml", r)
		}
	}
	for d := range documented {
		if !registered[d] {
			t.Errorf("openapi.yaml documents %q, which the server doesn't register", d)
		}
	}
	if len(registered) < 15 {
		t.Fatalf("only walked %d routes; the walk is broken", len(registered))
	}
}
