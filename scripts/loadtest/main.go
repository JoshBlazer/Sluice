// Command loadtest measures Sluice against its README performance targets.
//
// It submits jobs through the HTTP API from many goroutines and hosts the
// webhook endpoint those jobs call, so it can time both submission (HTTP
// round trip) and end-to-end latency (submit returned → webhook received).
//
// Workers must be allowed to call the receiver on a private address:
//
//	SLUICE_WEBHOOK_ALLOW_PRIVATE=true sluice --role worker
//	go run ./scripts/loadtest -key <api key> -n 20000 -c 64
//
// Use a tenant with -rate-limit 0 (sluice-cli create-tenant) so the limiter
// doesn't cap the run.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	apiFlag := flag.String("api", "http://localhost:8080", "Sluice API base URL; comma-separate several to spread submissions across them")
	key := flag.String("key", os.Getenv("SLUICE_API_KEY"), "tenant API key")
	n := flag.Int("n", 10000, "jobs to submit")
	c := flag.Int("c", 32, "concurrent submitters")
	listen := flag.String("listen", "127.0.0.1:9099", "address for the webhook receiver")
	callback := flag.String("callback", "", "URL workers should call (default http://<listen>/hook)")
	wait := flag.Duration("wait", 60*time.Second, "how long to wait for all jobs to execute")
	delay := flag.Duration("delay", 0, "how long the webhook receiver takes to respond, to simulate real work")
	flag.Parse()
	if *key == "" {
		fmt.Fprintln(os.Stderr, "-key (or SLUICE_API_KEY) is required")
		os.Exit(2)
	}
	if *callback == "" {
		*callback = "http://" + *listen + "/hook"
	}
	apis := strings.Split(*apiFlag, ",")

	var (
		mu       sync.Mutex
		accepted = make(map[int]time.Time, *n) // job seq → submit acknowledged
		e2e      = make([]time.Duration, 0, *n)
		received atomic.Int64
		dupes    atomic.Int64
		seen     sync.Map
	)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { //nolint:errcheck
		now := time.Now()
		if *delay > 0 {
			time.Sleep(*delay)
		}
		seq, err := strconv.Atoi(r.URL.Query().Get("seq"))
		if err != nil {
			return
		}
		if _, dup := seen.LoadOrStore(seq, true); dup {
			dupes.Add(1) // at-least-once: count each job once, but report re-runs
			return
		}
		mu.Lock()
		if t, ok := accepted[seq]; ok {
			e2e = append(e2e, now.Sub(t))
		}
		mu.Unlock()
		received.Add(1)
	}))

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *c * 2},
	}
	submit := make([]time.Duration, *n)
	var failures atomic.Int64
	var next atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < *c; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				seq := int(next.Add(1) - 1)
				if seq >= *n {
					return
				}
				body, _ := json.Marshal(map[string]any{
					"type":    "webhook",
					"payload": map[string]any{"url": fmt.Sprintf("%s?seq=%d", *callback, seq)},
				})
				req, _ := http.NewRequest("POST", apis[seq%len(apis)]+"/v1/jobs", bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+*key)
				req.Header.Set("Content-Type", "application/json")

				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					failures.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body) //nolint:errcheck
				resp.Body.Close()
				t1 := time.Now()
				if resp.StatusCode != http.StatusCreated {
					failures.Add(1)
					continue
				}
				submit[seq] = t1.Sub(t0)
				mu.Lock()
				accepted[seq] = t1
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	submitElapsed := time.Since(start)
	ok := int64(*n) - failures.Load()

	deadline := time.Now().Add(*wait)
	for received.Load() < ok && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	totalElapsed := time.Since(start)

	var subs []time.Duration
	for _, d := range submit {
		if d > 0 {
			subs = append(subs, d)
		}
	}
	mu.Lock()
	lat := append([]time.Duration(nil), e2e...)
	mu.Unlock()

	fmt.Printf("jobs submitted:        %d ok, %d failed\n", ok, failures.Load())
	fmt.Printf("submission throughput: %.0f jobs/sec (%d submitters, %d APIs)\n", float64(ok)/submitElapsed.Seconds(), *c, len(apis))
	fmt.Printf("submit latency:        p50 %v  p99 %v\n", pct(subs, 50), pct(subs, 99))
	fmt.Printf("jobs executed:         %d/%d in %v\n", received.Load(), ok, totalElapsed.Round(time.Millisecond))
	fmt.Printf("execution throughput:  %.0f jobs/sec\n", float64(received.Load())/totalElapsed.Seconds())
	fmt.Printf("submit→execute:        p50 %v  p99 %v\n", pct(lat, 50), pct(lat, 99))
	fmt.Printf("duplicate deliveries:  %d\n", dupes.Load())
	if received.Load() < ok {
		os.Exit(1)
	}
}

func pct(ds []time.Duration, p int) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	i := len(ds) * p / 100
	if i >= len(ds) {
		i = len(ds) - 1
	}
	return ds[i].Round(10 * time.Microsecond)
}
