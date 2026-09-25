// Command demo keeps a local Sluice busy with realistic-looking traffic, so the
// dashboard has something to show: steady jobs with varied latency, a flaky
// endpoint that succeeds on retry, a broken one whose jobs end in the dead
// letter, future-dated jobs, periodic bursts that build a visible backlog, and a
// cron schedule. It hosts the webhook endpoints itself.
//
// Workers must be allowed to call it on a private address:
//
//	SLUICE_WEBHOOK_ALLOW_PRIVATE=true sluice --role worker
//	go run ./scripts/demo -key dev-token
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	api := flag.String("api", "http://localhost:8080", "Sluice API base URL")
	key := flag.String("key", os.Getenv("SLUICE_API_KEY"), "tenant API key")
	listen := flag.String("listen", "127.0.0.1:9098", "address for the demo webhook endpoints")
	rate := flag.Float64("rate", 4, "average jobs per second between bursts")
	burst := flag.Int("burst", 150, "jobs submitted at once every -burst-every")
	burstEvery := flag.Duration("burst-every", 45*time.Second, "time between bursts (0 disables them)")
	flag.Parse()
	if *key == "" {
		fmt.Fprintln(os.Stderr, "-key (or SLUICE_API_KEY) is required")
		os.Exit(2)
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	go http.Serve(ln, endpoints()) //nolint:errcheck
	hooks := "http://" + *listen

	c := &client{api: *api, key: *key, http: &http.Client{Timeout: 10 * time.Second}}
	c.ensureSchedule(hooks)

	var nextBurst <-chan time.Time
	if *burstEvery > 0 {
		t := time.NewTicker(*burstEvery)
		defer t.Stop()
		nextBurst = t.C
	}
	log.Printf("sending demo traffic to %s (webhooks served on %s)", *api, hooks)
	for {
		select {
		case <-nextBurst:
			for range *burst {
				c.submit(randomJob(hooks))
			}
		case <-time.After(time.Duration(rand.ExpFloat64() / *rate * float64(time.Second))):
			c.submit(randomJob(hooks))
		}
	}
}

// endpoints serves the URLs demo jobs call: /work answers after ?ms
// milliseconds, /flaky fails about half the time, /broken always fails.
func endpoints() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		time.Sleep(time.Duration(ms) * time.Millisecond)
	})
	mux.HandleFunc("/flaky", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Duration(50+rand.IntN(200)) * time.Millisecond)
		if rand.IntN(2) == 0 {
			http.Error(w, "upstream timeout", http.StatusBadGateway)
		}
	})
	mux.HandleFunc("/broken", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	})
	return mux
}

func randomJob(hooks string) map[string]any {
	priority := []int{1, 5, 5, 5, 10, 10}[rand.IntN(6)]
	job := map[string]any{"type": "webhook", "priority": priority, "backoff_seconds": 3}
	switch n := rand.IntN(100); {
	case n < 80:
		ms := 50 + rand.IntN(950)
		job["payload"] = map[string]any{"url": fmt.Sprintf("%s/work?ms=%d", hooks, ms), "body": map[string]any{"order": rand.IntN(100000)}}
	case n < 92:
		job["payload"] = map[string]any{"url": hooks + "/flaky"}
		job["max_retries"] = 4
	case n < 96:
		job["payload"] = map[string]any{"url": hooks + "/broken"}
		job["max_retries"] = 2
	default:
		job["payload"] = map[string]any{"url": fmt.Sprintf("%s/work?ms=%d", hooks, 100)}
		job["run_at"] = time.Now().Add(time.Duration(30+rand.IntN(90)) * time.Second).UTC().Format(time.RFC3339)
	}
	return job
}

type client struct {
	api, key string
	http     *http.Client
}

func (c *client) do(method, path string, body any) (*http.Response, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest(method, c.api+path, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("Content-Type", "application/json")
	return c.http.Do(req)
}

func (c *client) submit(job map[string]any) {
	resp, err := c.do("POST", "/v1/jobs", job)
	if err != nil {
		log.Printf("submit: %v", err)
		time.Sleep(time.Second)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		log.Printf("submit: API returned %d", resp.StatusCode)
	}
}

// ensureSchedule registers a once-a-minute cron job, unless a previous run did.
func (c *client) ensureSchedule(hooks string) {
	const name = "demo-heartbeat"
	resp, err := c.do("GET", "/v1/schedules", nil)
	if err != nil {
		log.Fatalf("list schedules: %v", err)
	}
	var list struct {
		Schedules []struct {
			Name string `json:"name"`
		} `json:"schedules"`
	}
	err = json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if err != nil {
		log.Fatalf("list schedules: API returned %d", resp.StatusCode)
	}
	for _, s := range list.Schedules {
		if s.Name == name {
			return
		}
	}
	resp, err = c.do("POST", "/v1/schedules", map[string]any{
		"name": name,
		"cron": "* * * * *",
		"job_template": map[string]any{
			"type":    "webhook",
			"payload": map[string]any{"url": hooks + "/work?ms=200"},
		},
	})
	if err != nil {
		log.Fatalf("create schedule: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		log.Fatalf("create schedule: API returned %d", resp.StatusCode)
	}
}
