//go:build integration

package queue_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/testutil"
)

func TestDequeue_PriorityBeatsTenantOrder(t *testing.T) {
	ctx := context.Background()
	q := queue.New(testutil.Redis(t))
	a, b := uuid.New(), uuid.New()
	tenants := []queue.TenantWeight{{ID: a, Weight: 1000}, {ID: b, Weight: 1}}

	low := uuid.New()
	high := uuid.New()
	if err := q.Enqueue(ctx, a, low, job.PriorityLow); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, b, high, job.PriorityHigh); err != nil {
		t.Fatal(err)
	}

	// Tenant a is visited first almost always, but b's high-priority job must still win.
	got, err := q.Dequeue(ctx, "w1", tenants, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != high {
		t.Fatalf("dequeued %s, want the high-priority job %s", got, high)
	}
}

func TestDequeue_FIFOWithinLane(t *testing.T) {
	ctx := context.Background()
	q := queue.New(testutil.Redis(t))
	tn := uuid.New()
	tenants := []queue.TenantWeight{{ID: tn, Weight: 100}}

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for _, id := range ids {
		if err := q.Enqueue(ctx, tn, id, job.PriorityNormal); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range ids {
		got, err := q.Dequeue(ctx, "w1", tenants, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("dequeue %d = %s, want %s", i, got, want)
		}
	}
}

func TestDequeue_WeightedFairness(t *testing.T) {
	ctx := context.Background()
	q := queue.New(testutil.Redis(t))
	heavy, light := uuid.New(), uuid.New()
	tenants := []queue.TenantWeight{{ID: heavy, Weight: 300}, {ID: light, Weight: 100}}

	const perTenant = 400
	isHeavy := map[uuid.UUID]bool{}
	for i := 0; i < perTenant; i++ {
		h, l := uuid.New(), uuid.New()
		isHeavy[h] = true
		if err := q.Enqueue(ctx, heavy, h, job.PriorityNormal); err != nil {
			t.Fatal(err)
		}
		if err := q.Enqueue(ctx, light, l, job.PriorityNormal); err != nil {
			t.Fatal(err)
		}
	}

	// While both tenants have backlog, heavy should get ~75% of dequeues.
	const sample = 200
	heavyCount := 0
	for i := 0; i < sample; i++ {
		id, err := q.Dequeue(ctx, "w1", tenants, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if isHeavy[id] {
			heavyCount++
		}
	}
	share := float64(heavyCount) / sample
	if math.Abs(share-0.75) > 0.12 {
		t.Fatalf("heavy tenant share = %.2f, want ~0.75", share)
	}
}

func TestDequeue_TimesOutEmpty(t *testing.T) {
	q := queue.New(testutil.Redis(t))
	start := time.Now()
	id, err := q.Dequeue(context.Background(), "w1", []queue.TenantWeight{{ID: uuid.New(), Weight: 1}}, 300*time.Millisecond)
	if err != nil || id != uuid.Nil {
		t.Fatalf("got id=%s err=%v, want nil/nil", id, err)
	}
	if time.Since(start) < 250*time.Millisecond {
		t.Fatal("returned before the timeout")
	}
}

func TestDepthsAndProcessingList(t *testing.T) {
	ctx := context.Background()
	rdb := testutil.Redis(t)
	q := queue.New(rdb)
	tn := uuid.New()

	for i := 0; i < 3; i++ {
		q.Enqueue(ctx, tn, uuid.New(), job.PriorityHigh)
	}
	q.Enqueue(ctx, tn, uuid.New(), job.PriorityLow)

	depths, err := q.Depths(ctx, []uuid.UUID{tn})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, d := range depths {
		got[d.Priority] = d.Depth
	}
	if got["high"] != 3 || got["normal"] != 0 || got["low"] != 1 {
		t.Fatalf("depths = %v, want high=3 normal=0 low=1", got)
	}

	id, err := q.Dequeue(ctx, "w1", []queue.TenantWeight{{ID: tn, Weight: 1}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ttl := rdb.TTL(ctx, "processing:w1").Val(); ttl <= 0 {
		t.Fatalf("processing list TTL = %v, want a positive expiry", ttl)
	}
	if err := q.RemoveFromProcessing(ctx, "w1", id); err != nil {
		t.Fatal(err)
	}
	if n := rdb.LLen(ctx, "processing:w1").Val(); n != 0 {
		t.Fatalf("processing list length = %d after removal", n)
	}
}

func TestEnqueue_IdempotentWhileWaiting(t *testing.T) {
	ctx := context.Background()
	rdb := testutil.Redis(t)
	q := queue.New(rdb)
	tn := uuid.New()
	id := uuid.New()
	tenants := []queue.TenantWeight{{ID: tn, Weight: 1}}

	for i := 0; i < 5; i++ {
		if err := q.Enqueue(ctx, tn, id, job.PriorityNormal); err != nil {
			t.Fatal(err)
		}
	}
	depths, _ := q.Depths(ctx, []uuid.UUID{tn})
	var total int64
	for _, d := range depths {
		total += d.Depth
	}
	if total != 1 {
		t.Fatalf("queue holds %d entries after 5 enqueues of one job, want 1", total)
	}
	if ttl := rdb.TTL(ctx, "enqueued:"+id.String()).Val(); ttl <= 0 || ttl > queue.EnqueuedTTL {
		t.Fatalf("marker TTL = %v, want within (0, %v]", ttl, queue.EnqueuedTTL)
	}

	got, err := q.Dequeue(ctx, "w1", tenants, time.Second)
	if err != nil || got != id {
		t.Fatalf("dequeue = %s, %v", got, err)
	}
	// Once popped, the job can be enqueued again (e.g. for a retry).
	if err := q.Enqueue(ctx, tn, id, job.PriorityNormal); err != nil {
		t.Fatal(err)
	}
	if got, _ := q.Dequeue(ctx, "w1", tenants, time.Second); got != id {
		t.Fatalf("re-enqueued job not dequeued: got %s", got)
	}
}
