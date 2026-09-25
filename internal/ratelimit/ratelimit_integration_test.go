//go:build integration

package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sluice/internal/ratelimit"
	"github.com/sluice/internal/testutil"
)

func TestAllow_BurstThenRefill(t *testing.T) {
	ctx := context.Background()
	l := ratelimit.New(testutil.Redis(t))
	tn := uuid.New()
	const limit = 5

	// A burst of 10 immediate requests: the bucket holds 5, plus whatever refills
	// (at 5/s) while the calls run, which on a slow machine is not zero.
	start := time.Now()
	allowed := 0
	for i := 0; i < 2*limit; i++ {
		ok, err := l.Allow(ctx, tn, limit)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			allowed++
		}
	}
	refilled := int(time.Since(start).Seconds()*limit) + 1
	if allowed < limit || allowed > limit+refilled {
		t.Fatalf("allowed %d of %d burst requests; want %d plus at most %d refilled", allowed, 2*limit, limit, refilled)
	}
	if ok, _ := l.Allow(ctx, tn, limit); ok && refilled == 1 {
		t.Fatal("request beyond burst was allowed")
	}

	// Refill is continuous: after ~1/limit seconds one token is back.
	time.Sleep(250 * time.Millisecond)
	if ok, _ := l.Allow(ctx, tn, limit); !ok {
		t.Fatal("token did not refill")
	}
}

func TestAllow_TenantsAreIndependent(t *testing.T) {
	ctx := context.Background()
	l := ratelimit.New(testutil.Redis(t))
	a, b := uuid.New(), uuid.New()

	l.Allow(ctx, a, 1)
	if ok, _ := l.Allow(ctx, a, 1); ok {
		t.Fatal("tenant a should be limited")
	}
	if ok, _ := l.Allow(ctx, b, 1); !ok {
		t.Fatal("tenant b must not share a's bucket")
	}
}

func TestAllow_ZeroMeansUnlimited(t *testing.T) {
	l := ratelimit.New(testutil.Redis(t))
	for i := 0; i < 100; i++ {
		if ok, err := l.Allow(context.Background(), uuid.New(), 0); err != nil || !ok {
			t.Fatalf("limit 0 should be unlimited: ok=%v err=%v", ok, err)
		}
	}
}
