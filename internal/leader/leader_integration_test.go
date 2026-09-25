//go:build integration

package leader_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sluice/internal/leader"
	"github.com/sluice/internal/testutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// failoverTarget is the README / Phase 3 promise for a leader that shuts down.
const failoverTarget = 2 * time.Second

// crashFailoverLimit allows for lease expiry: a crashed leader holds its lease for
// up to DefaultTTLSeconds after its last keepalive (measured 1.5-2.1s locally).
const crashFailoverLimit = 2500 * time.Millisecond

func client(t *testing.T) *clientv3.Client {
	t.Helper()
	c, err := leader.NewClient(testutil.EtcdEndpoints(t))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// standbyTakeover campaigns b in the background and returns a channel that
// receives the moment b wins.
func standbyTakeover(t *testing.T, b *leader.Election) <-chan time.Time {
	won := make(chan time.Time, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, resign, err := b.Campaign(ctx, "b")
		if err != nil {
			t.Errorf("standby campaign: %v", err)
			return
		}
		won <- time.Now()
		resign()
	}()
	return won
}

func TestFailover_LeaderResigns(t *testing.T) {
	prefix := "/sluice-test/" + uuid.NewString()
	ca, cb := client(t), client(t)
	defer ca.Close()
	defer cb.Close()
	a := leader.New(ca, prefix, leader.DefaultTTLSeconds)
	b := leader.New(cb, prefix, leader.DefaultTTLSeconds)

	_, resign, err := a.Campaign(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	won := standbyTakeover(t, b)
	time.Sleep(500 * time.Millisecond) // let b queue behind a

	stoppedAt := time.Now()
	resign()
	took := (<-won).Sub(stoppedAt)
	t.Logf("graceful failover took %v", took)
	if took > failoverTarget {
		t.Fatalf("graceful failover took %v, want <= %v", took, failoverTarget)
	}
}

// A leader that dies without resigning (kill -9, node loss, partition) only
// loses leadership when its lease expires.
func TestFailover_LeaderCrashes(t *testing.T) {
	prefix := "/sluice-test/" + uuid.NewString()
	ca, cb := client(t), client(t)
	defer cb.Close()
	a := leader.New(ca, prefix, leader.DefaultTTLSeconds)
	b := leader.New(cb, prefix, leader.DefaultTTLSeconds)

	leaderCtx, _, err := a.Campaign(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	won := standbyTakeover(t, b)
	time.Sleep(500 * time.Millisecond)

	// Closing the client stops lease keepalives without revoking the lease.
	crashedAt := time.Now()
	ca.Close()
	took := (<-won).Sub(crashedAt)
	t.Logf("crash failover took %v", took)

	select {
	case <-leaderCtx.Done():
	case <-time.After(time.Second):
		t.Error("crashed leader's context was not cancelled")
	}
	if took > crashFailoverLimit {
		t.Fatalf("crash failover took %v, want <= %v", took, crashFailoverLimit)
	}
}
