package leader

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// DefaultTTLSeconds is the leader lease TTL. It bounds failover after a leader
// crashes (a clean shutdown resigns immediately). 2s is about etcd's minimum
// lease with default settings. A short lease can briefly yield two leaders after
// a long pause, which every scheduler loop tolerates (conditional updates,
// idempotent enqueue, cron idempotency keys).
const DefaultTTLSeconds = 2

// Election manages etcd-based leader election for the scheduler.
// Only one scheduler instance runs the active loops at a time; all others
// are hot standbys that block on Campaign until the leader vacates.
type Election struct {
	client *clientv3.Client
	prefix string
	ttl    int // lease TTL in seconds
}

func New(client *clientv3.Client, prefix string, ttlSeconds int) *Election {
	return &Election{client: client, prefix: prefix, ttl: ttlSeconds}
}

// NewClient creates an etcd v3 client. Endpoints should be passed as
// a slice of "host:port" strings (e.g. []string{"localhost:2379"}).
func NewClient(endpoints []string) (*clientv3.Client, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd dial: %w", err)
	}
	return c, nil
}

// Campaign blocks until this node wins the election.
// It returns a context that is cancelled when leadership is lost (e.g. lease
// expiry due to network partition) and a resign function to voluntarily
// relinquish the lease on clean shutdown.
//
// If ctx is cancelled before winning, Campaign returns ctx.Err().
func (e *Election) Campaign(ctx context.Context, val string) (leaderCtx context.Context, resign func(), err error) {
	sess, err := concurrency.NewSession(e.client,
		concurrency.WithTTL(e.ttl),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create etcd session: %w", err)
	}

	elect := concurrency.NewElection(sess, e.prefix)
	if err := elect.Campaign(ctx, val); err != nil {
		sess.Close()
		return nil, nil, fmt.Errorf("campaign: %w", err)
	}

	lctx, cancel := context.WithCancel(ctx)

	// Cancel leaderCtx if the etcd session expires (lease lost).
	go func() {
		select {
		case <-sess.Done():
			cancel()
		case <-lctx.Done():
		}
	}()

	resign = func() {
		elect.Resign(context.Background()) //nolint:errcheck
		sess.Close()
		cancel()
	}

	return lctx, resign, nil
}
