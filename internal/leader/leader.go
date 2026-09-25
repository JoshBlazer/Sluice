package leader

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
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

// ClientConfig describes how to reach etcd. Only Endpoints is required.
type ClientConfig struct {
	Endpoints []string // "host:port", or "https://host:port" with TLS
	Username  string
	Password  string
	// TLS: CAFile verifies the server; CertFile and KeyFile (together) present a
	// client certificate. Setting any of them enables TLS.
	CAFile   string
	CertFile string
	KeyFile  string
}

// NewClient creates an etcd v3 client.
func NewClient(cfg ClientConfig) (*clientv3.Client, error) {
	tlsCfg, err := cfg.tlsConfig()
	if err != nil {
		return nil, err
	}
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: 5 * time.Second,
		Username:    cfg.Username,
		Password:    cfg.Password,
		TLS:         tlsCfg,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd dial: %w", err)
	}
	return c, nil
}

func (cfg ClientConfig) tlsConfig() (*tls.Config, error) {
	if cfg.CAFile == "" && cfg.CertFile == "" && cfg.KeyFile == "" {
		return nil, nil
	}
	t := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read etcd CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("etcd CA file %s contains no PEM certificates", cfg.CAFile)
		}
		t.RootCAs = pool
	}
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return nil, fmt.Errorf("etcd client certificate needs both a cert file and a key file")
	}
	if cfg.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load etcd client certificate: %w", err)
		}
		t.Certificates = []tls.Certificate{cert}
	}
	return t, nil
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
