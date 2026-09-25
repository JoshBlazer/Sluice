package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryRecord_SucceedsAfterTransientErrors(t *testing.T) {
	calls := 0
	err := retryRecord(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("too many clients")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want nil after 3 calls", err, calls)
	}
}

func TestRetryRecord_GivesUpAtDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	boom := errors.New("down")
	start := time.Now()
	err := retryRecord(ctx, func(context.Context) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the last write error", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %v, should stop at the context deadline", took)
	}
}
