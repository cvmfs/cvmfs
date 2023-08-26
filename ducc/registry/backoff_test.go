package registry

import (
	"context"
	"math"
	"math/rand"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackoffTurnstile_New(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, 2, 1*time.Second, 10*time.Second)
	if turnstile == nil {
		t.Error("Failed to create a new BackoffTurnstile")
	}
}

func TestBackoffTurnstile_EnterExit(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, 2, 1*time.Second, 10*time.Second)

	go turnstile.Enter()
	turnstile.Exit()
}

func TestBackoffTurnstile_MultipleEnterExit(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, 2, 1*time.Second, 10*time.Second)

	var insideTurnstileCount int64 = 0

	enterExit := func() {
		turnstile.Enter()
		curCount := atomic.AddInt64(&insideTurnstileCount, 1)
		if curCount > 1 {
			t.Errorf("Multiple goroutines inside turnstile at the same time: %d", curCount)
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
		atomic.AddInt64(&insideTurnstileCount, -1)
		turnstile.Exit()
	}

	for i := 0; i < 50; i++ {
		go enterExit()
	}
}

func TestBackoffTurnstile_SetBackoffRetryAfterInt(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, 2, 1*time.Second, 10*time.Second)

	// Use a mock HTTP response
	res := &http.Response{
		Header: make(http.Header),
	}
	res.Header.Set("Retry-After", "1") // Retry after 1 second
	turnstile.SetBackoff(res)
	startTime := time.Now()
	turnstile.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < 1*time.Second || endTime.Sub(startTime) > 1*time.Second+10*time.Millisecond {
		t.Error("Did not backoff for the specified time")
	}
	turnstile.Exit()
}

func TestBackoffTurnstile_SetBackoffRetryAfterHttpDate(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, 2, 1*time.Second, 10*time.Second)

	// Use a mock HTTP response with a Retry-After header set to an HTTP date format.
	futureTime := time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat)
	res := &http.Response{
		Header: make(http.Header),
	}
	res.Header.Set("Retry-After", futureTime)

	turnstile.SetBackoff(res)
	startTime := time.Now()
	turnstile.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < 2*time.Second || endTime.Sub(startTime) > (2+1)*time.Second+10*time.Millisecond {
		t.Error("Did not backoff for the specified time")
	}
	turnstile.Exit()

}

func TestBackoffTurnstile_SetBackoffExponential(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, 2, 10*time.Millisecond, 1*time.Second)

	res := &http.Response{
		Header: make(http.Header),
	}

	turnstile.SetBackoff(res)
	startTime := time.Now()
	turnstile.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < 10*time.Millisecond || endTime.Sub(startTime) > 10*time.Millisecond+10*time.Millisecond {
		t.Error("Did not backoff for the specified time")
	}
	turnstile.Exit()
}

func TestBackoffTurnstile_MultipleExponentialBackoffs(t *testing.T) {
	const backoffBase = 1.5
	const initialBackoff = 10 * time.Millisecond

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, backoffBase, initialBackoff, 1*time.Second)

	res := &http.Response{
		Header: make(http.Header),
	}

	for i := 0; i < 5; i++ {
		turnstile.SetBackoff(res)
	}

	expectedTotalWait := initialBackoff * time.Duration(math.Pow(backoffBase, 4))

	startTime := time.Now()
	turnstile.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < expectedTotalWait || endTime.Sub(startTime) > expectedTotalWait+10*time.Millisecond {
		t.Errorf("Did not backoff for the specified time. Expected %s, got %s", expectedTotalWait, endTime.Sub(startTime))
	}
	turnstile.Exit()
}

func TestBackoffTurnstile_MaxBackoffExponential(t *testing.T) {
	const backoffBase = 1.5
	const initialBackoff = 10 * time.Millisecond
	const maxBackoff = 100 * time.Millisecond

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, backoffBase, initialBackoff, maxBackoff)

	res := &http.Response{
		Header: make(http.Header),
	}

	for i := 0; i < 10; i++ {
		turnstile.SetBackoff(res)
	}

	expectedTotalWait := maxBackoff

	startTime := time.Now()
	turnstile.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < expectedTotalWait || endTime.Sub(startTime) > expectedTotalWait+10*time.Millisecond {
		t.Errorf("Did not backoff for the specified time. Expected %s, got %s", expectedTotalWait, endTime.Sub(startTime))
	}
	turnstile.Exit()
}

func TestBackoffTurnstile_MaxBackoffRetryAfter(t *testing.T) {
	const maxBackoff = 100 * time.Millisecond

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	turnstile := NewBackoffTurnstile(ctx, 2, 1*time.Second, maxBackoff)

	res := &http.Response{
		Header: make(http.Header),
	}
	res.Header.Set("Retry-After", "5")

	turnstile.SetBackoff(res)
	startTime := time.Now()
	turnstile.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < maxBackoff || endTime.Sub(startTime) > maxBackoff+10*time.Millisecond {
		t.Errorf("Did not backoff for the specified time. Expected %s, got %s", maxBackoff, endTime.Sub(startTime))
	}
	turnstile.Exit()
}

func TestBackoffTurnstile_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	turnstile := NewBackoffTurnstile(ctx, 2, 1*time.Second, 10*time.Second)

	res := &http.Response{
		Header: make(http.Header),
	}
	res.Header.Set("Retry-After", "1")

	turnstile.SetBackoff(res)
	cancel()
	startTime := time.Now()
	turnstile.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) > 10*time.Millisecond {
		t.Error("Turnstile worker did not exit immediately after context cancellation")
	}
}
