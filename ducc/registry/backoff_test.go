package registry

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"
)

func TestBackoffGuard_New(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	guard := NewBackoffGuard(ctx, 2, 1*time.Second, 10*time.Second)
	if guard == nil {
		t.Error("Failed to create a new BackoffGuard")
	}
}

func TestBackoffGuard_SetBackoffRetryAfterInt(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	guard := NewBackoffGuard(ctx, 2, 1*time.Second, 10*time.Second)

	// Use a mock HTTP response
	res := &http.Response{
		Header: make(http.Header),
	}
	res.Header.Set("Retry-After", "1") // Retry after 1 second
	guard.SetBackoff(res)
	startTime := time.Now()
	guard.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < 1*time.Second || endTime.Sub(startTime) > 1*time.Second+10*time.Millisecond {
		t.Error("Did not backoff for the specified time")
	}
}

func TestBackoffGuard_SetBackoffRetryAfterHttpDate(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	guard := NewBackoffGuard(ctx, 2, 1*time.Second, 10*time.Second)

	// Use a mock HTTP response with a Retry-After header set to an HTTP date format.
	futureTime := time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat)
	res := &http.Response{
		Header: make(http.Header),
	}
	res.Header.Set("Retry-After", futureTime)

	guard.SetBackoff(res)
	startTime := time.Now()
	guard.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < 2*time.Second || endTime.Sub(startTime) > (2+1)*time.Second+10*time.Millisecond {
		t.Error("Did not backoff for the specified time")
	}
}

func TestBackoffGuard_SetBackoffExponential(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	guard := NewBackoffGuard(ctx, 2, 10*time.Millisecond, 1*time.Second)

	res := &http.Response{
		Header: make(http.Header),
	}

	guard.SetBackoff(res)
	startTime := time.Now()
	guard.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < 10*time.Millisecond || endTime.Sub(startTime) > 10*time.Millisecond+10*time.Millisecond {
		t.Error("Did not backoff for the specified time")
	}
}

func TestBackoffGuard_MultipleExponentialBackoffs(t *testing.T) {
	const backoffBase = 1.5
	const initialBackoff = 10 * time.Millisecond

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	guard := NewBackoffGuard(ctx, backoffBase, initialBackoff, 1*time.Second)

	res := &http.Response{
		Header: make(http.Header),
	}

	for i := 0; i < 5; i++ {
		guard.SetBackoff(res)
	}

	expectedTotalWait := initialBackoff * time.Duration(math.Pow(backoffBase, 4))

	startTime := time.Now()
	guard.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < expectedTotalWait || endTime.Sub(startTime) > expectedTotalWait+10*time.Millisecond {
		t.Errorf("Did not backoff for the specified time. Expected %s, got %s", expectedTotalWait, endTime.Sub(startTime))
	}
}

func TestBackoffGuard_MaxBackoffExponential(t *testing.T) {
	const backoffBase = 1.5
	const initialBackoff = 10 * time.Millisecond
	const maxBackoff = 100 * time.Millisecond

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	guard := NewBackoffGuard(ctx, backoffBase, initialBackoff, maxBackoff)

	res := &http.Response{
		Header: make(http.Header),
	}

	for i := 0; i < 10; i++ {
		guard.SetBackoff(res)
	}

	expectedTotalWait := maxBackoff

	startTime := time.Now()
	guard.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < expectedTotalWait || endTime.Sub(startTime) > expectedTotalWait+10*time.Millisecond {
		t.Errorf("Did not backoff for the specified time. Expected %s, got %s", expectedTotalWait, endTime.Sub(startTime))
	}
}

func TestBackoffGuard_MaxBackoffRetryAfter(t *testing.T) {
	const maxBackoff = 100 * time.Millisecond

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	guard := NewBackoffGuard(ctx, 2, 1*time.Second, maxBackoff)

	res := &http.Response{
		Header: make(http.Header),
	}
	res.Header.Set("Retry-After", "5")

	guard.SetBackoff(res)
	startTime := time.Now()
	guard.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) < maxBackoff || endTime.Sub(startTime) > maxBackoff+10*time.Millisecond {
		t.Errorf("Did not backoff for the specified time. Expected %s, got %s", maxBackoff, endTime.Sub(startTime))
	}
}

func TestBackoffGuard_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	guard := NewBackoffGuard(ctx, 2, 1*time.Second, 10*time.Second)

	res := &http.Response{
		Header: make(http.Header),
	}
	res.Header.Set("Retry-After", "1")

	guard.SetBackoff(res)
	cancel()
	startTime := time.Now()
	guard.Enter()
	endTime := time.Now()
	if endTime.Sub(startTime) > 10*time.Millisecond {
		t.Error("Turnstile worker did not exit immediately after context cancellation")
	}
}
