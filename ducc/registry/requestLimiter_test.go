package registry

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewRequestLimiter(t *testing.T) {
	tests := []struct {
		name                  string
		maxConcurrentRequests int
		rateLimits            []RateLimit
		expectedRateLimits    []RateLimit
	}{
		{
			name:                  "Empty rate limits",
			maxConcurrentRequests: 5,
			rateLimits:            []RateLimit{},
			expectedRateLimits:    []RateLimit{},
		},
		{
			name:                  "Sorted rate limits",
			maxConcurrentRequests: 5,
			rateLimits: []RateLimit{
				{Interval: 5 * time.Second, MaxCount: 2},
				{Interval: 2 * time.Second, MaxCount: 4},
			},
			expectedRateLimits: []RateLimit{
				{Interval: 2 * time.Second, MaxCount: 4},
				{Interval: 5 * time.Second, MaxCount: 2},
			},
		},
		{
			name:                  "Ignore rate limits with MaxCount <= 0",
			maxConcurrentRequests: 5,
			rateLimits: []RateLimit{
				{Interval: 2 * time.Second, MaxCount: 0},
				{Interval: 5 * time.Second, MaxCount: 2},
			},
			expectedRateLimits: []RateLimit{
				{Interval: 5 * time.Second, MaxCount: 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			previousRateLimits := make([]RateLimit, len(tt.rateLimits))
			copy(previousRateLimits, tt.rateLimits)

			rl := NewRequestLimiter(context.Background(), tt.maxConcurrentRequests, tt.rateLimits)

			// Check if rateLimits input is unchanged
			if !reflect.DeepEqual(tt.rateLimits, previousRateLimits) {
				t.Errorf("input rateLimits were modified")
			}

			// Check sorted rateLimits
			if !reflect.DeepEqual(rl.rateLimits, tt.expectedRateLimits) {
				t.Errorf("Sorted rateLimits = %v, want %v", rl.rateLimits, tt.expectedRateLimits)
			}

			// Check previousRequests buffer capacity
			expectedBufferCapacity := 0
			if len(tt.rateLimits) > 0 {
				expectedBufferCapacity = tt.expectedRateLimits[len(tt.expectedRateLimits)-1].MaxCount
			}
			if expectedBufferCapacity != rl.previousRequests.capacity {
				t.Errorf("previousRequests buffer capacity = %d, want %d", rl.previousRequests.capacity, expectedBufferCapacity)
			}
		})
	}
}

func TestRequestLimiter_Order(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	numRequests := 10

	rl := NewRequestLimiter(ctx, 1, nil) // max 1 concurrent request, no rate limits

	order := make([]int, 0, numRequests)
	ch := make(chan int, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(num int) {
			rl.Enter()
			ch <- num
			time.Sleep(100 * time.Millisecond)
			rl.Exit()
		}(i)
		time.Sleep(10 * time.Millisecond) // Make sure goroutines are started in order
	}

	for i := 0; i < 3; i++ {
		num := <-ch
		order = append(order, num)
	}

	for i := 1; i < len(order); i++ {
		if order[i] <= order[i-1] {
			t.Fatalf("Expected requests to be processed in ascending order, got order: %v", order)
		}
	}
}

func TestRequestLimiter_MaxConcurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const allowedConcurrent = 5
	const numRequests = 20
	rl := NewRequestLimiter(ctx, allowedConcurrent, nil)

	allRequestsDone := sync.WaitGroup{}
	allRequestsDone.Add(numRequests)

	var tooManyConcurrent int64 = 0
	var concurrentRequests int64 = 0
	firstBatchWg := sync.WaitGroup{}
	firstBatchWg.Add(1)

	for i := 0; i < numRequests; i++ {
		go func() {
			rl.Enter()
			if numConcurrent := atomic.AddInt64(&concurrentRequests, 1); numConcurrent > allowedConcurrent {
				atomic.StoreInt64(&tooManyConcurrent, numConcurrent)
			}
			time.Sleep(1 * time.Millisecond)
			firstBatchWg.Wait()
			atomic.AddInt64(&concurrentRequests, -1)
			rl.Exit()
			allRequestsDone.Done()
		}()
	}
	time.Sleep(20 * time.Millisecond) // Some time for goroutines to start

	// Test that we first go up to maxConcurrent
	if atomic.LoadInt64(&concurrentRequests) != allowedConcurrent {
		t.Fatalf("Expected to have %d concurrent requests, but got %d", allowedConcurrent, concurrentRequests)
	}
	firstBatchWg.Done()

	// Then we just let the rest of the requests go through
	allRequestsDone.Wait()

	if tooManyConcurrent > 0 {
		t.Fatalf("Expected to have at most %d concurrent requests, but got %d", allowedConcurrent, tooManyConcurrent)
	}
}

func TestRequestLimiter_MultipleRateLimits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rl := NewRequestLimiter(ctx, 100, []RateLimit{
		{Interval: 100 * time.Millisecond, MaxCount: 2},
		{Interval: 200 * time.Millisecond, MaxCount: 4},
	})

	// First 2 requests are instant
	startTime := time.Now()
	rl.Enter()
	rl.Exit()
	rl.Enter()
	rl.Exit()
	if elapsed := time.Since(startTime); elapsed > 10*time.Millisecond {
		t.Fatalf("Expected first 2 requests to be instant, but took %v", elapsed)
	}

	// The next request should wait because of the first rate limit
	rl.Enter()
	rl.Exit()
	elapsed := time.Since(startTime)
	if elapsed < 100*time.Millisecond {
		t.Fatalf("Expected at least 100 ms wait due to rate limit, but waited %v", elapsed)
	}

	// Fourth request, still within second interval. Should be instant.
	req4Start := time.Now()
	rl.Enter()
	rl.Exit()
	if elapsed := time.Since(req4Start); elapsed > 10*time.Millisecond {
		t.Fatalf("Expected fourth request to be instant, but took %v", elapsed)
	}

	// The fifth request should wait because of the second rate limit
	rl.Enter()
	rl.Exit()
	if elapsed := time.Since(startTime); elapsed < 200*time.Millisecond {
		t.Fatalf("Expected at least 2 ms wait due to rate limit, but waited %v", elapsed)
	}

}

func TestTimeUntilIntervalsOK(t *testing.T) {
	tests := []struct {
		name             string
		rateLimits       []RateLimit
		previousRequests []int64
		mockTime         int64
		expectedWaitTime time.Duration
	}{
		{
			name:             "No rate limits set",
			rateLimits:       []RateLimit{},
			previousRequests: []int64{},
			mockTime:         5 * time.Second.Nanoseconds(),
			expectedWaitTime: 0,
		},
		{
			name: "Rate limits not exceeded",
			rateLimits: []RateLimit{
				{Interval: 10 * time.Second, MaxCount: 4},
			},
			previousRequests: []int64{
				0 * time.Second.Nanoseconds(),
				2 * time.Second.Nanoseconds(),
				4 * time.Second.Nanoseconds(),
			},
			mockTime:         5 * time.Second.Nanoseconds(),
			expectedWaitTime: 0,
		},
		{
			name: "Only shorter interval exceeded",
			rateLimits: []RateLimit{
				{Interval: 5 * time.Second, MaxCount: 2},
				{Interval: 10 * time.Second, MaxCount: 5},
			},
			previousRequests: []int64{
				0,
				1 * time.Second.Nanoseconds(),
			},
			mockTime:         2 * time.Second.Nanoseconds(),
			expectedWaitTime: 3*time.Second + 1,
		},
		{
			name: "All intervals exceeded",
			rateLimits: []RateLimit{
				{Interval: 5 * time.Second, MaxCount: 2},
				{Interval: 100 * time.Second, MaxCount: 4},
			},
			previousRequests: []int64{
				0 * time.Second.Nanoseconds(),
				1 * time.Second.Nanoseconds(),
				6 * time.Second.Nanoseconds(),
				7 * time.Second.Nanoseconds(),
			},
			mockTime:         7 * time.Second.Nanoseconds(),
			expectedWaitTime: (100-7)*time.Second + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock the current time function
			currentTimeFunc = func() int64 {
				return tt.mockTime
			}

			rl := NewRequestLimiter(context.Background(), 10, tt.rateLimits)
			for _, req := range tt.previousRequests {
				rl.previousRequests.Push(req)
			}

			if got := rl.timeUntilRatesOK(); got != tt.expectedWaitTime {
				t.Errorf("timeUntilIntervalsOK() = %v, want %v", got, tt.expectedWaitTime)
			}
		})
	}
}
