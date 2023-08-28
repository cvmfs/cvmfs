package registry

import (
	"container/list"
	"context"
	"sort"
	"time"
)

// Mockable function for testing
var currentTimeFunc = func() int64 {
	return time.Now().UnixNano()
}

// RateLimit is a struct that holds the interval and count for a rate limit
// For example, if the rate limit is 100 requests per 5 minutes, the struct would be:
// RateLimit{ Interval: 5 * time.Minute, MaxCount: 100 }
// The MaxCount must be greater than 0.
type RateLimit struct {
	Interval time.Duration
	MaxCount int
}

type RequestLimiter struct {
	ctx context.Context

	rateLimits       []RateLimit
	rateLimitTimer   *time.Timer
	previousRequests *PreSortedRingBuffer
	waitingForRate   bool

	enter chan chan<- struct{}
	exit  chan struct{}
	queue *list.List

	maxConcurrentRequests int
	numConcurrentRequests int
}

func NewRequestLimiter(ctx context.Context, maxConcurrentRequests int, rateLimits []RateLimit) *RequestLimiter {
	// We make a copy of the rateLimits
	// It allows us to sort it, and to ignore rate limits with MaxCount <= 0
	// It also prevents the caller from modifying the rateLimits slice later
	internalRateLimits := make([]RateLimit, 0)
	for _, rateLimit := range rateLimits {
		if rateLimit.MaxCount <= 0 {
			// Ignore rate limits with MaxCount <= 0
			continue
		}
		rateLimit := rateLimit
		internalRateLimits = append(internalRateLimits, rateLimit)
	}

	maxCountForLongestInterval := 0
	if len(internalRateLimits) > 0 {
		// Sort the rate limits by interval
		sort.Slice(internalRateLimits, func(i, j int) bool {
			return internalRateLimits[i].Interval < internalRateLimits[j].Interval
		})
		maxCountForLongestInterval = internalRateLimits[len(internalRateLimits)-1].MaxCount
	}

	// Initialize the rateLimitTimer
	rateLimitTimer := time.NewTimer(0)
	if !rateLimitTimer.Stop() {
		<-rateLimitTimer.C
	}

	requestLimiter := RequestLimiter{
		ctx: ctx,

		enter: make(chan chan<- struct{}),
		exit:  make(chan struct{}),
		queue: list.New(),

		rateLimits:     internalRateLimits,
		rateLimitTimer: rateLimitTimer,
		// At most, we will have maxCountForLongestInterval requests in the previousRequests buffer
		// because if we get more, we would already be over the rate limit.
		previousRequests: NewPreSortedRingBuffer(maxCountForLongestInterval),

		maxConcurrentRequests: maxConcurrentRequests,
		numConcurrentRequests: 0,
	}
	go requestLimiter.work()
	return &requestLimiter
}

// Enter blocks until the request limiter is ready to perform a request.
// If the RequestLimiter's context is cancelled, this function returns immediately.
func (r *RequestLimiter) Enter() {
	ready := make(chan struct{})
	r.enter <- ready
	select {
	case <-ready:
	case <-r.ctx.Done(): // Context is cancelled, return immediately
	}
}

// Exit signals that this goroutine is done performing a request, freeing up space for another request.
// This function should be called after previously calling Enter.
func (r *RequestLimiter) Exit() {
	select {
	case r.exit <- struct{}{}:
	case <-r.ctx.Done(): // Context is cancelled, return immediately
	}

}

// work is the main function of the RequestLimiter.
// It exits when the context is cancelled.
func (r *RequestLimiter) work() {
workLoop:
	for {
		select {
		case <-r.ctx.Done():
			break workLoop
		case ready := <-r.enter:
			if (r.numConcurrentRequests < r.maxConcurrentRequests) && !r.waitingForRate {

				// We can allow the request to proceed
				ready <- struct{}{}
				if len(r.rateLimits) > 0 {
					r.previousRequests.Push(currentTimeFunc())
				}
				r.numConcurrentRequests++
				r.checkRatesAndUpdateTimer()
			} else {
				// We queue the request
				r.queue.PushBack(ready)
			}
		case <-r.exit:
			r.numConcurrentRequests--
			if r.queue.Len() > 0 && !r.waitingForRate {
				// We can allow the next request to proceed
				ready := r.queue.Remove(r.queue.Front()).(chan<- struct{})
				ready <- struct{}{}
				if len(r.rateLimits) > 0 {
					r.previousRequests.Push(currentTimeFunc())
				}
				r.numConcurrentRequests++
				r.checkRatesAndUpdateTimer()
			}
		case <-r.rateLimitTimer.C:
			r.waitingForRate = false
			// As long as nothing is stopping us from sending a request, we go through the queue
			for r.queue.Len() > 0 && (r.numConcurrentRequests < r.maxConcurrentRequests) && !r.waitingForRate {
				// We can allow the next request to proceed
				ready := r.queue.Remove(r.queue.Front()).(chan<- struct{})
				ready <- struct{}{}
				if len(r.rateLimits) > 0 {
					r.previousRequests.Push(currentTimeFunc())
				}
				r.numConcurrentRequests++
				r.checkRatesAndUpdateTimer()
			}
		}
	}
	if !r.rateLimitTimer.Stop() {
		select {
		case <-r.rateLimitTimer.C:
		default:
		}
	}
}

func (r *RequestLimiter) checkRatesAndUpdateTimer() {
	// Check if we need to wait for rates
	waitTime := r.timeUntilRatesOK()
	if waitTime > 0 {
		r.waitingForRate = true
		if !r.rateLimitTimer.Stop() {
			select {
			case <-r.rateLimitTimer.C:
			default:
			}
		}
		r.rateLimitTimer.Reset(waitTime)
	}
}

func (r *RequestLimiter) timeUntilRatesOK() (waitTime time.Duration) {

	if len(r.rateLimits) == 0 {
		// No rate limits, nothing to do
		return 0
	}
	if r.previousRequests.Size() == 0 {
		// No previous requests, nothing to do
		return 0
	}

	var waitTimeUntilOk time.Duration

	// Remove all elements from previousRequests that are older than the longest interval
	longestWindowStart := currentTimeFunc() - r.rateLimits[len(r.rateLimits)-1].Interval.Nanoseconds()
	r.previousRequests.RemoveSmallerThan(longestWindowStart)
	// We can already check if we are over the longest interval
	if r.previousRequests.Size() >= r.rateLimits[len(r.rateLimits)-1].MaxCount {
		// We need to wait until *after*, so we add 1 nanosecond
		waitTimeUntilOk = time.Duration(r.previousRequests.At(0)-longestWindowStart)*time.Nanosecond + 1
	}

	// We check the other intervals
	for _, rateLimit := range r.rateLimits[:len(r.rateLimits)-1] {
		windowStart := currentTimeFunc() - rateLimit.Interval.Nanoseconds()
		idx, val, found := r.previousRequests.BinarySearchGeq(windowStart)

		if found {
			// Number of requests in the current window
			requestsInWindow := r.previousRequests.Size() - idx

			if requestsInWindow >= rateLimit.MaxCount {
				// We need to wait until *after*, so we add 1 nanosecond
				currentWaitTime := time.Duration(val-windowStart)*time.Nanosecond + 1

				// Update the waitTimeUntilOk if it's longer than the previously calculated wait time
				if currentWaitTime > waitTimeUntilOk {
					waitTimeUntilOk = currentWaitTime
				}
			}
		}

	}
	return waitTimeUntilOk
}
