package registry

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// BackoffTurnstile is a turnstile that can be put in a backoff state.
// Enter will block until both the turnstile is free, and any backoff is over.
// Exit frees the turnstile.
// A backoff can be set by calling SetBackoff.
// A backoff can be reset by calling ResetExponentialBackoff.
// Use NewBackoffTurnstile to create a new BackoffTurnstile.
// Do not modify the fields of a BackoffTurnstile struct directly.
type BackoffTurnstile struct {
	ctx context.Context

	backoffUntil time.Time
	backoffTimer *time.Timer

	expoBase              float64
	numExpoBackoffs       uint64
	initialExpoBackoff    time.Duration
	maxExponentialBackoff time.Duration

	enter      chan struct{}
	exit       chan struct{}
	newBackoff chan *http.Response
}

// NewBackoffTurnstile creates a new BackoffTurnstile.
// exponentialBase is the base of the exponential backoff.
// initialBackoff is the initial backoff time for exponential backoff.
// maxBackoff is the maximum backoff time.
// The turnstile is initially free.
// The turnstile goroutine is started in the background. It is stopped when the context is cancelled.
func NewBackoffTurnstile(ctx context.Context, exponentialBase float64, initialBackoff, maxBackoff time.Duration) *BackoffTurnstile {
	backoff := BackoffTurnstile{
		ctx: ctx,

		backoffTimer: time.NewTimer(0),

		enter:      make(chan struct{}),
		exit:       make(chan struct{}),
		newBackoff: make(chan *http.Response),

		expoBase:              exponentialBase,
		numExpoBackoffs:       0,
		initialExpoBackoff:    initialBackoff,
		maxExponentialBackoff: maxBackoff,
	}

	if !backoff.backoffTimer.Stop() {
		<-backoff.backoffTimer.C
	}

	go backoff.work()
	return &backoff
}

// SetBackoff sets the backoff time.
// Enter() will block for any goroutine that calls it until the backoff is over.
// If the backoff is already set to a later time, it is not changed.
// If the backoff is further in the future than the max backoff time, it is set to the max backoff time.
func (b *BackoffTurnstile) SetBackoff(res *http.Response) {
	b.newBackoff <- res
}

// Enter blocks until the turnstile is free.
// If the turnstile is in a backoff, Enter blocks until the backoff is over.
// After Enter returns, the turnstile is occupied. No other goroutine can enter until Exit is called.
func (b *BackoffTurnstile) Enter() {
	b.enter <- struct{}{}
}

// Exit frees the turnstile.
// Only call this function after Enter has been called.
func (b *BackoffTurnstile) Exit() {
	b.exit <- struct{}{}
}

// ResetExponentialBackoff resets the exponential backoff counter.
// This function should be called upon a successful request to prevent a long backoff next time.
func (b *BackoffTurnstile) ResetExponentialBackoff() {
	atomic.StoreUint64(&b.numExpoBackoffs, 0)
}

// work is the main loop of the turnstile goroutine.
func (b *BackoffTurnstile) work() {
workLoop:
	for {
	enterLoop:
		// First, we wait for someone to enter the turnstile
		for {
			// In case we are in a backoff, wait for it to finish
			b.waitForBackoffInternal()
			select {
			case <-b.enter:
				break enterLoop
			case req := <-b.newBackoff:
				// We got a new backoff, wait for it to finish
				b.setBackoffInternal(req)
				continue enterLoop
			case <-b.ctx.Done():
				break workLoop
			}
		}
	exitLoop:
		// Someone entered the turnstile, now we wait for them to exit
		for {
			select {
			case <-b.exit:
				// Someone exited the turnstile, it is now free
				break exitLoop
			case req := <-b.newBackoff:
				b.setBackoffInternal(req)
			case <-b.ctx.Done():
				break workLoop
			}

		}
	}
	// Stop the timer and drain the channel
	if !b.backoffTimer.Stop() {
		select {
		case <-b.backoffTimer.C:
		default:
		}
	}
}

// waitForBackoffInternal waits for the backoff to finish, while handling new backoffs.
func (b *BackoffTurnstile) waitForBackoffInternal() {
backoffLoop:
	for b.backoffUntil.After(time.Now()) {
		select {
		case <-b.backoffTimer.C:
			// Backoff is over
			break backoffLoop
		case req := <-b.newBackoff:
			// We got a new backoff
			b.setBackoffInternal(req)
		case <-b.ctx.Done():
			return
		}
	}
}

// setBackoffInternal sets the backoff time, and starts the backoff timer.
func (b *BackoffTurnstile) setBackoffInternal(res *http.Response) {
	numBackoffs := atomic.LoadUint64(&b.numExpoBackoffs)
	// Try to get the header Retry-After
	backoffDuration, err := getRetryAfterDuration(res)
	if err != nil {
		// No Retry-After header, use exponential backoff
		exponential := math.Pow(b.expoBase, float64(numBackoffs))
		if math.IsInf(exponential, 0) {
			backoffDuration = b.maxExponentialBackoff
		} else {
			backoffDuration = time.Duration(exponential) * b.initialExpoBackoff
		}
		prevBackoffs := atomic.SwapUint64(&b.numExpoBackoffs, numBackoffs+1)
		if numBackoffs > 0 && prevBackoffs == 0 {
			// Someone reset the backoff count while we were calculating the backoff
			// set the backoff back to 0
			atomic.StoreUint64(&b.numExpoBackoffs, 0)
		}
	}

	// Cap the backoff time
	if backoffDuration > b.maxExponentialBackoff {
		backoffDuration = b.maxExponentialBackoff
	}

	// If the backoff is already set to a later time, don't change it
	if time.Now().Add(backoffDuration).Before(b.backoffUntil) {
		return
	}
	b.backoffUntil = time.Now().Add(backoffDuration)

	// Stop the timer and drain the channel
	if !b.backoffTimer.Stop() {
		select {
		case <-b.backoffTimer.C:
		default:
		}
	}
	// Restart the backoff timer
	b.backoffTimer.Reset(backoffDuration)
}

// getRetryAfterDuration tries to parse the Retry-After header as an integer or HTTP date.
// If the header is not set, or cannot be parsed, return 0 and an error.
func getRetryAfterDuration(r *http.Response) (time.Duration, error) {
	retryAfterString := r.Header.Get("Retry-After")
	if retryAfterString == "" {
		return 0, fmt.Errorf("no Retry-After header")
	}

	// Try parsing as an integer
	if delaySec, err := strconv.Atoi(retryAfterString); err == nil {
		return time.Duration(delaySec) * time.Second, nil
	}

	// Try parsing as HTTP date
	if delayTime, err := http.ParseTime(retryAfterString); err == nil {
		// Since HTTP dates are specified with second precision, we add a second to the delay,
		// to be sure we don't exit the backoff too early.
		return time.Until(delayTime) + time.Second, nil
	}

	return 0, fmt.Errorf("could not parse Retry-After header")
}
