package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSchedule(t *testing.T) {
	s := NewScheduler[string](1)
	// Schedule an operation and ensure it's present
	s.Schedule("op1", "obj1", "act1", time.Now().Add(5*time.Minute))

	// The operation should be the next to pop
	if *s.Peek() != "op1" {
		t.Fatal("Expected operation was not scheduled correctly")
	}
}

func TestPop(t *testing.T) {
	s := NewScheduler[string](1)
	if s.Pop() != nil {
		t.Fatal("Pop on an empty scheduler should return nil")
	}
	s.Schedule("op1", "obj1", "act1", time.Now().Add(5*time.Minute))

	// Pop should return the scheduled operation
	if *s.Pop() != "op1" {
		t.Fatal("Expected operation was not popped correctly")
	}
}

func TestRemoveActionForObject(t *testing.T) {
	s := NewScheduler[string](1)
	s.Schedule("op1", "obj1", "act1", time.Now().Add(5*time.Minute))

	// Remove the action
	s.RemoveActionForObject("obj1", "act1")

	if s.Peek() != nil {
		t.Fatal("Expected operation was not removed correctly")
	}
}

func TestMultipleScheduling(t *testing.T) {
	s := NewScheduler[string](1)
	s.Schedule("op1", "obj1", "act1", time.Now().Add(10*time.Minute))
	s.Schedule("op2", "obj2", "act2", time.Now().Add(5*time.Minute))

	// op2 should be the first to pop because it has an earlier nextRun
	if *s.Peek() != "op2" {
		t.Fatal("Expected op2 to be the first, but it wasn't")
	}
}

func TestRemoveObjectWithMultipleActions(t *testing.T) {
	s := NewScheduler[string](1)
	s.Schedule("op1", "obj1", "act1", time.Now().Add(10*time.Minute))
	s.Schedule("op2", "obj1", "act2", time.Now().Add(5*time.Minute))

	s.RemoveObject("obj1")

	if s.Peek() != nil {
		t.Fatal("Expected no operations after removing the object, but found some")
	}
}

func TestWaitForNextOperation(t *testing.T) {
	s := NewScheduler[string](1)
	s.Schedule("op1", "obj1", "act1", time.Now().Add(10*time.Millisecond))
	s.Schedule("op2", "obj1", "act1", time.Now().Add(5*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	result, _, _, _ := s.WaitForNextOperation(ctx)
	if *result != "op2" {
		t.Fatal("Expected op2 to be the next operation but got a different one")
	}
}

func TestPopWithEmptyQueue(t *testing.T) {
	s := NewScheduler[string](1)
	result := s.Pop()

	if result != nil {
		t.Fatal("Expected nil when popping from an empty queue")
	}
}

func TestRemoveNonExistentAction(t *testing.T) {
	s := NewScheduler[string](1)
	s.RemoveActionForObject("nonexistentObject", "nonexistentAction")
	// No panic or error means the function handled the non-existent action gracefully.
}

func TestConcurrentScheduling(t *testing.T) {
	s := NewScheduler[string](1)
	var wg sync.WaitGroup

	concurrentOps := 100
	wg.Add(concurrentOps)

	for i := 0; i < concurrentOps; i++ {
		go func(id int) {
			defer wg.Done()
			s.Schedule(fmt.Sprintf("op%d", i), fmt.Sprintf("obj%d", id), fmt.Sprintf("act%d", id), time.Now().Add(time.Duration(id)*time.Minute))
		}(i)
	}

	wg.Wait()

	if s.Len() != concurrentOps {
		t.Fatalf("Expected %d operations scheduled, but got %d", concurrentOps, s.Len())
	}
}

func TestConcurrentPopping(t *testing.T) {
	s := NewScheduler[string](1)
	var wg sync.WaitGroup

	// First, schedule some actions.
	for i := 0; i < 50; i++ {
		s.Schedule(fmt.Sprintf("op%d", i), fmt.Sprintf("obj%d", i), fmt.Sprintf("act%d", i), time.Now().Add(time.Duration(i)*time.Minute))
	}

	concurrentPops := 50
	wg.Add(concurrentPops)

	for i := 0; i < concurrentPops; i++ {
		go func() {
			defer wg.Done()
			s.Pop()
		}()
	}

	wg.Wait()

	if s.Len() != 0 {
		t.Fatalf("Expected no operations after concurrent pops, but got %d", s.Len())
	}
}

func TestWaitingWithNewOperationsAdded(t *testing.T) {
	s := NewScheduler[string](1)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() {
		time.Sleep(10 * time.Millisecond)
		s.Schedule("op1", "obj2", "act2", time.Now().Add(10*time.Millisecond))
	}()

	result, _, _, _ := s.WaitForNextOperation(ctx)
	if result == nil || *result != "op1" {
		t.Fatal("Expected op1 to be returned from WaitForNextOperation, but it wasn't")
	}
}

func TestWaitingWithOperationRemoved(t *testing.T) {
	s := NewScheduler[string](1)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	s.Schedule("op1", "obj1", "act1", time.Now().Add(40*time.Millisecond))
	s.Schedule("op2", "obj2", "act2", time.Now().Add(20*time.Millisecond))

	go func() {
		time.Sleep(10 * time.Millisecond)
		s.RemoveActionForObject("obj2", "act2")
	}()

	result, _, _, _ := s.WaitForNextOperation(ctx)
	if result == nil || *result != "op1" {
		t.Fatal("Expected op1 to be returned from WaitForNextOperation, but a different operation or none was returned")
	}
}

func TestCancelWaitForNextOperation(t *testing.T) {
	s := NewScheduler[string](1)
	ctx, cancel := context.WithCancel(context.Background())

	// Schedule the operation
	s.Schedule("op1", "obj1", "act1", time.Now().Add(50*time.Millisecond))

	// Immediately cancel the context
	cancel()

	startTime := time.Now()
	result, _, _, _ := s.WaitForNextOperation(ctx)
	elapsedTime := time.Since(startTime)

	if result != nil {
		t.Fatal("Expected nil to be returned from WaitForNextOperation after context cancellation, but an operation was returned")
	}

	if elapsedTime > 10*time.Millisecond {
		t.Fatalf("Expected WaitForNextOperation to return immediately after context cancellation, but it took %v", elapsedTime)
	}
}

func TestUseCount(t *testing.T) {
	s := NewScheduler[string](1)

	// Create two operations for the same object
	s.Schedule("op1", "obj1", "act1", time.Now())
	s.Schedule("op2", "obj1", "act2", time.Now().Add(1*time.Millisecond))

	// Increment use count for obj1
	s.IncrementObjectUseCount("obj1")

	// Ensure next operation respects use count and doesn't return obj1 operations
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	nextOp, _, _, _ := s.WaitForNextOperation(ctx)
	if nextOp != nil {
		t.Fatal("Expected nil operation due to max use count but got an operation.")
	}
	cancel()

	s.Schedule("op3", "obj2", "act1", time.Now().Add(2*time.Millisecond))

	// This should return the third operation, as it is the only one not blocked by the use count
	ctx = context.Background()
	nextOp, _, _, _ = s.WaitForNextOperation(ctx)
	if nextOp == nil || *nextOp != "op3" {
		t.Fatal("Expected to get op2 but got a different or nil operation.")
	}

	// Decrement use count for obj1
	s.DecrementObjectUseCount("obj1")

	// Now it should give us the next operation for obj1
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	nextOp, _, _, _ = s.WaitForNextOperation(ctx)
	if nextOp == nil || *nextOp != "op1" {
		t.Fatal("Expected op1 but got different or nil operation.")
	}
	cancel()

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	// The next operation should be act2 for obj1
	nextOp, _, _, _ = s.WaitForNextOperation(ctx)
	if nextOp == nil || *nextOp != "op2" {
		t.Fatal("Expected op2 but got different or nil operation.")
	}
	cancel()
}
