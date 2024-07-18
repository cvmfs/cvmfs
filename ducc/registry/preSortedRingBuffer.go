package registry

import "errors"

type PreSortedRingBuffer struct {
	data     []int64
	capacity int
	start    int
	end      int
	isFull   bool
}

func NewPreSortedRingBuffer(capacity int) *PreSortedRingBuffer {
	return &PreSortedRingBuffer{
		data:     make([]int64, capacity),
		capacity: capacity,
	}
}

func (rb *PreSortedRingBuffer) Size() int {
	if rb.isFull {
		return rb.capacity
	}
	if rb.end >= rb.start {
		return rb.end - rb.start
	}
	return rb.capacity - rb.start + rb.end
}

func (rb *PreSortedRingBuffer) At(index int) int64 {
	if index < 0 || index >= rb.capacity {
		panic("Index out of range")
	}
	return rb.data[(rb.start+index)%rb.capacity]
}

func (rb *PreSortedRingBuffer) Push(val int64) {
	if rb.isFull {
		rb.start = (rb.start + 1) % rb.capacity
	}
	rb.data[rb.end] = val
	rb.end = (rb.end + 1) % rb.capacity

	// Set isFull flag if end reaches start
	rb.isFull = rb.end == rb.start
}

func (rb *PreSortedRingBuffer) RemoveSmallerThan(val int64) {
	idx, _, found := rb.BinarySearchGeq(val)
	if !found {
		// All elements are smaller than val, clear the buffer
		rb.start = 0
		rb.end = 0
		rb.isFull = false
		return
	}

	// Move the start pointer to idx, thereby removing elements smaller than val
	if idx > 0 {
		rb.start += idx % rb.capacity
		rb.isFull = false // This will ensure that the buffer is not seen as full
	}
}

func (rb *PreSortedRingBuffer) Pop() (int64, error) {
	if rb.start == rb.end && !rb.isFull {
		return 0, errors.New("buffer is empty, nothing to remove")
	}
	popped := rb.data[rb.start]
	rb.start = (rb.start + 1) % rb.capacity
	rb.isFull = false
	return popped, nil
}

// BinarySearchGeq returns the index and value of the first element greater than or equal to val.
func (rb *PreSortedRingBuffer) BinarySearchGeq(val int64) (int, int64, bool) {
	low, high := 0, rb.Size()-1

	for low <= high {
		mid := (low + high) / 2
		// Adjust the mid to account for the ring buffer's circular nature
		realMid := (mid + rb.start) % rb.capacity

		if rb.data[realMid] < val {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	// If the element is not found, return false
	if low == rb.Size() {
		return -1, 0, false
	}

	// Adjust the index for the ring buffer's start and return
	return low, rb.At(low), true
}
