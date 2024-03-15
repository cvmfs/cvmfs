package registry

import "testing"

func TestPush(t *testing.T) {
	rb := NewPreSortedRingBuffer(5)
	data := []int64{10, 20, 30, 40, 50, 60, 70}

	for _, d := range data {
		rb.Push(d)
	}

	if size := rb.Size(); size != 5 {
		t.Errorf("Expected size 5 after pushing 7 elements, got %d", size)
	}

	// The buffer capacity is 5. Thus, after inserting 7 items, only the last 5 should remain.
	expectedData := []int64{30, 40, 50, 60, 70}
	for i := 0; i < 5; i++ {
		if val := rb.At(i); val != expectedData[i] {
			t.Errorf("Expected %d at index %d, got %d", expectedData[i], i, val)
		}
	}
}

func TestPop(t *testing.T) {
	rb := NewPreSortedRingBuffer(5)
	data := []int64{10, 20, 30, 40, 50}

	for _, d := range data {
		rb.Push(d)
	}

	for i := 0; i < len(data); i++ {
		val, err := rb.Pop()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if val != data[i] {
			t.Errorf("Expected %d at index %d after pop, got %d", data[i+1], i, val)
		}
		if rb.Size() != len(data)-i-1 {
			t.Errorf("Expected size %d after popping %d elements, got %d", len(data)-i-1, i+1, rb.Size())
		}
	}

	val, err := rb.Pop()
	if err == nil {
		t.Errorf("Expected error after popping from empty buffer, got %d", val)
	}

}

func TestBinarySearchGeq(t *testing.T) {
	tests := []struct {
		data          []int64
		val           int64
		expectedIndex int
		expectedVal   int64
		found         bool
	}{
		{
			data:          []int64{10, 20, 30, 40, 50},
			val:           25,
			expectedIndex: 2,
			expectedVal:   30,
			found:         true,
		},
		{
			data:          []int64{10, 20, 30, 40, 50},
			val:           5,
			expectedIndex: 0,
			expectedVal:   10,
			found:         true,
		},
		{
			data:          []int64{10, 20, 30, 40, 50},
			val:           55,
			expectedIndex: -1,
			expectedVal:   0,
			found:         false,
		},
		{
			data:          []int64{10, 10, 10, 40, 50},
			val:           10,
			expectedIndex: 0,
			expectedVal:   10,
			found:         true,
		},
		{
			data:          []int64{},
			val:           25,
			expectedIndex: -1,
			expectedVal:   0,
			found:         false,
		},
	}

	for _, test := range tests {
		rb := NewPreSortedRingBuffer(len(test.data))
		for _, d := range test.data {
			rb.Push(d)
		}

		index, val, found := rb.BinarySearchGeq(test.val)
		if index != test.expectedIndex || val != test.expectedVal || found != test.found {
			t.Errorf("For data %+v and val = %d, expected index = %d, val = %d and found = %v, got index = %d, val = %d and found = %v",
				test.data, test.val, test.expectedIndex, test.expectedVal, test.found, index, val, found)
		}
	}
}

func TestRemoveSmallerThan(t *testing.T) {
	tests := []struct {
		data     []int64
		val      int64
		expected []int64
	}{
		{
			data:     []int64{10, 20, 30, 40, 50},
			val:      25,
			expected: []int64{30, 40, 50},
		},
		{
			data:     []int64{10, 20, 30, 40, 50},
			val:      5,
			expected: []int64{10, 20, 30, 40, 50},
		},
		{
			data:     []int64{10, 20, 30, 40, 50},
			val:      55,
			expected: []int64{},
		},
		{
			data:     []int64{},
			val:      25,
			expected: []int64{},
		},
	}

	for _, test := range tests {
		rb := NewPreSortedRingBuffer(len(test.data))
		for _, d := range test.data {
			rb.Push(d)
		}

		rb.RemoveSmallerThan(test.val)

		if size := rb.Size(); size != len(test.expected) {
			t.Errorf("For data %+v and val = %d, expected size %d after removal, got %d", test.data, test.val, len(test.expected), size)
			continue
		}

		for i := 0; i < len(test.expected); i++ {
			if val := rb.At(i); val != test.expected[i] {
				t.Errorf("For data %+v and val = %d, expected %d at index %d after removal, got %d", test.data, test.val, test.expected[i], i, val)
			}
		}
	}
}

func TestLargeBuffer(t *testing.T) {
	rb := NewPreSortedRingBuffer(10000000)
	numElements := 10000000

	// Push ten million elements
	for i := 1; i <= numElements; i++ {
		rb.Push(int64(i))
	}

	if size := rb.Size(); size != numElements {
		t.Errorf("Expected size %d after pushing %d elements, got %d", numElements, numElements, size)
	}

	// Check BinarySearchGeq for several values
	tests := []struct {
		val            int64
		expectedIndex  int
		expectedVal    int64
		expectedResult bool
	}{
		{val: 1, expectedIndex: 0, expectedVal: 1, expectedResult: true},
		{val: 5000000, expectedIndex: 4999999, expectedVal: 5000000, expectedResult: true},
		{val: 9999999, expectedIndex: 9999998, expectedVal: 9999999, expectedResult: true},
		{val: int64(numElements) + 1, expectedIndex: -1, expectedVal: 0, expectedResult: false},
	}

	for _, test := range tests {
		index, val, result := rb.BinarySearchGeq(test.val)
		if index != test.expectedIndex || val != test.expectedVal || result != test.expectedResult {
			t.Errorf("For val = %d, expected index = %d, val = %d and result = %v, got index = %d, val = %d and result = %v",
				test.val, test.expectedIndex, test.expectedVal, test.expectedResult, index, val, result)
		}
	}
}
