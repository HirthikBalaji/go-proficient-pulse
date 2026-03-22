package ringbuffer

import (
	"errors"
	"sync/atomic"
)

var (
	ErrBufferFull  = errors.New("ring buffer is full")
	ErrBufferEmpty = errors.New("ring buffer is empty")
)

// RingBuffer is an ultra fast, lock-free ring buffer for uint64 event identifiers.
type RingBuffer struct {
	buffer []uint64
	capacity uint64
	mask     uint64
	head     uint64
	tail     uint64
}

// NewRingBuffer creates a new lock-free ring buffer with capacity rounded up to power of two.
func NewRingBuffer(size uint64) *RingBuffer {
	// Ensure power of 2
	capacity := uint64(1)
	for capacity < size {
		capacity <<= 1
	}

	return &RingBuffer{
		buffer:   make([]uint64, capacity),
		capacity: capacity,
		mask:     capacity - 1,
		head:     0,
		tail:     0,
	}
}

func (rb *RingBuffer) Push(val uint64) error {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	if head-tail >= rb.capacity {
		return ErrBufferFull
	}

	rb.buffer[head&rb.mask] = val
	atomic.AddUint64(&rb.head, 1)
	return nil
}

func (rb *RingBuffer) Pop() (uint64, error) {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	if tail >= head {
		return 0, ErrBufferEmpty
	}

	val := rb.buffer[tail&rb.mask]
	atomic.AddUint64(&rb.tail, 1)
	return val, nil
}

func (rb *RingBuffer) Len() uint64 {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	if head >= tail {
		return head - tail
	}
	return 0
}
