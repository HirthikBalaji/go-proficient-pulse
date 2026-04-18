package ringbuffer

import (
	"errors"
	"runtime"
	"sync/atomic"
)

var (
	ErrBufferFull  = errors.New("ring buffer is full")
	ErrBufferEmpty = errors.New("ring buffer is empty")
)

// node represents a single slot in the ring buffer padded to 64 bytes (1 CPU cache line)
// to prevent element-level false sharing between adjacent buffer slots across CPU cores.
type node struct {
	sequence uint64
	val      uint64
	_pad     [48]byte // 8 + 8 + 48 = 64 bytes (exact 64-byte cache line alignment)
}

// RingBuffer is a lock-free MPMC ring buffer implementing Dmitry Vyukov's algorithm
// with both struct-level and element-level 64-byte cache line padding.
type RingBuffer struct {
	_pad1    [56]byte
	capacity uint64
	mask     uint64
	buffer   []node
	_pad2    [56]byte
	head     uint64 // Producer index cursor
	_pad3    [56]byte
	tail     uint64 // Consumer index cursor
	_pad4    [56]byte
}

// NewRingBuffer creates a lock-free ring buffer with capacity rounded up to a power of 2.
func NewRingBuffer(size uint64) *RingBuffer {
	capacity := uint64(1)
	for capacity < size {
		capacity <<= 1
	}

	buffer := make([]node, capacity)
	for i := uint64(0); i < capacity; i++ {
		buffer[i].sequence = i
	}

	return &RingBuffer{
		buffer:   buffer,
		capacity: capacity,
		mask:     capacity - 1,
		head:     0,
		tail:     0,
	}
}

// Push inserts an element into the ring buffer in a thread-safe, lock-free manner.
func (rb *RingBuffer) Push(val uint64) error {
	var n *node
	pos := atomic.LoadUint64(&rb.head)

	for {
		n = &rb.buffer[pos&rb.mask]
		seq := atomic.LoadUint64(&n.sequence)
		diff := int64(seq) - int64(pos)

		if diff == 0 {
			if atomic.CompareAndSwapUint64(&rb.head, pos, pos+1) {
				break
			}
		} else if diff < 0 {
			return ErrBufferFull
		} else {
			pos = atomic.LoadUint64(&rb.head)
		}
		runtime.Gosched()
	}

	n.val = val
	atomic.StoreUint64(&n.sequence, pos+1)
	return nil
}

// Pop removes and returns an element from the ring buffer in a thread-safe, lock-free manner.
func (rb *RingBuffer) Pop() (uint64, error) {
	var n *node
	pos := atomic.LoadUint64(&rb.tail)

	for {
		n = &rb.buffer[pos&rb.mask]
		seq := atomic.LoadUint64(&n.sequence)
		diff := int64(seq) - int64(pos+1)

		if diff == 0 {
			if atomic.CompareAndSwapUint64(&rb.tail, pos, pos+1) {
				break
			}
		} else if diff < 0 {
			return 0, ErrBufferEmpty
		} else {
			pos = atomic.LoadUint64(&rb.tail)
		}
		runtime.Gosched()
	}

	val := n.val
	atomic.StoreUint64(&n.sequence, pos+rb.mask+1)
	return val, nil
}

// Len returns the current estimated number of elements in the buffer.
func (rb *RingBuffer) Len() uint64 {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	if head >= tail {
		return head - tail
	}
	return 0
}

// Capacity returns the total capacity of the ring buffer.
func (rb *RingBuffer) Capacity() uint64 {
	return rb.capacity
}
