package ringbuffer_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"go-proficient-pulse/pkg/ringbuffer"
)

func TestRingBuffer_SPSC(t *testing.T) {
	rb := ringbuffer.NewRingBuffer(16)
	for i := uint64(1); i <= 10; i++ {
		if err := rb.Push(i); err != nil {
			t.Fatalf("unexpected push error: %v", err)
		}
	}

	if rb.Len() != 10 {
		t.Fatalf("expected len 10, got %d", rb.Len())
	}

	for i := uint64(1); i <= 10; i++ {
		val, err := rb.Pop()
		if err != nil {
			t.Fatalf("unexpected pop error: %v", err)
		}
		if val != i {
			t.Fatalf("expected %d, got %d", i, val)
		}
	}

	if _, err := rb.Pop(); err != ringbuffer.ErrBufferEmpty {
		t.Fatalf("expected ErrBufferEmpty, got %v", err)
	}
}

func TestRingBuffer_MPMC_Concurrent(t *testing.T) {
	const (
		producers = 8
		consumers = 8
		itemsPerProducer = 10000
		capacity  = 4096
	)

	rb := ringbuffer.NewRingBuffer(capacity)
	var wg sync.WaitGroup
	var totalProduced uint64
	var totalConsumed uint64

	// Launch producers
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 1; i <= itemsPerProducer; i++ {
				val := uint64(id*itemsPerProducer + i)
				for {
					err := rb.Push(val)
					if err == nil {
						atomic.AddUint64(&totalProduced, 1)
						break
					}
					if err != ringbuffer.ErrBufferFull {
						t.Errorf("unexpected error: %v", err)
						return
					}
				}
			}
		}(p)
	}

	// Launch consumers
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				val, err := rb.Pop()
				if err == nil {
					if val == 0 {
						t.Errorf("received invalid 0 value")
					}
					if atomic.AddUint64(&totalConsumed, 1) == producers*itemsPerProducer {
						return
					}
				} else if err != ringbuffer.ErrBufferEmpty {
					t.Errorf("unexpected error: %v", err)
					return
				}
				if atomic.LoadUint64(&totalConsumed) >= producers*itemsPerProducer {
					return
				}
			}
		}()
	}

	wg.Wait()

	if totalProduced != producers*itemsPerProducer {
		t.Fatalf("expected produced %d, got %d", producers*itemsPerProducer, totalProduced)
	}
	if totalConsumed != producers*itemsPerProducer {
		t.Fatalf("expected consumed %d, got %d", producers*itemsPerProducer, totalConsumed)
	}
}
