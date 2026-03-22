package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Job func(ctx context.Context) error

type WorkerPool struct {
	workers    int
	jobQueue   chan Job
	wg         sync.WaitGroup
	processed  uint64
	failed     uint64
	cancelFunc context.CancelFunc
}

func NewWorkerPool(workers int, queueSize int) *WorkerPool {
	return &WorkerPool{
		workers:  workers,
		jobQueue: make(chan Job, queueSize),
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	ctx, wp.cancelFunc = context.WithCancel(ctx)
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx)
	}
}

func (wp *WorkerPool) Submit(job Job) bool {
	select {
	case wp.jobQueue <- job:
		return true
	default:
		return false
	}
}

func (wp *WorkerPool) worker(ctx context.Context) {
	defer wp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-wp.jobQueue:
			if !ok {
				return
			}
			if err := job(ctx); err != nil {
				atomic.AddUint64(&wp.failed, 1)
			} else {
				atomic.AddUint64(&wp.processed, 1)
			}
		}
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.jobQueue)
	if wp.cancelFunc != nil {
		wp.cancelFunc()
	}
	wp.wg.Wait()
}

func (wp *WorkerPool) Stats() (processed uint64, failed uint64) {
	return atomic.LoadUint64(&wp.processed), atomic.LoadUint64(&wp.failed)
}
