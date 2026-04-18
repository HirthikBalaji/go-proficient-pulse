package workerpool

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Job func(ctx context.Context) error

type workerDeque struct {
	_pad1 [56]byte
	head  int
	_pad2 [56]byte
	tail  int
	_pad3 [56]byte
	jobs  []Job
	mu    sync.Mutex
}

func newWorkerDeque(capacity int) *workerDeque {
	return &workerDeque{
		jobs: make([]Job, capacity),
	}
}

func (d *workerDeque) pushBack(job Job) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tail-d.head >= len(d.jobs) {
		return false
	}
	d.jobs[d.tail%len(d.jobs)] = job
	d.tail++
	return true
}

func (d *workerDeque) popBack() Job {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.head >= d.tail {
		return nil
	}
	d.tail--
	job := d.jobs[d.tail%len(d.jobs)]
	d.jobs[d.tail%len(d.jobs)] = nil
	return job
}

func (d *workerDeque) stealFront() Job {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.head >= d.tail {
		return nil
	}
	job := d.jobs[d.head%len(d.jobs)]
	d.jobs[d.head%len(d.jobs)] = nil
	d.head++
	return job
}

type WorkerPool struct {
	workers     int
	deques      []*workerDeque
	wg          sync.WaitGroup
	processed   uint64
	failed      uint64
	stolenJobs  uint64
	droppedJobs uint64
	submitIdx   uint64
	notifyChan  chan struct{}
	cancelFunc  context.CancelFunc
	ctx         context.Context
	closed      int32
}

func NewWorkerPool(workers int, perWorkerCap int) *WorkerPool {
	deques := make([]*workerDeque, workers)
	for i := 0; i < workers; i++ {
		deques[i] = newWorkerDeque(perWorkerCap)
	}

	return &WorkerPool{
		workers:    workers,
		deques:     deques,
		notifyChan: make(chan struct{}, workers*32),
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	wp.ctx, wp.cancelFunc = context.WithCancel(ctx)
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.workerLoop(i)
	}
}

func (wp *WorkerPool) Submit(job Job) bool {
	if atomic.LoadInt32(&wp.closed) == 1 {
		return false
	}
	idx := atomic.AddUint64(&wp.submitIdx, 1) % uint64(wp.workers)
	if wp.deques[idx].pushBack(job) {
		select {
		case wp.notifyChan <- struct{}{}:
		default:
		}
		return true
	}

	for i := 0; i < wp.workers; i++ {
		target := (int(idx) + i) % wp.workers
		if wp.deques[target].pushBack(job) {
			select {
			case wp.notifyChan <- struct{}{}:
			default:
			}
			return true
		}
	}

	atomic.AddUint64(&wp.droppedJobs, 1)
	return false
}

func (wp *WorkerPool) workerLoop(id int) {
	defer wp.wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	timer := time.NewTimer(5 * time.Millisecond)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	for {
		// 1. Try local deque pop (LIFO for cache locality)
		job := wp.deques[id].popBack()
		if job != nil {
			wp.exec(job)
			continue
		}

		// 2. Local queue empty -> STEAL from victim workers (FIFO)
		stolen := false
		startVictim := rng.Intn(wp.workers)
		for i := 0; i < wp.workers; i++ {
			victim := (startVictim + i) % wp.workers
			if victim == id {
				continue
			}
			stolenJob := wp.deques[victim].stealFront()
			if stolenJob != nil {
				atomic.AddUint64(&wp.stolenJobs, 1)
				wp.exec(stolenJob)
				stolen = true
				break
			}
		}

		if stolen {
			continue
		}

		// 3. No work available -> Reuse timer without creating heap timer allocations
		timer.Reset(2 * time.Millisecond)
		select {
		case <-wp.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			for {
				j := wp.deques[id].popBack()
				if j == nil {
					break
				}
				wp.exec(j)
			}
			return
		case <-wp.notifyChan:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (wp *WorkerPool) exec(job Job) {
	if err := job(wp.ctx); err != nil {
		atomic.AddUint64(&wp.failed, 1)
	} else {
		atomic.AddUint64(&wp.processed, 1)
	}
}

func (wp *WorkerPool) Stop() {
	if atomic.CompareAndSwapInt32(&wp.closed, 0, 1) {
		if wp.cancelFunc != nil {
			wp.cancelFunc()
		}
		wp.wg.Wait()
	}
}

func (wp *WorkerPool) Stats() (processed, failed, stolen, dropped uint64) {
	return atomic.LoadUint64(&wp.processed),
		atomic.LoadUint64(&wp.failed),
		atomic.LoadUint64(&wp.stolenJobs),
		atomic.LoadUint64(&wp.droppedJobs)
}
