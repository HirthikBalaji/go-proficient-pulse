package workerpool_test

import (
	"context"
	"sync/atomic"
	"testing"

	"go-proficient-pulse/pkg/workerpool"
)

func TestWorkerPool_WorkStealing(t *testing.T) {
	workers := 4
	pool := workerpool.NewWorkerPool(workers, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	var counter uint64
	numJobs := 1000

	for i := 0; i < numJobs; i++ {
		submitted := pool.Submit(func(c context.Context) error {
			atomic.AddUint64(&counter, 1)
			return nil
		})
		if !submitted {
			t.Logf("job %d was dropped", i)
		}
	}

	pool.Stop()

	processed, failed, stolen, dropped := pool.Stats()
	if processed+dropped != uint64(numJobs) {
		t.Fatalf("expected processed+dropped to equal %d, got processed=%d, dropped=%d", numJobs, processed, dropped)
	}

	t.Logf("Stats: Processed=%d, Failed=%d, Stolen=%d, Dropped=%d", processed, failed, stolen, dropped)
}
