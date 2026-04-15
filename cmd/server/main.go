package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"go-proficient-pulse/pkg/ringbuffer"
	"go-proficient-pulse/pkg/workerpool"
)

type ServerStatus struct {
	UptimeSeconds float64 `json:"uptime_seconds"`
	Goroutines    int     `json:"goroutines"`
	NumCPU        int     `json:"num_cpu"`
	ProcessedJobs uint64  `json:"processed_jobs"`
	FailedJobs    uint64  `json:"failed_jobs"`
	RingBufferLen uint64  `json:"ring_buffer_len"`
}

func main() {
	fmt.Println("=========================================================================")
	fmt.Println("⚡ GO PROFICIENT PULSE: Async Work Stealing & Lock-Free Stream Engine")
	fmt.Println("=========================================================================")

	startTime := time.Now()
	workers := runtime.NumCPU() * 2
	pool := workerpool.NewWorkerPool(workers, 100000)
	rb := ringbuffer.NewRingBuffer(65536)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	fmt.Printf("--> Started Worker Pool with %d concurrency workers\n", workers)

	// Producer simulation goroutine
	go func() {
		var counter uint64
		for {
			select {
			case <-ctx.Done():
				return
			default:
				atomic.AddUint64(&counter, 1)
				val := counter
				_ = rb.Push(val)

				pool.Submit(func(c context.Context) error {
					// Simulate workload
					_ = val * 42
					return nil
				})
				time.Sleep(10 * time.Microsecond)
			}
		}
	}()

	// HTTP Status & Dashboard endpoint
	http.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		processed, failed := pool.Stats()
		status := ServerStatus{
			UptimeSeconds: time.Since(startTime).Seconds(),
			Goroutines:    runtime.NumGoroutine(),
			NumCPU:        runtime.NumCPU(),
			ProcessedJobs: processed,
			FailedJobs:    failed,
			RingBufferLen: rb.Len(),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Go Proficient Pulse - Interactive Dashboard</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #f8fafc; padding: 2rem; margin: 0; }
        .card { background: #1e293b; border-radius: 12px; padding: 1.5rem; border: 1px solid #334155; margin-bottom: 1rem; max-width: 600px; }
        h1 { color: #38bdf8; font-size: 1.5rem; }
        .metric { font-size: 2rem; font-weight: bold; color: #4ade80; margin: 0.5rem 0; }
        .label { color: #94a3b8; font-size: 0.875rem; text-transform: uppercase; letter-spacing: 0.05em; }
    </style>
</head>
<body>
    <div class="card">
        <h1>⚡ Go Proficient Pulse Live Stream</h1>
        <div class="label">Total Processed Workloads</div>
        <div id="processed" class="metric">Loading...</div>
        <div class="label">Active Goroutines</div>
        <div id="goroutines" style="color:#60a5fa" class="metric">Loading...</div>
        <div class="label">Ring Buffer Backlog</div>
        <div id="ring" style="color:#f43f5e" class="metric">Loading...</div>
    </div>
    <script>
        async function update() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                document.getElementById('processed').innerText = data.processed_jobs.toLocaleString();
                document.getElementById('goroutines').innerText = data.goroutines;
                document.getElementById('ring').innerText = data.ring_buffer_len;
            } catch(e) {}
        }
        setInterval(update, 500);
        update();
    </script>
</body>
</html>
`)
	})

	fmt.Println("--> HTTP Telemetry server active at http://localhost:8080")
	fmt.Println("--> System ready. Press Ctrl+C to exit.")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Server exited: %v\n", err)
	}
}
