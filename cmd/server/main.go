package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"go-proficient-pulse/pkg/ringbuffer"
	"go-proficient-pulse/pkg/workerpool"
)

//go:embed web/index.html
var indexHTML []byte

type ServerStatus struct {
	UptimeSeconds float64 `json:"uptime_seconds"`
	Goroutines    int     `json:"goroutines"`
	NumCPU        int     `json:"num_cpu"`
	ProcessedJobs uint64  `json:"processed_jobs"`
	FailedJobs    uint64  `json:"failed_jobs"`
	StolenJobs    uint64  `json:"stolen_jobs"`
	DroppedJobs   uint64  `json:"dropped_jobs"`
	RingBufferLen uint64  `json:"ring_buffer_len"`
	RingBufferCap uint64  `json:"ring_buffer_cap"`
}

func main() {
	fmt.Println("=========================================================================")
	fmt.Println("⚡ GO PROFICIENT PULSE: Async Work Stealing & Lock-Free Stream Engine")
	fmt.Println("=========================================================================")

	startTime := time.Now()
	workers := runtime.NumCPU() * 2
	pool := workerpool.NewWorkerPool(workers, 2048)
	rb := ringbuffer.NewRingBuffer(65536)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	fmt.Printf("--> Started Work-Stealing Worker Pool with %d workers\n", workers)
	fmt.Printf("--> Initialized Lock-Free MPMC Ring Buffer (capacity: %d)\n", rb.Capacity())

	// 1. Producer goroutines: Push work items into the Lock-Free MPMC Ring Buffer
	const numProducers = 4
	for p := 0; p < numProducers; p++ {
		go func(id int) {
			var counter uint64
			for {
				select {
				case <-ctx.Done():
					return
				default:
					c := atomic.AddUint64(&counter, 1)
					_ = rb.Push(c)
					time.Sleep(1 * time.Microsecond)
				}
			}
		}(p)
	}

	// 2. Consumer goroutines: Pop work items from Ring Buffer & Submit to Work-Stealing Pool
	const numConsumers = 4
	for c := 0; c < numConsumers; c++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					val, err := rb.Pop()
					if err == nil {
						pool.Submit(func(c context.Context) error {
							_ = val * 42
							return nil
						})
					} else {
						time.Sleep(10 * time.Microsecond)
					}
				}
			}
		}()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		processed, failed, stolen, dropped := pool.Stats()
		status := ServerStatus{
			UptimeSeconds: time.Since(startTime).Seconds(),
			Goroutines:    runtime.NumGoroutine(),
			NumCPU:        runtime.NumCPU(),
			ProcessedJobs: processed,
			FailedJobs:    failed,
			StolenJobs:    stolen,
			DroppedJobs:   dropped,
			RingBufferLen: rb.Len(),
			RingBufferCap: rb.Capacity(),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		fmt.Println("--> Synapse Design System Website & HTTP Telemetry active at http://localhost:8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Server error: %v\n", err)
		}
	}()

	// Handle Graceful Shutdown
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)

	<-stopSignal
	fmt.Println("\n--> Graceful shutdown signal received. Shutting down engine...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("HTTP Shutdown error: %v\n", err)
	}

	cancel()
	pool.Stop()
	fmt.Println("--> Engine shut down cleanly.")
}
