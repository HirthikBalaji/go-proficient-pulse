# ⚡ Go Proficient Pulse

An asynchronous work-stealing & lock-free stream processing engine written in Go (1.22+).

## 🌟 Architecture & Features

- **True Lock-Free MPMC Ring Buffer**: Built using Dmitry Vyukov's bounded MPMC queue algorithm with sequence numbers, atomic CAS semantics, and cache-line padding to eliminate false sharing.
- **Genuine Work-Stealing Worker Pool**: Per-worker local job deques with inter-worker work stealing (LIFO local pops for cache locality, FIFO victim stealing) and notification-driven parked worker wakeups.
- **Integrated Producer-Consumer Pipeline**: Lock-free stream producers push events to the MPMC Ring Buffer, consumed asynchronously by work-stealing workers.
- **Graceful Shutdown & Context Control**: Complete `context.Context` propagation with OS signal handling (`SIGINT`, `SIGTERM`) and clean HTTP server shutdown.
- **Live Telemetry Dashboard**: Real-time HTTP dashboard exposing processed workloads, work-stolen metrics, active goroutines, and ring buffer utilization.

## 🚀 Getting Started

### Prerequisites
- [Go 1.22+](https://go.dev/doc/install)

### Run Unit Tests & Race Detector
```bash
go test -race -v ./...
```

### Run Server
```bash
go run ./cmd/server/main.go
```

Open `http://localhost:8080` in your web browser to view live telemetry metrics.
