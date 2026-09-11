# go-subprocess


[![test](https://github.com/kohkimakimoto/go-subprocess/actions/workflows/test.yml/badge.svg)](https://github.com/kohkimakimoto/go-subprocess/actions/workflows/test.yml)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/kohkimakimoto/go-subprocess/blob/main/LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kohkimakimoto/go-subprocess.svg)](https://pkg.go.dev/github.com/kohkimakimoto/go-subprocess)

A Go library for implementing subprocess management within Go applications.

## Motivation

This library provides a simplified version of supervisord's subprocess management capabilities, designed to be implemented within Go applications rather than running as a separate daemon.
It's particularly useful for managing auxiliary processes that should share the same lifecycle as the main application, such as:

- Development servers (e.g., Vite dev server alongside a Go web application)
- Worker processes and job queues
- Related services that should start/stop with the main application

Instead of managing these processes separately with external tools like supervisord or systemd, this library allows you to manage them directly within your Go application for tighter integration and simplified deployment.

## Features

- Context-based process lifecycle management
- Automatic restart with configurable policies (never, always, on-failure)
- Restart delay with optional exponential backoff
- Graceful shutdown with configurable timeout and signals
- Process-group stop on Unix, so descendant processes are signaled too
- Custom output formatting for stdout/stderr
- PID, running state, restart count, and exit code

## Installation

```sh
go get github.com/kohkimakimoto/go-subprocess
```

## Usage

Start a subprocess under a shared cancelable context, then call `Wait` on shutdown.

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kohkimakimoto/go-subprocess"
)

func main() {
	// One context owns app lifetime (signals + shared cancel).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start returns once supervision has begun; the child stops when ctx is canceled.
	p, err := subprocess.Start(ctx, subprocess.Config{
		Command:       "npm",
		Args:          []string{"run", "dev"},
		RestartPolicy: subprocess.RestartOnFail,
		StopTimeout:   10 * time.Second, // StopSignal, then group kill on Unix
		StopSignal:    os.Interrupt,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Your main http server.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		log.Printf("http listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http error: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Print("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	// Always Wait so graceful/group stop can finish.
	if err := p.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("subprocess: %v", err)
	}
}
```

## Format output

With a formatter, output is scanned line by line.
Without a formatter, `*os.File` destinations are connected directly (binary preserved);
other Writers are copied through a pipe (also binary-preserving).

```go
p, err := subprocess.Start(context.Background(), subprocess.Config{
	Command: "echo",
	Args:    []string{"test message"},
	StdoutFormatter: subprocess.ChainFormatters(
		subprocess.TimestampFormatter(time.RFC3339),
		subprocess.PrefixFormatter("[app] "),
	),
})
if err != nil {
	log.Fatal(err)
}
_ = p.Wait()
```

Example output:

```text
[app] [2026-09-10T01:48:00Z] test message
```

## Author

Kohki Makimoto <kohki.makimoto@gmail.com>

## License

The MIT License (MIT)
