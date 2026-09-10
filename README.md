# go-subprocess

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

### Run once

```go
err := subprocess.Run(subprocess.Config{
    Command: "echo",
    Args:    []string{"hello"},
})
```

### Supervise until the application context is canceled

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

err := subprocess.RunWithContext(ctx, subprocess.Config{
    Command:         "npm",
    Args:            []string{"run", "dev"},
    RestartPolicy:   subprocess.RestartOnFail,
    MaxRestarts:     5,                // restarts, not including the initial start
    RestartDelay:    time.Second,     // delay before the first restart
    RestartBackoff:  2,               // set 1 for a fixed delay
    RestartDelayMax: 30 * time.Second,
    StopTimeout:     10 * time.Second,
    StopSignal:      os.Interrupt,
    OnRestart: func(count int) {
        log.Printf("restarted %d times", count)
    },
    OnError: func(err error) {
        log.Printf("process error: %v", err)
    },
})
if errors.Is(err, context.Canceled) {
    // stopped because ctx was canceled, not because the child crashed
}
```

`RunWithContext` blocks until the process stops permanently or `ctx` is canceled.
It returns the last run's result. A successful restart after a failure returns nil.
Context cancellation is wrapped so `errors.Is(err, context.Canceled)` and `errors.Is(err, context.DeadlineExceeded)` work.
`OnError` is skipped for cancellation.
`OnRestart` and `OnError` must not call `Wait`.

The caller owns `Stdin` and closes it if needed.
Non-file readers are copied through a pipe shared across restarts.
`Wait` can return while an input `Read` is still pending; that read continues until the caller unblocks or closes the reader.

On Unix, the child runs in its own process group. Stop signals and the timeout kill go to that group, so descendants are included in shutdown.
Shell background jobs often ignore `SIGINT` and `SIGTERM`; those descendants are reaped when `StopTimeout` elapses and the group is killed.
`StopTimeout` also bounds how long output is drained after shutdown or cancellation.
Non-file Writers are copied through process-owned pipes so `Wait` is not tied to a blocked `Write`; a blocked write may still outlive `Wait` after that bound.

### Format output

With a formatter, output is scanned line by line.
Without a formatter, `*os.File` destinations are connected directly (binary preserved);
other Writers are copied through a pipe (also binary-preserving).

```go
err := subprocess.Run(subprocess.Config{
    Command: "echo",
    Args:    []string{"test message"},
    StdoutFormatter: subprocess.ChainFormatters(
        subprocess.TimestampFormatter(time.RFC3339),
        subprocess.PrefixFormatter("[app] "),
    ),
})
```

Example output:

```text
[app] [2026-09-10T01:48:00Z] test message
```

## Author

Kohki Makimoto <kohki.makimoto@gmail.com>

## License

The MIT License (MIT)
