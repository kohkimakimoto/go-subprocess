# go-subprocess

A Go library for managing subprocesses embedded within Go applications.

## Motivation

This library provides subprocess management capabilities similar to supervisord, but designed to be embedded within Go applications rather than running as a separate daemon. 
It's particularly useful for managing auxiliary processes that should share the same lifecycle as the main application, such as:

- Development servers (e.g., Vite dev server alongside a Go web application)
- Worker processes and job queues
- Related services that should start/stop with the main application

Instead of managing these processes separately with external tools like supervisord or systemd, this library allows you to manage them directly within your Go application for tighter integration and simplified deployment.

## Features

- Context-based process lifecycle management
- Automatic restart with configurable policies (never, always, on-failure)
- Graceful shutdown with configurable timeout and signals
- Custom output formatting for stdout/stderr

## Author

Kohki Makimoto <kohki.makimoto@gmail.com>

## License

The MIT License (MIT)
