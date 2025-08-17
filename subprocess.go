package subprocess

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// RestartPolicy defines how a process should be restarted
type RestartPolicy string

const (
	RestartNever  RestartPolicy = "never"
	RestartAlways RestartPolicy = "always"
	RestartOnFail RestartPolicy = "on-failure"
)

// LogFormatter is a function type that formats a single line of output
type LogFormatter func(line string) string

// Config contains all configuration for a subprocess
type Config struct {
	Command         string
	Args            []string
	StdoutFormatter LogFormatter
	StderrFormatter LogFormatter
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
	Dir             string
	Env             []string

	// Process management options
	RestartPolicy RestartPolicy
	MaxRestarts   int           // Maximum number of restarts (0 = unlimited)
	RestartDelay  time.Duration // Delay between restarts
	StopTimeout   time.Duration // Timeout for graceful shutdown
	StopSignal    os.Signal     // Signal to send for graceful shutdown (default: SIGINT)

	// Event callbacks
	OnRestart func(count int)
	OnError   func(error)
}

// Process represents a managed subprocess
type Process struct {
	config       *Config
	cmd          *exec.Cmd
	restartCount int
	done         chan struct{}
	lastError    error
}

// New creates a new managed process
func New(c *Config) *Process {
	if c.Stdin == nil {
		c.Stdin = os.Stdin
	}
	if c.Stdout == nil {
		c.Stdout = os.Stdout
	}
	if c.Stderr == nil {
		c.Stderr = os.Stderr
	}
	if c.RestartDelay == 0 {
		c.RestartDelay = 1 * time.Second
	}
	if c.StopTimeout == 0 {
		c.StopTimeout = 10 * time.Second
	}
	if c.StopSignal == nil {
		c.StopSignal = syscall.SIGINT
	}

	p := &Process{
		config: c,
		done:   make(chan struct{}),
	}
	return p
}

// Start starts the process with automatic restart capability
func (p *Process) Start(ctx context.Context) {
	go p.supervise(ctx)
}

// supervise manages the process lifecycle
func (p *Process) supervise(ctx context.Context) {
	defer close(p.done)
	for {
		err := p.runProcess(ctx)

		// Check context cancellation
		select {
		case <-ctx.Done():
			// Context is cancelled, do not restart
			return
		default:
		}

		// Check if we should restart
		if !p.shouldRestart(err) {
			return
		}

		// Wait before restarting
		select {
		case <-time.After(p.config.RestartDelay):
			// Increment restart counter
			p.restartCount++
			restartCount := p.restartCount

			if p.config.OnRestart != nil {
				p.config.OnRestart(restartCount)
			}
		case <-ctx.Done():
			// Check context cancellation
			return
		}
	}
}

// runProcess executes the actual subprocess
func (p *Process) runProcess(ctx context.Context) error {
	p.cmd = exec.Command(p.config.Command, p.config.Args...)
	p.cmd.Dir = p.config.Dir
	p.cmd.Env = p.config.Env
	p.cmd.Stdin = p.config.Stdin

	// Setup stdout/stderr pipes
	stdout, err := p.cmd.StdoutPipe()
	defer stdout.Close()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderr, err := p.cmd.StderrPipe()
	defer stderr.Close()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	// Start output processors
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		p.scanLines(stdout, p.config.Stdout, p.config.StdoutFormatter)
	}()

	go func() {
		defer wg.Done()
		p.scanLines(stderr, p.config.Stderr, p.config.StderrFormatter)
	}()

	// Start the process
	if err := p.cmd.Start(); err != nil {
		p.setLastError(err)
		return err
	}

	// Monitor context cancellation and handle graceful shutdown
	done := make(chan error, 1)
	go func() {
		done <- p.cmd.Wait()
	}()

	select {
	case err = <-done:
		// Process completed normally
		wg.Wait()
	case <-ctx.Done():
		// Context cancelled, perform graceful shutdown
		err = p.gracefulShutdown()
		wg.Wait()
	}

	if err != nil {
		p.setLastError(err)
	}

	return err
}

// gracefulShutdown performs graceful shutdown with timeout
func (p *Process) gracefulShutdown() error {
	if p.cmd.Process == nil {
		return nil
	}

	// Send graceful shutdown signal
	if err := p.cmd.Process.Signal(p.config.StopSignal); err != nil {
		// Signal failed, force kill immediately
		p.cmd.Process.Kill()
		return p.cmd.Wait()
	}

	// Wait for graceful shutdown or timeout
	done := make(chan error, 1)
	go func() {
		done <- p.cmd.Wait()
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(p.config.StopTimeout):
		// Timeout, force kill
		p.cmd.Process.Kill()
		return <-done
	}
}

// Wait blocks until the process stops
func (p *Process) Wait() error {
	<-p.done
	return p.lastError
}

// shouldRestart determines if the process should be restarted
func (p *Process) shouldRestart(err error) bool {
	// Check restart policy
	switch p.config.RestartPolicy {
	case RestartNever:
		return false
	case RestartAlways:
		// Check max restarts
		if p.config.MaxRestarts > 0 && p.restartCount >= p.config.MaxRestarts {
			return false
		}
		return true
	case RestartOnFail:
		if err == nil {
			return false
		}
		// Check max restarts
		if p.config.MaxRestarts > 0 && p.restartCount >= p.config.MaxRestarts {
			return false
		}
		return true
	default:
		return false
	}
}

// scanLines processes output lines with optional formatting
func (p *Process) scanLines(src io.ReadCloser, dest io.Writer, formatter LogFormatter) {
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()

		if formatter != nil {
			line = formatter(line)
		}

		fmt.Fprintf(dest, "%s\n", line)
	}
}

// Helper methods

func (p *Process) setLastError(err error) {
	p.lastError = err

	if p.config.OnError != nil {
		p.config.OnError(err)
	}
}

// Utility functions for simple execution

// Run executes a subprocess without management (simple execution)
func Run(config *Config) error {
	return RunWithContext(context.Background(), config)
}

// RunWithContext executes a subprocess with context (simple execution)
func RunWithContext(ctx context.Context, config *Config) error {
	// Set default restart policy for simple execution
	if config.RestartPolicy == "" {
		config.RestartPolicy = RestartNever
	}

	process := New(config)
	process.Start(ctx)
	return process.Wait()
}
