package subprocess

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// ProcessState represents the current state of a subprocess
type ProcessState int32

const (
	StateNotStarted ProcessState = iota
	StateRunning
	StateStopped
	StateFailed
	StateRestarting
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

	// Event callbacks
	OnStateChange func(old, new ProcessState)
	OnRestart     func(count int)
	OnError       func(error)
}

// SetDefaults sets default values for unset configuration options
func (c *Config) SetDefaults() {
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
		c.RestartDelay = 5 * time.Second
	}
	if c.StopTimeout == 0 {
		c.StopTimeout = 10 * time.Second
	}
}

// Process represents a managed subprocess
type Process struct {
	config       *Config
	cmd          *exec.Cmd
	state        atomic.Value // ProcessState
	restartCount int
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	lastError    error
	startTime    time.Time
}

// New creates a new managed process
func New(config *Config) *Process {
	config.SetDefaults()

	p := &Process{
		config: config,
		done:   make(chan struct{}),
	}
	p.setState(StateNotStarted)
	return p
}

// Start starts the process with automatic restart capability
func (p *Process) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.getState() == StateRunning {
		return fmt.Errorf("process is already running")
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	go p.supervise()

	return nil
}

// supervise manages the process lifecycle including restarts
func (p *Process) supervise() {
	defer close(p.done)

	for {
		select {
		case <-p.ctx.Done():
			p.stopProcess()
			return
		default:
			// Start the process
			err := p.runProcess()

			// Check if we should restart
			if !p.shouldRestart(err) {
				return
			}

			// Increment restart counter
			p.mu.Lock()
			p.restartCount++
			restartCount := p.restartCount
			p.mu.Unlock()

			p.setState(StateRestarting)
			if p.config.OnRestart != nil {
				p.config.OnRestart(restartCount)
			}

			// Wait before restarting
			select {
			case <-time.After(p.config.RestartDelay):
				continue
			case <-p.ctx.Done():
				return
			}
		}
	}
}

// runProcess executes the actual subprocess
func (p *Process) runProcess() error {
	p.mu.Lock()
	p.cmd = exec.CommandContext(p.ctx, p.config.Command, p.config.Args...)
	p.cmd.Dir = p.config.Dir
	p.cmd.Env = p.config.Env
	p.cmd.Stdin = p.config.Stdin
	p.startTime = time.Now()
	p.mu.Unlock()

	// Setup stdout/stderr pipes
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderr, err := p.cmd.StderrPipe()
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
		p.setState(StateFailed)
		p.setLastError(err)
		return err
	}

	p.setState(StateRunning)

	// Wait for process completion
	err = p.cmd.Wait()
	wg.Wait()

	if err != nil {
		p.setState(StateFailed)
		p.setLastError(err)
	} else {
		p.setState(StateStopped)
	}

	return err
}

// Stop gracefully stops the process
func (p *Process) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}

	// Wait for process to stop gracefully
	select {
	case <-p.done:
		return nil
	case <-time.After(p.config.StopTimeout):
		// Force kill if timeout
		if p.cmd != nil && p.cmd.Process != nil {
			return p.cmd.Process.Kill()
		}
	}

	return nil
}

// Wait blocks until the process stops
func (p *Process) Wait() error {
	<-p.done
	p.mu.RLock()
	err := p.lastError
	p.mu.RUnlock()
	return err
}

// stopProcess stops the current process without canceling supervision
func (p *Process) stopProcess() {
	p.mu.RLock()
	cmd := p.cmd
	p.mu.RUnlock()

	if cmd != nil && cmd.Process != nil {
		// Try graceful shutdown first
		cmd.Process.Signal(os.Interrupt)

		// Wait for graceful shutdown or force kill
		done := make(chan struct{})
		go func() {
			cmd.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Process stopped gracefully
		case <-time.After(p.config.StopTimeout):
			// Force kill
			cmd.Process.Kill()
		}
	}

	p.setState(StateStopped)
}

// shouldRestart determines if the process should be restarted
func (p *Process) shouldRestart(err error) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Check context cancellation
	select {
	case <-p.ctx.Done():
		return false
	default:
	}

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
	defer src.Close()
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
func (p *Process) setState(state ProcessState) {
	old := p.getState()
	p.state.Store(state)
	if p.config.OnStateChange != nil && old != state {
		p.config.OnStateChange(old, state)
	}
}

func (p *Process) getState() ProcessState {
	if v := p.state.Load(); v != nil {
		return v.(ProcessState)
	}
	return StateNotStarted
}

func (p *Process) setLastError(err error) {
	p.mu.Lock()
	p.lastError = err
	p.mu.Unlock()

	if p.config.OnError != nil {
		p.config.OnError(err)
	}
}

// GetState returns the current process state
func (p *Process) GetState() ProcessState {
	return p.getState()
}

// GetRestartCount returns the number of times the process has been restarted
func (p *Process) GetRestartCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.restartCount
}

// GetUptime returns how long the process has been running
func (p *Process) GetUptime() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.getState() != StateRunning {
		return 0
	}
	return time.Since(p.startTime)
}

// GetLastError returns the last error that occurred
func (p *Process) GetLastError() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastError
}

// Utility functions for simple execution (backward compatibility)

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

	if err := process.Start(ctx); err != nil {
		return err
	}

	return process.Wait()
}

// Built-in formatter functions

// PrefixFormatter creates a formatter that adds a prefix to each line
func PrefixFormatter(prefix string) LogFormatter {
	return func(line string) string {
		return prefix + line
	}
}

// TimestampFormatter adds a timestamp to each line
func TimestampFormatter(format string) LogFormatter {
	return func(line string) string {
		return fmt.Sprintf("[%s] %s", time.Now().Format(format), line)
	}
}

// ChainFormatters combines multiple formatters
func ChainFormatters(formatters ...LogFormatter) LogFormatter {
	return func(line string) string {
		for _, formatter := range formatters {
			if formatter != nil {
				line = formatter(line)
			}
		}
		return line
	}
}

// String returns the string representation of ProcessState
func (s ProcessState) String() string {
	switch s {
	case StateNotStarted:
		return "not_started"
	case StateRunning:
		return "running"
	case StateStopped:
		return "stopped"
	case StateFailed:
		return "failed"
	case StateRestarting:
		return "restarting"
	default:
		return "unknown"
	}
}
