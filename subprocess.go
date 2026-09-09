package subprocess

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

const (
	defaultRestartDelay    = time.Second
	defaultRestartBackoff  = 2
	defaultRestartDelayMax = 30 * time.Second
	defaultStopTimeout     = 10 * time.Second
	maxScanTokenSize       = 1024 * 1024
)

// RestartPolicy defines how a process should be restarted.
type RestartPolicy string

const (
	// RestartNever does not restart the process after it exits.
	RestartNever RestartPolicy = "never"
	// RestartAlways restarts the process after every exit, including a zero exit status.
	RestartAlways RestartPolicy = "always"
	// RestartOnFail restarts the process only when it exits with an error.
	RestartOnFail RestartPolicy = "on-failure"
)

// LogFormatter formats a single line of process output.
// The line does not include the trailing newline.
type LogFormatter func(line string) string

// Config contains all configuration for a subprocess.
// New copies Config, so later changes to the original value are not observed.
// Args and Env are copied; do not mutate them after Start.
type Config struct {
	Command         string
	Args            []string
	StdoutFormatter LogFormatter
	StderrFormatter LogFormatter
	// Stdin is the process standard input. Nil means the null device, not os.Stdin.
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	Dir    string
	// Env is the process environment. Nil inherits the current process environment.
	// An empty non-nil slice starts the process with an empty environment.
	Env []string

	// RestartPolicy controls whether the process is restarted after it exits.
	// The zero value is RestartNever.
	RestartPolicy RestartPolicy
	// MaxRestarts is the maximum number of restarts, not including the initial start.
	// Zero means unlimited restarts.
	MaxRestarts int
	// RestartDelay is the delay before the first restart. The zero value is 1s.
	RestartDelay time.Duration
	// RestartBackoff multiplies RestartDelay after each restart.
	// The zero value is 2 (exponential). Set 1 for a fixed delay.
	RestartBackoff float64
	// RestartDelayMax caps the restart delay. The zero value is 30s.
	RestartDelayMax time.Duration
	// StopTimeout is how long to wait after StopSignal before killing the process.
	// The zero value is 10s.
	StopTimeout time.Duration
	// StopSignal is sent on context cancellation. The zero value is os.Interrupt.
	StopSignal os.Signal

	// OnRestart is called after each restart is scheduled, with the number of
	// restarts so far (starting at 1). It must not call Wait.
	OnRestart func(count int)
	// OnError is called when a process run fails for a reason other than
	// context cancellation. It must not call Wait.
	OnError func(error)
}

// Process is a managed subprocess.
type Process struct {
	config Config

	mu           sync.Mutex
	cmd          *exec.Cmd
	started      bool
	running      bool
	restartCount int
	lastError    error
	exitCode     int
	exited       bool

	done chan struct{}
}

// ErrNotStarted is returned by Wait when Start has not been called.
var ErrNotStarted = errors.New("subprocess: not started")

// ErrAlreadyStarted is returned by Start when the process was already started.
var ErrAlreadyStarted = errors.New("subprocess: already started")

// New creates a managed process. It copies config and fills defaults.
func New(c *Config) (*Process, error) {
	if c == nil {
		return nil, errors.New("subprocess: config is nil")
	}
	if c.Command == "" {
		return nil, errors.New("subprocess: command is empty")
	}

	cfg := *c
	if c.Args != nil {
		cfg.Args = append([]string(nil), c.Args...)
	}
	if c.Env != nil {
		cfg.Env = append([]string(nil), c.Env...)
	}
	if cfg.Stdout == nil {
		cfg.Stdout = os.Stdout
	}
	if cfg.Stderr == nil {
		cfg.Stderr = os.Stderr
	}
	if cfg.RestartPolicy == "" {
		cfg.RestartPolicy = RestartNever
	}
	if cfg.RestartDelay <= 0 {
		cfg.RestartDelay = defaultRestartDelay
	}
	if cfg.RestartBackoff == 0 {
		cfg.RestartBackoff = defaultRestartBackoff
	} else if cfg.RestartBackoff < 1 {
		cfg.RestartBackoff = 1
	}
	if cfg.RestartDelayMax <= 0 {
		cfg.RestartDelayMax = defaultRestartDelayMax
	}
	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = defaultStopTimeout
	}
	if cfg.StopSignal == nil {
		cfg.StopSignal = os.Interrupt
	}

	return &Process{
		config: cfg,
		done:   make(chan struct{}),
	}, nil
}

// Start starts the process and supervises it until it stops permanently or ctx is canceled.
// Start is asynchronous. Call Wait to block until the supervisor exits.
// Calling Start more than once returns ErrAlreadyStarted.
func (p *Process) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return ErrAlreadyStarted
	}
	p.started = true
	go p.supervise(ctx)
	return nil
}

// Wait blocks until the supervisor stops. It returns the result of the last run.
// Context cancellation is returned as an error wrapping context.Canceled or
// context.DeadlineExceeded. Wait before Start returns ErrNotStarted.
func (p *Process) Wait() error {
	p.mu.Lock()
	started := p.started
	p.mu.Unlock()
	if !started {
		return ErrNotStarted
	}

	<-p.done

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastError
}

// Running reports whether the current child process is running.
func (p *Process) Running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// Pid returns the PID of the current or most recent child process.
// It returns 0 if the process has not started.
func (p *Process) Pid() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

// RestartCount returns how many times the process has been restarted.
func (p *Process) RestartCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.restartCount
}

// ExitCode returns the last child's exit code and whether a child has exited.
// The code is -1 if the process was terminated by a signal.
func (p *Process) ExitCode() (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exitCode, p.exited
}

func (p *Process) supervise(ctx context.Context) {
	defer close(p.done)

	for {
		if err := ctx.Err(); err != nil {
			p.setLastError(err)
			return
		}

		err := p.runProcess(ctx)
		p.setLastError(err)

		if ctx.Err() != nil {
			return
		}
		if !p.shouldRestart(err) {
			return
		}
		if err := p.waitRestart(ctx); err != nil {
			p.setLastError(err)
			return
		}
	}
}

func (p *Process) waitRestart(ctx context.Context) error {
	timer := time.NewTimer(p.restartDelay())
	defer timer.Stop()

	select {
	case <-timer.C:
		p.mu.Lock()
		p.restartCount++
		count := p.restartCount
		p.mu.Unlock()

		if p.config.OnRestart != nil {
			p.config.OnRestart(count)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Process) restartDelay() time.Duration {
	delay := p.config.RestartDelay
	maxDelay := p.config.RestartDelayMax
	backoff := p.config.RestartBackoff
	if backoff < 1 {
		backoff = 1
	}
	// A multiplier of 1 is a fixed delay. Do not treat next == d as overflow.
	if backoff == 1 {
		if maxDelay > 0 && delay > maxDelay {
			return maxDelay
		}
		return delay
	}

	p.mu.Lock()
	restarts := p.restartCount
	p.mu.Unlock()

	d := delay
	for i := 0; i < restarts; i++ {
		if maxDelay > 0 && d >= maxDelay {
			return maxDelay
		}
		next := time.Duration(float64(d) * backoff)
		if next <= d {
			if maxDelay > 0 {
				return maxDelay
			}
			return d
		}
		d = next
	}
	if maxDelay > 0 && d > maxDelay {
		return maxDelay
	}
	return d
}

func (p *Process) shouldRestart(err error) bool {
	p.mu.Lock()
	count := p.restartCount
	p.mu.Unlock()

	switch p.config.RestartPolicy {
	case RestartAlways:
		return p.config.MaxRestarts <= 0 || count < p.config.MaxRestarts
	case RestartOnFail:
		if err == nil {
			return false
		}
		return p.config.MaxRestarts <= 0 || count < p.config.MaxRestarts
	default:
		return false
	}
}

func (p *Process) runProcess(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cmd := exec.Command(p.config.Command, p.config.Args...)
	cmd.Dir = p.config.Dir
	cmd.Env = p.config.Env
	cmd.Stdin = p.config.Stdin
	configureSysProcAttr(cmd)

	var stdout io.ReadCloser
	var stderr io.ReadCloser
	var err error

	if p.config.StdoutFormatter != nil {
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return fmt.Errorf("subprocess: stdout pipe: %w", err)
		}
	} else if p.config.Stdout != nil {
		cmd.Stdout = p.config.Stdout
	}

	if p.config.StderrFormatter != nil {
		stderr, err = cmd.StderrPipe()
		if err != nil {
			if stdout != nil {
				stdout.Close()
			}
			return fmt.Errorf("subprocess: stderr pipe: %w", err)
		}
	} else if p.config.Stderr != nil {
		cmd.Stderr = p.config.Stderr
	}

	if err := cmd.Start(); err != nil {
		if stdout != nil {
			stdout.Close()
		}
		if stderr != nil {
			stderr.Close()
		}
		return err
	}

	p.mu.Lock()
	p.cmd = cmd
	p.running = true
	p.mu.Unlock()

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	if stdout != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if scanErr := scanLines(stdout, p.config.Stdout, p.config.StdoutFormatter); scanErr != nil {
				errCh <- scanErr
			}
		}()
	}
	if stderr != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if scanErr := scanLines(stderr, p.config.Stderr, p.config.StderrFormatter); scanErr != nil {
				errCh <- scanErr
			}
		}()
	}

	waitErr := p.waitOrShutdown(ctx, cmd)
	p.recordExit(cmd)
	wg.Wait()
	close(errCh)

	var scanErr error
	for e := range errCh {
		scanErr = errors.Join(scanErr, e)
	}
	return combineRunError(waitErr, scanErr)
}

func (p *Process) recordExit(cmd *exec.Cmd) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false
	if cmd.ProcessState == nil {
		return
	}
	p.exitCode = cmd.ProcessState.ExitCode()
	p.exited = true
}

func (p *Process) waitOrShutdown(ctx context.Context, cmd *exec.Cmd) error {
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	select {
	case err := <-waitDone:
		return err
	case <-ctx.Done():
		return p.shutdown(ctx, cmd, waitDone)
	}
}

// shutdown signals the child and waits on the single Wait already in progress.
func (p *Process) shutdown(ctx context.Context, cmd *exec.Cmd, waitDone <-chan error) error {
	if err := signalProcess(cmd, p.config.StopSignal); err != nil {
		killProcess(cmd)
		return wrapContextError(ctx, <-waitDone)
	}

	timer := time.NewTimer(p.config.StopTimeout)
	defer timer.Stop()

	select {
	case err := <-waitDone:
		return wrapContextError(ctx, err)
	case <-timer.C:
		killProcess(cmd)
		return wrapContextError(ctx, <-waitDone)
	}
}

func (p *Process) setLastError(err error) {
	p.mu.Lock()
	p.lastError = err
	p.mu.Unlock()

	if err == nil || p.config.OnError == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	p.config.OnError(err)
}

func wrapContextError(ctx context.Context, err error) error {
	ctxErr := ctx.Err()
	if ctxErr == nil {
		return err
	}
	if err == nil || errors.Is(err, ctxErr) {
		return ctxErr
	}
	return fmt.Errorf("%w: %v", ctxErr, err)
}

func combineRunError(waitErr, scanErr error) error {
	if waitErr == nil {
		return scanErr
	}
	if scanErr == nil {
		return waitErr
	}
	return fmt.Errorf("%w; output: %v", waitErr, scanErr)
}

// lineWriteMu serializes formatted line writes so concurrent stdout/stderr
// and multiple processes do not interleave mid-line.
var lineWriteMu sync.Mutex

func scanLines(src io.ReadCloser, dest io.Writer, formatter LogFormatter) error {
	defer src.Close()

	if dest == nil {
		dest = io.Discard
	}

	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 64*1024), maxScanTokenSize)
	for scanner.Scan() {
		line := scanner.Text()
		if formatter != nil {
			line = formatter(line)
		}
		if err := writeLine(dest, line); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("subprocess: scan output: %w", err)
	}
	return nil
}

func writeLine(dest io.Writer, line string) error {
	buf := make([]byte, len(line)+1)
	copy(buf, line)
	buf[len(line)] = '\n'

	lineWriteMu.Lock()
	defer lineWriteMu.Unlock()
	_, err := dest.Write(buf)
	return err
}

// Run executes a subprocess and waits for it to finish.
// An empty RestartPolicy is RestartNever.
func Run(config *Config) error {
	return RunWithContext(context.Background(), config)
}

// RunWithContext executes a subprocess with ctx and waits for it to finish.
func RunWithContext(ctx context.Context, config *Config) error {
	process, err := New(config)
	if err != nil {
		return err
	}
	if err := process.Start(ctx); err != nil {
		return err
	}
	return process.Wait()
}
