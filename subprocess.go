package subprocess

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"time"
)

const (
	defaultRestartDelay    = time.Second
	defaultRestartBackoff  = 2
	defaultRestartDelayMax = 30 * time.Second
	defaultStopTimeout     = 10 * time.Second
	maxScanTokenSize       = 1024 * 1024
	// outputDrainGrace is how long to wait for scanners after force-closing
	// their pipe ends when a drain timeout expires.
	outputDrainGrace = 100 * time.Millisecond
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

// Config holds subprocess settings for New, Run, and RunWithContext.
// It is passed by value. Args and Env are deep-copied; Stdin, Stdout, Stderr,
// and callbacks are shared references.
type Config struct {
	Command         string
	Args            []string
	StdoutFormatter LogFormatter
	StderrFormatter LogFormatter
	// Stdin is the process standard input. Nil means the null device, not os.Stdin.
	// The caller owns Stdin and closes it if needed. For a non-file Reader, a
	// blocked Read may outlive Wait until the caller unblocks or closes it.
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
	// StopTimeout bounds waiting for the process group after StopSignal, and
	// separately bounds draining formatted output after shutdown or cancellation.
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

// New creates a managed process. It snapshots c and fills defaults.
func New(c Config) (*Process, error) {
	if c.Command == "" {
		return nil, errors.New("subprocess: command is empty")
	}

	// Deep-copy so Env keeps nil vs empty distinct (nil inherits the parent env).
	c.Args = copyStrings(c.Args)
	c.Env = copyStrings(c.Env)
	if c.Stdout == nil {
		c.Stdout = os.Stdout
	}
	if c.Stderr == nil {
		c.Stderr = os.Stderr
	}
	if c.RestartPolicy == "" {
		c.RestartPolicy = RestartNever
	}
	if c.RestartDelay <= 0 {
		c.RestartDelay = defaultRestartDelay
	}
	if c.RestartBackoff == 0 {
		c.RestartBackoff = defaultRestartBackoff
	} else if c.RestartBackoff < 1 {
		c.RestartBackoff = 1
	}
	if c.RestartDelayMax <= 0 {
		c.RestartDelayMax = defaultRestartDelayMax
	}
	if c.StopTimeout <= 0 {
		c.StopTimeout = defaultStopTimeout
	}
	if c.StopSignal == nil {
		c.StopSignal = os.Interrupt
	}

	return &Process{
		config: c,
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

	var input *managedInput
	defer func() {
		if input != nil {
			closeFiles(input.reader, input.writer)
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			p.setLastError(err)
			return
		}

		// One input pump across restarts so a pending Read stays on a single Reader.
		if input == nil && p.config.Stdin != nil {
			if _, isFile := p.config.Stdin.(*os.File); !isFile {
				var err error
				input, err = newManagedInput()
				if err != nil {
					p.setLastError(err)
					return
				}
			}
		}
		err := p.runProcess(ctx, input)
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
		if input != nil {
			select {
			case <-input.done:
				if input.err != nil {
					// A subsequent Read may recover from a transient input error.
					closeFiles(input.reader, input.writer)
					input = nil
				}
			default:
			}
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
	// A multiplier of 1 is a fixed delay (next == d is not overflow).
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

// managedInput is the stdin pipe shared across restarts. The source Reader stays
// caller-owned; the pump may still be blocked in that Reader after Wait returns.
// At most one pump exists per Process.
type managedInput struct {
	reader, writer *os.File
	done           chan struct{}
	err            error // published by closing done
	started        bool  // accessed only by the supervisor
}

func newManagedInput() (*managedInput, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("subprocess: stdin pipe: %w", err)
	}
	return &managedInput{reader: r, writer: w, done: make(chan struct{})}, nil
}

func (in *managedInput) start(src io.Reader) {
	if in.started {
		return
	}
	in.started = true
	go func() {
		_, in.err = io.Copy(in.writer, src)
		// Close done before the writer so waiters see err before the child sees EOF.
		close(in.done)
		closeFiles(in.writer)
	}()
}

func (p *Process) runProcess(ctx context.Context, input *managedInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cmd := exec.Command(p.config.Command, p.config.Args...)
	cmd.Dir = p.config.Dir
	cmd.Env = p.config.Env
	cmd.Stdin = p.config.Stdin
	configureSysProcAttr(cmd)

	// Feed stdin from our pipe so Wait is independent of the caller's Reader.
	if input != nil {
		cmd.Stdin = input.reader
	}

	// Formatter pipes are process-owned so scanners can keep reading after Wait.
	var stdoutR, stdoutW *os.File
	var stderrR, stderrW *os.File
	var err error

	if p.config.StdoutFormatter != nil {
		stdoutR, stdoutW, err = os.Pipe()
		if err != nil {
			return fmt.Errorf("subprocess: stdout pipe: %w", err)
		}
		cmd.Stdout = stdoutW
	} else if p.config.Stdout != nil {
		cmd.Stdout = outputWriter(p.config.Stdout)
	}

	if p.config.StderrFormatter != nil {
		stderrR, stderrW, err = os.Pipe()
		if err != nil {
			closeFiles(stdoutR, stdoutW)
			return fmt.Errorf("subprocess: stderr pipe: %w", err)
		}
		cmd.Stderr = stderrW
	} else if p.config.Stderr != nil {
		cmd.Stderr = outputWriter(p.config.Stderr)
	}

	if err := cmd.Start(); err != nil {
		closeFiles(stdoutR, stdoutW, stderrR, stderrW)
		return err
	}
	// Drop the parent's write ends so readers see EOF when the child exits.
	closeFiles(stdoutW, stderrW)
	if input != nil {
		input.start(p.config.Stdin)
	}

	p.mu.Lock()
	p.cmd = cmd
	p.running = true
	p.mu.Unlock()

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	if stdoutR != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if scanErr := scanLines(stdoutR, p.config.Stdout, p.config.StdoutFormatter); scanErr != nil {
				errCh <- scanErr
			}
		}()
	}
	if stderrR != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if scanErr := scanLines(stderrR, p.config.Stderr, p.config.StderrFormatter); scanErr != nil {
				errCh <- scanErr
			}
		}()
	}

	waitErr, shutdown := p.waitOrShutdown(ctx, cmd)
	p.recordExit(cmd)
	outputDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(outputDone)
	}()
	// Descendants may still hold output pipes after the direct child exits.
	// Bound the drain after shutdown/cancellation so Wait can return.
	abandoned := false
	if shutdown {
		abandoned = waitOutputDrain(outputDone, p.config.StopTimeout, stdoutR, stderrR)
	} else {
		select {
		case <-outputDone:
		case <-ctx.Done():
			waitErr = p.shutdownGroup(ctx, cmd, nil, true, waitErr)
			abandoned = waitOutputDrain(outputDone, p.config.StopTimeout, stdoutR, stderrR)
		}
	}

	var scanErr error
	if abandoned {
		// Destination Write may still be blocked; collect only ready scan errors.
		scanErr = takeReadyErrors(errCh)
	} else {
		close(errCh)
		for e := range errCh {
			scanErr = errors.Join(scanErr, e)
		}
	}
	// Surface a finished input error without waiting on a blocked Reader.
	if input != nil && waitErr == nil {
		select {
		case <-input.done:
			if input.err != nil {
				waitErr = fmt.Errorf("subprocess: stdin: %w", input.err)
			}
		default:
		}
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

func (p *Process) waitOrShutdown(ctx context.Context, cmd *exec.Cmd) (err error, shutdown bool) {
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	select {
	case err := <-waitDone:
		if ctx.Err() == nil {
			return err, false
		}
		// Context is already canceled; still shut down remaining group members.
		return p.shutdownGroup(ctx, cmd, nil, true, err), true
	case <-ctx.Done():
		return p.shutdownGroup(ctx, cmd, waitDone, false, nil), true
	}
}

// shutdownGroup signals the process group and waits until the direct child and
// any remaining members are gone, or StopTimeout elapses. waitDone is nil when
// the child has already been waited.
func (p *Process) shutdownGroup(ctx context.Context, cmd *exec.Cmd, waitDone <-chan error, childExited bool, waitErr error) error {
	if err := signalProcess(cmd, p.config.StopSignal); err != nil {
		killProcess(cmd)
		if !childExited {
			waitErr = <-waitDone
		}
		waitForProcessGroup(cmd)
		return wrapContextError(ctx, waitErr)
	}

	timer := time.NewTimer(p.config.StopTimeout)
	defer timer.Stop()

	var poll *time.Ticker
	var pollC <-chan time.Time
	defer func() {
		if poll != nil {
			poll.Stop()
		}
	}()

	startPoll := func() {
		if poll != nil {
			return
		}
		poll = time.NewTicker(10 * time.Millisecond)
		pollC = poll.C
	}

	if childExited && !processGroupAlive(cmd) {
		return wrapContextError(ctx, waitErr)
	}
	if childExited {
		startPoll()
	}

	for {
		select {
		case err := <-waitDone:
			waitErr = err
			childExited = true
			waitDone = nil
			if !processGroupAlive(cmd) {
				return wrapContextError(ctx, waitErr)
			}
			startPoll()
		case <-pollC:
			if childExited && !processGroupAlive(cmd) {
				return wrapContextError(ctx, waitErr)
			}
		case <-timer.C:
			killProcess(cmd)
			if !childExited {
				waitErr = <-waitDone
			}
			waitForProcessGroup(cmd)
			return wrapContextError(ctx, waitErr)
		}
	}
}

// waitForProcessGroup waits briefly for the group to disappear after SIGKILL.
// Processes in uninterruptible sleep may outlive this bound.
func waitForProcessGroup(cmd *exec.Cmd) {
	deadline := time.Now().Add(500 * time.Millisecond)
	for processGroupAlive(cmd) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// waitOutputDrain waits for formatted output scanners to finish. On timeout it
// closes the pipe read ends and, after a short grace period, returns whether
// the wait was abandoned (scanner likely blocked in a destination Write).
func waitOutputDrain(done <-chan struct{}, timeout time.Duration, pipes ...*os.File) bool {
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return false
	case <-timer.C:
	}

	closeFiles(pipes...)

	grace := time.NewTimer(outputDrainGrace)
	defer grace.Stop()
	select {
	case <-done:
		return false
	case <-grace.C:
		return true
	}
}

func takeReadyErrors(errCh <-chan error) error {
	var joined error
	for {
		select {
		case e := <-errCh:
			joined = errors.Join(joined, e)
		default:
			return joined
		}
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

// writerLocks maps destination Writer pointer identity to a mutex so shared
// Writers are serialized across streams and processes.
var writerLocks sync.Map // uintptr -> *sync.Mutex

type lockedWriter struct{ dest io.Writer }

func (w lockedWriter) Write(b []byte) (int, error) {
	mu := mutexForWriter(w.dest)
	mu.Lock()
	defer mu.Unlock()
	return w.dest.Write(b)
}

func outputWriter(dest io.Writer) io.Writer {
	// *os.File keeps OS file semantics when connected directly to the child.
	if _, ok := dest.(*os.File); ok {
		return dest
	}
	return lockedWriter{dest: dest}
}

// mutexForWriter returns the lock for dest's pointer identity.
// Non-pointer Writers get a private mutex for that call only.
func mutexForWriter(dest io.Writer) *sync.Mutex {
	if dest == nil {
		mu := &sync.Mutex{}
		return mu
	}
	v := reflect.ValueOf(dest)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.UnsafePointer:
		if v.IsNil() {
			return &sync.Mutex{}
		}
		key := v.Pointer()
		if existing, ok := writerLocks.Load(key); ok {
			return existing.(*sync.Mutex)
		}
		mu := &sync.Mutex{}
		actual, _ := writerLocks.LoadOrStore(key, mu)
		return actual.(*sync.Mutex)
	default:
		return &sync.Mutex{}
	}
}

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

func copyStrings(s []string) []string {
	if s == nil {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

func closeFiles(files ...*os.File) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}

func writeLine(dest io.Writer, line string) error {
	buf := make([]byte, len(line)+1)
	copy(buf, line)
	buf[len(line)] = '\n'

	mu := mutexForWriter(dest)
	mu.Lock()
	defer mu.Unlock()
	_, err := dest.Write(buf)
	return err
}

// Run executes a subprocess and waits for it to finish.
// An empty RestartPolicy is RestartNever.
func Run(config Config) error {
	return RunWithContext(context.Background(), config)
}

// RunWithContext executes a subprocess with ctx and waits for it to finish.
func RunWithContext(ctx context.Context, config Config) error {
	process, err := New(config)
	if err != nil {
		return err
	}
	if err := process.Start(ctx); err != nil {
		return err
	}
	return process.Wait()
}
