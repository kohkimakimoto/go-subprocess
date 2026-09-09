package subprocess

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewValidation(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("expected error for nil config")
	}
	if _, err := New(&Config{}); err == nil {
		t.Fatal("expected error for empty command")
	}
}

func TestNewDoesNotMutateConfig(t *testing.T) {
	cfg := &Config{Command: "echo", Args: []string{"hi"}}
	if _, err := New(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Stdout != nil || cfg.Stderr != nil || cfg.StopSignal != nil {
		t.Fatal("New mutated the caller's config")
	}
	if cfg.Stdin != nil {
		t.Fatal("Stdin should stay nil")
	}
}

func TestRunEcho(t *testing.T) {
	var buf bytes.Buffer
	err := Run(&Config{
		Command: "echo",
		Args:    []string{"hello"},
		Stdout:  &buf,
		Stderr:  io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "hello\n" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestEmptyEnvIsNotInherited(t *testing.T) {
	const key = "GO_SUBPROCESS_EMPTY_ENV"
	t.Setenv(key, "inherited")

	env := []string{}
	p, err := New(&Config{Command: "sh", Env: env})
	if err != nil {
		t.Fatal(err)
	}
	if p.config.Env == nil {
		t.Fatal("empty Env became nil")
	}
	env = append(env, key+"=leaked")
	if len(p.config.Env) != 0 {
		t.Fatal("Env aliases the caller slice")
	}

	var buf bytes.Buffer
	err = Run(&Config{
		Command: "sh",
		Args:    []string{"-c", "printf %s \"$" + key + "\""},
		Env:     []string{},
		Stdout:  &buf,
		Stderr:  io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "" {
		t.Fatalf("empty Env inherited %s=%q", key, got)
	}
}

func TestNilEnvInherits(t *testing.T) {
	const key = "GO_SUBPROCESS_NIL_ENV"
	t.Setenv(key, "inherited")

	var buf bytes.Buffer
	err := Run(&Config{
		Command: "sh",
		Args:    []string{"-c", "printf %s \"$" + key + "\""},
		Stdout:  &buf,
		Stderr:  io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "inherited" {
		t.Fatalf("nil Env = %q, want inherited", got)
	}
}

func TestSlowFormatterKeepsSuccessfulOutput(t *testing.T) {
	const n = 200
	script := fmt.Sprintf(`i=1; while [ "$i" -le %d ]; do echo "out-$i"; echo "err-$i" >&2; i=$((i+1)); done`, n)

	var stdout, stderr bytes.Buffer
	p, err := New(&Config{
		Command: "sh",
		Args:    []string{"-c", script},
		Stdout:  &stdout,
		Stderr:  &stderr,
		StdoutFormatter: func(line string) string {
			time.Sleep(time.Millisecond)
			return line
		},
		StderrFormatter: func(line string) string {
			time.Sleep(time.Millisecond)
			return line
		},
		RestartPolicy:  RestartOnFail,
		MaxRestarts:    1,
		RestartDelay:   10 * time.Millisecond,
		RestartBackoff: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err != nil {
		t.Fatal(err)
	}
	if p.RestartCount() != 0 {
		t.Fatalf("restarts = %d, want 0", p.RestartCount())
	}
	if got := strings.Count(stdout.String(), "\n"); got != n {
		t.Fatalf("stdout lines = %d, want %d", got, n)
	}
	if got := strings.Count(stderr.String(), "\n"); got != n {
		t.Fatalf("stderr lines = %d, want %d", got, n)
	}
}

func TestFormatterAndLongLine(t *testing.T) {
	var buf bytes.Buffer
	err := Run(&Config{
		Command: "echo",
		Args:    []string{"hello"},
		Stdout:  &buf,
		Stderr:  io.Discard,
		StdoutFormatter: func(line string) string {
			return "X:" + line
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "X:hello\n" {
		t.Fatalf("stdout = %q", got)
	}

	line := strings.Repeat("a", 70*1024)
	buf.Reset()
	err = Run(&Config{
		Command: "sh",
		Args:    []string{"-c", "printf '%s\n' \"$1\"", "sh", line},
		Stdout:  &buf,
		Stderr:  io.Discard,
		StdoutFormatter: func(line string) string {
			return line
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSuffix(buf.String(), "\n"); got != line {
		t.Fatalf("long line len = %d, want %d", len(got), len(line))
	}
}

func TestStartFailure(t *testing.T) {
	p, err := New(&Config{
		Command: filepath.Join(t.TempDir(), "missing-binary"),
		Stdout:  io.Discard,
		Stderr:  io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = p.Wait()
	if err == nil {
		t.Fatal("expected start failure")
	}
	if p.Running() {
		t.Fatal("process should not be running")
	}
	if pid := p.Pid(); pid != 0 {
		t.Fatalf("pid = %d, want 0", pid)
	}
}

func TestWaitBeforeStart(t *testing.T) {
	p, err := New(&Config{Command: "echo", Stdout: io.Discard, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Wait() = %v, want ErrNotStarted", err)
	}
}

func TestDoubleStart(t *testing.T) {
	p, err := New(&Config{
		Command: "sleep",
		Args:    []string{"5"},
		Stdout:  io.Discard,
		Stderr:  io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start() = %v, want ErrAlreadyStarted", err)
	}
	cancel()
	_ = p.Wait()
}

func TestContextCancel(t *testing.T) {
	var onError int
	p, err := New(&Config{
		Command: "sleep",
		Args:    []string{"30"},
		Stdout:  io.Discard,
		Stderr:  io.Discard,
		OnError: func(error) { onError++ },
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitUntilRunning(t, p)
	cancel()

	err = p.Wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() = %v, want context.Canceled", err)
	}
	if onError != 0 {
		t.Fatalf("OnError called %d times, want 0", onError)
	}
	code, ok := p.ExitCode()
	if !ok {
		t.Fatal("expected an exit")
	}
	if code == 0 {
		t.Fatalf("exit code = %d, want non-zero", code)
	}
	if p.Running() {
		t.Fatal("still running")
	}
}

func TestDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	p, err := New(&Config{
		Command: "sleep",
		Args:    []string{"30"},
		Stdout:  io.Discard,
		Stderr:  io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	err = p.Wait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() = %v, want context.DeadlineExceeded", err)
	}
}

func TestRestartOnFailThenSuccessClearsError(t *testing.T) {
	dir := t.TempDir()
	countFile := filepath.Join(dir, "n")
	script := fmt.Sprintf(`n=$(cat %q 2>/dev/null || echo 0); n=$((n+1)); echo "$n" > %q; [ "$n" -ge 2 ]`, countFile, countFile)

	var (
		mu       sync.Mutex
		errorsN  int
		restarts []int
	)
	p, err := New(&Config{
		Command:        "sh",
		Args:           []string{"-c", script},
		Stdout:         io.Discard,
		Stderr:         io.Discard,
		RestartPolicy:  RestartOnFail,
		MaxRestarts:    3,
		RestartDelay:   10 * time.Millisecond,
		RestartBackoff: 1,
		OnRestart: func(count int) {
			mu.Lock()
			restarts = append(restarts, count)
			mu.Unlock()
		},
		OnError: func(error) {
			mu.Lock()
			errorsN++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err != nil {
		t.Fatalf("Wait() = %v, want nil after successful restart", err)
	}
	if p.RestartCount() != 1 {
		t.Fatalf("restarts = %d, want 1", p.RestartCount())
	}
	if len(restarts) != 1 || restarts[0] != 1 {
		t.Fatalf("OnRestart = %v", restarts)
	}
	if errorsN != 1 {
		t.Fatalf("OnError calls = %d, want 1", errorsN)
	}
	code, ok := p.ExitCode()
	if !ok || code != 0 {
		t.Fatalf("exit = %d, %v", code, ok)
	}
}

func TestMaxRestarts(t *testing.T) {
	p, err := New(&Config{
		Command:        "sh",
		Args:           []string{"-c", "exit 1"},
		Stdout:         io.Discard,
		Stderr:         io.Discard,
		RestartPolicy:  RestartOnFail,
		MaxRestarts:    2,
		RestartDelay:   10 * time.Millisecond,
		RestartBackoff: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = p.Wait()
	if err == nil {
		t.Fatal("expected final failure")
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("Wait() = %v, want ExitError", err)
	}
	if p.RestartCount() != 2 {
		t.Fatalf("restarts = %d, want 2", p.RestartCount())
	}
	code, ok := p.ExitCode()
	if !ok || code != 1 {
		t.Fatalf("exit = %d, %v", code, ok)
	}
}

func TestRestartNever(t *testing.T) {
	p, err := New(&Config{
		Command:       "sh",
		Args:          []string{"-c", "exit 1"},
		Stdout:        io.Discard,
		Stderr:        io.Discard,
		RestartPolicy: RestartNever,
		RestartDelay:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err == nil {
		t.Fatal("expected failure")
	}
	if p.RestartCount() != 0 {
		t.Fatalf("restarts = %d, want 0", p.RestartCount())
	}
}

func TestRestartDelayBackoff(t *testing.T) {
	p := &Process{config: Config{
		RestartDelay:    time.Second,
		RestartBackoff:  2,
		RestartDelayMax: 30 * time.Second,
	}}
	if d := p.restartDelay(); d != time.Second {
		t.Fatalf("delay = %s, want 1s", d)
	}
	p.restartCount = 1
	if d := p.restartDelay(); d != 2*time.Second {
		t.Fatalf("delay = %s, want 2s", d)
	}
	p.restartCount = 5
	if d := p.restartDelay(); d != 30*time.Second {
		t.Fatalf("delay = %s, want 30s", d)
	}

	p.config.RestartBackoff = 1
	p.restartCount = 4
	if d := p.restartDelay(); d != time.Second {
		t.Fatalf("fixed delay = %s, want 1s", d)
	}
}

func waitUntilRunning(t *testing.T, p *Process) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Running() && p.Pid() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("process did not start")
}
