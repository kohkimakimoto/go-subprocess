package subprocess

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelWithBlockedStdin(t *testing.T) {
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := New(Config{Command: "sleep", Args: []string{"30"}, Stdin: r, Stdout: io.Discard, Stderr: io.Discard, StopTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitUntilRunning(t, p)
	done := make(chan error, 1)
	go func() { done <- p.Wait() }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait blocked on stdin after cancellation")
	}
	if p.Running() {
		t.Fatal("process still running")
	}
	if _, exited := p.ExitCode(); !exited {
		t.Fatal("child was not reaped")
	}
}

func TestExitWithBlockedStdin(t *testing.T) {
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	done := make(chan error, 1)
	go func() {
		p, err := Start(context.Background(), Config{Command: "sh", Args: []string{"-c", "exit 0"}, Stdin: r, Stdout: io.Discard, Stderr: io.Discard})
		if err != nil {
			done <- err
			return
		}
		done <- p.Wait()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait blocked on stdin after normal exit")
	}
}

func TestStdinTransfer(t *testing.T) {
	input := bytes.Repeat([]byte{'a', 0, 'b', '\n'}, 32768)
	var output bytes.Buffer
	p, err := Start(context.Background(), Config{Command: "cat", Stdin: bytes.NewReader(input), Stdout: &output, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), input) {
		t.Fatalf("copied %d bytes, want %d", output.Len(), len(input))
	}
}

type failingInput struct{ err error }

func (r failingInput) Read([]byte) (int, error) { return 0, r.err }

func TestStdinError(t *testing.T) {
	want := errors.New("input failed")
	p, err := Start(context.Background(), Config{Command: "cat", Stdin: failingInput{want}, Stdout: io.Discard, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); !errors.Is(err, want) {
		t.Fatalf("Wait = %v, want %v", err, want)
	}
}

func TestBlockedStdinSurvivesRestart(t *testing.T) {
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	marker := filepath.Join(t.TempDir(), "started")
	script := fmt.Sprintf("if [ ! -f %q ]; then touch %q; exit 1; fi; cat", marker, marker)
	var output bytes.Buffer
	p, err := New(Config{
		Command: "sh", Args: []string{"-c", script},
		Stdin: r, Stdout: &output, Stderr: io.Discard,
		RestartPolicy: RestartOnFail, MaxRestarts: 1,
		RestartDelay: time.Millisecond,
		OnRestart: func(int) {
			// This satisfies the Read left pending by the first run. The bytes
			// must reach the next child instead of the first run's closed pipe.
			_, _ = io.WriteString(w, "after restart\n")
			_ = w.Close()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("restart blocked on stdin")
	}
	if got := output.String(); got != "after restart\n" {
		t.Fatalf("stdout = %q", got)
	}
	if p.RestartCount() != 1 {
		t.Fatalf("restarts = %d, want 1", p.RestartCount())
	}
}

type observedInput struct{ reads int32 }

func (r *observedInput) Read([]byte) (int, error) {
	atomic.AddInt32(&r.reads, 1)
	return 0, io.EOF
}

func TestStartFailureDoesNotReadStdin(t *testing.T) {
	r := &observedInput{}
	p, err := Start(context.Background(), Config{Command: filepath.Join(t.TempDir(), "missing"), Stdin: r})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err == nil {
		t.Fatal("expected start failure")
	}
	if got := atomic.LoadInt32(&r.reads); got != 0 {
		t.Fatalf("Read called %d times", got)
	}
}

type recoveringInput struct{ failed bool }

func (r *recoveringInput) Read([]byte) (int, error) {
	if !r.failed {
		r.failed = true
		return 0, errors.New("temporary input error")
	}
	return 0, io.EOF
}

func TestRestartRecoversFromStdinError(t *testing.T) {
	p, err := New(Config{
		Command: "cat", Stdin: &recoveringInput{}, Stdout: io.Discard, Stderr: io.Discard,
		RestartPolicy: RestartOnFail, MaxRestarts: 1, RestartDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err != nil {
		t.Fatalf("Wait = %v, want nil after recovery", err)
	}
	if p.RestartCount() != 1 {
		t.Fatalf("restarts = %d, want 1", p.RestartCount())
	}
}
