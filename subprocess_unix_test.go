//go:build unix

package subprocess

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	switch os.Getenv("GO_SUBPROCESS_HELPER") {
	case "parent", "child", "parent-ignore", "parent-exit", "ignore":
		runHelper(os.Getenv("GO_SUBPROCESS_HELPER"))
	default:
		os.Exit(m.Run())
	}
}

func TestCancelKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")

	ctx, cancel := context.WithCancel(context.Background())
	p, err := New(&Config{
		Command:     os.Args[0],
		Env:         helperEnv("parent", pidFile),
		Stdout:      io.Discard,
		Stderr:      io.Discard,
		StopTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	childPID := waitForPIDFile(t, pidFile)
	t.Cleanup(func() {
		_ = syscall.Kill(childPID, syscall.SIGKILL)
	})

	cancel()

	// StopTimeout is 5s and SIGKILL is the fallback. The grandchild must die
	// from the group signal well before that, or only the direct child was signaled.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(childPID, 0); err != nil {
			if err := p.Wait(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Wait() = %v, want context.Canceled", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grandchild %d still running", childPID)
}

func TestCancelKillsDescendantsThatIgnoreStopSignal(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")

	ctx, cancel := context.WithCancel(context.Background())
	p, err := New(&Config{
		Command:     os.Args[0],
		Env:         helperEnv("parent-ignore", pidFile),
		Stdout:      io.Discard,
		Stderr:      io.Discard,
		StopTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	childPID := waitForPIDFile(t, pidFile)
	t.Cleanup(func() {
		_ = syscall.Kill(childPID, syscall.SIGKILL)
	})

	started := time.Now()
	cancel()
	if err := p.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed < 80*time.Millisecond {
		t.Fatalf("Wait returned in %s, want to wait StopTimeout for the group", elapsed)
	}
	if processAlive(childPID) {
		t.Fatalf("grandchild %d still running after Wait", childPID)
	}
}

func runHelper(role string) {
	switch role {
	case "child":
		time.Sleep(time.Minute)
	case "ignore":
		signal.Ignore(os.Interrupt, syscall.SIGTERM)
		path := os.Getenv("GO_SUBPROCESS_PIDFILE")
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		time.Sleep(time.Minute)
	case "parent", "parent-ignore", "parent-exit":
		childRole := "child"
		if role == "parent-ignore" || role == "parent-exit" {
			childRole = "ignore"
		}
		cmd := exec.Command(os.Args[0])
		cmd.Env = helperEnv(childRole, os.Getenv("GO_SUBPROCESS_PIDFILE"))
		if role == "parent-exit" {
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
		}
		if err := cmd.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if role == "parent-exit" {
			// An optional argument makes the unformatted copier fail before exit.
			if len(os.Args) > 1 {
				if os.Args[1] == "stdout" {
					fmt.Fprintln(os.Stdout, "writer-error")
				} else if os.Args[1] == "stderr" {
					fmt.Fprintln(os.Stderr, "writer-error")
				}
			}
			os.Exit(0)
		}
		if role == "parent" {
			path := os.Getenv("GO_SUBPROCESS_PIDFILE")
			if err := os.WriteFile(path, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		_ = cmd.Wait()
	}
	os.Exit(0)
}

func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

func helperEnv(role, pidFile string) []string {
	base := os.Environ()
	out := make([]string, 0, len(base)+2)
	for _, e := range base {
		if strings.HasPrefix(e, "GO_SUBPROCESS_HELPER=") || strings.HasPrefix(e, "GO_SUBPROCESS_PIDFILE=") {
			continue
		}
		out = append(out, e)
	}
	out = append(out, "GO_SUBPROCESS_HELPER="+role)
	if pidFile != "" {
		out = append(out, "GO_SUBPROCESS_PIDFILE="+pidFile)
	}
	return out
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("child pid file was not written")
	return 0
}

func TestCancelWhileDrainingDescendantOutput(t *testing.T) {
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			pidFile := filepath.Join(t.TempDir(), "child.pid")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := &Config{
				Command:     os.Args[0],
				Env:         helperEnv("parent-exit", pidFile),
				Stdout:      os.Stdout,
				Stderr:      os.Stderr,
				StopTimeout: 50 * time.Millisecond,
			}
			formatter := func(s string) string { return s }
			if stream == "stdout" {
				cfg.StdoutFormatter = formatter
				cfg.Stdout = io.Discard
			} else {
				cfg.StderrFormatter = formatter
				cfg.Stderr = io.Discard
			}
			p, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Start(ctx); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- p.Wait() }()
			t.Cleanup(func() {
				cancel()
				if pid := p.Pid(); pid > 0 {
					_ = syscall.Kill(-pid, syscall.SIGKILL)
				}
			})
			childPID := waitForPIDFile(t, pidFile)
			deadline := time.Now().Add(3 * time.Second)
			for {
				if _, exited := p.ExitCode(); exited {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("parent did not exit")
				}
				time.Sleep(time.Millisecond)
			}
			// The direct child has exited, but the descendant still owns the pipe.
			select {
			case err := <-done:
				t.Fatalf("Wait returned before cancellation: %v", err)
			default:
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Wait = %v, want context.Canceled", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Wait blocked while draining descendant output after cancellation")
			}
			if processAlive(childPID) {
				t.Fatalf("descendant %d still alive", childPID)
			}
		})
	}
}

type contextErrorWriter struct{ err error }

func (w contextErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestCancelAfterOutputWriterContextError(t *testing.T) {
	for _, writerErr := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, stream := range []string{"stdout", "stderr"} {
			t.Run(writerErr.Error()+"/"+stream, func(t *testing.T) {
				pidFile := filepath.Join(t.TempDir(), "child.pid")
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cfg := &Config{
					Command:     os.Args[0],
					Args:        []string{stream},
					Env:         helperEnv("parent-exit", pidFile),
					Stdout:      io.Discard,
					Stderr:      io.Discard,
					StopTimeout: 50 * time.Millisecond,
				}
				formatter := func(s string) string { return s }
				if stream == "stdout" {
					cfg.Stdout = contextErrorWriter{writerErr}
					cfg.StderrFormatter = formatter
				} else {
					cfg.Stderr = contextErrorWriter{writerErr}
					cfg.StdoutFormatter = formatter
				}
				p, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if err := p.Start(ctx); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- p.Wait() }()
				t.Cleanup(func() {
					cancel()
					if pid := p.Pid(); pid > 0 {
						_ = syscall.Kill(-pid, syscall.SIGKILL)
					}
				})
				childPID := waitForPIDFile(t, pidFile)
				deadline := time.Now().Add(3 * time.Second)
				for {
					if _, exited := p.ExitCode(); exited {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("parent did not exit")
					}
					time.Sleep(time.Millisecond)
				}
				select {
				case err := <-done:
					t.Fatalf("Wait returned before cancellation: %v", err)
				default:
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("Wait = %v, want context.Canceled", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("Writer context error prevented cancellation while draining descendant output")
				}
				if processAlive(childPID) {
					t.Fatalf("descendant %d still alive", childPID)
				}
			})
		}
	}
}
