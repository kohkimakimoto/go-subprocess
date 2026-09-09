//go:build unix

package subprocess

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	switch os.Getenv("GO_SUBPROCESS_HELPER") {
	case "parent", "child":
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

func runHelper(role string) {
	switch role {
	case "child":
		time.Sleep(time.Minute)
	case "parent":
		cmd := exec.Command(os.Args[0])
		cmd.Env = helperEnv("child", "")
		if err := cmd.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		path := os.Getenv("GO_SUBPROCESS_PIDFILE")
		if err := os.WriteFile(path, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_ = cmd.Wait()
	}
	os.Exit(0)
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
