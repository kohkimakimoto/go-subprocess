//go:build unix

package subprocess

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func configureSysProcAttr(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

func signalProcess(cmd *exec.Cmd, sig os.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if s, ok := sig.(syscall.Signal); ok {
		// Signal the process group so descendants receive the same signal.
		// Children started as shell background jobs often ignore SIGINT/SIGTERM;
		// killProcess sends SIGKILL to the group after StopTimeout.
		if err := syscall.Kill(-cmd.Process.Pid, s); err == nil {
			return nil
		}
	}
	return cmd.Process.Signal(sig)
}

func killProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	_ = cmd.Process.Kill()
}

// processGroupAlive reports whether any process remains in the child's group.
// The direct child can exit while descendants that ignored the stop signal stay.
func processGroupAlive(cmd *exec.Cmd) bool {
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		return false
	}
	err := syscall.Kill(-cmd.Process.Pid, 0)
	return !errors.Is(err, syscall.ESRCH)
}
