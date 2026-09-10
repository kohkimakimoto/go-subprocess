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
		// Signal the whole process group, including descendants.
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
func processGroupAlive(cmd *exec.Cmd) bool {
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		return false
	}
	err := syscall.Kill(-cmd.Process.Pid, 0)
	return !errors.Is(err, syscall.ESRCH)
}
