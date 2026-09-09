//go:build !unix && !windows

package subprocess

import (
	"os"
	"os/exec"
)

func configureSysProcAttr(cmd *exec.Cmd) {}

func signalProcess(cmd *exec.Cmd, sig os.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Signal(sig)
}

func killProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}
