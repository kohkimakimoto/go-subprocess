package subprocess_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kohkimakimoto/go-subprocess"
)

func ExampleRun() {
	config := &subprocess.Config{
		Command: "echo",
		Args:    []string{"Hello, World!"},
	}

	err := subprocess.Run(config)
	if err != nil {
		fmt.Printf("Command failed: %v\n", err)
	}
	// Output: Hello, World!
}

func ExampleConfig_stdoutFormatter() {
	config := &subprocess.Config{
		Command: "echo",
		Args:    []string{"test message"},
		StdoutFormatter: func(line string) string {
			return fmt.Sprintf("[STDOUT] %s", strings.ToUpper(line))
		},
	}

	subprocess.Run(config)
	// Output: [STDOUT] TEST MESSAGE
}

func ExampleProcess_timeout() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	config := &subprocess.Config{
		Command: "sleep",
		Args:    []string{"10"},
		Stdout:  io.Discard,
		Stderr:  io.Discard,
	}

	process, err := subprocess.New(config)
	if err != nil {
		fmt.Printf("Failed to create process: %v\n", err)
		return
	}
	if err := process.Start(ctx); err != nil {
		fmt.Printf("Failed to start process: %v\n", err)
		return
	}
	err = process.Wait()

	if err != nil {
		fmt.Println("Process terminated by timeout")
	} else {
		fmt.Println("Process completed")
	}
	// Output: Process terminated by timeout
}
