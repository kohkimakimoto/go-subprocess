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
	// Simple execution using Run function
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
	// Using a custom formatter for stdout
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
	// Create a context with 1 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Run a long-running command that will be cancelled by timeout
	config := &subprocess.Config{
		Command: "sleep",
		Args:    []string{"10"}, // Sleep for 10 seconds
		Stdout:  io.Discard,     // Suppress output for testing
		Stderr:  io.Discard,     // Suppress output for testing
	}

	process := subprocess.New(config)
	process.Start(ctx)
	err := process.Wait()

	if err != nil {
		fmt.Println("Process terminated by timeout")
	} else {
		fmt.Println("Process completed")
	}
	// Output: Process terminated by timeout
}
