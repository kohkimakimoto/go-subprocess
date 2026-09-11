package subprocess_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kohkimakimoto/go-subprocess"
)

func ExampleStart() {
	config := subprocess.Config{
		Command: "echo",
		Args:    []string{"Hello, World!"},
	}

	p, err := subprocess.Start(context.Background(), config)
	if err != nil {
		fmt.Printf("Failed to start: %v\n", err)
		return
	}
	if err := p.Wait(); err != nil {
		fmt.Printf("Command failed: %v\n", err)
	}
	// Output: Hello, World!
}

func ExampleConfig_stdoutFormatter() {
	config := subprocess.Config{
		Command: "echo",
		Args:    []string{"test message"},
		StdoutFormatter: func(line string) string {
			return fmt.Sprintf("[STDOUT] %s", strings.ToUpper(line))
		},
	}

	p, err := subprocess.Start(context.Background(), config)
	if err != nil {
		fmt.Printf("Failed to start: %v\n", err)
		return
	}
	_ = p.Wait()
	// Output: [STDOUT] TEST MESSAGE
}

func ExampleProcess_timeout() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	config := subprocess.Config{
		Command: "sleep",
		Args:    []string{"10"},
		Stdout:  io.Discard,
		Stderr:  io.Discard,
	}

	p, err := subprocess.Start(ctx, config)
	if err != nil {
		fmt.Printf("Failed to start process: %v\n", err)
		return
	}
	err = p.Wait()

	if err != nil {
		fmt.Println("Process terminated by timeout")
	} else {
		fmt.Println("Process completed")
	}
	// Output: Process terminated by timeout
}
