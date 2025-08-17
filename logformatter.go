package subprocess

import (
	"fmt"
	"time"
)

// Built-in formatter functions

// PrefixFormatter creates a formatter that adds a prefix to each line
func PrefixFormatter(prefix string) LogFormatter {
	return func(line string) string {
		return prefix + line
	}
}

// TimestampFormatter adds a timestamp to each line
func TimestampFormatter(format string) LogFormatter {
	return func(line string) string {
		return fmt.Sprintf("[%s] %s", time.Now().Format(format), line)
	}
}

// ChainFormatters combines multiple formatters
func ChainFormatters(formatters ...LogFormatter) LogFormatter {
	return func(line string) string {
		for _, formatter := range formatters {
			if formatter != nil {
				line = formatter(line)
			}
		}
		return line
	}
}
