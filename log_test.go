// +build debug

package queue

import (
	"log"
	"os"
)

// Use to debug tests
type Logger struct {
	*log.Logger
}

func (l Logger) Verbose() bool {
	return true
}

var (
	debugLog = &Logger{Logger: log.New(os.Stderr, "test ", log.Lmicroseconds|log.LUTC)}
)
