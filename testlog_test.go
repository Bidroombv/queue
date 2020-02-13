package queue

import (
	"log"

	"github.com/Bidroombv/utils/test"
)

type Logger struct {
	*log.Logger
}

func (l Logger) Verbose() bool {
	return true
}

var (
	testLog    = test.Log{}
	testLogger = &Logger{
		Logger: log.New(&testLog, "test ", 0),
	}
)
