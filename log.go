package queue

// copied from https://godoc.org/github.com/golang-migrate/migrate#Logger

// LoggerI is an interface so you can pass in your own
// logging implementation.
type LoggerI interface {
	// Printf is like fmt.Printf
	Printf(format string, v ...interface{})

	// Verbose should return true when verbose logging output is wanted
	Verbose() bool
}

// logPrintf writes to m.Log if not nil
func (q *Queue) log(format string, v ...interface{}) {
	if q.Log != nil {
		q.Log.Printf(format, v...)
	}
}

// logVerbosePrintf writes to m.Log if not nil. Use for verbose logging output.
func (q *Queue) logVerbose(format string, v ...interface{}) {
	if q.Log != nil && q.Log.Verbose() {
		q.Log.Printf(format, v...)
	}
}
