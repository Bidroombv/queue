package queue

// Acknowledger  interface
type Acknowledger struct {
	ack    func(tag uint64, multiple bool) error
	nack   func(tag uint64, multiple bool, requeue bool) error
	reject func(tag uint64, requeue bool) error

	wait chan bool
}

// NewAcknowledger construct a new Acknowledger
func NewAcknowledger(ack func(tag uint64, multiple bool) error, nack func(tag uint64, multiple bool, requeue bool) error, reject func(tag uint64, requeue bool) error, wait bool) *Acknowledger {
	acker := &Acknowledger{
		ack:    ack,
		nack:   nack,
		reject: reject,
	}

	if wait {
		acker.wait = make(chan bool)
	}

	return acker
}

// Wait action
func (m *Acknowledger) Wait() {
	if m.wait != nil {
		<-m.wait
	}
}

// Ack action
func (m *Acknowledger) Ack(tag uint64, multiple bool) (err error) {
	err = m.ack(tag, multiple)

	if m.wait != nil {
		m.wait <- true
	}
	return
}

// Nack action
func (m *Acknowledger) Nack(tag uint64, multiple bool, requeue bool) (err error) {
	err = m.nack(tag, multiple, requeue)

	if m.wait != nil {
		m.wait <- true
	}
	return
}

// Reject action
func (m *Acknowledger) Reject(tag uint64, requeue bool) (err error) {
	err = m.reject(tag, requeue)

	if m.wait != nil {
		m.wait <- true
	}
	return
}
