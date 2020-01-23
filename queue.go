package queue

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

var (
	consumerWid  uint64 = 0
	publisherWid uint64 = 0
)

// WorkerFunc does all the work necessary on a Delivery message
type WorkerFunc func(amqp.Delivery) *amqp.Publishing

// worker defines consumer workers on Delivery messages
type worker struct {
	id       uint64
	channel  *amqp.Channel
	work     WorkerFunc
	confirms chan amqp.Confirmation
}

// setupChannel sets up a RabbitMQ Channel for a worker{}. It closes a
// previously opened channel, if any.
func (w *worker) setupChannel(q *Queue) error {
	if w.channel != nil {
		w.channel.Close() // It is safe to call this method multiple times.
	}

	if ch, err := q.getChannel(); err != nil {
		q.log("Failed to get channel for publisher on %s. Error: %s\n", q.name, err)
		return err
	} else {
		w.channel = ch
	}

	// this channel is going to be closed when the Queue Channel is closed
	confirms := make(chan amqp.Confirmation, 1)

	w.confirms = w.channel.NotifyPublish(confirms)
	if err := w.channel.Confirm(false); err != nil {
		return err
	}

	return nil
}

func (w *worker) stop() {
	if w.confirms != nil {
		close(w.confirms)
	}
}

// Queue represent a AMQP queue
type Queue struct {
	// Name of the queue
	name string
	// complete URL of the queue (i.e "amqp://guest:guest@localhost:5672/")
	url string
	// Connection to the server
	connection *amqp.Connection

	channel *amqp.Channel

	// ConnectionErr receives errors from the Connection in case of disconnection
	connectionErr chan *amqp.Error
	// closed signal a purposeful close of the queue
	closed bool
	// consumer specifies if this queue is for consuming messages or for publishing
	isConsumer bool
	// whether or not the AMQP queue is durable
	durable bool

	// Jobs is the channel where messages are sent
	Jobs chan amqp.Delivery
	Log  LoggerI

	cancelCtx    context.CancelFunc
	prefetchSize int
	workers      []worker
}

// NewQueue creates and returns a new Queue structure
func NewQueue(url string, name string, prefetchSize int, isConsumer, durable bool, jobs chan amqp.Delivery) (*Queue, error) {
	q := &Queue{
		name: name,
		url:  url,

		isConsumer:   isConsumer,
		durable:      durable,
		Jobs:         jobs,
		prefetchSize: prefetchSize,
		workers:      make([]worker, 0),
	}

	if err := q.connect(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.TODO())

	q.cancelCtx = cancel

	go q.reconnector(ctx)

	return q, nil
}

// Close closes queue channels and connections
func (q *Queue) Close() {
	q.logVerbose("Closing connection to %s", q.name)
	q.closed = true

	for i := range q.workers {
		var w = q.workers[i]
		w.stop()
	}
	q.workers = nil
	q.channel.Close()
	q.connection.Close()
}

func (q *Queue) connect() error {
	q.logVerbose("Connecting to AMQP Server on %s for queue: %s\n", q.url, q.name)

	// We want to retry amqp.Dial if it fails, but only if it's reasonable
	// to do so.
	// - It's reasonable if the connection was refused (RabbitMQ has not
	//   yet started when our app is starting), or if the connection is
	//   reset (network problem, RabbitMQ restart).
	// - It's unreasonable if the url is invalid (ParseURI() fails).
	// We have no easy way to differentiate between these two error
	// conditions, so as a workaround let's see if ParseURI() succeeds here.
	if _, err := amqp.ParseURI(q.url); err != nil {
		return err
	}

	var conn *amqp.Connection
	conn, err := amqp.Dial(q.url)
	for err != nil {
		// In addition to the errors returned by ParseURI we may
		// attempt to connect to a non-existent host
		if strings.Contains(err.Error(), ": no such host") {
			return err
		}
		q.log("Failed to dial to AMQP Server on %s for queue: %s. Error: %s\n", q.url, q.name, err)
		time.Sleep(500 * time.Millisecond)
		conn, err = amqp.Dial(q.url)
	}

	q.connection = conn

	// this channel will be closed when Queue Channel is closed
	q.connectionErr = q.connection.NotifyClose(make(chan *amqp.Error))

	q.logVerbose("Connection established on queue: %s", q.name)

	if ch, err := q.getChannel(); err != nil {
		return err
	} else {
		q.channel = ch
	}

	if q.isConsumer {
		if err := q.setConsumerQoS(q.prefetchSize, true); err != nil {
			q.log("setConsumerQoS() error: %s", err)
			// XXX error not really handled
		}
	}

	return q.setupQueue()
}

func (q *Queue) reconnector(ctx context.Context) {
	defer q.logVerbose("Exiting Reconnection goroutine")

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-q.connectionErr:
			if !q.closed && err != nil {
				q.log("Connection on queue %s closed with error %s. Reconnecting.", q.name, err)
				q.connect()
				q.reconnectWorkers(ctx)
			}
		}
	}

}

func (q *Queue) contestualizeReconnector(ctx context.Context) {
	q.logVerbose("Recontestualizing Connector------------------")
	q.cancelCtx()
	ctx, cancel := context.WithCancel(ctx)
	q.cancelCtx = cancel

	go q.reconnector(ctx)
}

func (q *Queue) reconnectWorkers(ctx context.Context) {
	for i := range q.workers {
		var worker = q.workers[i]
		q.logVerbose("Recovering worker: %d on queue: %s\n", worker.id, q.name)
		if q.isConsumer {
			q.receiver(&worker)
		} else {
			go q.sender(ctx, &worker)
		}
	}
}

func (q *Queue) receiver(w *worker) error {
	if ch, err := q.getChannel(); err != nil {
		q.log("Failed to get channel for consumer on %s. Error: %s", q.name, err)
		return err
	} else {
		w.channel = ch
	}

	msgs, err := w.channel.Consume(
		q.name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		q.log("Failed to register consumer on %s. Error: %s", q.name, err)
		return err
	}

	// listen on the Delivery channel and distribute jobs to workers
	go func() {
		q.logVerbose("START Listening on queue: %s", q.name)

		for m := range msgs {
			w.work(m)
		}
		q.logVerbose("STOP Listening on queue: %s", q.name)
	}()

	return nil
}

// sender listens on the Delivery RabbitMQ channel and fetches jobs for the
// given worker{}.
func (q *Queue) sender(ctx context.Context, w *worker) {
	q.logVerbose("Starting Publisher worker with id: %d", w.id)
	started := false

MAIN:
	for {
		if started {
			q.logVerbose("Restarting Publisher worker with id: %d", w.id)
		}
		started = true

		if err := w.setupChannel(q); err != nil {
			time.Sleep(time.Second) // avoid a quick succession of reconnects
			continue
		}

		// exit gracefully if this loop breaks
		for {
			m := q.receiveJob(ctx)
			if m == nil {
				break // graceful exit
			}

			q.logVerbose("Worker %d started job on correlationId: %s\n", w.id, m.CorrelationId)

			publishing := w.work(*m)

			if err := q.publish(publishing, w.channel); err != nil {
				q.log("Failed to publish message with CorrelationId %s. Error: %s",
					m.CorrelationId, err)
				m.Nack(false, false)

				continue MAIN
			}

			q.checkConfirmation(*m, w)

			q.logVerbose("Worker %d finished job with CorrelationId: %s\n", w.id, m.CorrelationId)
		}

		break // graceful exit
	}

	q.logVerbose("Stopping Publisher worker with id: %d", w.id)
	w.channel.Close()
}

func (q *Queue) checkConfirmation(m amqp.Delivery, w *worker) {
	if confirmed := <-w.confirms; confirmed.Ack {
		m.Ack(false)
		q.logVerbose("Message CONFIRMED on publishing: %+v\n", m)
	} else {
		m.Nack(false, false)
		q.log("Failed to CONFIRM publishing: %+v\n", m)
	}
}

// AddReceiver start consuming messages on channel ch from the queue and
// posts deliveries to the WorkerFunc
func (q *Queue) AddReceiver(cf WorkerFunc) error {
	if !q.isConsumer {
		return errors.New("Adding a Consumer on a publishing Queue")
	}

	newConsumerWid := atomic.AddUint64(&consumerWid, 1)

	consumer := &worker{id: newConsumerWid, work: cf}

	// add consumer to the list
	q.workers = append(q.workers, *consumer)

	if err := q.receiver(consumer); err != nil {
		q.workers = q.workers[:len(q.workers)-1]
		return err
	}

	q.logVerbose("Starting Consumer worker with id: %d", newConsumerWid)

	return nil
}

// AddPublisher adds a publisher to the worker pool
func (q *Queue) AddPublisher(ctx context.Context, pf WorkerFunc) error {
	if q.isConsumer {
		return errors.New("Adding a Publisher on a consumer Queue")
	}

	newPublisherWid := atomic.AddUint64(&publisherWid, 1)

	publisher := &worker{id: newPublisherWid, work: pf}

	if len(q.workers) == 0 {
		q.contestualizeReconnector(ctx)
	}
	// add publisher to the list
	q.workers = append(q.workers, *publisher)

	go q.sender(ctx, publisher)

	return nil
}

// getChannel gets a channel from the Queue
func (q *Queue) getChannel() (ch *amqp.Channel, err error) {
	return q.connection.Channel()
}

// SetupQueue declares a Queue named queueName
func (q *Queue) setupQueue() error {
	dead := "deadletter"
	if err := q.channel.ExchangeDeclare(dead, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := q.channel.QueueDeclare(dead, true, false, false, false, nil); err != nil {
		return err
	}
	if err := q.channel.QueueBind(dead, "*", dead, false, nil); err != nil {
		return err
	}

	if q.channel != nil && q.name != "" {
		// TODO make queue durability configurable
		if _, err := q.channel.QueueDeclare(
			q.name,    // name
			q.durable, // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // no-wait
			amqp.Table{"x-dead-letter-exchange": dead}, // arguments
		); err != nil {
			return err
		}
	}
	return nil
}

// setConsumerQoS sets QoS on a channel with a prefetch value
func (q *Queue) setConsumerQoS(prefetch int, global bool) error {
	if q.channel != nil && q.name != "" {
		return q.channel.Qos(
			prefetch, // prefetch count
			0,        // prefetch size
			global,   // global
		)
	}
	return nil
}

// publish sends a message on a channel if provided, otherwise get a new channel from the queue connection
func (q *Queue) publish(message *amqp.Publishing, ch *amqp.Channel) error {
	if q.isConsumer {
		return errors.New("Publishing on a consumer Queue")
	}

	if ch == nil {
		var err error
		ch, err = q.getChannel()
		if err != nil {
			return err
		}
	}
	// Publish the response
	puberr := ch.Publish(
		"",     // exchange
		q.name, // routing key
		false,  // mandatory
		false,  // immediate
		*message)

	return puberr
}

// receiveJob returns a job from the .Jobs queue, blocking if necessary. If
// execution is canceled in any way it returns nil.
func (q *Queue) receiveJob(ctx context.Context) *amqp.Delivery {
	select {
	case <-ctx.Done():
		return nil
	case <-q.connectionErr:
		return nil
	case job, ok := <-q.Jobs:
		if !ok { // q.Jobs was closed
			return nil
		}
		return &job
	}
}
