package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Netflix/go-env"
	"github.com/streadway/amqp"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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

type URL struct {
	HostName string `env:"RABBITMQ_HOSTNAME,required=true"`
	Port     string `env:"RABBITMQ_PORT,required=true"`
	UserName string `env:"RABBITMQ_USERNAME,required=true"`
	Password string `env:"RABBITMQ_PASSWORD,required=true"`
	Vhost    string `env:"RABBITMQ_VHOST,required=true"`
}

func (url *URL) urlString() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", url.UserName, url.Password, url.HostName, url.Port, url.Vhost)
}

// setupChannel sets up a *amqp.Channel for a worker{}. It closes a
// previously opened channel, if any.
func (w *worker) setupChannel(q *Client) error {
	if w.channel != nil {
		w.channel.Close() // It is safe to call this method multiple times.
	}

	ch, err := q.getChannel()
	if err != nil {
		return fmt.Errorf("failed to get channel for publisher on %s. Error: %s", q.name, err)
	}
	w.channel = ch

	// this channel is going to be closed when the Queue Client is closed
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

// Client is a wrapper around *amqp.Connection which allow interaction with one queue/exchange.
type Client struct {
	// Name of the queue/exchange
	name string
	// complete URL of the queue (i.e "amqp://guest:guest@localhost:5672/")
	url string
	// Connection to the server
	connection *amqp.Connection

	channel *amqp.Channel

	// ConnectionErr receives errors from the Connection in case of disconnection
	connectionErr chan *amqp.Error
	// consumer specifies if this queue is for consuming messages or for publishing
	isConsumer bool
	// isExchange specifies if this is queue or an exchange
	isExchange bool

	// Jobs is the channel where messages are sent
	Jobs chan amqp.Delivery

	cancelCtx    context.CancelFunc
	prefetchSize int
	workers      []worker

	dial func(string) (*amqp.Connection, error)

	log *zerolog.Logger

	healthCheck healthCheck
}

type Builder struct {
	name         string
	url          *URL
	isConsumer   bool
	isExchange   bool
	jobs         chan amqp.Delivery
	prefetchSize int
	dial         func(string) (*amqp.Connection, error)
}

func (b *Builder) Name(name string) *Builder {
	b.name = name
	return b
}

func (b *Builder) URL(url *URL) *Builder {
	b.url = url
	return b
}

func (b *Builder) Consumer(prefetchSize int) *Builder {
	b.isConsumer = true
	b.prefetchSize = prefetchSize
	return b
}

func (b *Builder) Producer() *Builder {
	b.isConsumer = false
	return b
}

func (b *Builder) Jobs(jobs chan amqp.Delivery) *Builder {
	b.jobs = jobs
	return b
}

// WithCustomDial is useful in testing when you want to mock *amqp.Connection.
func (b *Builder) WithCustomDial(dial func(string) (*amqp.Connection, error)) *Builder {
	b.dial = dial
	return b
}

func (b *Builder) Connect() (*Client, error) {
	if b.name == "" {
		return nil, errors.New("queue/exchange name is required")
	}
	if b.url == nil {
		return nil, errors.New("url is required")
	}
	if b.dial == nil {
		b.dial = amqp.Dial
	}
	c := &Client{
		name:         b.name,
		url:          b.url.urlString(),
		isConsumer:   b.isConsumer,
		isExchange:   b.isExchange,
		Jobs:         b.jobs,
		log:          setupLogger(*b.url, b.isExchange, b.isConsumer, b.name),
		prefetchSize: b.prefetchSize,
		workers:      make([]worker, 0),
		dial:         b.dial,
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.TODO())

	c.cancelCtx = cancel

	go c.reconnector(ctx)

	return c, nil
}

func NewQueue() *Builder {
	return &Builder{isExchange: false}
}

func NewExchange() *Builder {
	return &Builder{isExchange: true}
}

func setupLogger(url URL, isExchange bool, isConsumer bool, name string) *zerolog.Logger {
	var kind string
	if isExchange {
		kind = "exchange"
	} else {
		kind = "queue"
	}

	var mode string
	if isConsumer {
		mode = "consumer"
	} else {
		mode = "publisher"
	}

	l := log.Logger.With().Str(kind, name).Str("mode", mode).Logger()

	url.UserName = "***"
	url.Password = "***"
	l.Info().Str("url", url.urlString()).Msg("amqp url")

	return &l
}

// Close closes everything.
func (c *Client) Close() {
	c.log.Info().Msg("closing connection")
	c.cancelCtx()

	for _, w := range c.workers {
		w.stop()
	}
	c.workers = nil
	c.channel.Close()
	c.connection.Close()
}

// IsHealthy return true, if Client works as expected.
func (c *Client) IsHealthy() bool {
	return c.healthCheck.IsHealthy()
}

func (c *Client) connect() error {
	c.log.Info().Msg("connecting")

	// We want to retry amqp.Dial if it fails, but only if it's reasonable
	// to do so.
	// - It's reasonable if the connection was refused (RabbitMQ has not
	//   yet started when our app is starting), or if the connection is
	//   reset (network problem, RabbitMQ restart).
	// - It's unreasonable if the url is invalid (ParseURI() fails).
	// We have no easy way to differentiate between these two error
	// conditions, so as a workaround let's see if ParseURI() succeeds here.
	if _, err := amqp.ParseURI(c.url); err != nil {
		return err
	}

	var conn *amqp.Connection
	conn, err := c.dial(c.url)
	for err != nil {
		// In addition to the errors returned by ParseURI we may
		// attempt to connect to a non-existent host
		c.log.Error().Err(err).Msg("failed to dial")
		if strings.Contains(err.Error(), ": no such host") {
			return err
		}

		time.Sleep(500 * time.Millisecond)
		conn, err = c.dial(c.url)
	}

	c.connection = conn

	// this channel will be closed when Queue Client is closed
	c.connectionErr = c.connection.NotifyClose(make(chan *amqp.Error))

	c.log.Info().Msg("connection established")

	ch, err := c.getChannel()
	if err != nil {
		return err
	}

	c.channel = ch

	if c.isConsumer {
		if err := c.setConsumerQoS(c.prefetchSize, true); err != nil {
			c.log.Error().Err(err).Msg("setConsumerQoS()")
			// XXX error not really handled
		}
	}

	err = c.setupConnection()
	if err == nil {
		c.healthCheck.SetHealthy()
	}
	return err
}

func (c *Client) reconnector(ctx context.Context) {
	defer c.log.Debug().Msg("exiting reconnection goroutine")

	for {
		select {
		case <-ctx.Done():
			return
		case amqpError := <-c.connectionErr:
			c.healthCheck.SetUnhealthy()
			if amqpError != nil {
				c.log.Warn().Err(amqpError).Msg("connection closed, reconnecting")
				if err := c.connect(); err != nil {
					c.log.Error().Err(err).Msg("reconnect failed")
				}
				c.reconnectWorkers(ctx)
			}
		}
	}

}

func (c *Client) contestualizeReconnector(ctx context.Context) {
	c.log.Debug().Msg("recontestualizing reconnector")
	c.cancelCtx()
	ctx, cancel := context.WithCancel(ctx)
	c.cancelCtx = cancel
	go c.reconnector(ctx)
}

func logWorkerID(log *zerolog.Logger, id uint64) *zerolog.Logger {
	l := log.With().Uint64("worker", id).Logger()
	return &l
}

func (c *Client) reconnectWorkers(ctx context.Context) {
	for i := range c.workers {
		var worker = c.workers[i]
		logWorker := logWorkerID(c.log, c.workers[i].id)
		logWorker.Info().Msg("recovering worker")
		if c.isConsumer {
			if err := c.receiver(&worker, logWorker); err != nil {
				logWorker.Error().Err(err).Msg("c.receiver(&worker)")
			}
		} else {
			go c.sender(ctx, &worker, logWorker)
		}
	}
}

func (c *Client) receiver(w *worker, log *zerolog.Logger) error {
	ch, err := c.getChannel()
	if err != nil {
		log.Error().Err(err).Msg("c.getChannel()")
		return err
	}

	err = ch.Qos(
		c.prefetchSize, // prefetch count
		0,              // prefetch size -- UNUSED
		true,           // global
	)

	if err != nil {
		log.Error().Err(err).Msg("ch.Qox()")
		// this is non blocking error
	}
	w.channel = ch

	msgs, err := w.channel.Consume(
		c.name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Error().Err(err).Msg("register error")
		return err
	}

	// listen on the Delivery channel and distribute jobs to workers
	go func() {
		for m := range msgs {
			logMessage := logCorrelationID(log, m.CorrelationId)
			logMessage.Info().Msg("started job")
			w.work(m)
			logMessage.Info().Msg("finished job")
		}
	}()

	return nil
}

func logCorrelationID(log *zerolog.Logger, correlationID string) *zerolog.Logger {
	l := log.With().Str("correlation_id", correlationID).Logger()
	return &l
}

// sender listens on the Delivery RabbitMQ channel and fetches jobs for the
// given worker{}.
func (c *Client) sender(ctx context.Context, w *worker, log *zerolog.Logger) {
	log.Info().Msg("starting publisher")
	started := false

MAIN:
	for {
		if started {
			log.Info().Msg("restarting publisher")
		}
		started = true

		if err := w.setupChannel(c); err != nil {
			time.Sleep(time.Second) // avoid a quick succession of reconnects
			continue
		}

		// exit gracefully if this loop breaks
		for {
			m := c.receiveJob(ctx)
			if m == nil {
				break // graceful exit
			}

			logMessage := logCorrelationID(log, m.CorrelationId)
			logMessage.Info().Msg("started job")

			publishing := w.work(*m)

			// There are multiple possible failure scenarios that
			// result in either message Duplication or Loss.
			if err := c.publish(publishing, w.channel); err != nil {
				// Destination RabbitMQ didn't accept our message
				logMessage.Error().Err(err).Msg("publish fail")
				if err := m.Nack(false, false); err != nil {
					// Source RabbitMQ didn't receive our Nack
					//
					// If it crashed the message will be lost
					// If it's lagging the message will be retried
					logMessage.Error().Err(err).Msg("sender Nack()")
				}

				// Reconnect
				continue MAIN
			}

			confirmed := c.checkConfirmation(*m, w)
			if !confirmed {
				// Destination RabbitMQ didn't confirm our
				// message, but it accepted it earlier.
				logMessage.Error().Msg("check confirmation")

				// XXX reconnect, but only if no Ack received (as opposed to negative ack)?
			}

			if err := c.ackNack(*m, confirmed); err != nil {
				// Source RabbitMQ is unreachable
				//
				// If confirmed == false:
				//   If Source RabbitMQ crashed the message will be dropped
				//   If it's just lagging the message will be re-delivered
				// If confirmed == true:
				//   If Source RabbitMQ crashed it's OK
				//   If it's just lagging the message will be duplicated
				if err.Error() != "delivery not initialized" {
					logMessage.Error().Err(err).Msg("Ack/Nack")
				}
			}

			logMessage.Info().Msg("finished job")
		}

		break // graceful exit
	}

	log.Info().Msg("stopping publisher")
	w.channel.Close()
}

// https://www.rabbitmq.com/confirms.html#publisher-confirms:
//     basic.nack will only be delivered if an internal error occurs in the
//     Erlang process responsible for a queue.
//
// We're observing this when the server is restarted
//
// XXX what does the amqp library do when the server disappears without a trace?
func (c *Client) checkConfirmation(m amqp.Delivery, w *worker) bool {
	confirmed := <-w.confirms
	return confirmed.Ack
}

func (c *Client) ackNack(m amqp.Delivery, confirmed bool) error {
	if confirmed {
		if err := m.Ack(false); err != nil {
			return err
		}
	} else {
		if err := m.Nack(false, false); err != nil {
			return err
		}
	}

	return nil
}

// AddReceiver start consuming messages on channel ch from the queue and
// posts deliveries to the WorkerFunc
func (c *Client) AddReceiver(cf WorkerFunc) error {
	if !c.isConsumer {
		panic("Adding a Consumer on a publishing Queue")
	}

	newConsumerWid := atomic.AddUint64(&consumerWid, 1)

	consumer := &worker{id: newConsumerWid, work: cf}

	logWorker := logWorkerID(c.log, consumer.id)
	logWorker.Info().Msg("starting consumer")
	if err := c.receiver(consumer, logWorker); err != nil {
		return err
	}

	// add consumer to the list
	c.workers = append(c.workers, *consumer)

	return nil
}

// AddPublisher adds a publisher to the worker pool
func (c *Client) AddPublisher(ctx context.Context, pf WorkerFunc) error {
	if c.isConsumer {
		panic("Adding a Publisher on a consumer Queue")
	}

	newPublisherWid := atomic.AddUint64(&publisherWid, 1)

	publisher := &worker{id: newPublisherWid, work: pf}

	if len(c.workers) == 0 {
		c.contestualizeReconnector(ctx)
	}

	c.workers = append(c.workers, *publisher)

	logWorker := logWorkerID(c.log, publisher.id)
	go c.sender(ctx, publisher, logWorker)

	return nil
}

// getChannel gets a channel from the Queue
func (c *Client) getChannel() (ch *amqp.Channel, err error) {
	return c.connection.Channel()
}

// setupConnection declares a Queue or Exchange connection
func (c *Client) setupConnection() error {
	if c.name == "" {
		err := errors.New("no queue or exchange name provided")
		log.Error().Err(err).Send()
		return err
	}

	var err error
	if c.isExchange {
		err = c.channel.ExchangeDeclarePassive(c.name, "fanout", true, false, false, false, nil)
	} else {
		_, err = c.channel.QueueDeclarePassive(c.name, false, false, false, false, nil)
	}

	if err != nil {
		c.log.Error().Err(err).Msg("declare passive")
	}

	return err
}

// setConsumerQoS sets QoS on a channel with a prefetch value
func (c *Client) setConsumerQoS(prefetch int, global bool) error {
	if c.channel != nil && c.name != "" {
		return c.channel.Qos(
			prefetch, // prefetch count
			0,        // prefetch size
			global,   // global
		)
	}
	return nil
}

// publish sends a message on a channel if provided, otherwise get a new channel from the queue connection
func (c *Client) publish(message *amqp.Publishing, ch *amqp.Channel) error {
	if c.isConsumer {
		return errors.New("publishing on a consumer queue")
	}

	if ch == nil {
		var err error
		ch, err = c.getChannel()
		if err != nil {
			return err
		}
	}

	// If exchange is used to publish message
	exchangeName := ""
	queueName := c.name
	if c.isExchange {
		exchangeName = c.name
		// Since we are using fanout exchange, routing key can be ignored
		queueName = ""
	}

	// Publish the response
	puberr := ch.Publish(
		exchangeName, // exchange
		queueName,    // routing key
		false,        // mandatory
		false,        // immediate
		*message)

	return puberr
}

// receiveJob returns a job from the .Jobs queue, blocking if necessary. If
// execution is canceled in any way it returns nil.
func (c *Client) receiveJob(ctx context.Context) *amqp.Delivery {
	select {
	case <-ctx.Done():
		return nil
	case <-c.connectionErr:
		return nil
	case job, ok := <-c.Jobs:
		if !ok { // c.Jobs was closed
			return nil
		}
		return &job
	}
}

func ReadCfgFromEnv() (*URL, error) {
	url := &URL{}
	if _, err := env.UnmarshalFromEnviron(url); err != nil {
		return nil, err
	}

	return url, nil
}
