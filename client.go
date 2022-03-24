package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/pkgerrors"
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
	// ConsumerAdditionalLog can be used to extend the consumed message log
	ConsumerAdditionalLog func(e *zerolog.Event, d *amqp.Delivery)

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
	// Durable sets durability and persistence of the queue
	Durable bool

	// Jobs is the channel where messages are sent
	Jobs chan amqp.Delivery

	cancelCtx    context.CancelFunc
	prefetchSize int
	workers      []worker

	log *zerolog.Logger

	healthCheck healthCheck
}

// NewQueue connects to the queue.
func NewQueue(url *URL, name string, prefetchSize int, isConsumer, durable bool, jobs chan amqp.Delivery) (*Client, error) {
	return newClient(url, name, prefetchSize, isConsumer, false, durable, jobs)
}

// NewExchange connects to the exchange.
func NewExchange(url *URL, name string, prefetchSize int, durable bool, jobs chan amqp.Delivery) (*Client, error) {
	return newClient(url, name, prefetchSize, false, true, durable, jobs)
}

func newClient(url *URL, name string, prefetchSize int, isConsumer, isExchange, durable bool, jobs chan amqp.Delivery) (*Client, error) {
	c := &Client{
		ConsumerAdditionalLog: func(e *zerolog.Event, d *amqp.Delivery) {},
		name:                  name,
		url:                   url.urlString(),
		isConsumer:            isConsumer,
		isExchange:            isExchange,
		Durable:               durable,
		Jobs:                  jobs,
		log:                   setupLogger(*url, isExchange, isConsumer, name),
		prefetchSize:          prefetchSize,
		workers:               make([]worker, 0),
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.TODO())

	c.cancelCtx = cancel

	go c.reconnector(ctx)

	return c, nil
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
	conn, err := amqp.Dial(c.url)
	for err != nil {
		// In addition to the errors returned by ParseURI we may
		// attempt to connect to a non-existent host
		c.log.Error().Err(err).Msg("failed to dial")
		time.Sleep(500 * time.Millisecond)
		conn, err = amqp.Dial(c.url)
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
			if amqpError != nil {
				c.healthCheck.SetUnhealthy()
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
			func() {
				logMessage := logCorrelationID(log, m.CorrelationId)
				logMessage.Info().Func(c.logDelivery(&m)).Msg("consuming job")

				defer func() {
					if r := recover(); r != nil {
						stack := pkgerrors.MarshalStack(errors.New(""))
						logMessage.Error().Func(c.logDelivery(&m)).Interface(zerolog.ErrorStackFieldName, stack).Err(fmt.Errorf("%v", r)).Msg("panic, moving message to dlq")
						if err := m.Reject(false); err != nil {
							logMessage.Error().Err(err).Msg("cannot reject message after panic")
						}
					}
				}()

				w.work(m)
			}()
		}
	}()

	return nil
}

func logCorrelationID(log *zerolog.Logger, correlationID string) *zerolog.Logger {
	l := log.With().Str("correlation_id", correlationID).Logger()
	return &l
}

func logDeath(e *zerolog.Event, m *amqp.Delivery) {
	death, ok := m.Headers["x-death"]
	if !ok {
		return
	}

	items, ok := death.([]interface{})
	if !ok {
		return
	}

	if len(items) != 1 {
		return
	}
	t, ok := items[0].(amqp.Table)
	if !ok {
		return
	}

	count, ok := t["count"]
	if !ok {
		return
	}

	time, ok := t["time"]
	if !ok {
		return
	}

	e.Dict("death", zerolog.Dict().Interface("count", count).Interface("time", time))
}

func (c *Client) logDelivery(m *amqp.Delivery) func(*zerolog.Event) {
	return func(e *zerolog.Event) {
		if c.isConsumer {
			c.ConsumerAdditionalLog(e, m)
		}

		e = e.Str("contentType", m.ContentType).Str("type", m.Type).Int("bodySize", len(m.Body))
		if m.Redelivered {
			e.Bool("redelivered", true)
		}
		logDeath(e, m)

		if m.ContentType == "application/json" {
			var asJSON interface{}
			err := json.Unmarshal(m.Body, &asJSON)
			if err == nil {
				e.Interface("body", asJSON)
				return
			}
		}

		e.Interface("body", m.Body)
	}
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
			logMessage.Info().Func(c.logDelivery(m)).Msg("publishing message")

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

			logMessage.Info().Msg("message published")
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
		_, err = c.channel.QueueDeclarePassive(c.name, c.Durable, false, false, false, nil)
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

// GetReadyMessagesCount checks number of messages in ready state in queue
func (c *Client) GetReadyMessagesCount() (msgCount int, err error) {
	ch, err := c.getChannel()
	if err != nil {
		log.Error().Err(err).Msg("c.getChannel()")
		return 0, err
	}
	defer ch.Close()

	q, err := ch.QueueInspect(c.name)
	if err != nil {
		return
	}
	msgCount = q.Messages

	return
}

// GetReadyMessages fetches at most given number of messages from queue
// Note : close() should be called once all amqp.Delivery are processed
func (c *Client) GetReadyMessages(n int) (m []amqp.Delivery, close func() error, err error) {
	ch, err := c.getChannel()
	if err != nil {
		log.Error().Err(err).Msg("c.getChannel()")
		return nil, ch.Close, err
	}
	close = ch.Close

	for i := 0; i < n; i++ {
		d, ok, err := ch.Get(c.name, false)
		if err != nil {
			break
		}
		if ok {
			m = append(m, d)
		}
	}

	return
}
