package queue

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/furdarius/rabbitroutine"
	"time"

	"github.com/streadway/amqp"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// WorkerFunc does all the work necessary on a Delivery message
type WorkerFunc func(amqp.Delivery) *amqp.Publishing

// Client is a wrapper around *amqp.Connection which allow interaction with one queue/exchange.
type Client struct {
	// LogMessagesMaxSize indicate max size in bytes of messages, which are logged.
	LogMessagesMaxSize int

	name       string
	isConsumer bool
	isExchange bool

	jobs chan amqp.Delivery

	ctx           context.Context
	cancel        context.CancelFunc
	prefetchCount int

	log *zerolog.Logger

	conn *rabbitroutine.Connector

	isHealthy atomicBool
	// Only for publisher
	pool *rabbitroutine.Pool
}

func NewQueueConsumer(url *URL, name string, prefetchCount int) (*Client, error) {
	return newClient(url, name, prefetchCount, true, false, nil)
}

func NewQueuePublisher(url *URL, name string, jobs chan amqp.Delivery) (*Client, error) {
	return newClient(url, name, 0, false, false, jobs)
}

func NewExchangePublisher(url *URL, name string, jobs chan amqp.Delivery) (*Client, error) {
	return newClient(url, name, 0, false, true, jobs)
}

func newClient(url *URL, name string, prefetchCount int, isConsumer, isExchange bool, jobs chan amqp.Delivery) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		name:       name,
		isConsumer: isConsumer,
		isExchange: isExchange,
		ctx:        ctx,
		cancel:     cancel,
		log:        setupLogger(isExchange, isConsumer, name),
		jobs:       jobs,
	}
	c.conn = rabbitroutine.NewConnector(rabbitroutine.Config{
		ReconnectAttempts: 0,
		Wait:              1 * time.Second,
	})

	c.conn.AddRetriedListener(func(r rabbitroutine.Retried) {
		c.isHealthy.SetFalse()
		c.log.Error().Str("url", url.URLStringWithoutSecrets()).Uint("attempt", r.ReconnectAttempt).Err(r.Error).Msg("retry failed")
	})

	c.conn.AddDialedListener(func(_ rabbitroutine.Dialed) {
		c.isHealthy.SetTrue()
		c.log.Info().Str("url", url.URLStringWithoutSecrets()).Msg("successfully dialed")
	})

	c.conn.AddAMQPNotifiedListener(func(n rabbitroutine.AMQPNotified) {
		c.log.Error().Interface("amqpError", n.Error).Msg("AMQP error received")
	})

	c.pool = rabbitroutine.NewPool(c.conn)

	go func() {
		err := c.conn.Dial(ctx, url.URLString())
		if err != nil {
			c.log.Error().Err(err).Send()
		}
	}()

	return c, nil
}

// Close closes everything.
func (c *Client) Close() {
	c.log.Info().Msg("closing connection")
	if c.jobs != nil {
		close(c.jobs)
	}
	c.cancel()
}

// IsHealthy return true, if connection is established and Client work properly.
func (c *Client) IsHealthy() bool {
	return c.isHealthy.Value()
}

func setupLogger(isExchange bool, isConsumer bool, name string) *zerolog.Logger {
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
	return &l
}

func logCorrelationID(log *zerolog.Logger, correlationID string) *zerolog.Logger {
	l := log.With().Str("correlation_id", correlationID).Logger()
	return &l
}

func logDelivery(m *amqp.Delivery, maxMessageSize int) func(*zerolog.Event) {
	return func(e *zerolog.Event) {
		bodySize := len(m.Body)
		e = e.Str("contentType", m.ContentType).Str("type", m.Type).Int("bodySize", len(m.Body))

		if bodySize <= maxMessageSize {
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
}

type consumer struct {
	logMessagesMaxSize int
	queueName          string
	log                *zerolog.Logger
	prefetchCount      int
	workerFunc         WorkerFunc
}

func (c *consumer) Declare(ctx context.Context, ch *amqp.Channel) error {
	return nil
}

func (c *consumer) Consume(ctx context.Context, ch *amqp.Channel) error {
	err := ch.Qos(c.prefetchCount, 0, false)
	if err != nil {
		c.log.Warn().Err(err).Msg("failed to set qos")
		return err
	}

	msgs, err := ch.Consume(
		c.queueName, // queue
		"",          // consumer queueName
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		c.log.Warn().Err(err).Msg("failed to consume")
		return err
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}

			logMessage := logCorrelationID(c.log, msg.CorrelationId)
			logMessage.Info().Func(logDelivery(&msg, c.logMessagesMaxSize)).Msg("consuming message")
			c.workerFunc(msg)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) AddReceiver(cf WorkerFunc) error {
	if !c.isConsumer {
		panic(errors.New("can not add consumer to publisher"))
	}

	go func() {
		_ = c.conn.StartConsumer(c.ctx, &consumer{
			queueName:          c.name,
			log:                c.log,
			prefetchCount:      c.prefetchCount,
			workerFunc:         cf,
			logMessagesMaxSize: c.LogMessagesMaxSize},
		)
	}()
	return nil
}

// AddPublisher adds a publisher to the worker pool
func (c *Client) AddPublisher(unusedCtx context.Context, pf WorkerFunc) error {
	if c.isConsumer {
		panic(errors.New("can not add publisher to consumer"))
	}

	ensurePub := rabbitroutine.NewEnsurePublisher(c.pool)
	pub := rabbitroutine.NewRetryPublisher(
		ensurePub,
		rabbitroutine.PublishMaxAttemptsSetup(16),
		rabbitroutine.PublishDelaySetup(rabbitroutine.LinearDelay(10*time.Millisecond)),
	)
	go func() {
		var exchange string
		var routingKey string
		if c.isExchange {
			exchange = c.name
		} else {
			routingKey = c.name
		}
		for msg := range c.jobs {
			d := pf(msg)
			logMessage := logCorrelationID(c.log, d.CorrelationId)
			logMessage.Info().Func(logDelivery(&msg, c.LogMessagesMaxSize)).Msg("publishing message")
			err := pub.Publish(c.ctx, exchange, routingKey, *d)
			if err != nil {
				c.log.Error().Err(err).Msg("publish error")
				if c.ctx.Err() != nil {
					return
				}
			}

			// We use ack normally used by customers to notify the broker. Here we use it to notify the code in an app,
			// which send the message to the c.jobs queue. Why the hell? I have not idea :(
			if msg.Acknowledger != nil {
				err = msg.Ack(false)
				if err != nil {
					logMessage.Error().Err(err).Msg("Ack failed")
				}
			}
		}
	}()

	return nil
}
