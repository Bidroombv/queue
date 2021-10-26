package queue

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestClientStop(t *testing.T) {
	t.Parallel()
	rabbitC, url := runRabbitContainer(t)
	defer func() { require.NoError(t, rabbitC.Terminate(context.Background())) }()
	t.Run("Close Consumer", func(t *testing.T) {
		queueName := t.Name()
		require.NoError(t, declareQueue(t, rabbitC, queueName))
		jobChannel := make(chan amqp.Delivery)
		q, err := NewQueue(url, queueName, 3, true, false, jobChannel)
		require.NoError(t, err)

		rec := func(m amqp.Delivery) *amqp.Publishing {
			return nil
		}
		assert.NoError(t, q.AddReceiver(rec))
		time.Sleep(time.Millisecond * 50)
		assert.NoError(t, q.AddReceiver(rec))
		time.Sleep(time.Millisecond * 50)
		assert.NoError(t, q.AddReceiver(rec))
		time.Sleep(time.Millisecond * 50)

		q.Close()
		time.Sleep(time.Millisecond * 50)
	})
	t.Run("Close Publisher", func(t *testing.T) {
		queueName := t.Name()
		require.NoError(t, declareQueue(t, rabbitC, queueName))
		jobChannel := make(chan amqp.Delivery)
		q, err := NewQueue(url, queueName, 1, false, false, jobChannel)
		require.NoError(t, err)
		pub := func(d amqp.Delivery) *amqp.Publishing {
			return nil
		}
		assert.NoError(t, q.AddPublisher(context.TODO(), pub))
		time.Sleep(time.Millisecond * 50)
		assert.NoError(t, q.AddPublisher(context.TODO(), pub))
		time.Sleep(time.Millisecond * 50)
		assert.NoError(t, q.AddPublisher(context.TODO(), pub))
		time.Sleep(time.Millisecond * 50)

		q.Close()
		time.Sleep(time.Millisecond * 50)
	})
}

func TestClientSingle(t *testing.T) {
	t.Parallel()
	rabbitC, url := runRabbitContainer(t)
	defer func() { require.NoError(t, rabbitC.Terminate(context.Background())) }()

	const queueName = "queue"
	require.NoError(t, declareQueue(t, rabbitC, queueName))

	correlationId := "abc"
	received := make(chan bool) // this channel gets "released" on success

	// Consumer
	jobChannel := make(chan amqp.Delivery)
	qi, err := NewQueue(url, queueName, 1, true, false, jobChannel)
	assert.NoError(t, err)
	defer qi.Close()

	rec := func(m amqp.Delivery) *amqp.Publishing {
		assert.NoError(t, m.Ack(false))
		assert.Equal(t, correlationId, m.CorrelationId)
		received <- true
		return nil
	}
	assert.NoError(t, qi.AddReceiver(rec))

	jobChannel2 := make(chan amqp.Delivery)
	qo, err := NewQueue(url, queueName, 1, false, false, jobChannel2)
	assert.NoError(t, err)
	defer qo.Close()
	pub := func(d amqp.Delivery) *amqp.Publishing {
		return &amqp.Publishing{
			ContentType:   d.ContentType,
			CorrelationId: d.CorrelationId,
			Body:          d.Body,
			Type:          d.Type,
			Headers:       d.Headers,
		}
	}
	assert.NoError(t, qo.AddPublisher(context.TODO(), pub))

	t.Run("Send", func(t *testing.T) {
		jobChannel2 <- amqp.Delivery{
			CorrelationId: correlationId,
			Body:          []byte(`{"a": 4}`),
			Type:          "some message type",
			ContentType:   "application/json",
		}

		<-received
	})

	CheckNumMessages(t, rabbitC, queueName, 0)
}

// This test will send many messages, each with a short delay, and restart
// the rabbitmq server sometime in the middle of that.
func TestClientReconnect(t *testing.T) {
	t.Parallel()
	rabbitC, url := runRabbitContainer(t)
	defer func() { require.NoError(t, rabbitC.Terminate(context.Background())) }()

	const queueName = "queue"
	require.NoError(t, declareQueue(t, rabbitC, queueName))

	num := 5000                   // Number of messages to send
	delay := time.Millisecond * 5 // Delay between each message

	// We use these to see if the test succeeded
	var noReceived uint64
	var noSent uint64
	var noAck uint64
	var noNack uint64
	var noReject uint64

	// Consumer
	jobChannel := make(chan amqp.Delivery, num)
	qi, err := NewQueue(url, queueName, 1, true, true, jobChannel)
	assert.NoError(t, err)
	defer qi.Close()

	rec := func(m amqp.Delivery) *amqp.Publishing {
		assert.NoError(t, m.Ack(false))
		atomic.AddUint64(&noReceived, 1)
		return nil
	}
	assert.NoError(t, qi.AddReceiver(rec))

	jobChannel2 := make(chan amqp.Delivery, num)
	qo, err := NewQueue(url, queueName, 1, false, true, jobChannel2)
	assert.NoError(t, err)
	defer qo.Close()
	pub := func(d amqp.Delivery) *amqp.Publishing {
		return &amqp.Publishing{
			ContentType:   d.ContentType,
			CorrelationId: d.CorrelationId,
			Body:          d.Body,
			Headers:       d.Headers,
		}
	}
	assert.NoError(t, qo.AddPublisher(context.TODO(), pub))

	t.Run("Reconnect", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(2)

		// Container restarter
		go func() {
			defer wg.Done()
			time.Sleep(delay * time.Duration(num/2))
			out, err := exec.Command("docker", "restart", rabbitC.GetContainerID(), "-t", "60").CombinedOutput()
			t.Logf("Restarted rabbit instance container, output:\n%s", string(out))
			require.NoError(t, err)
		}()

		// Sender
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				acknowledger := NewAcknowledger(
					// ack
					func(tag uint64, multiple bool) error {
						atomic.AddUint64(&noAck, 1)
						return nil
					},
					// nack
					func(tag uint64, multiple bool, requeue bool) error {
						atomic.AddUint64(&noNack, 1)
						return nil
					},
					// reject
					func(tag uint64, requeue bool) error {
						atomic.AddUint64(&noReject, 1)
						return nil
					},
					// wait
					false,
				)

				jobChannel2 <- amqp.Delivery{
					CorrelationId: fmt.Sprintf("%d", i),
					Acknowledger:  acknowledger,
				}
				atomic.AddUint64(&noSent, 1)

				time.Sleep(delay)
			}
		}()
		wg.Wait()

		l := func(addr *uint64) int {
			return int(atomic.LoadUint64(addr))
		}

		const maxWaitTime = 5 * time.Minute
		start := time.Now()
		for {
			if l(&noReceived) == l(&noAck) && (l(&noAck)+l(&noNack)+l(&noReject) == num) {
				break
			}
			if time.Since(start) > maxWaitTime {
				break
			}
			time.Sleep(delay)
		}
		t.Logf("Waited %s for receiver to finish", time.Since(start))

		// By now we have sent `num` messages. Either one or zero of
		// them have been rejected because of the restart
		t.Logf("noReceived: %d\n", l(&noReceived))
		t.Logf("noSent: %d\n", l(&noSent))
		t.Logf("noAck: %d\n", l(&noAck))
		t.Logf("noNack: %d\n", l(&noNack))
		t.Logf("noReject: %d\n", l(&noReject))
		assert.Equal(t, l(&noAck), l(&noReceived))
		assert.Equal(t, num, l(&noAck)+l(&noNack)+l(&noReject))
		assert.LessOrEqual(t, l(&noNack), 1, "More than one message Nacked, expected 0 or 1")
	})

	CheckNumMessages(t, rabbitC, queueName, 0)

}

// Send many messages in parallel
func TestClientFast(t *testing.T) {
	t.Parallel()
	rabbitC, url := runRabbitContainer(t)
	defer func() { require.NoError(t, rabbitC.Terminate(context.Background())) }()
	const queueName = "queue"
	require.NoError(t, declareQueue(t, rabbitC, queueName))

	num := 100
	received := make(chan bool, num) // this channel gets "released" on message delivery
	// Consumer
	qi, err := NewQueue(url, queueName, 1, true, false, nil)
	require.NoError(t, err)
	defer qi.Close()

	rec := func(m amqp.Delivery) *amqp.Publishing {
		assert.NoError(t, m.Ack(false))
		received <- true
		return nil
	}
	assert.NoError(t, qi.AddReceiver(rec))

	// Publisher
	jobOutChannel := make(chan amqp.Delivery)
	qo, err := NewQueue(url, queueName, 1, false, false, jobOutChannel)
	assert.NoError(t, err)
	defer qo.Close()
	pub := func(d amqp.Delivery) *amqp.Publishing {
		return &amqp.Publishing{
			ContentType:   d.ContentType,
			CorrelationId: d.CorrelationId,
			Body:          d.Body,
			Headers:       d.Headers,
		}
	}
	assert.NoError(t, qo.AddPublisher(context.TODO(), pub))

	t.Run("Multiple", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(num)
		for i := 0; i < num; i++ {
			i := i
			go func() {
				defer wg.Done()
				jobOutChannel <- amqp.Delivery{CorrelationId: fmt.Sprintf("%d", i)}

				<-received
			}()
		}
		wg.Wait()
	})

	CheckNumMessages(t, rabbitC, queueName, 0)
}

// Send many messages in parallel with Exchange
func TestClientFastWithExchange(t *testing.T) {
	t.Parallel()
	rabbitC, url := runRabbitContainer(t)
	defer func() { require.NoError(t, rabbitC.Terminate(context.Background())) }()
	const queueName = "queue"
	const exchangeName = "exchange"
	require.NoError(t, declareQueue(t, rabbitC, queueName))
	require.NoError(t, declareExchange(t, rabbitC, exchangeName, "direct"))
	require.NoError(t, declareBinding(t, rabbitC, exchangeName, queueName))

	num := 30
	received := make(chan bool, num) // this channel gets "released" on message delivery
	// Consumer
	qi, err := NewQueue(url, queueName, 1, true, false, nil)
	assert.NoError(t, err)
	defer qi.Close()

	rec := func(m amqp.Delivery) *amqp.Publishing {
		assert.NoError(t, m.Ack(false))
		received <- true
		return nil
	}
	assert.NoError(t, qi.AddReceiver(rec))

	// Publisher
	jobOutChannel := make(chan amqp.Delivery)
	qo, err := NewExchange(url, exchangeName, 1, true, jobOutChannel)
	assert.NoError(t, err)
	defer qo.Close()
	pub := func(d amqp.Delivery) *amqp.Publishing {
		return &amqp.Publishing{
			ContentType:   d.ContentType,
			CorrelationId: d.CorrelationId,
			Body:          d.Body,
			Headers:       d.Headers,
		}
	}
	assert.NoError(t, qo.AddPublisher(context.TODO(), pub))

	t.Run("Multiple", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(num)
		for i := 0; i < num; i++ {
			i := i
			go func() {
				defer wg.Done()
				jobOutChannel <- amqp.Delivery{CorrelationId: fmt.Sprintf("%d", i)}

				<-received
			}()
		}
		wg.Wait()
	})

	CheckNumMessages(t, rabbitC, queueName, 0)
}

func assertIsHealthy(t *testing.T, client *Client, expected bool) bool {
	const maxWaitTime = time.Second * 60
	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()

	for {
		if client.IsHealthy() == expected {
			return true
		}
		if ctx.Err() != nil {
			return assert.Equal(t, expected, client.IsHealthy())
		}
		time.Sleep(time.Second)
	}
}
func TestHealthCheck(t *testing.T) {
	t.Parallel()
	rabbitC, url := runRabbitContainer(t)
	defer func() { require.NoError(t, rabbitC.Terminate(context.Background())) }()
	const queueName = "queue"
	const exchangeName = "exchange"
	require.NoError(t, declareQueue(t, rabbitC, queueName))
	require.NoError(t, declareExchange(t, rabbitC, exchangeName, "direct"))
	require.NoError(t, declareBinding(t, rabbitC, exchangeName, queueName))

	q, err := NewQueue(url, queueName, 1, true, false, nil)
	assert.NoError(t, err)
	defer q.Close()

	jobOutChannel := make(chan amqp.Delivery)
	ch, err := NewExchange(url, exchangeName, 1, true, jobOutChannel)
	assert.NoError(t, err)
	defer ch.Close()

	t.Run("should be healthy after connection setup", func(t *testing.T) {
		assertIsHealthy(t, q, true)
		assertIsHealthy(t, ch, true)
	})

	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("%d try", i), func(t *testing.T) {
			t.Run("stop the container should be unhealthy", func(t *testing.T) {
				_, err := exec.Command("docker", "stop", rabbitC.GetContainerID(), "-t", "60").CombinedOutput()
				require.NoError(t, err)

				assertIsHealthy(t, q, false)
				assertIsHealthy(t, ch, false)
			})

			t.Run("start the container", func(t *testing.T) {
				_, err := exec.Command("docker", "start", rabbitC.GetContainerID()).CombinedOutput()
				require.NoError(t, err)

				assertIsHealthy(t, q, true)
				assertIsHealthy(t, ch, true)
			})
		})
	}
}

const (
	user     = "user"
	password = "password"
	vhost    = "/"
	port     = "5672/tcp"
)

func runInContainer(t *testing.T, c testcontainers.Container, cmd string) (string, error) {
	t.Helper()
	out, err := exec.Command("docker", "exec", "-t", c.GetContainerID(), "bash", "-c", cmd).CombinedOutput()
	t.Log(string(out))
	return string(out), err
}

func runRabbitMqAdmin(t *testing.T, c testcontainers.Container, cmd string) (string, error) {
	t.Helper()
	return runInContainer(t, c, fmt.Sprintf(`rabbitmqadmin -u "%s" -p "%s" %s`, user, password, cmd))
}

func declareQueue(t *testing.T, c testcontainers.Container, name string) error {
	t.Helper()
	_, err := runRabbitMqAdmin(t, c, fmt.Sprintf(`declare queue name="%s"`, name))
	return err
}

func declareExchange(t *testing.T, c testcontainers.Container, name, ttype string) error {
	t.Helper()
	_, err := runRabbitMqAdmin(t, c, fmt.Sprintf(`declare exchange name="%s" type="%s"`, name, ttype))
	return err
}

func declareBinding(t *testing.T, c testcontainers.Container, source, destination string) error {
	t.Helper()
	_, err := runRabbitMqAdmin(t, c, fmt.Sprintf(`declare binding source="%s" destination_type="queue" destination="%s"`, source, destination))
	return err
}

func runRabbitContainer(t *testing.T) (testcontainers.Container, *URL) {
	t.Helper()

	// We want to restart container in tests.
	// If we do not provide the explicit port mapping, then docker will use a different port after the restart,
	// so we could not test the reconnection logic.
	freePort, err := freeport.GetFreePort()
	require.NoError(t, err)

	c, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:3-management",
			ExposedPorts: []string{fmt.Sprintf("%d:%s", freePort, port)},
			Env: map[string]string{
				"RABBITMQ_DEFAULT_USER":  user,
				"RABBITMQ_DEFAULT_PASS":  password,
				"RABBITMQ_DEFAULT_VHOST": vhost,
			},
			WaitingFor: wait.ForListeningPort(nat.Port(port)),
		},
	})
	require.NoError(t, err)

	host, err := c.Host(context.Background())
	if err != nil {
		_ = c.Terminate(context.Background())
		require.NoError(t, err)
	}

	mappedPort, err := c.MappedPort(context.Background(), nat.Port(port))
	if err != nil {
		_ = c.Terminate(context.Background())
		require.NoError(t, err)
	}

	url := &URL{
		HostName: host,
		Port:     mappedPort.Port(),
		UserName: user,
		Password: password,
		Vhost:    vhost,
	}

	return c, url
}

// This function checks the number of Messages on a queue. Usually, after a
// test finishes running, we want that value to be 0.
func CheckNumMessages(t *testing.T, c testcontainers.Container, queueName string, want int) {
	t.Helper()

	// amqp.QueueInspect() didn't work for me, it always returned 0
	// messages. Use rabbitmqctl, it's slow (0.6s) but bearable.
	out, err := runInContainer(t, c, "rabbitmqctl list_queues")
	require.NoError(t, err)

	// Sample output:
	//   Timeout: 60.0 seconds ...
	//   Listing queues for vhost / ...
	//   name    messages
	//   TestQueueX      210
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, queueName+"\t") {
			// sample value: "TestQueueX      210"
			vals := strings.Split(line, "\t")
			assert.Len(t, vals, 2)
			val, err := strconv.Atoi(strings.TrimSpace(vals[1]))
			assert.NoError(t, err)
			assert.Equal(t, want, val)
		}
	}
}
