// +build rabbitmq

package queue

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akfaew/test"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var (
	rabbitUrl = "amqp://guest:guest@localhost:35672/"
)

// This function will do some resiliency testing (invalid URL), and deliver a
// single message.
func TestQueueSingle(t *testing.T) {
	correlationId := "abc"
	received := make(chan bool) // this channel gets "released" on success

	// Consumer
	jobChannel := make(chan amqp.Delivery)
	qi, err := NewQueue(rabbitUrl, t.Name(), 1, true, false, jobChannel)
	assert.NoError(t, err)
	defer qi.Close()

	rec := func(m amqp.Delivery) *amqp.Publishing {
		assert.NoError(t, m.Ack(false))
		assert.Equal(t, correlationId, m.CorrelationId)
		received <- true
		return nil
	}
	assert.NoError(t, qi.AddReceiver(rec))

	t.Run("Non-AMQP URL", func(t *testing.T) {
		_, err := NewQueue("invalid_url", t.Name(), 1, false, false, jobChannel)
		assert.EqualError(t, err, "AMQP scheme must be either 'amqp://' or 'amqps://'")
	})

	t.Run("Non-Existent URL", func(t *testing.T) {
		_, err := NewQueue("amqp://blah", t.Name(), 1, false, false, jobChannel)
		assert.Contains(t, err.Error(), "no such host")
	})

	jobChannel2 := make(chan amqp.Delivery)
	qo, err := NewQueue(rabbitUrl, t.Name(), 1, false, false, jobChannel2)
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

	t.Run("Send", func(t *testing.T) {
		jobChannel2 <- amqp.Delivery{CorrelationId: correlationId}

		<-received
	})

	CheckNumMessages(t, t.Name(), 0)
}

// This test will send many messages, each with a short delay, and restart
// the rabbitmq server sometime in the middle of that.
//
// It should finish in a bit over 10 seconds.
//
// Don't run it in parallel as it restarts the docker container that other
// tests rely on.
func TestQueueReconnect(t *testing.T) {
	num := 10000                  // Number of messages to send
	delay := time.Millisecond * 1 // Delay between each message

	// We use these to see if the test succeeded
	var noReceived uint64
	var noSent uint64
	var noAck uint64
	var noNack uint64
	var noReject uint64

	// Consumer
	jobChannel := make(chan amqp.Delivery, num)
	qi, err := NewQueue(rabbitUrl, t.Name(), 1, true, false, jobChannel)
	// qi.Log = debugLog
	assert.NoError(t, err)
	defer qi.Close()

	rec := func(m amqp.Delivery) *amqp.Publishing {
		assert.NoError(t, m.Ack(false))
		atomic.AddUint64(&noReceived, 1)
		return nil
	}
	assert.NoError(t, qi.AddReceiver(rec))

	jobChannel2 := make(chan amqp.Delivery, num)
	qo, err := NewQueue(rabbitUrl, t.Name(), 1, false, false, jobChannel2)
	// qo.Log = debugLog
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

			time.Sleep(time.Millisecond * 200)
			t.Logf("Restarting docker container")
			cmd := exec.Command("docker", "restart", "test-rabbitmq")
			assert.NoError(t, cmd.Run())
		}()

		// Sender
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				acknowledger := NewAcknowledger(
					func(tag uint64, multiple bool) error {
						atomic.AddUint64(&noAck, 1)
						return nil
					},
					func(tag uint64, multiple bool, requeue bool) error {
						atomic.AddUint64(&noNack, 1)
						return nil
					},
					func(tag uint64, requeue bool) error {
						atomic.AddUint64(&noReject, 1)
						return nil
					},
					false,
				)

				jobChannel2 <- amqp.Delivery{
					CorrelationId: fmt.Sprintf("%d", i),
					Acknowledger:  acknowledger,
				}
				atomic.AddUint64(&noSent, 1)

				// debugLog.Printf("Sent message %d\n", i)
				time.Sleep(delay)
			}
		}()
		wg.Wait()
		time.Sleep(time.Millisecond * 200) // process the last message

		// By now we have sent `num` messages. Either one or zero of
		// them have been rejected because of the restart
		t.Logf("noReceived: %d\n", noReceived)
		t.Logf("noSent: %d\n", noSent)
		t.Logf("noAck: %d\n", noAck)
		t.Logf("noNack: %d\n", noNack)
		t.Logf("noReject: %d\n", noReject)
		test.EqualInt(t, int(noReceived), int(noAck))
		test.EqualInt(t, int(noAck+noNack+noReject), num)
		if noNack > 1 {
			t.Fatalf("More than one message Nacked, expected 0 or 1")
		}
	})

	CheckNumMessages(t, t.Name(), 0)
}

// Send many messages in parallel
func TestQueueFast(t *testing.T) {
	num := 30
	received := make(chan bool, num) // this channel gets "released" on message delivery

	// Consumer
	qi, err := NewQueue(rabbitUrl, t.Name(), 1, true, false, nil)
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
	qo, err := NewQueue(rabbitUrl, t.Name(), 1, false, false, jobOutChannel)
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

	CheckNumMessages(t, t.Name(), 0)
}

// This function checks the number of Messages on a queue. Usually, after a
// test finishes running, we want that value to be 0.
func CheckNumMessages(t *testing.T, queueName string, want int) {
	t.Helper()

	// amqp.QueueInspect() didn't work for me, it always returned 0
	// messages. Use rabbitmqctl, it's slow (0.6s) but bearable.
	cmd := exec.Command("docker", "exec", "-i", "test-rabbitmq", "rabbitmqctl", "list_queues")
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err)

	// Sample output:
	//   Timeout: 60.0 seconds ...
	//   Listing queues for vhost / ...
	//   name    messages
	//   TestQueueX      210
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, queueName+"\t") {
			// sample value: "TestQueueX      210"
			vals := strings.Split(line, "\t")
			assert.Len(t, vals, 2)
			val, err := strconv.Atoi(vals[1])
			assert.NoError(t, err)
			assert.Equal(t, want, val)
		}
	}
}
