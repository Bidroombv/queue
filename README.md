# Queue

Golang package with a Queue struct for interacting with RabbitMQ

Why use it instead of a plain [github.com/streadway/amqp](https://github.com/streadway/amqp):

* logging
* reconnection logic for broken connections
* worker (publisher/consumer) pools for more concurrency
