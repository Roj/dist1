package main

import (
	"fmt"
	"log"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func setupWorkQueue(ch *amqp.Channel, name string) amqp.Queue {
	work_q, err := ch.QueueDeclare(name,
		true, false, false, false, nil)
	failOnError(err, fmt.Sprintf("Failed to create queue named %s", name))
	return work_q
}
func setupStreamQueue(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "stream_queue")
}
func setupDemuxSurfaceQueue(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "demux_surface")
}
func setupDemuxJoinQueue(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "demux_join")
}

func setupJoinDistributeQueue(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "join_distribute")
}
func setupDirectExchange(ch *amqp.Channel, name string) {
	err := ch.ExchangeDeclare(
		name,   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to declare %s", name))
}
func setupMinutesExchange(ch *amqp.Channel) {
	setupDirectExchange(ch, "minutesExchange")
}
func setupMinutesBindedQueue(ch *amqp.Channel, key string) amqp.Queue{
	// This one is different because it is exclusive and non durable.
	q, err := ch.QueueDeclare(
		"", false, false, true, false, nil)
	failOnError(err, "Failed to declare a queue for minutes processing")

	err = ch.QueueBind(
		q.Name, key, "minutesExchange", false, nil)
	failOnError(err, "Failed to bind queue to minutesExchange")
	return q
}

func setupHandsExchange(ch *amqp.Channel) {
	setupDirectExchange(ch, "handsExchange")
}

func setupHandsBindedQueue(ch *amqp.Channel, key string) amqp.Queue {
	q, err := ch.QueueDeclare(
		"", false, false, true, false, nil)
	failOnError(err, "Failed to declare a queue for hands processing")

	err = ch.QueueBind(
		q.Name, key, "handsExchange", false, nil)
	failOnError(err, "Failed to bind queue to handsExchange")
	return q
}
func setupHandsSink(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "hands_sink")
}

func setupMinutesSink(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "minutes_sink")
}

func setupJoinAgeFilterQueue(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "join_agefilter")
}

func setupAgeFilterLogQueue(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "agefilter_log")
}

func setupCollectorQueue(ch *amqp.Channel) amqp.Queue {
	return setupWorkQueue(ch, "collector_queue")
}

func setupRabbitSession() (*amqp.Connection, *amqp.Channel) {
	// Setup RabbitMQ session
	conn, err := amqp.Dial("amqp://guest:guest@rabbit:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return conn, ch
}