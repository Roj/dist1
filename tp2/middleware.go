package main
import (
	"errors"
	"github.com/streadway/amqp"
	"fmt"
	"log"
)
const (
	Exchange = iota
	BindedQueue
	Queue
	Empty
)
type rabbitQueue struct {
	Type int
	Name string // in case of BindedQueue , it is the exchange name.
	Key string
	AmqpQueue amqp.Queue
	Channel *amqp.Channel
}
type Message struct {
	Content string
	Key string
}
type NodeData interface{}
type Process func(msg1 string, xtra NodeData) (res1 Message, res2 Message)
type Filter func(msg1 string) bool
var zeroQ = amqp.Queue{}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func (q rabbitQueue) Send(msg Message) error {
	if q.Type == Empty || msg.Content == "" {
		return nil
	}
	if q.Type == Exchange {
		return q.Channel.Publish(
			q.Name, // exchange
			msg.Key, // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
			  ContentType: "text/plain",
			  Body:        []byte(msg.Content),
		})
	}
	if q.Type == Queue {
		return q.Channel.Publish(
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body: []byte(msg.Content),
			})
	}
	return errors.New("Can not send a message to this type of queue")
}
func (q rabbitQueue) SetUp() rabbitQueue {
	if q.Type == Empty {
		return q
	}
	if q.Type == Queue {
		work_q, err := q.Channel.QueueDeclare(q.Name,
			true, false, false, false, nil)
		failOnError(err, fmt.Sprintf("Failed to create queue named %s", q.Name))
		q.AmqpQueue = work_q
		return q
	}
	// Only BindedQueue and Exchange remain. Both need to
	// declare an Exchange.
	err := q.Channel.ExchangeDeclare(
		q.Name,   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to declare %s", q.Name))
	if q.Type == Exchange {
		return q
	}

	// This one is different because it is exclusive and non durable.
	work_q, err := q.Channel.QueueDeclare(
		"", false, false, true, false, nil)
	failOnError(err, "Failed to declare a queue")

	err = q.Channel.QueueBind(
		work_q.Name, q.Key, q.Name, false, nil)
	failOnError(err, "Failed to bind queue to exchange")
	q.AmqpQueue = work_q
	return q
}
func setupNode(src rabbitQueue, dst1 rabbitQueue, dst2 rabbitQueue, nodeFn Process, extra NodeData) {
	src = src.SetUp()
	dst1 = dst1.SetUp()
	dst2 = dst2.SetUp()

	msgs, err := src.Channel.Consume(
		src.AmqpQueue.Name,
		"", false, false, false, false, nil)
	failOnError(err, "[node] Could not consume")
	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[%s] processing message %d of %d bytes - msg is %s",
				src.Name, i, len(msg), msg)
			// If this looks like a hack it might be because it is.
			// A proper way to do it would be to register exchange keys
			// in a configuration structure such that this function can
			// load up all the different keys.
			// It couldn't be done using the current interfaces because
			// a function can only return at most two values and therefore
			// send no more than two messages.
			if msg == "EOS" && dst1.Type == Exchange {
				log.Printf("[%s] received EOS and is of type exchange", src.Name)
				if dst1.Name == "minutesExchange" {
					dst1.Send(Message{"EOS", "Hard"})
					dst1.Send(Message{"EOS", "Clay"})
					dst1.Send(Message{"EOS", "Grass"})
					dst1.Send(Message{"EOS", "Carpet"})
				} else if dst1.Name == "handsExchange" {
					dst1.Send(Message{"EOS", "L"})
					dst1.Send(Message{"EOS", "R"})
				}
			} else {

				res1, res2 := nodeFn(msg, extra)
				log.Printf("[%s] sending %s and %s", src.Name, res1.Content, res2.Content)
				dst1.Send(res1)
				dst2.Send(res2)
			}
			d.Ack()
		}
	}()

}
func setupFilter(src rabbitQueue, dst rabbitQueue, filterFn Filter) {
	src = src.SetUp()
	dst = dst.SetUp()

	msgs, err := src.Channel.Consume(
		src.AmqpQueue.Name,
		"", false, false, false, false, nil)
	failOnError(err, "[filter node] Could not consume")
	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[filter node] processing message %d of %d bytes - msg is %s",
				i, len(msg), msg)
			if filterFn(msg) {
				dst.Send(Message{msg, ""})
			}
			d.Ack()
		}
	}()

}
//TODO: **ACK __after__**

func setupRabbitSession() (*amqp.Connection, *amqp.Channel) {
	// Setup RabbitMQ session
	conn, err := amqp.Dial("amqp://guest:guest@rabbit:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return conn, ch
}