package main
import (
	"errors"
	"github.com/streadway/amqp"
	"fmt"
	"log"
	"strings"
)
const EOS = "EOS"
const STOP = "STOP"
const REG = "REG"
const UNREG = "UNREG"

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


	work_q, err := q.Channel.QueueDeclare(
		q.Name+"_"+q.Key, true, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	err = q.Channel.QueueBind(
		work_q.Name, q.Key, q.Name, false, nil)
	failOnError(err, "Failed to bind queue to exchange")
	q.AmqpQueue = work_q
	return q
}
func registerWithWatcher(src rabbitQueue, watcher_queue rabbitQueue) {
	watcher_queue.Send(Message{fmt.Sprintf("REG,%s", src.AmqpQueue.Name), ""})
}
func notifyEOSWatcher(src rabbitQueue, watcher_queue rabbitQueue) {
	watcher_queue.Send(Message{fmt.Sprintf("EOS,%s", src.AmqpQueue.Name), ""})
}
func unregisterWithWatcher(src rabbitQueue, dst1 rabbitQueue, dst2 rabbitQueue, watcher_queue rabbitQueue) {
	watcher_queue.Send(Message{
		fmt.Sprintf("UNREG,%s,%s,%s", src.AmqpQueue.Name, dst1.Name, dst2.Name),
		""})
}
func setupNode(src rabbitQueue, dst1 rabbitQueue, dst2 rabbitQueue, nodeFn Process, extra NodeData) {
	src = src.SetUp()
	dst1 = dst1.SetUp()
	dst2 = dst2.SetUp()
	watcher := rabbitQueue{Queue, "watcher_queue", "", zeroQ, src.Channel}
	watcher = watcher.SetUp()

	msgs, err := src.Channel.Consume(
		src.AmqpQueue.Name,
		"", false, false, false, false, nil)
	failOnError(err, "[node] Could not consume")

	registerWithWatcher(src, watcher)
	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[%s] processing message %d of %d bytes - msg is %s",
				src.Name, i, len(msg), msg)

			if msg == EOS {
				notifyEOSWatcher(src, watcher)
			} else {
				//Business logic also should handle STOP case.
				//In case of aggregators, it is when they should send their data.
				res1, res2 := nodeFn(msg, extra)
				log.Printf("[%s] sending %s and %s", src.Name, res1.Content, res2.Content)
				dst1.Send(res1)
				dst2.Send(res2)
			}

			d.Ack(false)

			if msg == STOP {
				break
			}
		}
		unregisterWithWatcher(src, dst1, dst2, watcher)
	}()

}

func setupFilter(src rabbitQueue, dst rabbitQueue, filterFn Filter) {
	src = src.SetUp()
	dst = dst.SetUp()
	watcher := rabbitQueue{Queue, "watcher_queue", "", zeroQ, src.Channel}
	watcher = watcher.SetUp()

	msgs, err := src.Channel.Consume(
		src.AmqpQueue.Name,
		"", false, false, false, false, nil)
	failOnError(err, "[filter node] Could not consume")

	registerWithWatcher(src, watcher)

	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[filter node] processing message %d of %d bytes - msg is %s",
				i, len(msg), msg)
			// TODO: IF EOS SEND, IF STOP, FINISH FOR REAL
			if msg == EOS {
				notifyEOSWatcher(src, watcher)
			} else if msg != STOP {
				if filterFn(msg) {
					dst.Send(Message{msg, ""})
				}
			}
			d.Ack(false)
			if msg == STOP {
				break
			}
		}
		unregisterWithWatcher(src, dst, rabbitQueue{Empty, "", "", zeroQ, nil}, watcher)
	}()

}

// The protocol for the watcher is as follows.
// New processes register to the watcher by a REG,q_name message.
// If a process receives an EOS, they notify the watcher via a EOS,q_name message.
// Then the watcher send stop messages to all node instances of that queue.
// As they stop, they unregister by an UNREG,q_name,nextq1,nextq2 message.
// When the last process of that type unregisters the watcher forwards an EOS
// to the next level (given by nextq1, nextq2). Therefore no EOSs are received by
// a level before all nodes in the previous one stop.
func processWatcher(ch *amqp.Channel) {
	watcherQ := rabbitQueue{Queue, "watcher_queue", "", zeroQ, ch}
	watcherQ = watcherQ.SetUp()
	storedQueues := make(map[string]rabbitQueue)
	processCounter := make(map[string]int)

	msgs, err := ch.Consume(
		watcherQ.AmqpQueue.Name,
		"", false, true, false, false, nil)
	failOnError(err, "[watcher node] Could not consume")

	i := 0
	for d := range msgs {
		i = i+1
		msg := string(d.Body)
		log.Printf("[watcher node] processing message %d of %d bytes -- msg is %s",
			i, len(msg), msg)

		parts := strings.Split(msg, ",")
		command := parts[0]
		queue_name := parts[1]

		queue, ok := storedQueues[queue_name]
		if !ok {
			queue := rabbitQueue{Queue, queue_name, "", zeroQ, ch}
			queue = queue.SetUp()
			storedQueues[queue_name] = queue
			processCounter[queue_name] = 0
		}

		if command == REG {
			processCounter[queue_name] = processCounter[queue_name] + 1
		} else if command == EOS {
			for j := 0; j < processCounter[queue_name]; j++ {
				queue.Send(Message{STOP, ""}) // y si hay
			}
		} else if command == UNREG {
			processCounter[queue_name] = processCounter[queue_name] - 1
			if processCounter[queue_name] == 0 {
				nextQName := parts[2]
				log.Printf("[watcher] UNREG reached 0 for %s, nextQName is %s\n",queue_name,nextQName)
				if nextQName == "minutesExchange" {
					log.Printf("[watcher] sending EOS to binded minute queues")
					mEx := rabbitQueue{Exchange, "minutesExchange", "", zeroQ, ch}
					mEx.Send(Message{EOS, "Hard"})
					mEx.Send(Message{EOS, "Clay"})
					mEx.Send(Message{EOS, "Grass"})
					mEx.Send(Message{EOS, "Carpet"})
				} else if nextQName == "handsExchange" {
					log.Printf("[watcher] sending EOS to binded hand queues")
					hEx := rabbitQueue{Exchange, "handsExchange", "", zeroQ, ch}
					hEx.Send(Message{EOS, "L"})
					hEx.Send(Message{EOS, "R"})
				} else if nextQName != "collector_queue" && nextQName != "" {

					nextQ, ok := storedQueues[nextQName]
					if !ok {
						nextQ = rabbitQueue{Queue, queue_name, "", zeroQ, ch}
						nextQ = nextQ.SetUp()
					}
					nextQ.Send(Message{EOS, ""})
				}
				if parts[3] != "" {
					otherQ := storedQueues[parts[3]]
					otherQ.Send(Message{EOS, ""})
				}
			}
		}
		d.Ack(false)
	}
}
func setupRabbitSession() (*amqp.Connection, *amqp.Channel) {
	// Setup RabbitMQ session
	conn, err := amqp.Dial("amqp://guest:guest@rabbit:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return conn, ch
}