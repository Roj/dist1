import (
	"errors"
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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func (q rabbitQueue) Send(msg Message) error {
	if q.Type == Empty || msg == nil{
		return nil
	}
	if q.Type == Exchange {
		return ch.Publish(
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
		return ch.Publish(
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
		work_q, err := ch.QueueDeclare(q.Name,
			true, false, false, false, nil)
		failOnError(err, fmt.Sprintf("Failed to create queue named %s", name))
		q.AmqpQueue = work_q
		return q
	}
	// Only BindedQueue and Exchange remain. Both need to
	// declare an Exchange.
	err := ch.ExchangeDeclare(
		q.Name,   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to declare %s", name))
	if q.Type == Exchange {
		return q
	}

	// This one is different because it is exclusive and non durable.
	work_q, err := ch.QueueDeclare(
		"", false, false, true, false, nil)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		work_q.Name, q.key, q.Name, false, nil)
	failOnError(err, "Failed to bind queue to exchange")
	q.AmqpQueue = work_q
	return q
}
func setupNode(src rabbitQueue, dst1 rabbitQueue, dst2 rabbitQueue, nodeFn Process, extra NodeData) {
	srcQ = src.SetUp()
	dst1 = src.SetUp()
	dst2 = src.SetUp()

	msgs, err := srcQ.Channel.Consume(
		srcQ.AmqpQueue.Name,
		"", true, false, false, false, nil)
	failOnError(err, "[node] Could not consume")
	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[node] processing message %d of %d bytes",
				i, len(msg))
			res1, res2 := nodeFn(msg, extra)
			dst1.Send(res1)
			dst2.Send(res2)
		}
	}()

} //Demux, handProcessor, surface (demuxsurface to minutes), distrib hands
func setupFilter(src rabbitQueue, dst rabbitQueue, filterFn Filter) {
	srcQ = src.SetUp()
	dst = src.SetUp()

	msgs, err := srcQ.Channel.Consume(
		srcQ.AmqpQueue.Name,
		"", true, false, false, false, nil)
	failOnError(err, "[filter node] Could not consume")
	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[filter node] processing message %d of %d bytes",
				i, len(msg))
			if filterFn(msg) {
				dst.Send(msg)
			}
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