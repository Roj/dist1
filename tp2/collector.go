package main
import (
	"github.com/streadway/amqp"
	"log"
)
func age_log(msg string) {
	log.Printf("[age sink log] EVENT - name %s", msg)
}


func setupAgeLog(ch *amqp.Channel) {
	age_sink_q := setupAgeFilterLogQueue(ch)
	msgs, err := ch.Consume(
		age_sink_q.Name, "", true, false, false, false, nil)
	failOnError(err, "[age sink log] Could not consume")
	go func(){
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[age sink log] processing message %d of %d bytes",
				i, len(msg))
			age_log(msg)
		}
	}()
}
func setupCollector(ch *amqp.Channel) {
	collector_q := setupCollectorQueue(ch)
	msgs, err := ch.Consume(
		collector_q.Name, "", true, false, false, false, nil)
	failOnError(err, "[results collector] Could not consume")
	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[results collector] processing message %d of %d bytes",
				i, len(msg))
			if msg == "EOS" {
				//send(ch, collector_q, fmt.Sprintf("surface,%s,%d,%d",key,i,minutes))
				log.Printf("[results collector] received EOS?")
			} else {
				log.Printf("[results collector] received msg - %s", msg)
			}
		}
	}()
}


func main() {
	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	forever := make(chan bool)

	setupAgeLog(ch)
	setupCollector(ch)
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}