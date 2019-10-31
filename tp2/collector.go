package main
import (
	"github.com/streadway/amqp"
	"log"
)
func age_log(msg string) (string, string) {
	log.Printf("[age sink log] EVENT - name %s", msg)
	return "", ""
}


func collect(msg string) (string, string) {
	if msg == "EOS" {
		//send(ch, collector_q, fmt.Sprintf("surface,%s,%d,%d",key,i,minutes))
		log.Printf("[results collector] received EOS?")
	} else {
		log.Printf("[results collector] received msg - %s", msg)
	}
	return "", ""
}



func main() {
	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	forever := make(chan bool)

	setupNode(ageLogQueue, emptyQueue, emptyQueue, age_log, nil)
	setupNode(collectorQueue, emptyQueue, emptyQueue, collect, nil)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}