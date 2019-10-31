package main
import (
	"log"
)
func age_log(msg string, _ NodeData) (Message, Message) {
	log.Printf("[age sink log] EVENT - name %s", msg)
	return Message{"", ""}, Message{"", ""}
}


func collect(msg string, _ NodeData) (Message, Message) {
	if msg == "EOS" {
		//send(ch, collector_q, fmt.Sprintf("surface,%s,%d,%d",key,i,minutes))
		log.Printf("[results collector] received EOS?")
	} else {
		log.Printf("[results collector] received msg - %s", msg)
	}
	return Message{"", ""}, Message{"", ""}
}



func main() {
	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	forever := make(chan bool)

	ageLogQueue := rabbitQueue{Queue, "agefilter_log", "", zeroQ, ch}
	collectorQueue := rabbitQueue{Queue, "collector_queue", "", zeroQ, ch}
	emptyQueue := rabbitQueue{Empty, "", "", zeroQ, ch}

	setupNode(ageLogQueue, emptyQueue, emptyQueue, age_log, nil)
	setupNode(collectorQueue, emptyQueue, emptyQueue, collect, nil)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}