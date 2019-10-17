package main
import (
	"os"
	"log"
	"fmt"
	"bufio"
	"github.com/streadway/amqp"
)
func send(ch *amqp.Channel, queue amqp.Queue, message string) error {
	return ch.Publish(
		"",
		queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body: []byte(message),
		})
}
func main() {
	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	stream_q := setupStreamQueue(ch)

	file, err := os.Open("data/atp_matches_2000.csv")
	//file, err := os.Open("data/atp_head.csv")

	failOnError(err, "Could not read file")

	defer file.Close()

	fmt.Println("Reading..")
	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		eachline := scanner.Text()
		if i == 0 {
			eachline = "0"+eachline
		} else {
			eachline = "1"+eachline
		}
		//fmt.Println(eachline)
		err = send(ch, stream_q, eachline)
		failOnError(err, "Error publishing")
	}
	err = send(ch, stream_q, "EOS")
	failOnError(err, "Error sending end of stream")
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}