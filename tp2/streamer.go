package main
import (
	"os"
	"log"
	"fmt"
	"bufio"
)

func main() {
	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	stream_q := rabbitQueue{Queue, "stream_queue", "", zeroQ, ch}

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
		stream_q.Send(Message{eachline, ""})
		i = i + 1
	}
	err = stream_q.Send(Message{"EOS", ""})
	failOnError(err, "Error sending end of stream")
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}