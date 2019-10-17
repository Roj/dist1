package main
import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"fmt"
	"strings"
	"strconv"
)
type Results struct {
	l_wins float64
	r_wins float64
	hard_minutes float64
	clay_minutes float64
	grass_minutes float64
	carpet_minutes float64
}
func write_wrapper(f *os.File, msg string) {
	written := 0
	for written < len(msg) {
		l, _ := f.WriteString(msg[written:])
		written = written + l
	}
}
func age_log(msg string, f *os.File) {
	log.Printf("[age sink log] EVENT - name %s", msg)
	write_wrapper(f, msg)
}

func parse_result(msg string, res Results) Results {
	parts := strings.Split(msg, ",")
	if parts[0] == "surface" {
		minutes, _ := strconv.ParseFloat(parts[3], 64)
		games, _ := strconv.ParseFloat(parts[2], 64)
		if parts[1] == "Hard" {
			res.hard_minutes = minutes/games
		}
		if parts[1] == "Clay" {
			res.clay_minutes = minutes/games
		}
		if parts[1] == "Grass" {
			res.grass_minutes = minutes/games
		}
		if parts[1] == "Carpet" {
			res.carpet_minutes = minutes/games
		}
	} else if parts[0] == "hand" {
		games, _ := strconv.ParseFloat(parts[2], 64)
		if parts[1] == "L" {
			res.l_wins = games
		}
		if parts[1] == "R" {
			res.r_wins = games
		}
	}
	return res
}
func write_results(res Results) {
	f, err := os.Create("resultados.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	lr_results := fmt.Sprintf("LR wins: L %f, R %f\n",
	res.l_wins/(res.l_wins + res.r_wins),
	res.r_wins/(res.l_wins + res.r_wins))

	minutes_results := fmt.Sprintf("Minutes per surface: %f clay %f hard %f carpet %f grass\n",
	res.clay_minutes, res.hard_minutes, res.carpet_minutes, res.grass_minutes)

	write_wrapper(f, lr_results)
	write_wrapper(f, minutes_results)
	log.Printf("[collector log] lr_results = %s", lr_results)
	log.Printf("[collector log] minutes_results = %s", minutes_results)

}
func setupAgeLog(ch *amqp.Channel) {
	age_sink_q := setupAgeFilterLogQueue(ch)
	msgs, err := ch.Consume(
		age_sink_q.Name, "", true, false, false, false, nil)
	failOnError(err, "[age sink log] Could not consume")
	f, err := os.Create("nombres.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	go func(){
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[age sink log] processing message %d of %d bytes",
				i, len(msg))
			if msg == "EOS" {
				break
			}
			age_log(msg, f)
		}
	}()
}

func setupCollector(ch *amqp.Channel) {
	collector_q := setupCollectorQueue(ch)
	msgs, err := ch.Consume(
		collector_q.Name, "", true, false, false, false, nil)
	failOnError(err, "[results collector] Could not consume")
	var res Results
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
				break
			}
			log.Printf("[results collector] received msg - %s", msg)

			res = parse_result(msg, res)
			if res.l_wins > 0 && res.r_wins > 0 && res.hard_minutes > 0 &&
				res.clay_minutes > 0 && res.carpet_minutes > 0 && res.grass_minutes > 0 {
				break
			}
		}
		write_results(res)
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