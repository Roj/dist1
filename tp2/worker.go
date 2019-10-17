package main
import (
	"log"
	"strings"
	"github.com/streadway/amqp"
	"fmt"
	"os"
	"bufio"
	"strconv"
)
type tennisPlayer struct  {
	name string
	hand string
	birthdate int
}
type playersMap map[string]tennisPlayer

func loadPlayers() playersMap {
	players := playersMap{}
	file, err := os.Open("data/atp_players.csv")
	failOnError(err, "Could not read file")
	defer file.Close()
	log.Print("Reading..")
	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		i = i+1
		eachline := scanner.Text()
		if i == 0 { //Skip header
			continue
		}
		parts := strings.Split(eachline, ",")

		birthdate, _ := strconv.ParseInt(parts[4], 10, 64)
		players[parts[0]] = tennisPlayer{parts[1]+" "+parts[2],
			parts[3], int(birthdate)}
	}
	return players
}

func send_exchange(ch *amqp.Channel, ex string, key string, msg string) error {
	return ch.Publish(
		ex, // exchange
		key, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
		  ContentType: "text/plain",
		  Body:        []byte(msg),
	})
}

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

// Recibe una fila del cs y hace del demultiplicador.
// La fila tiene un prefijo 0 si es header o 1 si no lo es.
func demux(msg string, ch *amqp.Channel, join_q amqp.Queue, surface_q amqp.Queue) {
	if msg[0] == 0 {
		return
	} else if msg == "EOS" {
		send(ch, join_q, "EOS")
		send(ch, surface_q, "EOS")
		return
	}
	parts := strings.Split(msg[1:], ",")

	//winner_id, loser_id
	send(ch, join_q, fmt.Sprintf("%s,%s", parts[4], parts[5]))
	//surface minutes
	log.Printf("[demux] sending %s to surface out of %s", fmt.Sprintf("%s,%s", parts[3], parts[9]), parts)
	send(ch, surface_q, fmt.Sprintf("%s,%s", parts[3], parts[9]))
}

func join(msg string, ch *amqp.Channel, distrib_q amqp.Queue, age_q amqp.Queue, players playersMap) {
	if msg == "EOS" {
		send(ch, age_q, "EOS")
		send(ch, distrib_q, "EOS")
		return
	}
	parts := strings.Split(msg, ",")
	winner := players[parts[0]]
	loser := players[parts[1]]

	send(ch, age_q, fmt.Sprintf("%d,%d,%s", winner.birthdate, loser.birthdate, winner.name))
	send(ch, distrib_q, fmt.Sprintf("%s,%s", winner.hand, loser.hand))
	log.Printf("[join] winner(hand=%s,bd=%d) loser(hand=%s,bd=%d)", winner.hand, winner.birthdate, loser.hand, loser.birthdate)
}

func distribute_hands(msg string, ch *amqp.Channel) {
	if msg == "EOS" {
		err := send_exchange(ch, "handsExchange", "R", "EOS")
		if err != nil {
			log.Fatal("Could not send End of Stream Message to right hand exchange")
		}
		err = send_exchange(ch, "handsExchange", "L", "EOS")
		if err != nil {
			log.Fatal("Could not send End of Stream Message to left hand exchange")
		}
		return
	}
	parts := strings.Split(msg, ",")
	w_hand := parts[0]
	l_hand := parts[1]
	if w_hand != l_hand && (w_hand == "R" || w_hand == "L") {
		err := send_exchange(ch, "handsExchange", w_hand, fmt.Sprintf("%s,%s", w_hand, l_hand))
		if err != nil {
			log.Fatal("Could not publish message to exchange")
		}
	}
}

func age_filter(msg string, ch *amqp.Channel, age_sink_q amqp.Queue) {
	log.Printf("[age filter] received %s", msg)
	if msg == "EOS" {
		send(ch, age_sink_q, "EOS")
		return
	}
	parts := strings.Split(msg, ",")
	winner_birthdate, _ := strconv.ParseInt(parts[0], 10, 32)
	loser_birthdate, _ := strconv.ParseInt(parts[1], 10, 32)
	log.Printf("[age_filter] first one is %d and second one is %d", winner_birthdate, loser_birthdate)
	if winner_birthdate - loser_birthdate > 200000 {
		send(ch, age_sink_q, parts[2])
	}
}

func distribute_surface(msg string, ch *amqp.Channel) {
	log.Printf("Distribute surface: received %s", msg)
	if msg == "EOS" {
		send_exchange(ch, "minutesExchange", "Clay", "EOS")
		send_exchange(ch, "minutesExchange", "Hard", "EOS")
		send_exchange(ch, "minutesExchange", "Carpet", "EOS")
		send_exchange(ch, "minutesExchange", "Grass", "EOS")
		return
	}
	parts := strings.Split(msg, ",")
	send_exchange(ch, "minutesExchange", parts[0], parts[1])
}

func setupSurfaceProcessor(ch *amqp.Channel, key string) {
	setupMinutesExchange(ch)
	q := setupMinutesBindedQueue(ch, key)
	collector_q := setupCollectorQueue(ch)
	msgs, err := ch.Consume(
		q.Name, "", true, false, false, false, nil)
	failOnError(err, "[surface processor] Could not consume")
	go func() {
		i := 0
		minutes := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[surface processor %s] processing message %d of %d bytes >%s<",
				key, i, len(msg), msg)
			if msg == "EOS" {
				send(ch, collector_q, fmt.Sprintf("surface,%s,%d,%d",key,i,minutes))
				minutes = 0
				i = 0
			} else {
				game_minutes, _ := strconv.ParseFloat(msg, 64)
				minutes = minutes + int(game_minutes)
				log.Printf("[surface processor %s] received %d minutes, now at %d",
					game_minutes, minutes)
			}

		}
	}()
}

func setupHandProcessor(ch *amqp.Channel, key string) {
	setupHandsExchange(ch)
	q := setupHandsBindedQueue(ch, key)
	collector_q := setupCollectorQueue(ch)
	msgs, err := ch.Consume(
		q.Name, "", true, false, false, false, nil)
	failOnError(err, "[hand processor]Could not consume")
	go func(){
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[hand %s] processing message %d of %d bytes",
				key, i, len(msg))
			if msg == "EOS" {
				send(ch, collector_q, fmt.Sprintf("hand,%s,%d",key,i))
				i = 0
			}

		}
	}()
}



func setupAgeFilter(ch *amqp.Channel) {
	age_q := setupJoinAgeFilterQueue(ch)
	age_sink_q := setupAgeFilterLogQueue(ch)
	msgs, err := ch.Consume(
		age_q.Name, "", true, false, false, false, nil)
	failOnError(err, "[age filter] Could not consume")
	go func(){
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[age filter] processing message %d of %d bytes",
				i, len(msg))
			age_filter(msg, ch, age_sink_q)
		}
	}()
}

func setupSurface(ch *amqp.Channel) {
	surface_q := setupDemuxSurfaceQueue(ch)
	setupMinutesExchange(ch)
	msgs, err := ch.Consume(
		surface_q.Name, "", true, false, false, false, nil)
	failOnError(err, "[surface] Could not consume")
	go func(){
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[surface] processing message %d of %d bytes",
				i, len(msg))
			distribute_surface(msg, ch)
		}
	}()
}

func setupDistributeHands(ch *amqp.Channel) {
	distrib_q := setupJoinDistributeQueue(ch)
	setupHandsExchange(ch)
	msgs, err := ch.Consume(
		distrib_q.Name, "", true, false, false, false, nil)
	failOnError(err, "[distrib hands] Could not consume")
	go func(){
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[distrib hands] processing message %d of %d bytes",
				i, len(msg))
			distribute_hands(msg, ch)
		}
	}()

}

func setupJoin(ch *amqp.Channel) {
	join_q := setupDemuxJoinQueue(ch)
	distrib_q := setupJoinDistributeQueue(ch)
	age_q := setupJoinAgeFilterQueue(ch)


	msgs, err := ch.Consume(
		join_q.Name,
		"", true, false, false, false, nil)
	failOnError(err, "Could not consume")
	players := loadPlayers()

	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[join] processing message %d of %d bytes",
				i, len(msg))
			join(msg, ch, distrib_q, age_q, players)
		}
	}()

}

func setupDemux(ch *amqp.Channel) {
	stream_q := setupStreamQueue(ch)
	join_q := setupDemuxJoinQueue(ch)
	surface_q := setupDemuxSurfaceQueue(ch)

	msgs, err := ch.Consume(
		stream_q.Name,
		"", true, false, false, false, nil)
	failOnError(err, "[demux] Could not consume")
	go func() {
		i := 0
		for d := range msgs {
			i = i+1
			msg := string(d.Body)
			log.Printf("[demux] processing message %d of %d bytes",
				i, len(msg))
			demux(msg, ch, join_q, surface_q)
		}
	}()

}
func main() {
	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	forever := make(chan bool)

	setupDemux(ch)
	setupJoin(ch)
	setupDistributeHands(ch)
	setupAgeFilter(ch)
	setupSurface(ch)
	setupHandProcessor(ch, "R")
	setupHandProcessor(ch, "L")
	setupSurfaceProcessor(ch, "Hard")
	setupSurfaceProcessor(ch, "Clay")
	setupSurfaceProcessor(ch, "Grass")
	setupSurfaceProcessor(ch, "Carpet")
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}