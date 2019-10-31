package main
import (surface processor
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
type AdderData struct {
	Key string
	Val int
}

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
// Recibe una fila del cs y hace del demultiplicador.
// La fila tiene un prefijo 0 si es header o 1 si no lo es.
func demux(msg string, _ NodeData) (Message, Message) {
	if msg[0] == 0 {
		return nil, nil
	} else if msg == "EOS" {
		return Message{"EOS", ""}, Message{"EOS", ""}
	}
	parts := strings.Split(msg[1:], ",")

	//TODO: use constants for this
	player_ids := Message{fmt.Sprintf("%s,%s", parts[4], parts[5]),""}
	surface_minutes := Message{fmt.Sprintf("%s,%s", parts[3], parts[9]),""}
	log.Printf("[demux] sending %s to surface out of %s", fmt.Sprintf("%s,%s", parts[3], parts[9]), parts)
	return player_ids, surface_minutes
}

func join(msg string, xtra NodeData) (Message, Message) {
	if msg == "EOS" {
		return "EOS", "EOS"
	}
	players, ok := xtra.(playersMap)
	if !ok {
		panic("Could not assert type players map")
	}
	parts := strings.Split(msg, ",")
	winner := players[parts[0]]
	loser := players[parts[1]]

	distrib_str := fmt.Sprintf("%s,%s", winner.hand, loser.hand)
	age_q_str := fmt.Sprintf("%d,%d,%s", winner.birthdate, loser.birthdate, winner.name)

	log.Printf("[join] winner(hand=%s,bd=%d) loser(hand=%s,bd=%d)", winner.hand, winner.birthdate, loser.hand, loser.birthdate)
	return Message{distrib_str, ""}, Message{age_q_str, ""}
}

func distribute_hands(msg string, _ NodeData) (Message, Message){
	if msg == "EOS" {
		// . TODO TODO TODO TODO
		return Message{"EOS", "R"}, Message{"EOS", "L"}

	}
	parts := strings.Split(msg, ",")
	w_hand := parts[0]
	l_hand := parts[1]
	if w_hand != l_hand && (w_hand == "R" || w_hand == "L") {
		return Message{"1", w_hand}, nil
	}
}

func age_filter(msg string) bool {
	log.Printf("[age filter] received %s", msg)
	if msg == "EOS" {
		send(ch, age_sink_q, "EOS") // . TODO TODO TODO
		return
	}
	//TODO use constants
	parts := strings.Split(msg, ",")
	winner_birthdate, _ := strconv.ParseInt(parts[0], 10, 32)
	loser_birthdate, _ := strconv.ParseInt(parts[1], 10, 32)
	log.Printf("[age_filter] first one is %d and second one is %d", winner_birthdate, loser_birthdate)
	return winner_birthdate - loser_birthdate > 200000
}

func distribute_surface(msg string, _ NodeData) (Message, Message) {
	log.Printf("Distribute surface: received %s", msg)
	if msg == "EOS" { // . TODO TODO TODO
		send_exchange(ch, "minutesExchange", "Clay", "EOS")
		send_exchange(ch, "minutesExchange", "Hard", "EOS")
		send_exchange(ch, "minutesExchange", "Carpet", "EOS")
		send_exchange(ch, "minutesExchange", "Grass", "EOS")
		return
	}
	parts := strings.Split(msg, ",")
	return Message{parts[1], parts[0]}, nil
}
setupNode(hardMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Hard", 0})

func adder(msg string, xtra NodeData) (Message, Message) {
	data, ok := xtra.(*AdderData)
	if !ok {
		panic("Could not assert type adder data")
	}
	value, err := strconv.ParseFloat(msg, 64)
	failOnError(err, "Could not parse number")
	*data.Val = data.Val + int(value)
	log.Printf("[adder %s] received %d minutes, now at %d",
		data.Key, value, data.Val)

}