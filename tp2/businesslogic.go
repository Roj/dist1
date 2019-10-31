package main
import (
	"log"
	"strings"
	"fmt"
	"os"
	"bufio"
	"strconv"
)
// Constantes del CSV
const BIRTHDATE_PLAYERS_COL = 4
const ID_PLAYERS_COL = 0
const FIRSTNAME_PLAYERS_COL = 1
const LASTNAME_PLAYERS_COL = 2
const HAND_PLAYERS_COL = 3

const WINNERID_GAME_COL = 4
const LOSERID_GAME_COL = 5
const SURFACE_GAME_COL = 3
const MINUTES_GAME_COL = 9

// Types
type tennisPlayer struct  {
	name string
	hand string
	birthdate int
}
type playersMap map[string]tennisPlayer
type AdderData struct {
	Key string
	Val int
	Count int
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

		birthdate, _ := strconv.ParseInt(parts[BIRTHDATE_PLAYERS_COL], 10, 64)
		players[parts[ID_PLAYERS_COL]] = tennisPlayer{
			parts[FIRSTNAME_PLAYERS_COL]+" "+parts[LASTNAME_PLAYERS_COL],
			parts[HAND_PLAYERS_COL], int(birthdate)}
	}
	return players
}
// Recibe una fila del cs y hace del demultiplicador.
// La fila tiene un prefijo 0 si es header o 1 si no lo es.
func demux(msg string, _ NodeData) (Message, Message) {
	if msg[:1] == "0" {
		return Message{"", ""}, Message{"", ""}
	} else if msg == "EOS" {
		return Message{"EOS", ""}, Message{"EOS", ""}
	}
	parts := strings.Split(msg[1:], ",")

	player_ids := Message{fmt.Sprintf("%s,%s", parts[WINNERID_GAME_COL], parts[LOSERID_GAME_COL]),""}
	surface_minutes := Message{fmt.Sprintf("%s,%s", parts[SURFACE_GAME_COL], parts[MINUTES_GAME_COL]),""}
	log.Printf("[demux] sending %s to surface out of %s",
		surface_minutes, parts)
	return player_ids, surface_minutes
}

func join(msg string, xtra NodeData) (Message, Message) {
	if msg == "EOS" {
		return Message{"EOS",""}, Message{"EOS",""}
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
	parts := strings.Split(msg, ",")
	w_hand := parts[0]
	l_hand := parts[1]
	if w_hand != l_hand && (w_hand == "R" || w_hand == "L") {
		return Message{"1", w_hand}, Message{"", ""}
	}
	return Message{"", ""}, Message{"", ""}
}

func age_filter(msg string) bool {
	log.Printf("[age filter] received %s", msg)
	//TODO use constants
	if msg == "EOS" {
		return true // let it trickle down
	}
	parts := strings.Split(msg, ",")
	winner_birthdate, _ := strconv.ParseInt(parts[0], 10, 32)
	loser_birthdate, _ := strconv.ParseInt(parts[1], 10, 32)
	log.Printf("[age_filter] first one is %d and second one is %d", winner_birthdate, loser_birthdate)
	return winner_birthdate - loser_birthdate > 200000
}

func distribute_surface(msg string, _ NodeData) (Message, Message) {
	log.Printf("Distribute surface: received %s", msg)
	parts := strings.Split(msg, ",")
	return Message{parts[1], parts[0]}, Message{"", ""}
}

func adder(msg string, xtra NodeData) (Message, Message) {
	data, ok := xtra.(*AdderData)
	if !ok {
		panic("Could not assert type adder data")
	}

	if msg == "EOS" {

		results := fmt.Sprintf("%s,%d,%d",data.Key, data.Val, data.Count)
		log.Printf("[adder %s] received EOS, sending total %s",
		data.Key, results)
		return Message{results, ""}, Message{"", ""}
	}

	value, err := strconv.ParseFloat(msg, 64)
	failOnError(err, "Could not parse number")
	data.Val = data.Val + int(value)
	data.Count = data.Count + 1
	log.Printf("[adder %s] received %d minutes, now at %d",
		data.Key, value, data.Val)
	return Message{"", ""}, Message{"", ""}
}