package main
import (
	"log"
	"os"
	"fmt"
	"strconv"
	"strings"
	"github.com/streadway/amqp"
)
type Lambda func()
func runN(f Lambda, n int) {
	for i:=0; i < n; i++ {
		f()
	}
}
func dispatcher(arg string, ch *amqp.Channel) {
	colonpos := strings.Index(arg, ":")

	node := arg[:colonpos]
	val := arg[colonpos+1:]

	number, err := strconv.Atoi(val)
	if err != nil {
		panic("Parámetro incorrecto para número")
	}

	fmt.Printf("Lanzando %d instancias de %s\n", number, node)
	emptyQueue := rabbitQueue{Empty, "", "", zeroQ, ch}
	switch node {
	case "demux":
		streamQueue := rabbitQueue{Queue, "stream_queue", "", zeroQ, ch}
		demuxSurfaceQueue := rabbitQueue{Queue, "demux_surface", "", zeroQ, ch}
		demuxJoinQueue := rabbitQueue{Queue, "demux_join", "", zeroQ, ch}

		runN(
			func(){setupNode(streamQueue, demuxJoinQueue, demuxSurfaceQueue, demux, nil)},
			number)

	case "join":
		demuxJoinQueue := rabbitQueue{Queue, "demux_join", "", zeroQ, ch}
		joinAgeQueue := rabbitQueue{Queue, "join_agefilter", "", zeroQ, ch}
		joinDistributeQueue := rabbitQueue{Queue, "join_distribute", "", zeroQ, ch}
		players := loadPlayers()

		runN(func(){
			setupNode(demuxJoinQueue, joinDistributeQueue, joinAgeQueue, join, players)},
			number)

	case "distribute_hands":
		joinDistributeQueue := rabbitQueue{Queue, "join_distribute", "", zeroQ, ch}
		handsExchange := rabbitQueue{Exchange, "handsExchange", "", zeroQ, ch}

		runN(func(){
			setupNode(joinDistributeQueue, handsExchange, emptyQueue, distribute_hands, nil)},
			number)

	case "distribute_surface":
		demuxSurfaceQueue := rabbitQueue{Queue, "demux_surface", "", zeroQ, ch}
		minutesExchange := rabbitQueue{Exchange, "minutesExchange", "", zeroQ, ch}

		runN(func(){
			setupNode(demuxSurfaceQueue, minutesExchange, emptyQueue, distribute_surface, nil)},
			number)

	case "hands_processor":
		rightHandQueue := rabbitQueue{BindedQueue, "handsExchange", "R", zeroQ, ch}
		leftHandQueue := rabbitQueue{BindedQueue, "handsExchange", "L", zeroQ, ch}
		collectorQueue := rabbitQueue{Queue, "collector_queue", "", zeroQ, ch}
		setupNode(rightHandQueue, collectorQueue, emptyQueue, adder, &AdderData{"R", 0, 0})
		setupNode(leftHandQueue, collectorQueue, emptyQueue, adder, &AdderData{"L", 0, 0})

	case "surface_processor":
		hardMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Hard", zeroQ, ch}
		clayMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Clay", zeroQ, ch}
		grassMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Grass", zeroQ, ch}
		carpetMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Carpet", zeroQ, ch}
		collectorQueue := rabbitQueue{Queue, "collector_queue", "", zeroQ, ch}
		setupNode(hardMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Hard", 0, 0})
		setupNode(clayMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Clay", 0, 0})
		setupNode(grassMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Grass", 0, 0})
		setupNode(carpetMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Carpet", 0, 0})
	case "age_filter":
		joinAgeQueue := rabbitQueue{Queue, "join_agefilter", "", zeroQ, ch}
		ageLogQueue := rabbitQueue{Queue, "agefilter_log", "", zeroQ, ch}

		runN(func(){
			setupFilter(joinAgeQueue, ageLogQueue, age_filter)},
			number)
	}
}

func main() {

	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	forever := make(chan bool)

	argsWithoutProg := os.Args[2:]
	for _, arg := range argsWithoutProg {
		dispatcher(arg, ch)
	}

	//Now that all goroutines are running, if we are tasked
	//with the watcher role we can occupy the main thread with it.
	for _, arg := range argsWithoutProg {
		if arg == "watcher:1" {
			log.Printf("[*] Running watcher.")
			processWatcher(ch)
		}
	}
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}