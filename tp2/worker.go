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


func main() {
	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	forever := make(chan bool)

	streamQueue := rabbitQueue{Queue, "stream_queue", "", nil, ch}
	demuxSurfaceQueue := rabbitQueue{Queue, "demux_surface", "", nil, ch}
	demuxJoinQueue := rabbitQueue{Queue, "demux_join", "", nil, ch}
	joinAgeQueue := rabbitQueue{Queue, "join_agefilter", "", nil, ch}
	ageLogQueue := rabbitQueue{Queue, "agefilter_log", "", nil, ch}
	joinDistributeQueue := rabbitQueue{Queue, "join_distribute", "", nil, ch}
	minutesExchange := rabbitQueue{Exchange, "minutesExchange", "", nil, ch}
	hardMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Hard", nil, ch}
	clayMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Clay", nil, ch}
	grassMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Grass", nil, ch}
	carpetMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Carpet", nil, ch}
	handsExchange := rabbitQueue{Exchange, "handsExchange", "", nil, ch}
	rightHandQueue := rabbitQueue{BindedQueue, "handsExchange", "R", nil, ch}
	leftHandQueue := rabbitQueue{BindedQueue, "handsExchange", "L", nil, ch}
	emptyQueue := rabbitQueue{Empty, "", "", nil, ch}
	handsSink := rabbitQueue{Queue, "hands_sink", "", nil, ch}
	collectorQueue := rabbitQueue{Queue, "collector_queue", "", nil, ch}

	//y los sinks??? => creo que son particulares por la cosa de esperar de todos

	setupNode(streamQueue, demuxJoinQueue, demuxSurfaceQueue, demux)
	setupNode(joinDistributeQueue, handsExchange, emptyQueue, distribute_hands)
	setupFilter(joinAgeQueue, ageLogQueue, age_filter)
	setupNode(demuxSurfaceQueue, minutesExchange, emptyQueue, distribute_surface)
	setupNode(rightHandQueue, handsSink, emptyQueue, adder, &AdderData{"R", 0})
	setupNode(leftHandQueue, handsSink, emptyQueue, adder, &AdderData{"L", 0})
	setupNode(hardMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Hard", 0})
	setupNode(clayMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Clay", 0})
	setupNode(grassMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Grass", 0})
	setupNode(carpetMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Carpet", 0})

	players := loadPlayers()
	setupNode(demuxJoinQueue, joinDistributeQueue, joinAgeQueue, joiner, players)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}