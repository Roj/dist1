package main
import (
	"log"
)


func main() {
	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()

	forever := make(chan bool)

	streamQueue := rabbitQueue{Queue, "stream_queue", "", zeroQ, ch}
	demuxSurfaceQueue := rabbitQueue{Queue, "demux_surface", "", zeroQ, ch}
	demuxJoinQueue := rabbitQueue{Queue, "demux_join", "", zeroQ, ch}
	joinAgeQueue := rabbitQueue{Queue, "join_agefilter", "", zeroQ, ch}
	ageLogQueue := rabbitQueue{Queue, "agefilter_log", "", zeroQ, ch}
	joinDistributeQueue := rabbitQueue{Queue, "join_distribute", "", zeroQ, ch}
	minutesExchange := rabbitQueue{Exchange, "minutesExchange", "", zeroQ, ch}
	hardMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Hard", zeroQ, ch}
	clayMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Clay", zeroQ, ch}
	grassMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Grass", zeroQ, ch}
	carpetMinutesQueue := rabbitQueue{BindedQueue, "minutesExchange", "Carpet", zeroQ, ch}
	handsExchange := rabbitQueue{Exchange, "handsExchange", "", zeroQ, ch}
	rightHandQueue := rabbitQueue{BindedQueue, "handsExchange", "R", zeroQ, ch}
	leftHandQueue := rabbitQueue{BindedQueue, "handsExchange", "L", zeroQ, ch}
	emptyQueue := rabbitQueue{Empty, "", "", zeroQ, ch}
	handsSink := rabbitQueue{Queue, "hands_sink", "", zeroQ, ch}
	collectorQueue := rabbitQueue{Queue, "collector_queue", "", zeroQ, ch}

	//y los sinks??? => creo que son particulares por la cosa de esperar de todos

	setupNode(streamQueue, demuxJoinQueue, demuxSurfaceQueue, demux, nil)
	setupNode(joinDistributeQueue, handsExchange, emptyQueue, distribute_hands, nil)
	setupFilter(joinAgeQueue, ageLogQueue, age_filter)
	setupNode(demuxSurfaceQueue, minutesExchange, emptyQueue, distribute_surface, nil)
	setupNode(rightHandQueue, handsSink, emptyQueue, adder, &AdderData{"R", 0, 0})
	setupNode(leftHandQueue, handsSink, emptyQueue, adder, &AdderData{"L", 0, 0})
	setupNode(hardMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Hard", 0, 0})
	setupNode(clayMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Clay", 0, 0})
	setupNode(grassMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Grass", 0, 0})
	setupNode(carpetMinutesQueue, collectorQueue, emptyQueue, adder, &AdderData{"Carpet", 0, 0})

	players := loadPlayers()
	setupNode(demuxJoinQueue, joinDistributeQueue, joinAgeQueue, join, players)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}

//Demux => EOS, EOS
//Join => EOS, EOS
//distrib hands => manejado
//Filter EOS, zeroQ
//el único diferente es adder, que sólo envía la respuesta con el EOS