package main
import (
	"fmt"
	"log"
	"os"
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

func parse_result(msg string, res Results) Results {
	parts := strings.Split(msg, ",")
	if parts[0] == "L" || parts[0] == "R" {
		games, err := strconv.ParseFloat(parts[1], 64)
		failOnError(err, "No se pudo tomar la cantidad de partidos")
		if parts[0] == "L" {
			res.l_wins = games
		}
		if parts[0] == "R" {
			res.r_wins = games
		}
	} else {
		minutes, err := strconv.ParseFloat(parts[1], 64)
		failOnError(err, "No se pudo tomar la cantidad de minutos")
		games, err := strconv.ParseFloat(parts[2], 64)
		failOnError(err, "No se pudo tomar la cantidad de partidos de superficie")
		if parts[0] == "Hard" {
			res.hard_minutes = minutes/games
		}
		if parts[0] == "Clay" {
			res.clay_minutes = minutes/games
		}
		if parts[0] == "Grass" {
			res.grass_minutes = minutes/games
		}
		if parts[0] == "Carpet" {
			res.carpet_minutes = minutes/games
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


func age_log(msg string, data NodeData) (Message, Message) {
	f, ok := data.(*os.File)
	if !ok {
		panic("No se pudo castear la data del age log a node")
	}

	write_wrapper(f, msg)

	log.Printf("[age sink log] EVENT - name %s", msg)

	return Message{"", ""}, Message{"", ""}
}


func collect(msg string, data NodeData) (Message, Message) {
	res, ok := data.(*Results)
	if !ok{
		panic("No se pudo castear la data del collect a Results")
	}

	if msg == "EOS" {
		log.Printf("[results collector] received EOS?")
	} else {
		log.Printf("[results collector] received msg - %s", msg)
	}
	*res = parse_result(msg, *res)
	if res.l_wins > 0 && res.r_wins > 0 && res.hard_minutes > 0 &&
	res.clay_minutes > 0 && res.carpet_minutes > 0 && res.grass_minutes > 0 {
		write_results(*res)
	}


	return Message{"", ""}, Message{"", ""}
}



func main() {
	f, err := os.Create("nombres.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	conn, ch := setupRabbitSession()
	defer conn.Close()
	defer ch.Close()


	forever := make(chan bool)

	ageLogQueue := rabbitQueue{Queue, "agefilter_log", "", zeroQ, ch}
	collectorQueue := rabbitQueue{Queue, "collector_queue", "", zeroQ, ch}
	emptyQueue := rabbitQueue{Empty, "", "", zeroQ, ch}

	setupNode(ageLogQueue, emptyQueue, emptyQueue, age_log, f)
	setupNode(collectorQueue, emptyQueue, emptyQueue, collect, &Results{0,0,0,0,0,0})

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}