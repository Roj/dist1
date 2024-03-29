package storage

import (
	"encoding/json"
	"fmt"
	"net"
	"bufio"
	"time"
)
const DBHOSTPORT = "db:11000"
const SUCCESS_RESPONSE = "OK\n"
const ALREADY_EXISTS_RESPONSE="ALREADYEXISTS\n"

const (
	Read = iota
	Write
	Newserver
	Finishserver
)


type Query struct {
	Type int
	Hostname string
	Node Node
}
type ResultsResponse struct {
	Finished bool
	Node Node
}
// Wrap sencillo de write para poder imprimir e ir viendo.
func send(conn net.Conn, s string) {
	//fmt.Printf(">%s\n", s)
	buf := []byte(s)
	count := 0
	for count < len(buf) {
		byteSent, err := conn.Write(buf[count:])
		count += byteSent
		if err == nil {
			return
		}
	}
}
// Manda una query al servidor de storage.
// Devuelve la respuesta, y si falló la transmisión o no.
// La transmisión puede ser reintentable.
func (query Query) Send() (string, bool) {
	conn, err := net.Dial("tcp", DBHOSTPORT)
	if err != nil {
		fmt.Println(err)
		return "", false
	}
	defer conn.Close()

	bytejson, err := json.Marshal(query)
	if err != nil {
		fmt.Println("Error de serializacion:", err)
		//De este error no se puede recuperar (query malformada)
		panic("Query no valida")
	}

	send(conn, fmt.Sprintf("%s\n", bytejson))
	connbuf := bufio.NewReader(conn)
	str, e := connbuf.ReadString('\n')
	if e != nil {
		return "", false
	}
	fmt.Printf("Recibido de la DB -- %s --\n",str)
	return str, true
}
// Manda un comando al servidor de storage.
// Si hay error de TCP, reintenta en un segundo.
// Espera hasta el OK del servidor.
func sendCommand(query Query) {
	str, success := query.Send()
	i := 0
	for !success && i < 10 {
		time.Sleep(time.Second)
		str, success = query.Send()
		i+=1
	}
	if i == 10 {
		panic("El servidor de storage no se encuentra disponible. Es posible que se haya caído")
	}

	if str != SUCCESS_RESPONSE {
		fmt.Println("Unknown message ", str)
	}
}
func UpdateNode(host string, node Node) {
	query := Query{Write, host, node}
	sendCommand(query)
}
func FinishJob(host string) {
	fmt.Println("Job finished for host ", host)
	var _node Node
	query := Query{Finishserver, host, _node}
	sendCommand(query)
}