package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"net"
	"bufio"
)
const (
	Read = iota
	Write
	Newserver
	Finishserver
)
const DBHOSTPORT = "db:11000"

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
	//fmt.Printf(">%s", s)
	conn.Write([]byte(s))
}
func SendQuery(query Query) string {
	conn, err := net.Dial("tcp", DBHOSTPORT)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer conn.Close()

	bytejson, err := json.Marshal(query)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	send(conn, fmt.Sprintf("%s\n", bytejson))
	connbuf := bufio.NewReader(conn)
	str, _ := connbuf.ReadString('\n')
	fmt.Printf("Recibido de la DB -- %s --\n",str)
	return str
}
func sendCommand(query Query) {
	str := SendQuery(query)
	if str != "OK\n" {
		fmt.Println("Unknown message ", str)
		//TODO: error
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