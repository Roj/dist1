package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"encoding/json"
)
const DBHOSTPORT = "db:11000"
const WORKERHOSTPORT = "worker:50000"
func send(conn net.Conn, s string) {
	fmt.Printf(">%s\n", s)
	conn.Write([]byte(s))
}
// Initializes the DB structure for server host
// if it did not exist. Returns true if server
// was created, false if it already existed.
func create_db_analysis(host string) bool {
	conn, err := net.Dial("tcp", DBHOSTPORT)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	node := Node{dir, 0, "/", make(NodeMap)}
	query := Query{newserver, host, node}
	bytejson, err := json.Marshal(query)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	send(conn, fmt.Sprintf("%s\n", bytejson))
	connbuf := bufio.NewReader(conn)
	for {
		str, _ := connbuf.ReadString('\n')
		fmt.Println(str)
		if str == "OK\n" {
			return true
		} else if str == "ALREADYEXISTS\n" {
			return false
		}
		fmt.Println("Unknown message ", str)
	}

}
func runanalysis(host string) {

	conn, err := net.Dial("tcp", WORKERHOSTPORT)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	//TODO: if false..
	create_db_analysis(host)

	send(conn, fmt.Sprintf("%s /\n", host))
}
func process_request(command string) {
	//TODO: analyze results..
	host := ""
	fmt.Sscanf(command, "analyze %s", &host)
	runanalysis(host)
}
func main() {
	server, err := net.Listen("tcp", ":11001")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Listening on port 11001")
	for {
		conn, err := server.Accept()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		buf := bufio.NewScanner(conn)
		for buf.Scan() {
			process_request(buf.Text())

		}
	}



	//crear la estructura en la DB
}