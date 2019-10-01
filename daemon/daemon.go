package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"encoding/json"
	"strings"
	"../storage"
)
const DBHOSTPORT = "db:11000"
const WORKERHOSTPORT = "worker:50000"
func send(conn net.Conn, s string) {
	fmt.Printf(">%s\n", s)
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

// Initializes the DB structure for server host
// if it did not exist. Returns true if server
// was created, false if it already existed.
func createDBAnalysis(host string) bool {
	node := storage.Node{storage.Dir, 0, "/", make(storage.NodeMap)}
	query := storage.Query{storage.Newserver, host, node}
	str := storage.SendQuery(query)
	if str == "OK\n" {
		return true
	} else if str == "ALREADYEXISTS\n" {
		return false
	}
	fmt.Println("Unknown message ", str)
	return false
}
func runAnalysis(host string) string {
	conn, err := net.Dial("tcp", WORKERHOSTPORT)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if !createDBAnalysis(host) {
		return fmt.Sprintf("Ya hay un pedido de analisis para el host %s", host)
	}

	send(conn, fmt.Sprintf("%s /\n", host))
	return "OK"
}
func prettyPrintResults(node storage.Node) string {
	str := ""
	str = str + fmt.Sprintf("%s (%d bytes)\n", node.Path, node.Size)
	for k, v := range node.Files {
		str = str + fmt.Sprintf("\t%s (%s, %d bytes)\n", k, storage.StrNodeType(*v), v.Size)
	}
	return str

}
func getResult(path string, host string) string {
	fmt.Println("Armando resultados")
	node := storage.Node{storage.Dir, 0, path +"/", make(storage.NodeMap)}
	query := storage.Query{storage.Read, host, node}
	str := storage.SendQuery(query)
	var response storage.ResultsResponse
	err := json.Unmarshal([]byte(str), &response)
	if err != nil {
		fmt.Printf("Error decoding:%s", err)
	}
	str = prettyPrintResults(response.Node)
	fmt.Println("Lo que armo pretty print:", str)
	if ! response.Finished {
		return fmt.Sprintf("El host %s no tiene un analisis terminado. El resultado parcial es: \n %s", host, str)
	}
	return str
}

func processRequest(command string) string {
	fmt.Println("Recibido: %s\n", command)
	if strings.Contains(command, "analyze") {
		host := ""
		fmt.Sscanf(command, "analyze %s", &host)
		return runAnalysis(host) + "\n"
	} else if strings.Contains(command, "summary") {
		host := ""
		path := ""
		fmt.Sscanf(command, "summary %s %s", &host, &path)
		return getResult(path, host) + "\n"
	}
	return fmt.Sprintf("Unknown command received: %s", command)
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
			send(conn, processRequest(buf.Text()) + "\n")

		}
		conn.Close()
	}



	//crear la estructura en la DB
}