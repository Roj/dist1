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
	conn.Write([]byte(s))
}
func send_query_db(query storage.Query) string {
	conn, err := net.Dial("tcp", DBHOSTPORT)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

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
// Initializes the DB structure for server host
// if it did not exist. Returns true if server
// was created, false if it already existed.
func create_db_analysis(host string) bool {
	node := storage.Node{storage.Dir, 0, "/", make(storage.NodeMap)}
	query := storage.Query{storage.Newserver, host, node}
	str := send_query_db(query)
	if str == "OK\n" {
		return true
	} else if str == "ALREADYEXISTS\n" {
		return false
	}
	fmt.Println("Unknown message ", str)
	return false
}
func runanalysis(host string) string {
	conn, err := net.Dial("tcp", WORKERHOSTPORT)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if !create_db_analysis(host) {
		return fmt.Sprintf("Ya hay un pedido de analisis para el host %s", host)
	}

	send(conn, fmt.Sprintf("%s /\n", host))
	return "OK"
}
func pretty_print_results(node storage.Node) string {
	str := ""
	str = str + fmt.Sprintf("%s (%d bytes)\n", node.Path, node.Size)
	for k, v := range node.Files {
		str = str + fmt.Sprintf("\t%s (%s, %d bytes)\n", k, storage.Str_nodetype(*v), v.Size)
	}
	return str

}
func getresult(path string, host string) string {
	fmt.Println("Armando resultados")
	node := storage.Node{storage.Dir, 0, path +"/", make(storage.NodeMap)}
	query := storage.Query{storage.Read, host, node}
	str := send_query_db(query)
	var response storage.ResultsResponse
	err := json.Unmarshal([]byte(str), &response)
	if err != nil {
		fmt.Printf("Error decoding:%s", err)
	}
	str = pretty_print_results(response.Node)
	fmt.Println("Lo que armo pretty print:", str)
	if ! response.Finished {
		return fmt.Sprintf("El host %s no tiene un analisis terminado. El resultado parcial es: \n %s", host, str)
	}
	return str
}

func process_request(command string) string {
	//TODO: analyze results..
	if strings.Contains(command, "analyze") {
		host := ""
		fmt.Sscanf(command, "analyze %s", &host)
		return runanalysis(host) + "\n"
	} else if strings.Contains(command, "summary") {
		host := ""
		path := ""
		fmt.Sscanf(command, "summary %s %s", &host, &path)
		return getresult(path, host) + "\n"
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
			send(conn, process_request(buf.Text()) + "\n")

		}
		conn.Close()
	}



	//crear la estructura en la DB
}