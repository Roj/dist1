package main

import (
	"fmt"
	"net"
	"time"
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
	str, sent := storage.SendQuery(query)
	if !sent {
		time.Sleep(time.Second)
		return createDBAnalysis(host)
	}
	if str == "OK\n" {
		return true
	} else if str == "ALREADYEXISTS\n" {
		return false
	}
	fmt.Println("Unknown message ", str)
	return false
}
func runAnalysis(host string) string {
	conn, err := net.DialTimeout("tcp", WORKERHOSTPORT, time.Second)
	if err != nil {
		if neterr, ok := err.(net.Error); ok && neterr.Timeout()  {
			fmt.Println("Timeout con worker nameserver. Reintentando en 1s.")
			conn, err = net.DialTimeout("tcp", WORKERHOSTPORT, time.Second)
			if err != nil {
				return "TIMEOUT"
			}
		}
		return "Error en conexión al worker nameserver"
	}
	defer conn.Close()
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
	str, sent := storage.SendQuery(query)
	if !sent {
		return "No se pudo enviar el comando al servidor"
	}
	//Procesar respuesta
	var response storage.ResultsResponse
	err := json.Unmarshal([]byte(str), &response)
	if err != nil {
		fmt.Printf("Error decoding:%s", err)
	}
	str = prettyPrintResults(response.Node)

	//fmt.Println("Lo que armo pretty print:", str)

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
		panic("No se pudo levantar el demonio")
	}
	defer server.Close()
	fmt.Printf("Listening on port 11001")
	for {
		conn, err := server.Accept()
		if err != nil {
			fmt.Println("No fue posible recibir la conexion: ", err)
			continue
		}
		buf := bufio.NewScanner(conn)
		for buf.Scan() {
			send(conn, processRequest(buf.Text()) + "\n")
		}
		conn.Close()
	}



	//crear la estructura en la DB
}