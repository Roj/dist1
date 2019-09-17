package main
import (
	"fmt"
	"net"
	"bufio"
	"encoding/json"
	"os"
)

func add_dir(dict ServerMap, server string, node Node) {
	fmt.Printf("Es una escritura del host %s sobre el directorio %s\n", server, node.Path)
	// It always exists
	encoded, _ := json.Marshal(node)
	fmt.Printf("add_dir a escribir: %s\n", encoded)

	dbnode := get_subdir(node.Path, dict[server].root_dir)
	encoded, _ = json.Marshal(node)
	fmt.Printf("add_dir -- en la DB: %s\n", encoded)
	dbnode.Files = node.Files
	update_parents_size(node.Path, dict[server].root_dir, node.Size)
}
func get_dir(dict ServerMap, server string, path string) {
	//TODO: response
	fmt.Printf("Es una consulta del host %s sobre el directorio %s\n", server, path)
}
func send(conn net.Conn, s string) {
	fmt.Printf(">%s\n", s)
	conn.Write([]byte(s))
}
// Process a given query and produces a response to be sent to the
// client (without \n)
func process_query(query Query, servermap ServerMap) string {
	//TODO: queryResponse
	fmt.Printf("El nodo recibido es el de path %s\n", query.Node.Path)
	switch query.Type {
	case read:
		get_dir(servermap, query.Hostname, query.Node.Path)
	case write:
		add_dir(servermap, query.Hostname, query.Node)
	case newserver:
		if _, ok := servermap[query.Hostname]; ok {
			return "ALREADYEXISTS"
		}
		servermap[query.Hostname] = &Server{
			query.Hostname, false, &Node{dir, 0, "/", make(NodeMap)}}

	case finishserver:
		servermap[query.Hostname].finished = true
	}
	//encoded, _ := json.Marshal(servermap[query.Hostname].root_dir)
	//fmt.Printf("Ahora el servidor queda como: %s\n", encoded)
	fmt.Printf("El tamaño actual del host %s es %d\n", query.Hostname, servermap[query.Hostname].root_dir.Size)
	return "OK"
}
func main() {
	//Setup listen socket
	dblisten, err := net.Listen("tcp", ":11000")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Listening on port 11000")
	servermap := make(ServerMap)
	for {
		fmt.Println("Esperando conexion..")
		conn, err := dblisten.Accept()
		fmt.Println("[DB] Recibida conexion")
		if err != nil {
			fmt.Println("Error!!")
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("Leyendo...")
		msg, err := bufio.NewReader(conn).ReadString('\n')
		fmt.Println("Leido!")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		msg = msg[:len(msg)-1] //trailing \n
		// Unserialize message
		var query Query
		err = json.Unmarshal([]byte(msg), &query)
		response := process_query(query, servermap)
		send(conn, fmt.Sprintf("%s\n", response))
		conn.Close()
	}
}