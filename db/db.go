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
	dbnode := get_subdir(node.Path, dict[server].root_dir)
	/*encoded, _ := json.Marshal(node)
	fmt.Printf("add_dir a escribir: %s\n", encoded)
	encoded, _ = json.Marshal(node)
	fmt.Printf("add_dir -- en la DB: %s\n", encoded)*/
	dbnode.Files = node.Files
	update_parents_size(node.Path, dict[server].root_dir, node.Size)
}
func make_shallow_node(n Node) Node {
	shallow := Node{n.Type, n.Size, n.Path, make(NodeMap)}
	for k, v := range n.Files {
		shallow.Files[k] = &Node{v.Type, v.Size, v.Path, make(NodeMap)}
	}
	return shallow
}
func get_dir(dict ServerMap, host string, path string) string {
	fmt.Printf("Es una consulta del host %s sobre el directorio %s\n", host, path)

	var response ResultsResponse
	if server, ok := dict[host]; ok {
		response.Finished = server.finished
		response.Node = Node{file, 0, "/", make(NodeMap)}
		if server.finished {
			node := get_subdir(path, server.root_dir)
			shallow_node := make_shallow_node(*node)
			response.Node = shallow_node
		}
	} else {
		response.Finished = false
		response.Node = Node{file, -1, "/", make(NodeMap)}
	}

	encoded, _ := json.Marshal(response)
	return fmt.Sprintf("%s",encoded)

}
func send(conn net.Conn, s string) {
	fmt.Printf(">%s\n", s)
	conn.Write([]byte(s))
}
// Process a given query and produces a response to be sent to the
// client (without \n)
func process_query(query Query, servermap ServerMap) string {
	//TODO: queryResponse
	//fmt.Printf("El nodo recibido es el de path %s\n", query.Node.Path)
	switch query.Type {
	case read:

		return get_dir(servermap, query.Hostname, query.Node.Path)
	case write:
		add_dir(servermap, query.Hostname, query.Node)
	case newserver:
		if _, ok := servermap[query.Hostname]; ok {
			return "ALREADYEXISTS"
		}
		servermap[query.Hostname] = &Server{
			query.Hostname, false, &Node{dir, 0, "/", make(NodeMap)}}

	case finishserver:
		fmt.Printf("Terminando server %s\n", query.Hostname)
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
		//fmt.Println("Esperando conexion..")
		conn, err := dblisten.Accept()
		//fmt.Println("[DB] Recibida conexion")
		if err != nil {
			fmt.Println("Error!!")
			fmt.Println(err)
			os.Exit(1)
		}
		//fmt.Println("Leyendo...")
		msg, err := bufio.NewReader(conn).ReadString('\n')
		//fmt.Println("Leido!")
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