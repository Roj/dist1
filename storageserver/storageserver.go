package main
import (
	"fmt"
	"net"
	"bufio"
	"encoding/json"
	"os"
	"../storage"
)

func get_dir(dict storage.ServerMap, host string, path string) string {
	fmt.Printf("Es una consulta del host %s sobre el directorio %s\n", host, path)

	var response storage.ResultsResponse
	if server, ok := dict[host]; ok {
		response.Finished = server.Finished
		response.Node = storage.Node{storage.File, 0, "/", make(storage.NodeMap)}
		if server.Finished {
			node := storage.Get_subdir(path, server.Root_dir)
			shallow_node := storage.Shallow_copy(*node)
			response.Node = shallow_node
		}
	} else {
		response.Finished = false
		response.Node = storage.Node{storage.File, -1, "/", make(storage.NodeMap)}
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
func process_query(query storage.Query, servermap storage.ServerMap) string {
	//TODO: queryResponse
	//fmt.Printf("El nodo recibido es el de path %s\n", query.Node.Path)
	switch query.Type {
	case storage.Read:

		return get_dir(servermap, query.Hostname, query.Node.Path)
	case storage.Write:
		fmt.Printf("Es una escritura del host %s sobre el directorio %s\n", query.Hostname, query.Node.Path)
		storage.Add_dir(servermap, query.Hostname, query.Node)
	case storage.Newserver:
		if _, ok := servermap[query.Hostname]; ok {
			return "ALREADYEXISTS"
		}
		servermap[query.Hostname] = &storage.Server{
			query.Hostname, false, &storage.Node{storage.Dir, 0, "/", make(storage.NodeMap)}}

	case storage.Finishserver:
		fmt.Printf("Terminando server %s\n", query.Hostname)
		servermap[query.Hostname].Finished = true
	}
	//encoded, _ := json.Marshal(servermap[query.Hostname].root_dir)
	//fmt.Printf("Ahora el servidor queda como: %s\n", encoded)
	fmt.Printf("El tamaño actual del host %s es %d\n", query.Hostname, servermap[query.Hostname].Root_dir.Size)
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
	servermap := make(storage.ServerMap)
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
		var query storage.Query
		err = json.Unmarshal([]byte(msg), &query)
		response := process_query(query, servermap)
		send(conn, fmt.Sprintf("%s\n", response))
		conn.Close()
	}
}