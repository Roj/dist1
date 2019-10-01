package storage
import (
	"fmt"
	"net"
	"bufio"
	"encoding/json"
	"os"
)

// Process a given query and produces a response to be sent to the
// client (without \n)
func process_query(query Query, servermap ServerMap) string {
	//TODO: queryResponse
	//fmt.Printf("El nodo recibido es el de path %s\n", query.Node.Path)
	switch query.Type {
	case Read:
		fmt.Printf("Es una consulta del host %s sobre el directorio %s\n", query.Hostname, query.Node.Path)
		response := Get_dir(servermap, query.Hostname, query.Node.Path)
		encoded, _ := json.Marshal(response)
		return fmt.Sprintf("%s",encoded)
	case Write:
		fmt.Printf("Es una escritura del host %s sobre el directorio %s\n", query.Hostname, query.Node.Path)
		Add_dir(servermap, query.Hostname, query.Node)
	case Newserver:
		if _, ok := servermap[query.Hostname]; ok {
			return "ALREADYEXISTS"
		}
		servermap[query.Hostname] = &Server{
			query.Hostname, false, &Node{Dir, 0, "/", make(NodeMap)}}

	case Finishserver:
		fmt.Printf("Terminando server %s\n", query.Hostname)
		servermap[query.Hostname].Finished = true
	}
	//encoded, _ := json.Marshal(servermap[query.Hostname].root_dir)
	//fmt.Printf("Ahora el servidor queda como: %s\n", encoded)
	fmt.Printf("El tamaño actual del host %s es %d\n", query.Hostname, servermap[query.Hostname].Root_dir.Size)
	return "OK"
}
func StartServer() net.Listener {
	//Setup listen socket
	dblisten, err := net.Listen("tcp", ":11000")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Listening on port 11000")
	return dblisten
}
func ProcessRequests(dblisten net.Listener) {
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