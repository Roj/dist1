package storage
import (
	"fmt"
	"net"
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"io/ioutil"
)
type persistence_resources struct {
	lock *sync.Mutex
	persisted_size int
}
type persistence_resources_map map[string]*persistence_resources
func persist_server(servermap ServerMap, presmap persistence_resources_map, host string) {
	resources := presmap[host]
	resources.lock.Lock()
	defer resources.lock.Unlock()
	encoded, _ := json.Marshal(servermap[host].Root_dir)
	err := ioutil.WriteFile(host, encoded, 0644)
	if err != nil {
		panic(err)
	}
	resources.persisted_size = servermap[host].Root_dir.Size
}
// Process a given query and produces a response to be sent to the
// client (without \n)
func process_query(query Query, servermap ServerMap, presmap persistence_resources_map) string {
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
		if servermap[query.Hostname].Root_dir.Size > presmap[query.Hostname].persisted_size {
			persist_server(servermap, presmap, query.Hostname)
		}
	case Newserver:
		if _, ok := servermap[query.Hostname]; ok {
			return "ALREADYEXISTS"
		}
		servermap[query.Hostname] = &Server{
			query.Hostname, false, &Node{Dir, 0, "/", make(NodeMap)}}
		presmap[query.Hostname] = &persistence_resources{&sync.Mutex{}, 0}

	case Finishserver:
		fmt.Printf("Terminando server %s\n", query.Hostname)
		servermap[query.Hostname].Finished = true
		persist_server(servermap, presmap, query.Hostname)
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
	presmap := make(persistence_resources_map)
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
		response := process_query(query, servermap, presmap)
		send(conn, fmt.Sprintf("%s\n", response))
		conn.Close()
	}
}