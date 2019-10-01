package storage
import (
	"fmt"
	"net"
	"bufio"
	"encoding/json"
	"sync"
	"io/ioutil"
)
type persistenceResources struct {
	lock *sync.Mutex
	persistedSize int
}
type persistenceResourcesMap map[string]*persistenceResources
func persistServer(servermap ServerMap, presmap persistenceResourcesMap, host string) {
	resources := presmap[host]
	resources.lock.Lock()
	defer resources.lock.Unlock()
	encoded, _ := json.Marshal(servermap[host].Root_dir)
	err := ioutil.WriteFile(host, encoded, 0644)
	if err != nil {
		panic(err)
	}
	resources.persistedSize = servermap[host].Root_dir.Size
}
// Process a given query and produces a response to be sent to the
// client (without \n)
func processQuery(query Query, servermap ServerMap, presmap persistenceResourcesMap) string {
	//TODO: queryResponse
	//fmt.Printf("El nodo recibido es el de path %s\n", query.Node.Path)
	switch query.Type {
	case Read:
		fmt.Printf("Es una consulta del host %s sobre el directorio %s\n", query.Hostname, query.Node.Path)
		response := GetDir(servermap, query.Hostname, query.Node.Path)
		encoded, _ := json.Marshal(response)
		return fmt.Sprintf("%s",encoded)
	case Write:
		fmt.Printf("Es una escritura del host %s sobre el directorio %s\n", query.Hostname, query.Node.Path)
		AddDir(servermap, query.Hostname, query.Node)
		if servermap[query.Hostname].Root_dir.Size > presmap[query.Hostname].persistedSize {
			persistServer(servermap, presmap, query.Hostname)
		}
	case Newserver:
		if _, ok := servermap[query.Hostname]; ok {
			return "ALREADYEXISTS"
		}
		servermap[query.Hostname] = &Server{
			query.Hostname, false, &Node{Dir, 0, "/", make(NodeMap)}}
		presmap[query.Hostname] = &persistenceResources{&sync.Mutex{}, 0}

	case Finishserver:
		fmt.Printf("Terminando server %s\n", query.Hostname)
		servermap[query.Hostname].Finished = true
		persistServer(servermap, presmap, query.Hostname)
	}
	//encoded, _ := json.Marshal(servermap[query.Hostname].root_dir)
	//fmt.Printf("Ahora el servidor queda como: %s\n", encoded)
	fmt.Printf("El tamaño actual del host %s es %d\n", query.Hostname, servermap[query.Hostname].Root_dir.Size)
	return "OK"
}
func StartServer() (net.Listener, error) {
	//Setup listen socket
	dblisten, err := net.Listen("tcp", ":11000")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Printf("Listening on port 11000")
	return dblisten, nil
}
func ProcessRequests(dblisten net.Listener) {
	servermap := make(ServerMap)
	presmap := make(persistenceResourcesMap)
	for {
		//fmt.Println("Esperando conexion..")
		conn, err := dblisten.Accept()
		//fmt.Println("[DB] Recibida conexion")
		if err != nil {
			fmt.Println("No se pudo aceptar la conexion: ", err)
			continue
		}

		msg, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			fmt.Println("Problema leyendo mensaje:", err)
			conn.Close()
			continue
		}
		msg = msg[:len(msg)-1] //trailing \n
		// Unserialize message
		var query Query

		err = json.Unmarshal([]byte(msg), &query)
		if err != nil {
			fmt.Println("No se pudo de-serializar el mensaje: ", err)
		} else {
			response := processQuery(query, servermap, presmap)
			send(conn, fmt.Sprintf("%s\n", response))
		}
		fmt.Println("Cerrando conexion.")
		conn.Close()
	}
}