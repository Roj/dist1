package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"math/rand"
	"sync"
	"time"
	"encoding/json"
)
// Para manejar los recursos que utiliza el analisis de
// un servidor necesitamos un lock. Esto también, en
// la prueba de concepto, permite fijarnos cuándo terminó
// el análisis.
type ServerResources struct {
	nqueued int
	nthreads int
	lock *sync.Mutex
}
type ServerResourcesMap map[string]*ServerResources

const MAXTHREADSANDQUEUED = 1

// Inicia los recursos para un servidor FTP si no estaba ya disponible
// Poscondicion: existe en el mapa la key host, hay al menos un task
// queued.
func init_serverresources(host string, dict ServerResourcesMap) {
	if _, ok := dict[host]; !ok {
		dict[host] = &ServerResources{1, 0, &sync.Mutex{}}
	}
}

// Wrap sencillo de write para poder imprimir e ir viendo.
func send(conn net.Conn, s string) {
	fmt.Printf(">%s", s)
	conn.Write([]byte(s))
}

// Inicia la conexión de control al host destino en puerto 21
// e inicia la escucha en un puerto indicado para la conexion de datos.
// Devuelve la conexión de control, el buffer de control y el escuchante
// en el puerto aleatorio.
// TODO: credenciales
func setup_ftp(host string, port int) (net.Conn, *bufio.Reader, net.Listener) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:21", host), time.Second)
	if err != nil {
		fmt.Println(err)
		return setup_ftp(host, port)
		os.Exit(1)
	}
	lhost, lport, _ := net.SplitHostPort(conn.LocalAddr().String())
	fmt.Printf("Local address is is %s:%s\n", lhost, lport)
	connbuf := bufio.NewReader(conn)
	send(conn, "USER joaquintz\n")
	send(conn, "PASS charmander12!\n")


	dataserver, err := net.Listen("tcp", fmt.Sprintf(":%d",port))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return conn, connbuf, dataserver
}

// Envía el comando cmd (no es necesario el \n) por la conexión de
// control conn, indicando que mande los datos al puerto port.
// Espera a que el servidor indique que se transfirieron los datos.
func send_command(cmd string, conn net.Conn, connbuf *bufio.Reader, port int) {
	first_octet := port/256
	second_octet := port - first_octet*256
	//TODO: ver si tira error, reintentar, backoff, agregar al queue-socket.
	//TODO IP
	send(conn, fmt.Sprintf("PORT 127,0,0,1,%d,%d\n", first_octet, second_octet))
	for {
		str, err := connbuf.ReadString('\n')
		if len(str) > 0 {
			fmt.Printf("<%s", str)
		}
		if err != nil {
			fmt.Printf("Error: %s", err)
			return
		}
		if strings.Contains(str, "200") {
			break
		}
	}
	send(conn, fmt.Sprintf("%s\n", cmd))
	//Now we wait until the command is complete (226)
	for {
		str, _ := connbuf.ReadString('\n')
		if len(str) > 0 {
			fmt.Printf("<%s", str)
		}
		if strings.Contains(str, "226") {
			break;
		}
		if strings.Contains(str, "425") {
			fmt.Printf("Could not build data connection, retrying in one second.\n")
			time.Sleep(time.Second)
			send_command(cmd, conn, connbuf, port)
			return
		}
	}
}

// Procesa los resultados de un ls que recibio el escuchante.
// Partiendo desde path, agrega los nodos nuevos
// al árbol que arranca en root_dir y encola los directorios a procesar en
// queue.
func parse_ls(path string, dataserver net.Listener) (Node, []string) {
	node := Node{dir, 0, path, make(NodeMap)}
	queue := make([]string, 0)
	dconn, err := dataserver.Accept()
	if err != nil {
		// handle error
	}
	dconnbuf := bufio.NewScanner(dconn)
	i := 0
	for dconnbuf.Scan() {
		i = i +1
		if i == 1 {
			continue
		}
		_str, _node := parse_ls_line(dconnbuf.Text())
		_node.Path = fmt.Sprintf("%s%s/",path,_str)  //TODO if not dir do not add /

		node.Files[_str] = &_node
		node.Size += _node.Size

		if _node.Type == dir {
			fmt.Printf("Encolamos %s\n", _node.Path)
			queue = append(queue, _node.Path)
		}
		fmt.Printf("Path=%s, _str = %s, node = %q\n", path, _str, _node)
		//fmt.Println(_node.nodetype, _str, _node.size)


	}
	return node, queue
}
func update_db(host string, node Node) {
	query := Query{write, host, node}
	bytejson, err := json.Marshal(query)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	conn, err := net.Dial("tcp", "127.0.0.1:11000")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer conn.Close()
	for _, _ = range []int{1} {
		send(conn, fmt.Sprintf("%s\n", bytejson))
	}
	connbuf := bufio.NewReader(conn)
	//Now we wait until the command is complete (226)
	for {
		str, _ := connbuf.ReadString('\n')
		if str == "OK\n" {
			fmt.Println("POBRE VICKY")
			break
		} else {
			fmt.Println("Unknown message ", str)
		}
	}
}

func register_thread(host string, dict ServerResourcesMap) {
	init_serverresources(host, dict)
	hostres := dict[host]
	hostres.lock.Lock()
	fmt.Printf("[THREAD] Registering thread!!")
	hostres.nqueued  = hostres.nqueued  - 1
	hostres.nthreads = hostres.nthreads + 1
	hostres.lock.Unlock()
}

func unregister_thread(host string, dict ServerResourcesMap) {
	hostres := dict[host]
	hostres.lock.Lock()
	fmt.Printf("[THREAD] UNRegistering thread!!")
	hostres.nthreads = hostres.nthreads - 1
	hostres.lock.Unlock()
}
func distribute_jobs(queue []string, host string, hostres *ServerResources) []string {
	hostres.lock.Lock()
	for i:=0; i < MAXTHREADSANDQUEUED - hostres.nthreads - hostres.nqueued; i++ {
		if len(queue) > 0 {
			subpath := queue[0]
			queue = queue[1:]
			hostres.nqueued = hostres.nqueued + 1
			also_process(fmt.Sprintf("%s %s\n", host, subpath))
		}
	}
	hostres.lock.Unlock()
	return queue
}
func run_analysis(host string, path string, idworker int, resourcesmap ServerResourcesMap) {
	register_thread(host, resourcesmap)
	hostres := resourcesmap[host]
	port := 9100 + idworker
	conn, connbuf, dataserver := setup_ftp(host, port)

	it := 0
	next_thread_check := 0
	queue := []string{path}
	for len(queue) > 0 {
		cpath := queue[0]
		queue = queue[1:]
		send_command(fmt.Sprintf("LIST -AQ %s", cpath), conn, connbuf, port)
		node, new_queue := parse_ls(cpath, dataserver)
		update_db(host, node)

		// Skip /proc/ because it doesn't make sense and causes special errors
		queue = filter_strlist(append(queue, new_queue...), "/proc/")
		fmt.Printf("Adjusted queue is %s\n", queue)

		// See if we can parallelize
		if it == next_thread_check {
			next_thread_check = it + len(new_queue) // check after cleaning out this level
			queue = distribute_jobs(queue, host, hostres)
		}
		it = it + 1
	}

	send(conn, "BYE\n")
	conn.Close()
	dataserver.Close()
	unregister_thread(host, resourcesmap)
}
func also_process(msg string) {
	fmt.Printf("Also process.. -- '%s'", msg)
	conn, err := net.DialTimeout("tcp", ":50000", time.Second)
	if err != nil {
		fmt.Println(err)

		fmt.Printf("Maybe timed out... trying again")
		also_process(msg)
		return
		//TODO

		os.Exit(1)
	}
	defer conn.Close()
	send(conn, msg)
	fmt.Println("Cerrando alsoprocess")
}

func worker(i int, linkChan chan string, wg *sync.WaitGroup, resourcesmap ServerResourcesMap) {
	rand.Seed(time.Now().UnixNano()+int64(i)*27)
	defer wg.Done()
	fmt.Println("Started worker ", i)
	for dest := range linkChan {

		parts := strings.Split(dest, " ")
		host := parts[0]
		path := "/"
		if len(parts) == 2 {
			path = parts[1]
		}
		fmt.Println("Recibido trabajo! ",host, " y path ", path, " en worker ", i)
		run_analysis(host, path, i, resourcesmap)
		fmt.Println("Finalizado el procesaje")
	}
}
func main() {
	//Set up nameserver
	nameserver, err := net.Listen("tcp", ":50000")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Listening on port 50000")

	//Set up workers
	resourcesmap := make(ServerResourcesMap)
	lCh := make(chan string, 10000)
	wg := new(sync.WaitGroup)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker(i, lCh, wg, resourcesmap)
	}

	//Process requests
	j := 0
	for {
		fmt.Println("Recibida conexion nueva #", j)
		j = j + 1
		conn, err := nameserver.Accept()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		msg, _ := bufio.NewReader(conn).ReadString('\n')
		lCh <- msg[:len(msg)-1] //remove trailing \n
		conn.Close()

	}
	fmt.Println("Oops?")
	close(lCh)
	wg.Wait()
}