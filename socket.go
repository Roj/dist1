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
)

func send(conn net.Conn, s string) {
	fmt.Printf(">%s", s)
	conn.Write([]byte(s))
}

// Inicia la conexión de control al host destino en puerto 21
// e inicia la escucha en un puerto aleatorio para la conexion de datos.
// Devuelve la conexión de control, el buffer de control, el escuchante
// en el puerto aleatorio y el puerto.
// TODO: credenciales
func setup_ftp(host string) (net.Conn, *bufio.Reader, net.Listener, int) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:21", host))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	lhost, lport, _ := net.SplitHostPort(conn.LocalAddr().String())
	fmt.Printf("Local address is is %s:%s\n", lhost, lport)
	connbuf := bufio.NewReader(conn)
	send(conn, "USER joaquintz\n")
	send(conn, "PASS charmander12!\n")

	port := rand.Int()%1000 + 9000
	dataserver, err := net.Listen("tcp", fmt.Sprintf(":%d",port))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return conn, connbuf, dataserver, port
}
// Envía el comando cmd (no es necesario el \n) por la conexión de
// control conn, indicando que mande los datos al puerto port.
// Espera a que el servidor indique que se transfirieron los datos.
func send_command(cmd string, conn net.Conn, connbuf *bufio.Reader, port int) {
	first_octet := port/256
	second_octet := port - first_octet*256
	send(conn, fmt.Sprintf("PORT 127,0,0,1,%d,%d\n", first_octet, second_octet))
	for {
		str, err := connbuf.ReadString('\n')
		if len(str) > 0 {
			fmt.Printf("<%s", str)
		}
		if err != nil {
			break;
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
			if strings.Contains(str, "226") {
				break;
			}
		}
	}
}
// Procesa los resultados de un ls que recibio el escuchante.
// Partiendo desde path, agrega los nodos nuevos
// al árbol que arranca en root_dir y encola los directorios a procesar en
// queue.
// TODO: que tal si devuelve la queue en vez de modificar la vieja? Tal vez
// sirve para jerarquizar y pasar por niveles.
func run_ls(path string, root_dir *Node, queue *[]string, dataserver net.Listener) {
	current_dir := root_dir
	if path != "" {
		current_dir = get_subdir(path, root_dir)
	}
	//So now we can read the result
	i := 0

	dconn, err := dataserver.Accept()
	if err != nil {
		// handle error
	}
	dconnbuf := bufio.NewScanner(dconn)
	for dconnbuf.Scan() {
		i = i +1
		if i == 1 {
			continue
		}
		_str, _node := parse_ls_line(dconnbuf.Text())
		if _node.nodetype == dir {
			*queue = append(*queue, fmt.Sprintf("%s/%s",path,_str))
		}
		//fmt.Println(_node.nodetype, _str, _node.size)
		current_dir.files[_str] = &_node
		current_dir.size += _node.size
		if path != "" {
			root_dir.size += _node.size
		}
	}
}
func run_analysis(host string) {
	conn, connbuf, dataserver, port := setup_ftp(host)
	root_dir := Node{dir, 0, make(NodeMap)}

	send_command("LIST -AQ /", conn, connbuf, port)

	queue := make([]string, 0)
	run_ls("", &root_dir, &queue, dataserver)

	for j := 0; j < 10; j++ {
		head := queue[0]
		queue = queue[1:]
		send_command(fmt.Sprintf("LIST -AQ %s", head), conn, connbuf, port)
		fmt.Println("Passing it head - ", head)
		run_ls(head, &root_dir, &queue, dataserver)
	}
	fmt.Printf("El tamaño total es %d\n", root_dir.size)
	fmt.Printf("%q\n", queue)
}
//Navegar la estructura para actualizar los sizes.
//Armar el wrap worker _por tcp? nameserver?
//Empezar a pensar en goroutines
func worker(i int, linkChan chan string, wg *sync.WaitGroup) {
	rand.Seed(time.Now().UnixNano()+int64(i)*27)
	defer wg.Done()
	fmt.Println("Started worker ", i)
	for url := range linkChan {
		fmt.Println("Recibido trabajo! ",url)
		run_analysis(url)
	}
}
func main() {
	lCh := make(chan string)
	wg := new(sync.WaitGroup)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker(i, lCh, wg)
	}
	urls := []string{"127.0.0.1", "127.0.0.2"}
	for _, link := range urls {
		lCh <- link
	}
	close(lCh)
	wg.Wait()
}