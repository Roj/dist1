package worker
import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)
const (
	noError = iota
	timeoutError
)
// Inicia la conexión de control al host destino en puerto 21
// e inicia la escucha en un puerto indicado para la conexion de datos.
// Devuelve la conexión de control, el buffer de control y el escuchante
// en el puerto aleatorio.
// TODO: credenciales
func setup_ftp(host string, port int) (net.Conn, *bufio.Reader, net.Listener, int)  {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:21", host), time.Second)
	if err != nil {
		fmt.Println(err)
		return nil, nil, nil, timeoutError
	}
	lhost, lport, _ := net.SplitHostPort(conn.LocalAddr().String())
	fmt.Printf("Local address is is %s:%s\n", lhost, lport)
	connbuf := bufio.NewReader(conn)
	for {
		str, _ := connbuf.ReadString('\n')
		if strings.Contains(str, "220") {
			break
		}
	}
	dataserver, err := net.Listen("tcp", fmt.Sprintf(":%d",port))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	//time.Sleep(2*time.Second)
	send(conn, "USER anonymous\r\n")
	send(conn, "SYST\r\n")
	for {
		str, err := connbuf.ReadString('\n')
		/*if len(str) > 0 {
			fmt.Printf("<%s", str)
		}*/
		if err != nil {
			fmt.Printf("Error: %s", err)
			os.Exit(1)
		}
		if strings.Contains(str, "230") {
			break
		}
	}

	return conn, connbuf, dataserver, noError
}

// Envía el comando cmd (no es necesario el \n) por la conexión de
// control conn, indicando que mande los datos al puerto port.
// Espera a que el servidor indique que se transfirieron los datos.
func send_command(cmd string, conn net.Conn, connbuf *bufio.Reader, port int) {
	first_octet := port/256
	second_octet := port - first_octet*256
	//TODO: ver si tira error, reintentar, backoff, agregar al queue-socket.
	//TODO IP
	send(conn, fmt.Sprintf("PORT 127,0,0,1,%d,%d\r\n", first_octet, second_octet))
	for {
		str, err := connbuf.ReadString('\n')
		/*if len(str) > 0 {
			fmt.Printf("<%s", str)
		}*/
		if err != nil {
			fmt.Printf("Error: %s", err)
			return
		}
		if strings.Contains(str, "200") {
			break
		}
	}
	send(conn, fmt.Sprintf("%s\r\n", cmd))
	//Now we wait until the command is complete (226)
	for {
		str, _ := connbuf.ReadString('\n')
		/*if len(str) > 0 {
			fmt.Printf("<%s", str)
		}*/
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