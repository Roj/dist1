package worker

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
	"errors"
)
const DEFAULT_FTP_PORT = 21
const (
	noError = iota
	timeoutError
	noListener
)

// Inicia la conexión de control al host destino en puerto 21
// e inicia la escucha en un puerto indicado para la conexion de datos.
// Devuelve la conexión de control, el buffer de control y el escuchante
// en el puerto aleatorio.

func setupFTP(host string, local_port int) (net.Conn, *bufio.Reader, net.Listener, int) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, DEFAULT_FTP_PORT), time.Second)
	if err != nil {
		fmt.Println(err)
		return nil, nil, nil, timeoutError
	}
	lhost, lport, _ := net.SplitHostPort(conn.LocalAddr().String())
	fmt.Printf("Local address is is %s:%s\n", lhost, lport)
	connbuf := bufio.NewReader(conn)
	waitForCode(connbuf, "220")

	dataserver, err := net.Listen("tcp", fmt.Sprintf(":%d", local_port))
	if err != nil {
		fmt.Println("No se pudo levantar la conexion de datos:", err)
		defer conn.Close()
		return nil, nil, nil, noListener
	}
	//time.Sleep(2*time.Second)
	send(conn, "USER anonymous\r\n")
	send(conn, "SYST\r\n")
	waitForCode(connbuf, "230")

	return conn, connbuf, dataserver, noError
}

func waitForCode(connbuf *bufio.Reader, code string) {
	for {
		str, err := connbuf.ReadString('\n')
		if err != nil {
			panic("Error de parseo")
		}
		if strings.Contains(str, code) {
			return
		}
	}
}
// Envía el comando cmd (no es necesario el \n) por la conexión de
// control conn, indicando que mande los datos al puerto port.
// Espera a que el servidor indique que se transfirieron los datos.
func sendFTPCommand(cmd string, conn net.Conn, connbuf *bufio.Reader, port int) error{
	first_octet := port / 256
	second_octet := port - first_octet*256

	send(conn, fmt.Sprintf("PORT 127,0,0,1,%d,%d\r\n", first_octet, second_octet))
	waitForCode(connbuf, "200")
	send(conn, fmt.Sprintf("%s\r\n", cmd))
	//Now we wait until the command is complete (226)
	for {
		str, _ := connbuf.ReadString('\n')
		/*if len(str) > 0 {
			fmt.Printf("<%s", str)
		}*/
		if strings.Contains(str, "226") {
			break
		}
		if strings.Contains(str, "425") {
			return errors.New("Could not build data connection")
		}
	}
	return nil
}
