package worker
import (
	"net"
	"fmt"
	"bufio"
)
func ProcessRequests(ns net.Listener, lCh chan string) {
	j := 0
	for {
		j = j + 1
		conn, err := ns.Accept()
		fmt.Println("Recibida conexion nueva #", j)
		if err != nil {
			fmt.Println("No se pudo aceptar la conexion: ", err)
			fmt.Println("Ignorando..")
			continue
		}

		msg, _ := bufio.NewReader(conn).ReadString('\n')
		lCh <- msg[:len(msg)-1] //remove trailing \n
		conn.Close()
	}
}
func NameServer(lCh chan string) (net.Listener, error) {
	//Set up nameserver
	ns, err := net.Listen("tcp", ":50000")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Printf("Listening on port 50000")

	return ns, nil
}