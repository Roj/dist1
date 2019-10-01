package worker
import (
	"net"
	"fmt"
	"bufio"
	"os"
)
func ProcessRequests(ns net.Listener, lCh chan string) {
	j := 0
	for {
		fmt.Println("Recibida conexion nueva #", j)
		j = j + 1
		conn, err := ns.Accept()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		msg, _ := bufio.NewReader(conn).ReadString('\n')
		lCh <- msg[:len(msg)-1] //remove trailing \n
		conn.Close()
	}
}
func NameServer(lCh chan string) net.Listener {
	//Set up nameserver
	ns, err := net.Listen("tcp", ":50000")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Listening on port 50000")

	return ns
}