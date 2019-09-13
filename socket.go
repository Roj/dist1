package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func send(conn net.Conn, s string) {
	fmt.Printf(">%s", s)
	conn.Write([]byte(s))
}
func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:21")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	lhost, lport, _ := net.SplitHostPort(conn.LocalAddr().String())
	fmt.Printf("Local address is is %s:%s\n", lhost, lport)
	connbuf := bufio.NewReader(conn)
	send(conn, "USER joaquintz\n")
	send(conn, "PASS xxxxxxxxx\n")
	send(conn, "EPSV\n")
	send(conn, "LIST -AQ\n")
	dataport := 1
	for {
		str, err := connbuf.ReadString('\n')
		if len(str) > 0 {
			fmt.Printf("<%s", str)
		}
		if err != nil {
			break;
		}
		if strings.Contains(str, "229") {
			fmt.Sscanf(str, "229 Extended Passive Mode OK (|||%d|)", &dataport)
			fmt.Printf("Read 229 - port is %d\n", dataport)
			break
		}
	}
	fmt.Printf("Data port is %d\n", dataport)

	dconn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", dataport))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	dconnbuf := bufio.NewScanner(dconn)
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
	//So now we can read the result
	for dconnbuf.Scan() {
		fmt.Println(dconnbuf.Text(), "<-END->")
	}
	if err := dconnbuf.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}
