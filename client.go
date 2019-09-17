package main

import (
	"fmt"
	"net"
	"os"
)

func send(conn net.Conn, s string) {
	fmt.Printf("Sending: %s\n", s)
	conn.Write([]byte(s))
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:11001")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	send(conn, "analyze ftpserver\n")
}