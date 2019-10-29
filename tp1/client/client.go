package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func send(conn net.Conn, s string) {
	fmt.Printf(">%s\n", s)
	buf := []byte(s)
	count := 0
	for count < len(buf) {
		byteSent, err := conn.Write(buf[count:])
		count += byteSent
		if err == nil {
			return
		}
	}
}

func main() {
	args := os.Args[1:]
	conn, err := net.Dial("tcp", "daemon:11001")
	if err != nil {
		fmt.Println(err)
		panic("No se pudo conectar con el demonio")
	}
	defer conn.Close()
	if args[0] == "analyze" {
		send(conn, fmt.Sprintf("analyze %s\n", args[1]))
	} else if args[0] == "summary" {
		send(conn, fmt.Sprintf("summary %s %s\n", args[1], args[2]))
	} else {
		fmt.Printf("No entendido: %s\n", args)
	}

	dconnbuf := bufio.NewScanner(conn)

	for dconnbuf.Scan() {
		str := dconnbuf.Text()
		fmt.Println(str)
		if len(str) == 0 {
			break
		}
	}
}
