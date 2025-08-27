package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var _ = os.Exit

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Start to bind to port 6379")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	var parts []string

	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println("Received command: " + text)

		// Collect RESP parts
		parts = append(parts, text)

		// If we have enough parts for a simple command
		if len(parts) >= 3 {
			cmd := strings.ToUpper(parts[2])
			if cmd == "PING" {
				conn.Write([]byte("+PONG\r\n"))
				parts = nil // reset for next command
			} else if cmd == "ECHO" && len(parts) >= 4 {
				// parts[3] is the argument
				arg := parts[3]
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				conn.Write([]byte(resp))
				parts = nil // reset for next command
			} else if len(parts) >= 4 {
				conn.Write([]byte("-Error invalid command\r\n"))
				parts = nil // reset for next command
			}
		}
	}
}
