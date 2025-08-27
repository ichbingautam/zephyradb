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

var store = make(map[string]string)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	var parts []string

	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println("Received command: " + text)

		parts = append(parts, text)

		// Handle ECHO command with 5 parts
		if len(parts) == 5 && strings.ToUpper(parts[2]) == "ECHO" {
			arg := parts[4]
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			conn.Write([]byte(resp))
			parts = nil
		} else if len(parts) == 3 && strings.ToUpper(parts[2]) == "PING" {
			conn.Write([]byte("+PONG\r\n"))
			parts = nil
		} else if len(parts) == 5 && strings.ToUpper(parts[2]) == "SET" {
			key := parts[3]
			value := parts[4]
			store[key] = value
			conn.Write([]byte("+OK\r\n"))
			parts = nil
		} else if len(parts) == 4 && strings.ToUpper(parts[2]) == "GET" {
			key := parts[3]
			value, ok := store[key]
			if ok {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
				conn.Write([]byte(resp))
			} else {
				conn.Write([]byte("$-1\r\n")) // RESP nil
			}
			parts = nil
		}
	}
}
