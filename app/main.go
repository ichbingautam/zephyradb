package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
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

type entry struct {
	value     string
	expiresAt int64 // unix ms, 0 means no expiry
}

var store = make(map[string]entry)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	var parts []string
	expectedParts := 0

	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println("Received command: " + text)

		// If this is the first line and starts with '*', parse expected parts
		if len(parts) == 0 && strings.HasPrefix(text, "*") {
			n, err := parseArrayLength(text)
			if err == nil {
				expectedParts = n*2 + 1 // Each argument is 2 lines ($len, value), plus the array header
			}
		}

		parts = append(parts, text)

		if expectedParts > 0 && len(parts) == expectedParts {
			cmdIdx := 2
			cmd := strings.ToUpper(parts[cmdIdx])
			if cmd == "PING" {
				conn.Write([]byte("+PONG\r\n"))
			} else if cmd == "ECHO" && expectedParts == 5 {
				arg := parts[4]
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				conn.Write([]byte(resp))
			} else if cmd == "SET" {
				key := parts[4]
				value := parts[6]
				var expiresAt int64 = 0
				// Look for PX argument (case-insensitive)
				if expectedParts == 11 {
					// SET key value PX ms
					pxIdx := 8
					pxArg := strings.ToUpper(parts[pxIdx])
					if pxArg == "PX" {
						msStr := parts[10]
						ms, err := strconv.ParseInt(msStr, 10, 64)
						if err == nil {
							expiresAt = nowMs() + ms
						}
					}
				}
				store[key] = entry{value: value, expiresAt: expiresAt}
				conn.Write([]byte("+OK\r\n"))
			} else if cmd == "GET" && expectedParts == 5 {
				key := parts[4]
				ent, ok := store[key]
				expired := ent.expiresAt > 0 && nowMs() > ent.expiresAt
				if ok && !expired {
					resp := fmt.Sprintf("$%d\r\n%s\r\n", len(ent.value), ent.value)
					conn.Write([]byte(resp))
				} else {
					conn.Write([]byte("$-1\r\n")) // RESP nil
				}
			}
			// Reset for next command
			parts = nil
			expectedParts = 0
		}
	}
}

// Helper to parse RESP array length
func parseArrayLength(line string) (int, error) {
	if strings.HasPrefix(line, "*") {
		return strconv.Atoi(line[1:])
	}
	return 0, fmt.Errorf("not an array header")
}

// Helper to get current time in ms
func nowMs() int64 {
	return time.Now().UnixMilli()
}
