package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// Server represents a Redis server
type Server struct {
	store *storage.Store
}

// New creates a new Server instance
func New() *Server {
	return &Server{
		store: storage.New(),
	}
}

// Start starts the server on the specified address
func (s *Server) Start(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", addr, err)
	}
	defer l.Close()

	fmt.Printf("Server listening on %s\n", addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	var parts []string
	expectedParts := 0

	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println("Received command:", text)

		if len(parts) == 0 && strings.HasPrefix(text, "*") {
			n, err := resp.ParseArrayLength(text)
			if err == nil {
				expectedParts = n*2 + 1 // Array header + pairs of ($len, value)
			}
		}

		parts = append(parts, text)

		if expectedParts > 0 && len(parts) == expectedParts {
			s.handleCommand(conn, parts)
			parts = nil
			expectedParts = 0
		}
	}
}

func (s *Server) handleCommand(conn net.Conn, parts []string) {
	cmdIdx := 2
	cmd := strings.ToUpper(parts[cmdIdx])

	switch cmd {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))

	case "ECHO":
		if len(parts) == 5 {
			arg := parts[4]
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			conn.Write([]byte(resp))
		}

	case "SET":
		if len(parts) >= 7 {
			key := parts[4]
			value := parts[6]
			var expiryMs int64

			// Handle PX argument
			if len(parts) == 11 && strings.ToUpper(parts[8]) == "PX" {
				if ms, err := strconv.ParseInt(parts[10], 10, 64); err == nil {
					expiryMs = ms
				}
			}

			s.store.Set(key, value, expiryMs)
			conn.Write([]byte("+OK\r\n"))
		}

	case "GET":
		if len(parts) == 5 {
			key := parts[4]
			if value, ok := s.store.Get(key); ok {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
				conn.Write([]byte(resp))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		}

	case "RPUSH":
		if len(parts) >= 7 { // *N, $5, RPUSH, $key_len, key, $val_len, value, ...
			key := parts[4]
			var values []string
			// Collect all values (they're at even indices starting from 6)
			for i := 6; i < len(parts); i += 2 {
				values = append(values, parts[i])
			}
			length := s.store.RPush(key, values...)
			resp := fmt.Sprintf(":%d\r\n", length)
			conn.Write([]byte(resp))
		}
	}
}
