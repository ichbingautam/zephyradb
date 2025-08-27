// Package server implements the Redis server protocol and command handling.
// It provides TCP connection handling, RESP protocol parsing, and command routing
// to the appropriate storage operations.
package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/types"

	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// Server represents a Redis-compatible server that handles client connections
// and executes Redis commands. It implements the core Redis server functionality
// including:
//   - TCP connection handling
//   - RESP protocol parsing
//   - Command routing and execution
//   - Data persistence
//   - Pub/Sub messaging
type Server struct {
	// store is the thread-safe storage engine that handles all data operations
	store *storage.Store
}

// New creates and initializes a new Server instance with a fresh storage backend.
// The server supports the full range of Redis commands and maintains data
// consistency across concurrent client connections.
//
// Returns:
//   - A new Server instance ready to accept connections
func New() *Server {
	return &Server{
		store: storage.New(),
	}
}

// Start initializes the server and begins listening for client connections.
// It accepts TCP connections on the specified address and spawns a goroutine
// to handle each connection independently.
// Start begins listening for Redis client connections on the specified address.
// For each new connection, it spawns a goroutine to handle client commands.
// The server continues running until an error occurs or it is explicitly stopped.
//
// Parameters:
//   - addr: The network address to listen on (e.g., "localhost:6379")
//
// Returns:
//   - An error if the server fails to start or encounters a fatal error
//
// This is a blocking call that runs indefinitely while handling connections.
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

// handleConnection processes a single client connection.
// It reads RESP-formatted commands from the connection, executes them,
// and writes the responses back to the client. The connection is
// automatically closed when the client disconnects or an error occurs.
//
// The method handles these responsibilities:
//   - Reading commands using the RESP protocol
//   - Parsing and validating command arguments
//   - Executing commands against the storage
//   - Writing formatted responses back to the client
//   - Managing connection state and cleanup
//
// Parameters:
//   - conn: The TCP connection to the client
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	var parts []string
	expectedParts := 0

	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println("Received command:", text)

		if len(parts) == 0 && strings.HasPrefix(text, "*") {
			n, err := strconv.Atoi(text[1:])
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
			if value, exists := s.store.GetString(key); exists {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
				conn.Write([]byte(resp))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		}

	case "RPUSH":
		if len(parts) >= 7 {
			key := parts[4]
			values := make([]string, 0)
			for i := 6; i < len(parts); i += 2 {
				values = append(values, parts[i])
			}
			length := s.store.RPush(key, values...)
			resp := fmt.Sprintf(":%d\r\n", length)
			conn.Write([]byte(resp))
		}

	case "LPUSH":
		if len(parts) >= 7 {
			key := parts[4]
			values := make([]string, 0)
			for i := 6; i < len(parts); i += 2 {
				values = append(values, parts[i])
			}
			length := s.store.LPush(key, values...)
			resp := fmt.Sprintf(":%d\r\n", length)
			conn.Write([]byte(resp))
		}

	case "LPOP":
		if len(parts) < 5 {
			conn.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n"))
			return
		}
		key := parts[4]
		count := int64(1)
		if len(parts) >= 7 {
			var err error
			count, err = strconv.ParseInt(parts[6], 10, 64)
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				return
			}
		}

		values, ok := s.store.LPop(key, count)
		if !ok {
			conn.Write([]byte("$-1\r\n"))
			return
		}

		if len(parts) >= 7 {
			// Multi-element response
			resp := fmt.Sprintf("*%d\r\n", len(values))
			for _, v := range values {
				resp += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
			}
			conn.Write([]byte(resp))
		} else {
			// Single-element response
			if len(values) == 0 {
				conn.Write([]byte("$-1\r\n"))
			} else {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(values[0]), values[0])
				conn.Write([]byte(resp))
			}
		}

	case "LLEN":
		if len(parts) == 5 {
			key := parts[4]
			length := s.store.LLen(key)
			resp := fmt.Sprintf(":%d\r\n", length)
			conn.Write([]byte(resp))
		}

	case "LRANGE":
		if len(parts) == 9 {
			key := parts[4]
			start, err := strconv.ParseInt(parts[6], 10, 64)
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				return
			}
			stop, err := strconv.ParseInt(parts[8], 10, 64)
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				return
			}

			elements := s.store.LRange(key, start, stop)
			resp := fmt.Sprintf("*%d\r\n", len(elements))
			for _, elem := range elements {
				resp += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
			}
			conn.Write([]byte(resp))
		}

	case "LREM":
		if len(parts) == 9 {
			key := parts[4]
			count, err := strconv.ParseInt(parts[6], 10, 64)
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				return
			}
			value := parts[8]

			removed := s.store.LRem(key, count, value)
			resp := fmt.Sprintf(":%d\r\n", removed)
			conn.Write([]byte(resp))
		}

	case "BLPOP":
		if len(parts) >= 7 {
			keys := make([]string, 0)
			for i := 4; i < len(parts)-2; i += 2 {
				keys = append(keys, parts[i])
			}
			timeout, err := strconv.ParseFloat(parts[len(parts)-1], 64)
			if err != nil {
				conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
				return
			}

			key, value, ok := s.store.BLPop(context.Background(), timeout, keys...)
			if !ok {
				conn.Write([]byte("$-1\r\n"))
				return
			}

			resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
			conn.Write([]byte(resp))
		}

	case "TYPE":
		if len(parts) == 5 {
			key := parts[4]
			entry, exists := s.store.Get(key)
			if !exists {
				conn.Write([]byte("+none\r\n"))
				return
			}

			var typeStr string
			switch entry.Type {
			case types.TypeString:
				typeStr = "string"
			case types.TypeList:
				typeStr = "list"
			case types.TypeStream:
				typeStr = "stream"
			default:
				typeStr = "none"
			}
			resp := fmt.Sprintf("+%s\r\n", typeStr)
			conn.Write([]byte(resp))
		}

	case "XADD":
		if len(parts) < 9 || len(parts)%2 != 1 {
			conn.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
			return
		}
		key := parts[4]
		streamID := parts[6]

		// Parse stream ID
		var id *storage.StreamID
		if streamID != "*" {
			parsedID, err := storage.ParseStreamID(streamID)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("-ERR invalid stream ID: %v\r\n", err)))
				return
			}
			id = &parsedID
		}

		// Parse field-value pairs
		fields := make(map[string]string)
		for i := 8; i < len(parts); i += 4 {
			field := parts[i]
			if i+2 >= len(parts) {
				conn.Write([]byte("-ERR unbalanced stream field-value pairs\r\n"))
				return
			}
			value := parts[i+2]
			fields[field] = value
		}

		// Add to stream
		newID, err := s.store.XADD(key, id, fields)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("-ERR %v\r\n", err)))
			return
		}

		// Return the new entry's ID
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(newID.String()), newID.String())
		conn.Write([]byte(resp))
	}
}
