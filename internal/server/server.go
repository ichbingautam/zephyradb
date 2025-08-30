// Package server implements the Redis server protocol and command handling.
// It provides TCP connection handling, RESP protocol parsing, and command routing
// to the appropriate storage operations.
package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

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
	// role indicates whether this server is a master or a slave (replica)
	role string // "master" or "slave"
	// master connection info (used in later stages)
	masterHost string
	masterPort int
	// replication identity and offset
	replID     string
	replOffset int64
	// local listening port for REPLCONF listening-port
	listenPort int
	// replicaConns holds active replication connections to replicas
	replicaConns []net.Conn
	// repMu guards access to replicaConns and writes to them
	repMu sync.Mutex
}

// propagate sends a parsed RESP command (as captured in parts) to the replica connection, if present.
// parts holds each RESP line without CRLF, as read by bufio.Scanner. We reconstruct CRLF before writing.
func (s *Server) propagate(parts []string) {
    if s == nil || s.role != "master" {
        return
    }
    s.repMu.Lock()
    defer s.repMu.Unlock()
    if len(s.replicaConns) == 0 {
        return
    }
    var buf bytes.Buffer
    for _, line := range parts {
        buf.WriteString(line)
        buf.WriteString("\r\n")
    }
    payload := buf.Bytes()
    for _, rc := range s.replicaConns {
        if rc == nil {
            continue
        }
        _, _ = rc.Write(payload)
    }
}

// emptyRDB returns a minimal, valid empty RDB payload.
// This is a hardcoded RDB representing an empty database.
// The tester accepts any valid empty RDB.
func emptyRDB() []byte {
    // This payload corresponds to a minimal empty RDB created by Redis.
    // Header: "REDIS0006"
    // EOF opcode and 8-byte checksum follow. This is sufficient for tests.
    return []byte{
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x36, // "REDIS0006"
        0xFF, // EOF opcode
        // 8-byte checksum (zeros acceptable for this challenge)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    }
}

// startReplicaHandshake performs the initial step of the replica->master handshake:
// send a PING to the configured master. Subsequent steps (REPLCONF/PSYNC) are
// implemented in later stages.
func (s *Server) startReplicaHandshake() {
    addr := net.JoinHostPort(s.masterHost, strconv.Itoa(s.masterPort))
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        fmt.Printf("[replica] failed to connect to master %s: %v\n", addr, err)
        return
    }
    r := bufio.NewReader(conn)

    // RESP-encoded PING: *1\r\n$4\r\nPING\r\n
    if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
        fmt.Printf("[replica] failed to send PING to master %s: %v\n", addr, err)
        _ = conn.Close()
        return
    }
    fmt.Printf("[replica] sent PING to master %s\n", addr)

    // Read PONG (ignore content, but wait up to 5s)
    _ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
    _, _ = r.ReadString('\n')
    _ = conn.SetReadDeadline(time.Time{})

    // 1) REPLCONF listening-port <PORT>
    portStr := strconv.Itoa(s.listenPort)
    replconf1 := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(portStr), portStr)
    if _, err := conn.Write([]byte(replconf1)); err != nil {
        fmt.Printf("[replica] failed to send REPLCONF listening-port to %s: %v\n", addr, err)
        _ = conn.Close()
        return
    }
    _ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
    _, _ = r.ReadString('\n') // expect +OK

    // 2) REPLCONF capa psync2
    replconf2 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    if _, err := conn.Write([]byte(replconf2)); err != nil {
        fmt.Printf("[replica] failed to send REPLCONF capa to %s: %v\n", addr, err)
        _ = conn.Close()
        return
    }
    _ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
    _, _ = r.ReadString('\n') // expect +OK
    _ = conn.SetReadDeadline(time.Time{})

    // 3) PSYNC ? -1
    psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    if _, err := conn.Write([]byte(psync)); err != nil {
        fmt.Printf("[replica] failed to send PSYNC to %s: %v\n", addr, err)
        _ = conn.Close()
        return
    }
    // Read FULLRESYNC line (ignore content for now)
    _ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
    _, _ = r.ReadString('\n')
    _ = conn.SetReadDeadline(time.Time{})
}

// connState holds per-connection state such as transaction mode
type connState struct {
	inMulti bool
	// queue holds RESP command parts queued during a MULTI transaction
	queue [][]string
}

// respCaptureConn is a minimal net.Conn that captures writes into a buffer.
// It implements the net.Conn interface to be used in place of a real connection
// when we want to capture RESP output produced by command handlers.
type respCaptureConn struct {
	buf bytes.Buffer
}

func (c *respCaptureConn) Read(b []byte) (n int, err error)  { return 0, fmt.Errorf("not supported") }
func (c *respCaptureConn) Write(b []byte) (n int, err error) { return c.buf.Write(b) }
func (c *respCaptureConn) Close() error                      { return nil }
func (c *respCaptureConn) LocalAddr() net.Addr               { return &net.IPAddr{} }
func (c *respCaptureConn) RemoteAddr() net.Addr              { return &net.IPAddr{} }
func (c *respCaptureConn) SetDeadline(t time.Time) error     { return nil }
func (c *respCaptureConn) SetReadDeadline(t time.Time) error { return nil }
func (c *respCaptureConn) SetWriteDeadline(t time.Time) error { return nil }

// New creates and initializes a new Server instance with a fresh storage backend.
// The server supports the full range of Redis commands and maintains data
// consistency across concurrent client connections.
//
// Returns:
//   - A new Server instance ready to accept connections
func New() *Server {
	return &Server{
		store: storage.New(),
		role:  "master",
		// Hardcode a 40-char pseudo random string as replication ID for this stage
		replID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replOffset: 0,
	}
}

// SetReplicaOf configures the server as a replica of the given master host:port
func (s *Server) SetReplicaOf(host string, port int) {
	s.role = "slave"
	s.masterHost = host
	s.masterPort = port
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
    // Remember our listening port for replication handshake
    if _, p, err := net.SplitHostPort(addr); err == nil {
        if pi, err := strconv.Atoi(p); err == nil {
            s.listenPort = pi
        }
    }
    l, err := net.Listen("tcp", addr)
    if err != nil {
        return fmt.Errorf("failed to bind to %s: %w", addr, err)
    }
    defer l.Close()

	fmt.Printf("Server listening on %s\n", addr)

    // If configured as a replica, begin the replication handshake with master.
    if s.role == "slave" && s.masterHost != "" && s.masterPort != 0 {
        go s.startReplicaHandshake()
    }

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
	state := &connState{}

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
			s.handleCommand(conn, parts, state)
			parts = nil
			expectedParts = 0
		}
	}
}

func (s *Server) handleCommand(conn net.Conn, parts []string, state *connState) {
	cmdIdx := 2
	cmd := strings.ToUpper(parts[cmdIdx])

	// If we're in a MULTI transaction, queue all commands except transaction controls
	if state.inMulti {
		switch cmd {
		case "MULTI":
			// Redis errors on nested MULTI, but for this stage we'll just acknowledge current MULTI state
			// and return OK again (keeps behavior simple). Alternatively, we could return an error.
			conn.Write([]byte("+OK\r\n"))
			return
		case "EXEC":
			// Allow EXEC to be handled below
		case "DISCARD":
			// Allow DISCARD to be handled below
		default:
			// Queue the command without executing it and acknowledge with QUEUED
			state.queue = append(state.queue, append([]string(nil), parts...))
			conn.Write([]byte("+QUEUED\r\n"))
			return
		}
	}

	switch cmd {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))

	case "REPLCONF":
		// For this stage, acknowledge REPLCONF commands with OK
		conn.Write([]byte("+OK\r\n"))

	case "PSYNC":
		// Respond with FULLRESYNC <replid> 0 for initial sync
		resp := fmt.Sprintf("+FULLRESYNC %s 0\r\n", s.replID)
		conn.Write([]byte(resp))
		// Then send an empty RDB file as a bulk string: $<len>\r\n<binary>
		rdb := emptyRDB()
		conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(rdb))))
		conn.Write(rdb) // No trailing CRLF after binary contents
		// Register this connection as a replication connection to propagate future writes
		s.repMu.Lock()
		s.replicaConns = append(s.replicaConns, conn)
		s.repMu.Unlock()

	case "ECHO":
		if len(parts) == 5 {
			arg := parts[4]
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			conn.Write([]byte(resp))
		}

	case "DISCARD":
		if !state.inMulti {
			conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			return
		}
		// Abort transaction: clear queue and exit multi
		state.inMulti = false
		state.queue = nil
		conn.Write([]byte("+OK\r\n"))

	case "MULTI":
		// Start a transaction and reset any previous queue
		state.inMulti = true
		state.queue = nil
		conn.Write([]byte("+OK\r\n"))

	case "EXEC":
		if !state.inMulti {
			// EXEC without an active MULTI returns an error
			conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			return
		}
		// Execute queued commands in order and collect their RESP replies
		queued := state.queue
		// Exit MULTI mode before executing, so commands run normally
		state.inMulti = false
		state.queue = nil

		// Build RESP array header with number of results
		resp := fmt.Sprintf("*%d\r\n", len(queued))
		for _, q := range queued {
			capConn := &respCaptureConn{}
			// Reuse same state; we're out of MULTI so commands will execute
			s.handleCommand(capConn, q, state)
			resp += capConn.buf.String()
		}
		conn.Write([]byte(resp))

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
			// Propagate write to replica
			s.propagate(parts)
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

	case "INCR":
		if len(parts) == 5 {
			key := parts[4]
			if value, exists := s.store.GetString(key); exists {
				// Parse existing value
				iv, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					return
				}
				iv++
				s.store.Set(key, fmt.Sprintf("%d", iv), 0)
				conn.Write([]byte(fmt.Sprintf(":%d\r\n", iv)))
			} else {
				// If key doesn't exist, set to 1
				s.store.Set(key, "1", 0)
				conn.Write([]byte(":1\r\n"))
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
				// For BLPOP timeout, Redis returns a null array (not a null bulk string)
				conn.Write([]byte("*-1\r\n"))
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

	case "INFO":
		// Support: INFO replication -> returns bulk string with replication section
		if len(parts) >= 5 && strings.ToLower(parts[4]) == "replication" {
			payload := fmt.Sprintf("# Replication\r\nrole:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n", s.role, s.replID, s.replOffset)
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
			conn.Write([]byte(resp))
		} else {
			// For other sections or missing arg, return a null bulk string
			conn.Write([]byte("$-1\r\n"))
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
				// Use simple string format for the error response
				conn.Write([]byte(fmt.Sprintf("+%s\r\n", "ERR Invalid stream ID specified as stream command argument")))
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

	case "XRANGE":
		if len(parts) != 9 {
			conn.Write([]byte("-ERR wrong number of arguments for 'xrange' command\r\n"))
			return
		}
		key := parts[4]
		start := parts[6]
		end := parts[8]

		entries, err := s.store.XRANGE(key, start, end)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("-ERR %v\r\n", err)))
			return
		}

		// Format as RESP array of arrays
		// Top level array
		resp := fmt.Sprintf("*%d\r\n", len(entries))
		for _, entry := range entries {
			// Each entry is an array with ID and field array
			resp += "*2\r\n"
			// Entry ID
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID.String()), entry.ID.String())
			// Field array
			fieldCount := len(entry.Fields) * 2 // Each field has key and value
			resp += fmt.Sprintf("*%d\r\n", fieldCount)
			for k, v := range entry.Fields {
				// Field key
				resp += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
				// Field value
				resp += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
			}
		}
		conn.Write([]byte(resp))

	case "XREAD":
		// Support: XREAD [BLOCK ms] STREAMS key1 ... keyN id1 ... idN
		if len(parts) < 9 {
			conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
			return
		}

		argIdx := 4
		block := false
		var timeoutMs int64 = 0
		// Optional BLOCK
		if strings.ToUpper(parts[argIdx]) == "BLOCK" {
			if len(parts) < argIdx+4 { // need BLOCK ms STREAMS at least
				conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
				return
			}
			ms, err := strconv.ParseInt(parts[argIdx+2], 10, 64)
			if err != nil || ms < 0 {
				conn.Write([]byte("-ERR invalid block timeout\r\n"))
				return
			}
			block = true
			timeoutMs = ms
			argIdx += 4 // skip BLOCK, $len, ms, and next $len will be STREAMS
		}

		if strings.ToUpper(parts[argIdx]) != "STREAMS" {
			conn.Write([]byte("-ERR syntax error\r\n"))
			return
		}

		// After STREAMS, remaining values are keys then ids
		remaining := (len(parts) - (argIdx + 1)) / 2
		if remaining <= 0 || remaining%2 != 0 {
			conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
			return
		}
		n := remaining / 2
		keys := make([]string, 0, n)
		ids := make([]string, 0, n)
		// keys
		for i := 0; i < n; i++ {
			idx := argIdx + 2 + i*2
			if idx >= len(parts) {
				conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
				return
			}
			keys = append(keys, parts[idx])
		}
		// ids
		for j := 0; j < n; j++ {
			idx := argIdx + 2 + n*2 + j*2
			if idx >= len(parts) {
				conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
				return
			}
			ids = append(ids, parts[idx])
		}

		// Support `$` as ID: translate to the current last ID of each stream so we only return new entries
		for i := range ids {
			if ids[i] == "$" {
				entries, _ := s.store.XRANGE(keys[i], "-", "+")
				if len(entries) > 0 {
					last := entries[len(entries)-1].ID.String()
					ids[i] = last
				} else {
					// Empty stream, use 0-0 so XREAD exclusive start returns nothing until something is added
					ids[i] = "0-0"
				}
			}
		}

		entriesByKey, err := s.store.XREAD(keys, ids, block, timeoutMs)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("-ERR %v\r\n", err)))
			return
		}

		// Build RESP reply: array of [ key, [ [id, [field, value, ...]], ... ] ]
		// Only include streams that have results
		// Count non-empty streams first
		count := 0
		for _, k := range keys {
			if es, ok := entriesByKey[k]; ok && len(es) > 0 {
				count++
			}
		}
		if count == 0 {
			// Return null array when nothing to return (matches Codecrafters tests)
			conn.Write([]byte("*-1\r\n"))
			return
		}

		resp := fmt.Sprintf("*%d\r\n", count)
		for _, k := range keys {
			entries, ok := entriesByKey[k]
			if !ok || len(entries) == 0 {
				continue
			}
			// [ key, entries_array ]
			resp += "*2\r\n"
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
			// entries_array
			resp += fmt.Sprintf("*%d\r\n", len(entries))
			for _, e := range entries {
				// each entry: [ id, fields_array ]
				resp += "*2\r\n"
				idStr := e.ID.String()
				resp += fmt.Sprintf("$%d\r\n%s\r\n", len(idStr), idStr)
				// fields array (flat)
				fieldCount := len(e.Fields) * 2
				resp += fmt.Sprintf("*%d\r\n", fieldCount)
				for fk, fv := range e.Fields {
					resp += fmt.Sprintf("$%d\r\n%s\r\n", len(fk), fk)
					resp += fmt.Sprintf("$%d\r\n%s\r\n", len(fv), fv)
				}
			}
		}
		conn.Write([]byte(resp))
	}
}
