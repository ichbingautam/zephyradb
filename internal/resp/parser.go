// Package resp implements the Redis Serialization Protocol (RESP).
// It provides functionality to parse and format RESP messages used
// in Redis client-server communication.
package resp

import (
	"strconv"
	"strings"
)

// ParseArrayLength parses a RESP array length from an input line.
// RESP arrays start with a '*' followed by the number of elements.
// For example: "*3\r\n" indicates an array with 3 elements.
//
// Parameters:
//   - line: A string containing a RESP array header
//
// Returns:
//   - The number of elements in the array
//   - An error if the input is not a valid array header
func ParseArrayLength(line string) (int, error) {
	if strings.HasPrefix(line, "*") {
		return strconv.Atoi(line[1:])
	}
	return 0, ErrNotArrayHeader
}

// Common errors returned by the RESP parser
var (
	// ErrNotArrayHeader indicates that the input line was not
	// a valid RESP array header (doesn't start with '*')
	ErrNotArrayHeader = NewRESPError("not an array header")
)

// RESPError represents a RESP protocol error
type RESPError struct {
	message string
}

func (e RESPError) Error() string {
	return e.message
}

// NewRESPError creates a new RESP error
func NewRESPError(msg string) error {
	return &RESPError{message: msg}
}
