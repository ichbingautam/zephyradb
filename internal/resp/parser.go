package resp

import (
	"strconv"
	"strings"
)

// ParseArrayLength parses the RESP array length from a line
func ParseArrayLength(line string) (int, error) {
	if strings.HasPrefix(line, "*") {
		return strconv.Atoi(line[1:])
	}
	return 0, ErrNotArrayHeader
}

// Common errors
var (
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
