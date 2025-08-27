package resp

import "fmt"

// Value represents a RESP (REdis Serialization Protocol) value.
// This interface is implemented by all RESP data types including
// Simple Strings, Errors, Integers, Bulk Strings and Arrays.
type Value interface {
	// Format returns the RESP wire protocol encoded bytes for this value.
	// The returned bytes should follow the RESP specification format:
	// https://redis.io/docs/reference/protocol-spec/
	Format() []byte
}

// SimpleString represents a RESP Simple String type.
// In the RESP protocol, Simple Strings are prefixed with "+" and can't contain newlines.
// They are typically used for simple status replies like "OK".
type SimpleString string

// Format encodes a Simple String in RESP format.
// The encoded format is: "+<string>\r\n"
func (s SimpleString) Format() []byte {
	return []byte("+" + string(s) + "\r\n")
}

// Error represents a RESP Error type.
// RESP Errors are used to return error messages to clients
// and are prefixed with "-" in the protocol.
type Error string

// Format encodes an Error in RESP format.
// The encoded format is: "-<error>\r\n"
func (e Error) Format() []byte {
	return []byte("-" + string(e) + "\r\n")
}

// NewError creates a new Error with the given message
func NewError(msg string) Error {
	return Error(msg)
}

// Nil represents a RESP Null value.
// In RESP, nulls are encoded as "$-1\r\n" for Bulk Strings
// or "*-1\r\n" for Arrays.
type Nil struct{}

// Format encodes a Nil value in RESP format as a null array
func (n Nil) Format() []byte {
	return []byte("*-1\r\n")
}

// Integer represents a RESP Integer
type Integer int64

func (i Integer) Format() []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i))
}

// BulkString represents a RESP Bulk String
type BulkString struct {
	Value  string
	IsNull bool
}

func (b BulkString) Format() []byte {
	if b.IsNull {
		return []byte("$-1\r\n")
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(b.Value), b.Value))
}

// Array represents a RESP Array
type Array []Value

func (a Array) Format() []byte {
	if len(a) == 0 {
		return []byte("*0\r\n")
	}
	result := []byte(fmt.Sprintf("*%d\r\n", len(a)))
	for _, v := range a {
		result = append(result, v.Format()...)
	}
	return result
}
