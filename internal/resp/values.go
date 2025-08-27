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

func (e Error) Format() []byte {
	return []byte("-" + string(e) + "\r\n")
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
type Array struct {
	Values []Value
	IsNull bool
}

func (a Array) Format() []byte {
	if a.IsNull {
		return []byte("*-1\r\n")
	}
	result := []byte(fmt.Sprintf("*%d\r\n", len(a.Values)))
	for _, v := range a.Values {
		result = append(result, v.Format()...)
	}
	return result
}
