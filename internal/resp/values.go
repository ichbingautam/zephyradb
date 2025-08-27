package resp

import "fmt"

// Value represents a RESP protocol value
type Value interface {
	// Format returns the RESP encoded value
	Format() []byte
}

// SimpleString represents a RESP Simple String
type SimpleString string

func (s SimpleString) Format() []byte {
	return []byte("+" + string(s) + "\r\n")
}

// Error represents a RESP Error
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
