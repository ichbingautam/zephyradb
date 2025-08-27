// Package types provides the core data types and constants used throughout the Redis implementation.
package types

// DataType represents the type of Redis value stored in the key-value store.
// This is used to distinguish between different Redis data structures like strings and lists.
type DataType int

const (
	// TypeString represents a simple string value in Redis.
	// This is the most basic data type, storing plain strings.
	TypeString DataType = iota

	// TypeList represents a list of strings in Redis.
	// Lists are implemented as arrays of strings that maintain insertion order
	// and support operations from both ends (head and tail).
	TypeList
)
