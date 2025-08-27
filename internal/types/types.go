package types

// DataType represents the type of Redis value
type DataType int

const (
	// TypeString represents a simple string value
	TypeString DataType = iota

	// TypeList represents a list of strings
	TypeList
)
