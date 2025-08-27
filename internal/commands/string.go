package commands

import (
	"context"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// StringCommand represents a command that operates on string values
type StringCommand struct {
	name string
	args []string
}

// SET command implementation
type SetCommand struct {
	StringCommand
	key      string
	value    string
	expiryMs int64
}

func NewSetCommand(args []string) (*SetCommand, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'set' command")
	}

	cmd := &SetCommand{
		StringCommand: StringCommand{name: "SET", args: args},
		key:           args[0],
		value:         args[1],
	}

	// Parse PX if present
	if len(args) > 3 && args[2] == "PX" {
		if ms, err := strconv.ParseInt(args[3], 10, 64); err == nil {
			cmd.expiryMs = ms
		}
	}

	return cmd, nil
}

func (c *SetCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	store.Set(c.key, c.value, c.expiryMs)
	return resp.SimpleString("OK")
}

// GET command implementation
type GetCommand struct {
	StringCommand
	key string
}

func NewGetCommand(args []string) (*GetCommand, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'get' command")
	}

	return &GetCommand{
		StringCommand: StringCommand{name: "GET", args: args},
		key:           args[0],
	}, nil
}

func (c *GetCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	value, exists := store.GetString(c.key)
	if !exists {
		return resp.BulkString{IsNull: true}
	}
	return resp.BulkString{Value: value}
}

// INCR command implementation
type IncrCommand struct {
	StringCommand
	key string
}

func NewIncrCommand(args []string) (*IncrCommand, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'incr' command")
	}

	return &IncrCommand{
		StringCommand: StringCommand{name: "INCR", args: args},
		key:           args[0],
	}, nil
}

func (c *IncrCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	value, exists := store.GetString(c.key)

	var num int64 = 0
	if exists {
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			num = n
		} else {
			return resp.Error("ERR value is not an integer or out of range")
		}
	}

	num++
	store.Set(c.key, strconv.FormatInt(num, 10), 0)
	return resp.Integer(num)
}
