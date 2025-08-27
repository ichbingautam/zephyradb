package commands

import (
	"context"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// ListCommand represents a command that operates on list values
type ListCommand struct {
	name string
	args []string
}

// LLEN command implementation
type LLenCommand struct {
	ListCommand
	key string
}

func NewLLenCommand(args []string) (*LLenCommand, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'llen' command")
	}

	return &LLenCommand{
		ListCommand: ListCommand{name: "LLEN", args: args},
		key:         args[0],
	}, nil
}

func (c *LLenCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	length := store.LLen(c.key)
	return resp.Integer(length)
}

// LPUSH command implementation
type LPushCommand struct {
	ListCommand
	key    string
	values []string
}

func NewLPushCommand(args []string) (*LPushCommand, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'lpush' command")
	}

	return &LPushCommand{
		ListCommand: ListCommand{name: "LPUSH", args: args},
		key:         args[0],
		values:      args[1:],
	}, nil
}

func (c *LPushCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	length := store.LPush(c.key, c.values...)
	return resp.Integer(length)
}

// LRANGE command implementation
type LRangeCommand struct {
	ListCommand
	key   string
	start int64
	stop  int64
}

func NewLRangeCommand(args []string) (*LRangeCommand, error) {
	if len(args) != 3 {
		return nil, errors.New("ERR wrong number of arguments for 'lrange' command")
	}

	start, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}

	stop, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}

	return &LRangeCommand{
		ListCommand: ListCommand{name: "LRANGE", args: args},
		key:         args[0],
		start:       start,
		stop:        stop,
	}, nil
}

func (c *LRangeCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	elements := store.LRange(c.key, c.start, c.stop)

	values := make([]resp.Value, len(elements))
	for i, elem := range elements {
		values[i] = resp.BulkString{Value: elem}
	}

	return resp.Array{Values: values}
}

// LREM command implementation
type LRemCommand struct {
	ListCommand
	key   string
	count int64
	value string
}

func NewLRemCommand(args []string) (*LRemCommand, error) {
	if len(args) != 3 {
		return nil, errors.New("ERR wrong number of arguments for 'lrem' command")
	}

	count, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}

	return &LRemCommand{
		ListCommand: ListCommand{name: "LREM", args: args},
		key:         args[0],
		count:       count,
		value:       args[2],
	}, nil
}

func (c *LRemCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	removed := store.LRem(c.key, c.count, c.value)
	return resp.Integer(removed)
}

// BLPOP command implementation
type BLPopCommand struct {
	ListCommand
	keys    []string
	timeout float64
}

func NewBLPopCommand(args []string) (*BLPopCommand, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'blpop' command")
	}

	timeout, err := strconv.ParseFloat(args[len(args)-1], 64)
	if err != nil {
		return nil, errors.New("ERR timeout is not a float or out of range")
	}

	return &BLPopCommand{
		ListCommand: ListCommand{name: "BLPOP", args: args},
		keys:        args[:len(args)-1],
		timeout:     timeout,
	}, nil
}

func (c *BLPopCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	key, value, ok := store.BLPop(ctx, c.timeout, c.keys...)
	if !ok {
		return resp.BulkString{IsNull: true}
	}

	values := []resp.Value{
		resp.BulkString{Value: key},
		resp.BulkString{Value: value},
	}
	return resp.Array{Values: values}
}
