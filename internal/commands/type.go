package commands

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
	"github.com/codecrafters-io/redis-starter-go/internal/types"
)

// TypeCommand represents the TYPE command that returns the data type of a key.
type TypeCommand struct {
	key string
}

func NewTypeCommand(args []string) (*TypeCommand, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'type' command")
	}

	return &TypeCommand{
		key: args[0],
	}, nil
}

func (c *TypeCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	entry, exists := store.Get(c.key)
	if !exists {
		return resp.SimpleString("none")
	}

	switch entry.Type {
	case types.TypeString:
		return resp.SimpleString("string")
	case types.TypeList:
		return resp.SimpleString("list")
	case types.TypeStream:
		return resp.SimpleString("stream")
	case types.TypeZSet:
		return resp.SimpleString("zset")
	default:
		return resp.SimpleString("none")
	}
}

func (c *TypeCommand) Name() string {
	return "TYPE"
}
