package commands

import (
	"context"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// Command represents a Redis command
type Command interface {
	// Execute runs the command and returns RESP response
	Execute(ctx context.Context, store *storage.Store) resp.Value
	// Name returns the command name
	Name() string
}

// CommandFactory creates Command instances
type CommandFactory interface {
	Create(args []string) (Command, error)
}

// Transaction represents a Redis transaction
type Transaction struct {
	commands []Command
	store    *storage.Store
}

func NewTransaction(store *storage.Store) *Transaction {
	return &Transaction{
		store: store,
	}
}

func (t *Transaction) Queue(cmd Command) {
	t.commands = append(t.commands, cmd)
}

func (t *Transaction) Execute(ctx context.Context) []resp.Value {
	results := make([]resp.Value, 0, len(t.commands))
	for _, cmd := range t.commands {
		results = append(results, cmd.Execute(ctx, t.store))
	}
	return results
}

// BlockingCommand represents a command that can block
type BlockingCommand interface {
	Command
	// ExecuteBlocking runs the command with timeout
	ExecuteBlocking(ctx context.Context, store *storage.Store, timeout time.Duration) resp.Value
}

// PubSubCommand represents a publish/subscribe command
type PubSubCommand interface {
	Command
	// Topic returns the channel/pattern
	Topic() string
}
