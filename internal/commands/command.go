package commands

import (
	"context"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// Command represents a Redis command that can be executed against the storage.
// Each Redis command (like GET, SET, LPUSH, etc.) implements this interface.
type Command interface {
	// Execute runs the command against the storage and returns RESP response.
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - store: The Redis storage instance to operate on
	// Returns:
	//   - A RESP-encoded response value
	Execute(ctx context.Context, store *storage.Store) resp.Value

	// Name returns the command name as used in Redis protocol.
	// For example: "GET", "SET", "LPUSH"
	Name() string
}

// CommandFactory creates Command instances from raw arguments.
// Each command type has its own factory to handle argument parsing
// and validation specific to that command.
type CommandFactory interface {
	// Create instantiates a new command from string arguments.
	// Parameters:
	//   - args: Raw command arguments from the Redis protocol
	// Returns:
	//   - A new Command instance
	//   - Error if arguments are invalid
	Create(args []string) (Command, error)
}

// Transaction represents a Redis transaction that can execute
// multiple commands atomically. It supports the MULTI/EXEC
// command pattern.
type Transaction struct {
	commands []Command
	store    *storage.Store
}

// NewTransaction creates a new transaction for the given storage.
func NewTransaction(store *storage.Store) *Transaction {
	return &Transaction{
		store: store,
	}
}

// Queue adds a command to be executed when the transaction commits.
// Commands are executed in the order they are queued.
func (t *Transaction) Queue(cmd Command) {
	t.commands = append(t.commands, cmd)
}

// Execute runs all queued commands in order and returns their results.
// This implements the EXEC command functionality.
// Parameters:
//   - ctx: Context for cancellation and timeouts
//
// Returns:
//   - Array of RESP values, one for each command executed
func (t *Transaction) Execute(ctx context.Context) []resp.Value {
	results := make([]resp.Value, 0, len(t.commands))
	for _, cmd := range t.commands {
		results = append(results, cmd.Execute(ctx, t.store))
	}
	return results
}

// BlockingCommand represents a command that can block waiting for a condition.
// This is used for commands like BLPOP that can wait for data to become available.
type BlockingCommand interface {
	Command
	// ExecuteBlocking runs the command with a specified timeout.
	// Parameters:
	//   - ctx: Context for cancellation
	//   - store: The Redis storage instance to operate on
	//   - timeout: Maximum time to wait for the condition
	// Returns:
	//   - A RESP-encoded response value. Returns nil if timeout occurs
	ExecuteBlocking(ctx context.Context, store *storage.Store, timeout time.Duration) resp.Value
}

// PubSubCommand represents a publish/subscribe pattern command.
// This includes commands like PUBLISH, SUBSCRIBE, and PSUBSCRIBE.
type PubSubCommand interface {
	Command
	// Topic returns the channel name or pattern this command operates on.
	// For PUBLISH, this is the target channel.
	// For SUBSCRIBE/PSUBSCRIBE, this is the channel/pattern to listen on.
	Topic() string
}
