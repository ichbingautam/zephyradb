package commands

import (
	"context"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// SubscribeCommand represents the SUBSCRIBE command
type SubscribeCommand struct {
	channels []string
}

// NewSubscribeCommand creates a new SUBSCRIBE command
func NewSubscribeCommand(channels []string) (*SubscribeCommand, error) {
	if len(channels) == 0 {
		return nil, fmt.Errorf("wrong number of arguments for 'subscribe' command")
	}

	return &SubscribeCommand{
		channels: channels,
	}, nil
}

// Execute runs the SUBSCRIBE command
func (cmd *SubscribeCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	// In a real implementation, this would subscribe the client to the channels
	// For now, we'll just return the subscription confirmation for each channel
	responses := make([]resp.Value, 0, len(cmd.channels))
	
	for i, channel := range cmd.channels {
		response := resp.Array{
			resp.BulkString{Value: "subscribe"},
			resp.BulkString{Value: channel},
			resp.Integer(i + 1), // 1-based index for number of subscribed channels
		}
		responses = append(responses, response)
	}

	// If there's only one channel, return a single response
	if len(responses) == 1 {
		return responses[0]
	}

	// Otherwise, return an array of responses (though in practice, Redis sends them as separate responses)
	return resp.Array(responses)
}

// Name returns the command name
func (cmd *SubscribeCommand) Name() string {
	return "subscribe"
}

// Topic returns the first channel name (implements PubSubCommand interface)
func (cmd *SubscribeCommand) Topic() string {
	if len(cmd.channels) > 0 {
		return cmd.channels[0]
	}
	return ""
}

// SubscribeCommandFactory creates SUBSCRIBE commands
type SubscribeCommandFactory struct{}

// CreateCommand creates a new SUBSCRIBE command
func (f *SubscribeCommandFactory) CreateCommand(args []string) (Command, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("wrong number of arguments for 'subscribe' command")
	}

	return NewSubscribeCommand(args)
}

// CommandName returns the command name
func (f *SubscribeCommandFactory) CommandName() string {
	return "subscribe"
}
