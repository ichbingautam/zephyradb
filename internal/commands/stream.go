package commands

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// XReadCommand represents the XREAD command
type XReadCommand struct {
	streams map[string]string // stream key -> entry ID
	block  int64              // block timeout in milliseconds, -1 for no blocking
}

// NewXReadCommand creates a new XREAD command
func NewXReadCommand(streams map[string]string, block int64) (*XReadCommand, error) {
	if len(streams) == 0 {
		return nil, fmt.Errorf("wrong number of arguments for 'xread' command")
	}

	return &XReadCommand{
		streams: streams,
		block:   block,
	}, nil
}

// Execute runs the XREAD command
func (cmd *XReadCommand) Execute(ctx context.Context, store *storage.Store) resp.Value {
	// Prepare slices for keys and IDs
	keys := make([]string, 0, len(cmd.streams))
	ids := make([]string, 0, len(cmd.streams))
	useLatestID := make(map[string]bool, len(cmd.streams))

	// Convert the map to slices for the storage layer
	for key, id := range cmd.streams {
		keys = append(keys, key)
		if id == "$" {
			// For $, we'll first get the last ID and then use it
			entries, _ := store.XRANGE(key, "-", "+")
			if len(entries) > 0 {
				ids = append(ids, getLastEntryID(entries))
			} else {
				ids = append(ids, "0-0")
			}
			useLatestID[key] = true
		} else {
			ids = append(ids, id)
			useLatestID[key] = false
		}
	}

	// If block is specified, perform a blocking read
	if cmd.block >= 0 {
		// Create a new context with timeout if block is greater than 0
		if cmd.block > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(cmd.block)*time.Millisecond)
			defer cancel()
		} else {
			// For block 0, use a context that will never time out on its own
			// but can still be cancelled by the parent context
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			defer cancel()
		}

		// Try to get data with blocking
		allEntries, err := store.XREAD(keys, ids, true, cmd.block)
		if err != nil {
			return resp.Error(fmt.Sprintf("ERR %s", err.Error()))
		}

		// If we used $, filter out any entries that existed when we started
		if len(allEntries) > 0 {
			filteredEntries := make(map[string][]storage.StreamEntry)
			for key, entries := range allEntries {
				if shouldUseLatest, ok := useLatestID[key]; ok && shouldUseLatest && len(entries) > 0 {
					// For $, only return entries that are newer than what we initially found
					filtered := make([]storage.StreamEntry, 0, len(entries))
					for _, entry := range entries {
						if entry.ID.String() > ids[0] { // We know the ID is in ids[0] because we put it there
							filtered = append(filtered, entry)
						}
					}
					if len(filtered) > 0 {
						filteredEntries[key] = filtered
					}
				} else {
					filteredEntries[key] = entries
				}
			}
			allEntries = filteredEntries
		}

		// If we have data, return it
		if len(allEntries) > 0 {
			return formatXReadResponse(keys, allEntries)
		}

		// If we get here, the context was cancelled or timed out with no data
		// Return a null bulk string (as per stage instruction)
		return resp.BulkString{IsNull: true}
	}

	// Non-blocking read
	allEntries, err := store.XREAD(keys, ids, false, 0)
	if err != nil {
		return resp.Error(fmt.Sprintf("ERR %s", err.Error()))
	}

	// If we used $, filter out any entries that existed when we started
	if len(allEntries) > 0 {
		filteredEntries := make(map[string][]storage.StreamEntry)
		for i, key := range keys {
			if entries, exists := allEntries[key]; exists && useLatestID[key] && len(entries) > 0 {
				// For $, only return entries that are newer than what we initially found
				filtered := make([]storage.StreamEntry, 0, len(entries))
				for _, entry := range entries {
					if entry.ID.String() > ids[i] {
						filtered = append(filtered, entry)
					}
				}
				if len(filtered) > 0 {
					filteredEntries[key] = filtered
				}
			} else if exists {
				filteredEntries[key] = entries
			}
		}
		allEntries = filteredEntries
	}

	if len(allEntries) == 0 {
		// If no results (non-blocking path), return a null array
		return resp.Nil{}
	}

	return formatXReadResponse(keys, allEntries)
}

// getLastEntryID returns the ID of the last entry in the stream, or "0-0" if empty
func getLastEntryID(entries []storage.StreamEntry) string {
	if len(entries) == 0 {
		return "0-0"
	}
	return entries[len(entries)-1].ID.String()
}

// formatXReadResponse formats the XREAD response in the expected format
func formatXReadResponse(keys []string, allEntries map[string][]storage.StreamEntry) resp.Value {
	results := make([]resp.Value, 0, len(keys))

	// Process streams in the order they were requested
	for _, key := range keys {
		streamEntries, exists := allEntries[key]
		if !exists || len(streamEntries) == 0 {
			continue
		}

		// Convert entries to RESP format
		entryValues := make([]resp.Value, 0, len(streamEntries))
		for _, entry := range streamEntries {
			// Convert entry fields to flat string slice: [field1, value1, field2, value2, ...]
			fields := make([]resp.Value, 0, len(entry.Fields)*2)
			for k, v := range entry.Fields {
				fields = append(fields, resp.BulkString{Value: k}, resp.BulkString{Value: v})
			}

			// Each entry is [id, [field1, value1, field2, value2, ...]]
			entryValues = append(entryValues, resp.Array{
				resp.BulkString{Value: entry.ID.String()},
				resp.Array(fields),
			})
		}

		// Add stream results: [streamKey, [entry1, entry2, ...]]
		results = append(results, resp.Array{
			resp.BulkString{Value: key},
			resp.Array(entryValues),
		})
	}

	// Return array of results
	return resp.Array(results)
}

// Name returns the command name
func (cmd *XReadCommand) Name() string {
	return "XREAD"
}

// XReadCommandFactory creates XREAD commands
type XReadCommandFactory struct{}

// CreateCommand creates a new XREAD command
func (f *XReadCommandFactory) CreateCommand(args []string) (Command, error) {
	// XREAD [BLOCK milliseconds] STREAMS key1 key2 ... id1 id2 ...
	block := int64(-1) // -1 means no blocking

	// Check for BLOCK option
	if len(args) > 1 && strings.ToUpper(args[0]) == "BLOCK" {
		if len(args) < 2 {
			return nil, fmt.Errorf("wrong number of arguments for 'xread' command")
		}

		blockMs, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || blockMs < 0 {
			return nil, fmt.Errorf("invalid block timeout")
		}

		block = blockMs
		args = args[2:] // Remove BLOCK and its argument
	}

	// Check for STREAMS keyword
	if len(args) < 3 || strings.ToUpper(args[0]) != "STREAMS" {
		return nil, fmt.Errorf("wrong number of arguments for 'xread' command")
	}
	args = args[1:] // Remove STREAMS keyword

	// The keys and IDs are interleaved after STREAMS
	streams := make(map[string]string)
	numStreams := len(args) / 2

	if numStreams == 0 || len(args) != numStreams*2 {
		return nil, fmt.Errorf("wrong number of arguments for 'xread' command")
	}

	for i := 0; i < numStreams; i++ {
		key := args[i]
		id := args[numStreams+i]

		// Special case: $ means only new entries
		if id == "$" {
			id = "0-0" // Will be replaced with last ID in Execute
		}

		streams[key] = id
	}

	return NewXReadCommand(streams, block)
}

// CommandName returns the command name
func (f *XReadCommandFactory) CommandName() string {
	return "XREAD"
}