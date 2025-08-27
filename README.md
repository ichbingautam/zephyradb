# zephyradb
ZephyraDB is unique, impactful, and aligns beautifully with the concept of an elegant, memory-fast, Go-based KV store.
[![progress-banner](https://backend.codecrafters.io/progress/redis/508a86ce-5c54-4c58-b100-628a1d9cfa21)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This project is a feature-rich Redis server implementation in Go, created as part of the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis). It implements core Redis functionality including data structures, persistence, and pub/sub messaging.

## Features

### Core Functionality

- Full RESP (Redis Serialization Protocol) implementation
- Thread-safe data operations with RWMutex protection
- Support for concurrent client connections
- Command pipelining

### Data Structures

- Lists with operations:
  - LPUSH/RPUSH: Add elements to list ends
  - LPOP/RPOP: Remove and return elements
  - LLEN: Get list length
  - LRANGE: Retrieve range of elements
  - LREM: Remove elements matching value

### Advanced Features

- Blocking operations (BLPOP)
- Transaction support (MULTI/EXEC)
- Publish/Subscribe messaging
- Expiring keys with TTL

## Project Structure

```tree
├── app/
│   └── main.go           # Application entry point
├── internal/
│   ├── commands/         # Command implementations
│   │   ├── command.go    # Command interfaces
│   │   ├── list.go      # List operations
│   │   └── string.go    # String operations
│   ├── resp/            # RESP protocol
│   │   ├── parser.go    # Protocol parser
│   │   └── values.go    # RESP data types
│   ├── server/          # Server implementation
│   │   └── server.go    # TCP server & connections
│   └── storage/         # Data storage
│       ├── store.go     # Main storage engine
│       └── lpop.go      # List operations
```

## Getting Started

### Prerequisites

- Go 1.24 or later

### Running the Server

1. Clone the repository and navigate to project directory
2. Run the server with:

```sh
./your_program.sh
```

1. Test with redis-cli:

```sh
redis-cli -p 6379
```

## Usage Examples

Here are some common Redis operations you can try:

```bash
# Start a Redis client
$ redis-cli

# Basic list operations
redis> LPUSH mylist first second third
(integer) 3
redis> LRANGE mylist 0 -1
1) "third"
2) "second"
3) "first"

# Using transactions
redis> MULTI
OK
redis> LPUSH users alice
QUEUED
redis> LPUSH users bob
QUEUED
redis> EXEC
1) (integer) 1
2) (integer) 2
```

## Architecture

### Core Design

1. **Thread Safety**: Concurrent access protected by RWMutex
2. **Modularity**: Clean separation of protocol, commands, and storage
3. **Extensibility**: Simple interface for adding new commands
4. **Performance**: Optimized for concurrent operations

### System Components

- **Protocol Layer**: RESP implementation for Redis wire format
- **Command Layer**: Dynamic command routing and execution
- **Storage Layer**: Thread-safe in-memory data store
- **Network Layer**: Efficient client connection handling

## Development

We welcome contributions! You can:

- Report bugs or suggest features through issues
- Submit pull requests with improvements
- Share feedback about the project

Thank you for checking out our Redis implementation!
