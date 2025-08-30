package main

import (
    "flag"
    "fmt"
    "os"
    "strconv"
    "strings"

    "github.com/codecrafters-io/redis-starter-go/internal/server"
)

func main() {
    // Parse --port flag (default 6379)
    port := flag.Int("port", 6379, "port to listen on")
    replicaOf := flag.String("replicaof", "", "<host> <port> of master to replicate from")
    flag.Parse()

    addr := fmt.Sprintf("0.0.0.0:%d", *port)
    srv := server.New()
    // Configure replica mode if requested
    if *replicaOf != "" {
        parts := strings.Fields(*replicaOf)
        if len(parts) == 2 {
            if p, err := strconv.Atoi(parts[1]); err == nil {
                srv.SetReplicaOf(parts[0], p)
            }
        }
    }
    if err := srv.Start(addr); err != nil {
        fmt.Println("Error:", err)
        os.Exit(1)
    }
}
