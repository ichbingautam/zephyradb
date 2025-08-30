package main

import (
    "flag"
    "fmt"
    "os"

    "github.com/codecrafters-io/redis-starter-go/internal/server"
)

func main() {
    // Parse --port flag (default 6379)
    port := flag.Int("port", 6379, "port to listen on")
    flag.Parse()

    addr := fmt.Sprintf("0.0.0.0:%d", *port)
    srv := server.New()
    if err := srv.Start(addr); err != nil {
        fmt.Println("Error:", err)
        os.Exit(1)
    }
}
