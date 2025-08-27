package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/server"
)

func main() {
	srv := server.New()
	if err := srv.Start("0.0.0.0:6379"); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}
