package main

import (
	"context"
	"log"

	"github.com/NicolasDutronc/goblins/server"
)

func main() {
	srv, err := server.CreateGoblinsServer("127.0.0.1:9092", "127.0.0.1:9042")
	if err != nil {
		log.Fatalf("could not create server: %v\n", err)
	}
	loggingServer := server.NewLoggingServer(srv)

	// starting the server on port 9 3/4
	log.Fatalf("error in server: %v", server.StartServer(context.Background(), loggingServer, 9075))
}
