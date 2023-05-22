package main

import (
	"context"
	"log"

	"github.com/NicolasDutronc/goblins"
)

func main() {
	server, err := goblins.NewGoblinsServer("127.0.0.1:9092", "127.0.0.1:9042")
	if err != nil {
		log.Fatalf("could not create server: %v\n", err)
	}
	monitoringServer := goblins.NewMonitoringServer(server)

	// starting the server on port 9 3/4
	log.Fatalf("error in server: %v", goblins.StartServer(context.Background(), monitoringServer, 9075))
}
