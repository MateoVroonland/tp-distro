package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/resuscitator"
)

func main() {
	err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Failed to load environment variables: %v", err)
	}

	log.Printf("Starting resuscitator to monitor all services in the distributed system...")
	server := resuscitator.NewServer()
	server.Start()
}
