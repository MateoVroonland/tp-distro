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

	nodeID := env.AppEnv.ID
	totalNodes := env.AppEnv.REPLICAS

	log.Printf("Starting resuscitator node %d of %d to monitor all services in the distributed system...", nodeID, totalNodes)
	server := resuscitator.NewServer(nodeID, totalNodes)
	server.Start()
}
