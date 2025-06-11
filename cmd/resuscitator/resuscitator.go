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

	server := resuscitator.NewServer(env.AppEnv.SERVICE_TYPE)
	server.Start()

}
