package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"strconv"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/joiners"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Failed to load environment variables: %v", err)
	}

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.ID)
	go healthCheckServer.Start()

	newClientQueue, err := utils.NewConsumerFanout(conn, "new_client_fanout_q3")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	stateFile, err := os.ReadFile("data/ratings_joiner_state.gob")
	ratingsJoiner := joiners.NewRatingsJoiner(conn, newClientQueue)

	if os.IsNotExist(err) {
		log.Printf("State file does not exist, creating new state file")
	} else if err != nil {
		log.Fatalf("Failed to read state file: %v", err)
	} else {
		var state joiners.RatingsJoinerState
		err = gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state file: %v", err)
		}

		ratingsJoiner.ClientsJoiners = state.CurrentClients

		log.Printf("Restored state for joiner %v with clients: %v ", env.AppEnv.ID, state.CurrentClients)
	}

	log.Printf("Ratings joiner initialized with id '%d'", env.AppEnv.ID)

	id := strconv.Itoa(env.AppEnv.ID)
	ratingsJoiner.JoinRatings(id)

}
