package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/receiver"
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

	ratingsConsumer, err := utils.NewConsumerQueue(conn, "ratings", "ratings", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	stateFile, readErr := os.ReadFile("data/ratings_receiver_state.gob")
	if os.IsNotExist(readErr) {
		log.Println("State file does not exist, creating new state")
	} else if readErr != nil {
		log.Fatalf("Failed to read state: %v", readErr)
	} else {
		var state receiver.RatingsReceiverState
		err := gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state: %v", err)
		}

		ratingsConsumer.RestoreState(state.RatingsConsumer)
	}

	receiver := receiver.NewRatingsReceiver(conn, ratingsConsumer)
	receiver.ReceiveRatings()
}
