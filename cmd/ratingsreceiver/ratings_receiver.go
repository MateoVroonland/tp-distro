package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
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
	joinerProducers := make(map[string]*utils.ProducerQueue)
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
		for clientId, producerState := range state.JoinerProducers {
			producerName := fmt.Sprintf("ratings_joiner_client_%s", clientId)
			producer, err := utils.NewProducerQueue(conn, producerName, env.AppEnv.RATINGS_JOINER_AMOUNT)
			if err != nil {
				log.Fatalf("Failed to create producer: %v", err)
			}
			producer.RestoreState(producerState)
			joinerProducers[clientId] = producer
		}
		log.Println("State restored")
		log.Printf("%+v", state)
	}

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.ID)
	go healthCheckServer.Start()

	receiver := receiver.NewRatingsReceiver(conn, ratingsConsumer)
	receiver.JoinerProducers = joinerProducers
	receiver.ReceiveRatings()
}
