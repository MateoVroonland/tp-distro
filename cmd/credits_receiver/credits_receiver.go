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

	rawCredtisConsumer, err := utils.NewConsumerQueue(conn, "credits", "credits", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	stateFile, readErr := os.ReadFile("data/credits_receiver_state.gob")
	clientProducers := make(map[string]*utils.ProducerQueue)
	if os.IsNotExist(readErr) {
		log.Println("State file does not exist, creating new state")
	} else if readErr != nil {
		log.Fatalf("Failed to read state: %v", readErr)
	} else {
		var state receiver.CreditsReceiverState
		err := gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state: %v", err)
		}

		rawCredtisConsumer.RestoreState(state.CreditsConsumer)

		for clientId, producerState := range state.ClientProducers {
			producerName := fmt.Sprintf("credits_joiner_client_%s", clientId)
			producer, err := utils.NewProducerQueue(conn, producerName, env.AppEnv.CREDITS_JOINER_AMOUNT)
			if err != nil {
				log.Printf("Failed to create producer for client %s: %v", clientId, err)
				continue
			}
			producer.RestoreState(producerState)
			clientProducers[clientId] = producer
		}
		log.Println("State restored")
		log.Printf("%+v", state)
	}

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.ID)
	go healthCheckServer.Start()

	receiver := receiver.NewCreditsReceiver(conn, rawCredtisConsumer)
	receiver.ClientProducers = clientProducers
	receiver.ReceiveCredits()
}
