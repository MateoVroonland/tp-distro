package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

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

	newClientQueue, err := utils.NewConsumerFanout(conn, "new_client_fanout_q4")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	stateFile, err := os.ReadFile("data/credits_joiner_state.gob")
	creditsJoiner := joiners.NewCreditsJoiner(conn, newClientQueue)

	if os.IsNotExist(err) {
		log.Printf("State file does not exist, creating new state file")
	} else if err != nil {
		log.Fatalf("Failed to read state file: %v", err)
	} else {
		var state joiners.CreditsJoinerState
		err = gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state file: %v", err)
		}

		for clientId, moviesConsumerState := range state.MoviesConsumers {
			moviesConsumer, err := utils.NewConsumerQueue(conn, "filter_q4_client_"+clientId, "filter_q4_client_"+clientId, env.AppEnv.MOVIES_RECEIVER_AMOUNT)
			if err != nil {
				log.Fatalf("Failed to create movies consumer: %v", err)
			}
			moviesConsumer.RestoreState(moviesConsumerState)

			creditsJoiner.MoviesConsumers[clientId] = moviesConsumer
		}

		for clientId, creditsConsumerState := range state.CreditsConsumers {
			creditsConsumer, err := utils.NewConsumerQueue(conn, "credits_joiner_client_"+clientId, "credits_joiner_client_"+clientId, env.AppEnv.CREDITS_RECEIVER_AMOUNT)
			if err != nil {
				log.Fatalf("Failed to create credits consumer: %v", err)
			}
			creditsConsumer.RestoreState(creditsConsumerState)

			creditsJoiner.CreditsConsumers[clientId] = creditsConsumer
		}
	}

	creditsJoiner.JoinCredits(env.AppEnv.ID)

}
