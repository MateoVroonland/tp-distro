package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/sentiment"
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

	previousReplicas := env.AppEnv.MOVIES_RECEIVER_AMOUNT
	inputQueue, err := utils.NewConsumerQueue(conn, "movies_metadata_q5", "movies_metadata_q5", previousReplicas)
	if err != nil {
		log.Fatalf("Failed to declare input queue: %v", err)
	}

	nextReplicas := env.AppEnv.SENTIMENT_REDUCER_AMOUNT
	outputQueue, err := utils.NewProducerQueue(conn, "movies_sentiment_processed", nextReplicas)
	if err != nil {
		log.Fatalf("Failed to declare output queue: %v", err)
	}

	var state sentiment.SentimentWorkerState
	stateFile, err := os.ReadFile("data/sentiment_worker_state.gob")

	worker := sentiment.NewSentimentWorker(inputQueue, outputQueue)

	if os.IsNotExist(err) {
		log.Println("State file does not exist, creating new state")
	} else if err != nil {
		log.Fatalf("Failed to read state: %v", err)
	} else {
		err := gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state: %v", err)
		}
		inputQueue.RestoreState(state.InputQueue)
		outputQueue.RestoreState(state.PublishQueue)
		log.Printf("State restored up to sequence number: %v", state.InputQueue.SequenceNumbers)
	}

	log.Printf("Sentiment worker initialized - consuming from %d receivers, producing to %d reducers",
		previousReplicas, nextReplicas)

	// Start health check server
	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.ID)
	go healthCheckServer.Start()

	worker.Start()
}
