package main

import (
	"log"

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

	log.Printf("Sentiment worker initialized - consuming from %d receivers, producing to %d reducers",
		previousReplicas, nextReplicas)

	worker := sentiment.NewSentimentWorker(inputQueue, outputQueue)
	worker.Start()
}
