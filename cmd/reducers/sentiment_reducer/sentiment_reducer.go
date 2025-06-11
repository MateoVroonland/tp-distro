package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/reducers"
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

	previousReplicas := env.AppEnv.SENTIMENT_WORKER_AMOUNT
	inputQueue, err := utils.NewConsumerQueue(conn, "movies_sentiment_processed", "movies_sentiment_processed", previousReplicas)
	if err != nil {
		log.Fatalf("Failed to declare input queue: %v", err)
	}

	nextReplicas := env.AppEnv.SENTIMENT_SINK_AMOUNT
	outputQueue, err := utils.NewProducerQueue(conn, "sentiment_sink", nextReplicas)
	if err != nil {
		log.Fatalf("Failed to declare output queue: %v", err)
	}

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.PORT, env.AppEnv.ID)
	go healthCheckServer.Start()

	log.Printf("Sentiment reducer initialized - consuming from %d workers, producing to %d sinks",
		previousReplicas, nextReplicas)

	reducer := reducers.NewSentimentReducer(inputQueue, outputQueue)
	reducer.Reduce()
}
