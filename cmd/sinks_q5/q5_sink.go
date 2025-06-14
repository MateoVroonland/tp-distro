package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/sinks"
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

	previousReplicas := env.AppEnv.SENTIMENT_REDUCER_AMOUNT
	sinkConsumer, err := utils.NewConsumerQueue(conn, "sentiment_sink", "sentiment_sink", previousReplicas)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	resultReplicas := 1
	resultsProducer, err := utils.NewProducerQueue(conn, "results", resultReplicas)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.ID, env.AppEnv.SERVICE_TYPE)
	go healthCheckServer.Start()

	log.Printf("Sentiment sink initialized - consuming from %d reducers", previousReplicas)

	sink := sinks.NewSentimentSink(sinkConsumer, resultsProducer)
	sink.Sink()
}
