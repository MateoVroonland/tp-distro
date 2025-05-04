package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	"github.com/MateoVroonland/tp-distro/internal/sinks"
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

	sinkConsumer, err := utils.NewConsumerQueue(conn, "q3_sink", "q3_sink", env.AppEnv.RATINGS_JOINER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sinkProducer, err := utils.NewProducerQueue(conn, "results", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	q3Sink := sinks.NewQ3Sink(sinkConsumer, sinkProducer)

	q3Sink.GetMaxAndMinMovies()

}
