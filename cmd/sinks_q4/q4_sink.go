package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/sinks"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	sinkConsumer, err := utils.NewConsumerQueue(conn, "sink_q4", "sink_q4")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	resultsProducer, err := utils.NewProducerQueue(conn, "results", "results")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sink := sinks.NewCreditsSink(sinkConsumer, resultsProducer)

	sink.Sink()

}
