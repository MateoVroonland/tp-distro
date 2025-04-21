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

	filteredByYearConsumer, err := utils.NewConsumerQueue(conn, "movies_filtered_by_year_q1", "movies_filtered_by_year_q1", "q1_sink_internal")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	resultsProducer, err := utils.NewProducerQueue(conn, "results", "results")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	log.Printf("Q1 sink initialized")

	sink := sinks.NewQ1Sink(filteredByYearConsumer, resultsProducer)

	forever := make(chan bool)
	go sink.Reduce()

	<-forever
}
