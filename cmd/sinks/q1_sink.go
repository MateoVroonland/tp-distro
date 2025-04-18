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

	filteredByYearConsumer, err := utils.NewQueue(conn, "movies_filtered_by_year_q1", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	resultsProducer, err := utils.NewQueue(conn, "results", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	log.Printf("Q1 sink initialized")

	sink := sinks.NewQ1Sink(filteredByYearConsumer, resultsProducer)

	forever := make(chan bool)
	go sink.Reduce()

	<-forever
}
