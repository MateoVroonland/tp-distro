package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/filters"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	filteredByCountryConsumer, err := utils.NewQueue(conn, "movies_metadata_q1", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	filteredByYearProducer, err := utils.NewQueue(conn, "movies_filtered_by_year_q1", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	log.Printf("Filter 2000s initialized")

	filter := filters.NewFilter2000s(filteredByCountryConsumer, filteredByYearProducer)

	forever := make(chan bool)
	go filter.FilterAndPublish()

	<-forever
}
