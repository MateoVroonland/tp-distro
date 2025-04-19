package main

import (
	"fmt"
	"log"
	"os"

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

	query := os.Getenv("QUERY")

	if query == "" {
		log.Fatalf("QUERY must be set")
	}

	consumeQueueName := fmt.Sprintf("movies_metadata_q%s", query)
	publishQueueName := fmt.Sprintf("movies_filtered_by_year_q%s", query)

	filteredByCountryConsumer, err := utils.NewConsumerQueue(conn, consumeQueueName, "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	filteredByYearProducer, err := utils.NewProducerQueue(conn, publishQueueName, publishQueueName)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	log.Printf("Filter 2000s initialized")

	filter := filters.NewFilter2000s(filteredByCountryConsumer, filteredByYearProducer)

	forever := make(chan bool)
	go filter.FilterAndPublish()

	<-forever
}
