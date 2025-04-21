package main

import (
	"fmt"
	"log"
	"os"

	"github.com/MateoVroonland/tp-distro/internal/filters"
	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
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

	if query != "1" && query != "3" && query != "4" {
		log.Fatalf("QUERY must be set to 1, 3 or 4")
	}

	consumeQueueName := fmt.Sprintf("movies_metadata_q%s", query)
	publishQueueName := fmt.Sprintf("movies_filtered_by_year_q%s", query)
	consumeQueueNameInternal := fmt.Sprintf("filter_q%s_internal", query)

	filteredByCountryConsumer, err := utils.NewConsumerQueue(conn, consumeQueueName, "movies", consumeQueueNameInternal)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	filteredByYearProducer, err := utils.NewProducerQueue(conn, publishQueueName, publishQueueName)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	log.Printf("Filter 2000s initialized")

	var outputMessage protocol.MovieToFilter
	switch query {
	case "1":
		outputMessage = &messages.Q1Movie{}
	case "3":
		log.Fatalf("Query 3 not implemented")
	case "4":
		outputMessage = &messages.Q4Movie{}
	}
	filter := filters.NewFilter(filteredByCountryConsumer, filteredByYearProducer, outputMessage)

	forever := make(chan bool)
	go filter.FilterAndPublish()

	<-forever
}
