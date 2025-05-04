package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/filters"
	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
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

	query := os.Getenv("QUERY")

	if query != "1" && query != "3" && query != "4" {
		log.Fatalf("QUERY must be set to 1, 3 or 4")
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	consumeQueueName := fmt.Sprintf("movies_metadata_q%s", query)
	publishQueueName := fmt.Sprintf("movies_filtered_by_year_q%s", query)

	filteredByCountryConsumer, err := utils.NewConsumerQueue(conn, consumeQueueName, consumeQueueName, env.AppEnv.MOVIES_RECEIVER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	filteredByYearProducer, err := utils.NewProducerQueue(conn, publishQueueName, env.AppEnv.Q1_SINK_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	log.Printf("Filter 2000s initialized")

	var outputMessage protocol.MovieToFilter
	switch query {
	case "1":
		outputMessage = &messages.Q1Movie{}
	case "3":
		outputMessage = &messages.Q3Movie{}
	case "4":
		outputMessage = &messages.Q4Movie{}
	}

	filter := filters.NewFilter(filteredByCountryConsumer, filteredByYearProducer, outputMessage)
	go filter.FilterAndPublish()

	<-sigs
	log.Printf("Received SIGTERM signal, closing connection")
}
