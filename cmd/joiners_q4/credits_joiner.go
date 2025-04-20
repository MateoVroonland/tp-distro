package main

import (
	"log"
	"os"

	"github.com/MateoVroonland/tp-distro/internal/joiners"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	id := os.Getenv("ID")

	if id == "" {
		log.Fatalf("ID is not set")
	}

	creditsJoinerConsumer, err := utils.NewConsumerQueueWithRoutingKey(conn, "credits_joiner", "credits_joiner", id)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	moviesJoinerConsumer, err := utils.NewConsumerQueue(conn, "movies_filtered_by_year_q4", "movies_filtered_by_year_q4")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sinkProducer, err := utils.NewProducerQueue(conn, "sink_q4", "sink_q4")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	creditsJoiner := joiners.NewCreditsJoiner(creditsJoinerConsumer, moviesJoinerConsumer, sinkProducer)
	log.Printf("Credits joiner initialized with id '%s'", id)
	creditsJoiner.JoinCredits()

}
