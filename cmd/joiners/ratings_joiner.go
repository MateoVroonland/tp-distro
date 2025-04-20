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

	ratingsJoinerConsumer, err := utils.NewConsumerQueueWithRoutingKey(conn, "ratings_joiner", "ratings_joiner", id)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	moviesJoinerConsumer, err := utils.NewConsumerQueue(conn, "movies_filtered_by_year_q3", "movies_filtered_by_year_q3")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sinkProducer, err := utils.NewProducerQueue(conn, "q3_sink", "q3_sink")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	ratingsJoiner := joiners.NewRatingsJoiner(ratingsJoinerConsumer, moviesJoinerConsumer, sinkProducer)
	log.Printf("Ratings joiner initialized with id '%s'", id)
	ratingsJoiner.JoinRatings()

}
