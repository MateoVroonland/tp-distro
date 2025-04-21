package main

import (
	"log"

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

	ratingsJoinerConsumer, err := utils.NewConsumerQueue(conn, "ratings_joiner", "ratings_joiner", "ratings_joiner_ratings_internal")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	moviesJoinerConsumer, err := utils.NewConsumerQueue(conn, "movies_metadata_q3", "movies_metadata_q3", "ratings_joiner_movies_internal")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sinkProducer, err := utils.NewProducerQueue(conn, "sink", "sink")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	ratingsJoiner := joiners.NewRatingsJoiner(ratingsJoinerConsumer, moviesJoinerConsumer, sinkProducer)

	go ratingsJoiner.JoinRatings()

}
