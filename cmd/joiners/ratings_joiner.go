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

	ratingsJoinerConsumer, err := utils.NewQueue(conn, "ratings_joiner", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	moviesJoinerConsumer, err := utils.NewQueue(conn, "movies_metadata_q3", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sinkConsumer, err := utils.NewQueue(conn, "sink", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	var forever chan struct{}

	ratingsJoiner := joiners.NewRatingsJoiner(ratingsJoinerConsumer, moviesJoinerConsumer, sinkConsumer)

	go ratingsJoiner.JoinRatings()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
