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

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	ratingsJoinerConsumer, err := utils.NewQueue(ch, "ratings_joiner", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	moviesJoinerConsumer, err := utils.NewQueue(ch, "movies_metadata_q3", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sinkConsumer, err := utils.NewQueue(ch, "sink", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	var forever chan struct{}

	ratingsJoiner := joiners.NewRatingsJoiner(ch, ratingsJoinerConsumer, moviesJoinerConsumer, sinkConsumer)

	go ratingsJoiner.JoinRatings()


	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
