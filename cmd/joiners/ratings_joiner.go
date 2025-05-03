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

	newClientQueue, err := utils.NewConsumerFanout(conn, "new_client_fanout_q3")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}


	ratingsJoiner := joiners.NewRatingsJoiner(conn, newClientQueue)
	log.Printf("Ratings joiner initialized with id '%s'", id)
	ratingsJoiner.JoinRatings(id)

}