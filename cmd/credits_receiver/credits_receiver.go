package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/receiver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	rawRatingsConsumer, err := utils.NewQueue(conn, "ratings", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	joinerProducer, err := utils.NewQueue(conn, "ratings_joiner", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	receiver := receiver.NewCreditsReceiver(conn, rawRatingsConsumer, joinerProducer)
	receiver.ReceiveCredits()

}
