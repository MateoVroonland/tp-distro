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

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	rawRatingsConsumer, err := utils.NewQueue(ch, "ratings", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	joinerProducer, err := utils.NewQueue(ch, "ratings_joiner", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	var forever chan struct{}

	receiver := receiver.NewRatingsReceiver(ch, rawRatingsConsumer, joinerProducer)
	go receiver.ReceiveRatings()


	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
