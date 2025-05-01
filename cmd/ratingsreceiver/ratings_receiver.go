package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

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

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)


	rawRatingsConsumer, err := utils.NewConsumerQueue(conn, "ratings", "ratings", "ratings_receiver_ratings_internal")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	joinerProducer, err := utils.NewProducerQueue(conn, "ratings_joiner", "ratings_joiner")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}


	receiver := receiver.NewRatingsReceiver(conn, rawRatingsConsumer, joinerProducer)
	go receiver.ReceiveRatings()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-sigs
	log.Printf("Received SIGTERM signal, closing connection")
}