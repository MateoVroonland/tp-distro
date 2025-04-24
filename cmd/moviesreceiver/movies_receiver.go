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


	q, err := utils.NewConsumerQueue(conn, "movies_metadata", "movies_metadata", "movies_receiver_internal")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	q1, err := utils.NewProducerQueue(conn, "movies_metadata_q1", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q1.CloseChannel()

	q2, err := utils.NewProducerQueue(conn, "movies_metadata_q2", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q2.CloseChannel()

	q3, err := utils.NewProducerQueue(conn, "movies_metadata_q3", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q3.CloseChannel()

	q4, err := utils.NewProducerQueue(conn, "movies_metadata_q4", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q4.CloseChannel()

	q5, err := utils.NewProducerQueue(conn, "movies_metadata_q5", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q5.CloseChannel()

	receiver := receiver.NewMoviesReceiver(conn, q, q1, q2, q3, q4, q5)
	receiver.ReceiveMovies()

	

}
