package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/receiver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Failed to load environment variables: %v", err)
	}

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	q, err := utils.NewConsumerQueue(conn, "movies", "movies", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	q1, err := utils.NewProducerQueue(conn, "movies_metadata_q1", env.AppEnv.Q1_FILTER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q1.CloseChannel()

	// q2, err := utils.NewProducerQueue(conn, "movies_metadata_q2", "movies")
	// if err != nil {
	// 	log.Fatalf("Failed to declare a queue: %v", err)
	// }
	// defer q2.CloseChannel()

	q3, err := utils.NewProducerQueue(conn, "movies_metadata_q3", env.AppEnv.Q3_FILTER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q3.CloseChannel()

	q4, err := utils.NewProducerQueue(conn, "movies_metadata_q4", env.AppEnv.Q4_FILTER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q4.CloseChannel()

	q5, err := utils.NewProducerQueue(conn, "movies_metadata_q5", env.AppEnv.SENTIMENT_WORKER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q5.CloseChannel()

	receiver := receiver.NewMoviesReceiver(conn, q, q1, nil, q3, q4, q5)
	receiver.ReceiveMovies()

}
