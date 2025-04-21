package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/reducers"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	q, err := utils.NewConsumerQueue(conn, "movies_metadata_q2", "movies", "budget_reducer_internal")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	publishQueue, err := utils.NewProducerQueue(conn, "budget_sink", "budget_sink")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	reducers.NewBudgetReducer(q, publishQueue).Reduce()
}
