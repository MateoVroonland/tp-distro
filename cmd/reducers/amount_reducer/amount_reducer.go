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

	q, err := utils.NewQueue(conn, "movies_metadata_q2", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	publishQueue, err := utils.NewQueue(conn, "budget_sink", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	reducers.NewBudgetReducer(q, publishQueue).Reduce()
}
