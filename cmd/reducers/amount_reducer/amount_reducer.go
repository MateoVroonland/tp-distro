package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/reducers/budget_reducer"
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

	q, err := utils.NewQueue(ch, "movies_metadata_q2", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	msgs, err := q.Consume()
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	budgetPerCountry := budget_reducer.ReduceBudget(msgs)
	log.Printf("Budget per country: %v", budgetPerCountry)
}
