package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/reducers"
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

	q, err := utils.NewConsumerQueue(conn, "movies_metadata_q2", "movies_metadata_q2", env.AppEnv.MOVIES_RECEIVER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	publishQueue, err := utils.NewProducerQueue(conn, "budget_sink", env.AppEnv.BUDGET_SINK_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.PORT, env.AppEnv.ID)
	go healthCheckServer.Start()

	reducers.NewBudgetReducer(q, publishQueue).Reduce()
}
