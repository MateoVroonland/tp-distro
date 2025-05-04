package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/joiners"
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


	newClientQueue, err := utils.NewConsumerFanout(conn, "new_client_fanout_q4")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	creditsJoiner := joiners.NewCreditsJoiner(conn, newClientQueue)
	creditsJoiner.JoinCredits(env.AppEnv.ID)

}
