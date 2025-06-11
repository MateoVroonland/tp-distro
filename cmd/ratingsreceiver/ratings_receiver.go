package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

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

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	rawRatingsConsumer, err := utils.NewConsumerQueue(conn, "ratings", "ratings", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.PORT, env.AppEnv.ID)
	go healthCheckServer.Start()



	receiver := receiver.NewRatingsReceiver(conn, rawRatingsConsumer)
	receiver.ReceiveRatings()

	// log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	// <-sigs
	// log.Printf("Received SIGTERM signal, closing connection")
}
