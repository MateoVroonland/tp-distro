package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/requesthandler"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	server := requesthandler.NewServer(conn)
	server.Start()
}
