package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/sinks"
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

	filteredByYearConsumer, err := utils.NewConsumerQueue(conn, "movies_filtered_by_year_q1", "movies_filtered_by_year_q1", env.AppEnv.Q1_FILTER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	resultsProducer, err := utils.NewProducerQueue(conn, "results", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.ID, env.AppEnv.SERVICE_TYPE)
	go healthCheckServer.Start()

	log.Printf("Q1 sink initialized")

	sink := sinks.NewQ1Sink(filteredByYearConsumer, resultsProducer)

	go sink.Reduce()

	<-sigs
	log.Printf("Received SIGTERM signal, closing connection")
	conn.Close()
}
