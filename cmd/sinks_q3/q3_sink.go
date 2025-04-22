package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/sinks"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	sinkConsumer, err := utils.NewConsumerQueue(conn, "q3_sink", "q3_sink", "q3_sink_internal")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sinkProducer, err := utils.NewProducerQueue(conn, "results", "results")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	
	q3Sink := sinks.NewQ3Sink(sinkConsumer, sinkProducer)

	q3Sink.GetMaxAndMinMovies()

	
	

	
}