package main

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/env"
)

func main() {
	err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Failed to load environment variables: %v", err)
	}

	// conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	// if err != nil {
	// 	log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	// }
	// defer conn.Close()

	// sinkConsumer, err := utils.NewConsumerQueue(conn, "sink_q4", "sink_q4", "q4_sink_internal")
	// if err != nil {
	// 	log.Fatalf("Failed to declare a queue: %v", err)
	// }

	// resultsProducer, err := utils.NewProducerQueue(conn, "results", "results")
	// if err != nil {
	// 	log.Fatalf("Failed to declare a queue: %v", err)
	// }

	// sink := sinks.NewCreditsSink(sinkConsumer, resultsProducer)

	// sink.Sink()

}
