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

	// inputQueue, err := utils.NewConsumerQueue(conn, "movies_sentiment_processed", "movies_sentiment_processed", "sentiment_reducer_internal")
	// if err != nil {
	// 	log.Fatalf("Failed to declare input queue: %v", err)
	// }

	// outputQueue, err := utils.NewProducerQueue(conn, "sentiment_sink", "sentiment_sink")
	// if err != nil {
	// 	log.Fatalf("Failed to declare output queue: %v", err)
	// }

	// log.Printf("Sentiment reducer initialized")
	// reducers.NewSentimentReducer(inputQueue, outputQueue).Reduce()
}
