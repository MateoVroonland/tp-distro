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

	inputQueue, err := utils.NewConsumerQueue(conn, "movies_sentiment_processed", "movies_sentiment_processed")
	if err != nil {
		log.Fatalf("Failed to declare input queue: %v", err)
	}

	outputQueue, err := utils.NewProducerQueue(conn, "sentiment_sink", "sentiment_sink")
	if err != nil {
		log.Fatalf("Failed to declare output queue: %v", err)
	}

	log.Printf("Sentiment reducer initialized")
	reducers.NewSentimentReducer(inputQueue, outputQueue).Reduce()
}
