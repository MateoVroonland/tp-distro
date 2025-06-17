package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

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

	previousReplicas := env.AppEnv.SENTIMENT_REDUCER_AMOUNT
	sinkConsumer, err := utils.NewConsumerQueue(conn, "sentiment_sink", "sentiment_sink", previousReplicas)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	resultReplicas := 1
	resultsProducer, err := utils.NewProducerQueue(conn, "results", resultReplicas)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	var state sinks.SentimentSinkState
	stateFile, err := os.ReadFile("data/sentiment_sink_state.gob")

	sink := sinks.NewSentimentSink(sinkConsumer, resultsProducer)

	if os.IsNotExist(err) {
		log.Println("State file does not exist, creating new state")
	} else if err != nil {
		log.Fatalf("Failed to read state: %v", err)
	} else {
		err := gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state: %v", err)
		}
		sinkConsumer.RestoreState(state.Queue)
		resultsProducer.RestoreState(state.ResultsProducer)
		log.Printf("State restored up to sequence number: %v", state.Queue.SequenceNumbers)
		sink.SetClientResults(state.ClientResults)
	}

	log.Printf("Sentiment sink initialized - consuming from %d reducers", previousReplicas)
	sink.Sink()
}
