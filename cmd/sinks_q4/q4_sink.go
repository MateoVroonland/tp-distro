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

	sinkConsumer, err := utils.NewConsumerQueue(conn, "sink_q4", "sink_q4", env.AppEnv.CREDITS_JOINER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	resultsProducer, err := utils.NewProducerQueue(conn, "results", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	var state sinks.CreditsSinkState
	stateFile, err := os.ReadFile("data/credits_sink_state.gob")

	sink := sinks.NewCreditsSink(sinkConsumer, resultsProducer)

	if os.IsNotExist(err) {
		log.Println("State file does not exist, creating new state")
	} else if err != nil {
		log.Fatalf("Failed to read state: %v", err)
	} else {
		err := gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state: %v", err)
		}
		sinkConsumer.RestoreState(state.SinkConsumer)
		resultsProducer.RestoreState(state.ResultsProducer)
		log.Printf("State restored up to sequence number: %v", state.SinkConsumer.SequenceNumbers)
		sink.SetActors(state.Actors)
	}

	sink.Sink()
}
