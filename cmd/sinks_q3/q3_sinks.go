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

	sinkConsumer, err := utils.NewConsumerQueue(conn, "q3_sink", "q3_sink", env.AppEnv.RATINGS_JOINER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	sinkProducer, err := utils.NewProducerQueue(conn, "results", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	var state sinks.Q3SinkState
	stateFile, err := os.ReadFile("data/q3_sink_state.gob")

	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.ID)
	go healthCheckServer.Start()

	q3Sink := sinks.NewQ3Sink(sinkConsumer, sinkProducer)

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
		sinkProducer.RestoreState(state.ResultsProducer)
		log.Printf("State restored up to sequence number: %v", state.SinkConsumer.SequenceNumbers)
		q3Sink.SetClientsResults(state.ClientsResults)
	}

	q3Sink.GetMaxAndMinMovies()
}
