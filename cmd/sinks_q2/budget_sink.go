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

	budgetSinkConsumer, err := utils.NewConsumerQueue(conn, "budget_sink", "budget_sink", env.AppEnv.BUDGET_REDUCER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	resultsProducer, err := utils.NewProducerQueue(conn, "results", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	var state sinks.BudgetSinkState
	stateFile, err := os.ReadFile("data/budget_sink_state.gob")
	healthCheckServer := utils.NewHealthCheckServer(env.AppEnv.ID, env.AppEnv.SERVICE_TYPE)
	go healthCheckServer.Start()

	sink := sinks.NewBudgetSink(budgetSinkConsumer, resultsProducer)

	if os.IsNotExist(err) {
		log.Println("State file does not exist, creating new state")
	} else if err != nil {
		log.Fatalf("Failed to read state: %v", err)
	} else {
		err := gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state: %v", err)
		}
		budgetSinkConsumer.RestoreState(state.Queue)
		resultsProducer.RestoreState(state.ResultsProducer)
		log.Printf("State restored up to sequence number: %v", state.Queue.SequenceNumbers)
		sink.BudgetPerCountry = state.BudgetPerCountry

	}

	sink.Sink()

}
