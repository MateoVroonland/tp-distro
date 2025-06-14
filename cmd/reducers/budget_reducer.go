package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/reducers"
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

	q, err := utils.NewConsumerQueue(conn, "movies_metadata_q2", "movies_metadata_q2", env.AppEnv.MOVIES_RECEIVER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	publishQueue, err := utils.NewProducerQueue(conn, "budget_sink", env.AppEnv.BUDGET_SINK_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	reducer := reducers.NewBudgetReducer(q, publishQueue)

	stateFile, readErr := os.ReadFile("data/budget_reducer_state.gob")
	var state reducers.BudgetReducerState

	if os.IsNotExist(readErr) {
		log.Println("State file does not exist, creating new state")
	} else if readErr != nil {
		log.Fatalf("Failed to read state: %v", readErr)
	} else {
		err := gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state: %v", err)
		}

		q.RestoreState(state.Queue)
		publishQueue.RestoreState(state.PublishQueue)
		log.Printf("State restored up to sequence number: %v", state.Queue.SequenceNumbers)
		reducer.BudgetPerCountry = state.BudgetPerCountry
	}

	reducer.Reduce()
}
