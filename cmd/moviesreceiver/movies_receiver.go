package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/receiver"
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

	stateFile, readErr := os.ReadFile("data/movies_receiver_state.gob")

	var q *utils.ConsumerQueue
	var q1, q2, q3, q4, q5 *utils.ProducerQueue

	q, err = utils.NewConsumerQueue(conn, "movies", "movies", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	q1, err = utils.NewProducerQueue(conn, "movies_metadata_q1", env.AppEnv.Q1_FILTER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q1.CloseChannel()

	q2, err = utils.NewProducerQueue(conn, "movies_metadata_q2", env.AppEnv.BUDGET_REDUCER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q2.CloseChannel()

	q3, err = utils.NewProducerQueue(conn, "movies_metadata_q3", env.AppEnv.Q3_FILTER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q3.CloseChannel()

	q4, err = utils.NewProducerQueue(conn, "movies_metadata_q4", env.AppEnv.Q4_FILTER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q4.CloseChannel()

	q5, err = utils.NewProducerQueue(conn, "movies_metadata_q5", env.AppEnv.SENTIMENT_WORKER_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q5.CloseChannel()

	if os.IsNotExist(readErr) {
		log.Println("State file does not exist, creating new state")
	} else if readErr != nil {
		log.Fatalf("Failed to read state: %v", readErr)
	} else {
		var state receiver.MoviesReceiverState
		err := gob.NewDecoder(bytes.NewReader(stateFile)).Decode(&state)
		if err != nil {
			log.Fatalf("Failed to decode state: %v", err)
		}

		q.RestoreState(state.MoviesConsumer)
		q1.RestoreState(state.Q1Producer)
		q2.RestoreState(state.Q2Producer)
		q3.RestoreState(state.Q3Producer)
		q4.RestoreState(state.Q4Producer)
		q5.RestoreState(state.Q5Producer)
	}

	receiver := receiver.NewMoviesReceiver(conn, q, q1, q2, q3, q4, q5)

	if err != nil {
		log.Fatalf("Failed to create receiver: %v", err)
	}

	receiver.ReceiveMovies()
}
