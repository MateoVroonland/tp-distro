package sinks

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Q1Sink struct {
	ch                     *amqp.Channel
	filteredByYearConsumer *utils.Queue
	resultsProducer        *utils.Queue
}

func NewQ1Sink(ch *amqp.Channel, filteredByYearConsumer *utils.Queue, resultsProducer *utils.Queue) *Q1Sink {
	return &Q1Sink{
		ch:                ch,
		filteredByYearConsumer: filteredByYearConsumer,
		resultsProducer:        resultsProducer,
	}
}

func (s *Q1Sink) Reduce() {
	msgs, err := s.filteredByYearConsumer.Consume()
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	rows := []messages.Q1Row{}

	log.Printf("Q1 sink consuming messages")
	for msg := range msgs {
		log.Printf("Received message: %s", string(msg.Body))
		stringLine := string(msg.Body)
		if stringLine == "FINISHED" {
			log.Printf("Received termination message")
			break
		}
		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Fatalf("Failed to read record: %v", err)
		}
		var movie messages.Q1SinkMovie
		err = movie.Deserialize(record)
		if err != nil {
			log.Fatalf("Failed to deserialize movie: %v", err)
		}
		rows = append(rows, *messages.NewQ1Row(movie.ID, movie.Title, movie.Genres))
	}
	log.Printf("Rows: %v", rows)
	rowsBytes, err := json.Marshal(rows)
	if err != nil {
		log.Printf("Failed to marshal rows: %v", err)
		return
	}

	results := messages.RawResult{
		QueryID: "query1",
		Results: rowsBytes,
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	err = s.resultsProducer.Publish(bytes)
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}
}