package sinks

import (
	"encoding/csv"
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

	results := []messages.Q1SinkMovie{}

	log.Printf("Q1 sink consuming messages")
	for msg := range msgs {
		log.Printf("Received message: %s", string(msg.Body))
		stringLine := string(msg.Body)
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
		results = append(results, movie)
		log.Printf("results: %v", len(results))
		log.Printf("results: %v", results)
	}

	log.Printf("Received %d movies", len(results))


}