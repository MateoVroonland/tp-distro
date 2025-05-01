package sinks

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type Q1Sink struct {
	filteredByYearConsumer *utils.ConsumerQueue
	resultsProducer        *utils.ProducerQueue
	clientResults          map[string][]messages.Q1Row
}

func NewQ1Sink(filteredByYearConsumer *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *Q1Sink {
	return &Q1Sink{
		filteredByYearConsumer: filteredByYearConsumer,
		resultsProducer:        resultsProducer,
		clientResults:          make(map[string][]messages.Q1Row),
	}
}

func (s *Q1Sink) SendClientIdResults(clientId string) {
	log.Printf("Sending results for clientId: %s", clientId)
	rows := s.clientResults[clientId]
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

	err = s.resultsProducer.Publish(bytes, clientId)
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}
}

func (s *Q1Sink) Reduce() {
	log.Printf("Q1 sink consuming messages")

	for msg := range s.filteredByYearConsumer.ConsumeSink() {
		var clientId string
		var ok bool

		if clientId, ok = msg.Headers["clientId"].(string); !ok {
			log.Printf("Failed to get clientId from message headers")
			msg.Nack(false, false)
			continue
		}

		stringLine := string(msg.Body)

		if stringLine == "FINISHED" {
			log.Printf("Received FINISHED message")
			s.SendClientIdResults(clientId)
			msg.Ack(false)
			continue
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false, false)
			continue
		}
		var movie messages.Q1SinkMovie
		err = movie.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false, false)
			continue
		}

		row, ok := s.clientResults[clientId]
		if !ok {
			row = make([]messages.Q1Row, 0)
		}
		row = append(row, *messages.NewQ1Row(movie.ID, movie.Title, movie.Genres))
		s.clientResults[clientId] = row

		msg.Ack(false)
	}
}
