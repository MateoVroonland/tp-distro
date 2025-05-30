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

	err = s.resultsProducer.PublishResults(bytes, clientId, "q1")

	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}
}

func (s *Q1Sink) Reduce() {
	// log.Printf("Q1 sink consuming messages")

	for msg := range s.filteredByYearConsumer.ConsumeInfinite() {

		if msg.Body == "FINISHED" {
			log.Printf("Received FINISHED message for client %s", msg.ClientId)
			s.SendClientIdResults(msg.ClientId)
			msg.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(msg.Body))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false)
			continue
		}
		var movie messages.Q1SinkMovie
		err = movie.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false)
			continue
		}

		row, ok := s.clientResults[msg.ClientId]
		if !ok {
			row = make([]messages.Q1Row, 0)
		}
		row = append(row, *messages.NewQ1Row(movie.ID, movie.Title, movie.Genres))
		s.clientResults[msg.ClientId] = row

		msg.Ack()
	}
}
