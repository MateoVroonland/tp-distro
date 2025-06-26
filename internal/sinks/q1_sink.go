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

func (s *Q1Sink) SetClientResults(results map[string][]messages.Q1Row) {
	s.clientResults = results
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
	q1SinkStateSaver := NewQ1SinkStateSaver()
	log.Printf("Q1 sink consuming messages")

	for msg := range s.filteredByYearConsumer.ConsumeInfinite() {
		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := q1SinkStateSaver.SaveStateAck(&msg, s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				continue
			}

			if _, ok := s.clientResults[msg.ClientId]; !ok {
				log.Printf("No client results to send for client %s, skipping", msg.ClientId)
			} else {
				log.Printf("Received FINISHED message for client %s", msg.ClientId)
				s.SendClientIdResults(msg.ClientId)
				delete(s.clientResults, msg.ClientId)

				err := q1SinkStateSaver.SaveStateAck(&msg, s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				q1SinkStateSaver.ForceFlush()
			}
			continue
		}

		reader := csv.NewReader(strings.NewReader(msg.Body))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			q1SinkStateSaver.SaveStateNack(&msg, s, false)
			continue
		}
		var movie messages.Q1SinkMovie
		err = movie.Deserialize(record)
		if err != nil {
			q1SinkStateSaver.SaveStateNack(&msg, s, false)
			continue
		}

		row, ok := s.clientResults[msg.ClientId]
		if !ok {
			row = make([]messages.Q1Row, 0)
		}
		row = append(row, *messages.NewQ1Row(movie.ID, movie.Title, movie.Genres))
		s.clientResults[msg.ClientId] = row

		err = q1SinkStateSaver.SaveStateAck(&msg, s)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}
	}
}
