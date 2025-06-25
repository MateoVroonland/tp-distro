package sinks

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"math"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type SentimentSinkResults struct {
	PositiveRatio      float64
	PositiveMovies     int
	PositiveTotalRatio float64
	NegativeRatio      float64
	NegativeMovies     int
	NegativeTotalRatio float64
}

func NewSentimentSinkResults() SentimentSinkResults {
	return SentimentSinkResults{
		PositiveRatio:      0,
		PositiveMovies:     0,
		PositiveTotalRatio: 0,
		NegativeRatio:      0,
		NegativeMovies:     0,
		NegativeTotalRatio: 0,
	}
}

type SentimentSink struct {
	sinkConsumer    *utils.ConsumerQueue
	resultsProducer *utils.ProducerQueue
	clientResults   map[string]SentimentSinkResults
}

func NewSentimentSink(queue *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *SentimentSink {
	return &SentimentSink{
		sinkConsumer:    queue,
		resultsProducer: resultsProducer,
		clientResults:   make(map[string]SentimentSinkResults),
	}
}

func (s *SentimentSink) SetClientResults(results map[string]SentimentSinkResults) {
	s.clientResults = results
}

func (s *SentimentSink) SendClientResults(clientId string) {
	log.Printf("Sending sentiment results for client: %s", clientId)
	results := s.clientResults[clientId]

	positiveRatio := math.Round(results.PositiveRatio*100) / 100
	negativeRatio := math.Round(results.NegativeRatio*100) / 100

	row := messages.NewQ5Row(positiveRatio, negativeRatio)
	rows := []messages.Q5Row{*row}

	rowsBytes, err := json.Marshal(rows)
	if err != nil {
		log.Printf("Failed to marshal rows: %v", err)
		return
	}

	result := messages.RawResult{
		QueryID: messages.Query5Type,
		Results: rowsBytes,
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.Printf("Failed to marshal result: %v", err)
		return
	}

	err = s.resultsProducer.PublishResults(resultBytes, clientId, "q5")
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}

	log.Printf("Client %s: Sentiment results published to client", clientId)
}

func (s *SentimentSink) Sink() {
	log.Printf("Sentiment sink started, consuming messages...")

	for msg := range s.sinkConsumer.ConsumeInfinite() {
		clientId := msg.ClientId

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := SaveSentimentSinkState(s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				continue
			}

			if _, ok := s.clientResults[clientId]; !ok {
				log.Printf("No client results to send for client %s, skipping", clientId)
			} else {
				log.Printf("Received FINISHED message for client %s", clientId)
				s.SendClientResults(clientId)
				delete(s.clientResults, clientId)

				err := SaveSentimentSinkState(s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
			}
			msg.Ack()
			continue
		}

		if _, ok := s.clientResults[clientId]; !ok {
			s.clientResults[clientId] = NewSentimentSinkResults()
		}

		reader := csv.NewReader(strings.NewReader(msg.Body))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false)
			err := SaveSentimentSinkState(s)
			if err != nil {
				log.Printf("Failed to save state: %v", err)
			}
			continue
		}

		var sentimentResult messages.SentimentResult
		err = sentimentResult.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize sentiment result: %v", err)
			msg.Nack(false)
			err := SaveSentimentSinkState(s)
			if err != nil {
				log.Printf("Failed to save state: %v", err)
			}
			continue
		}

		clientResults := s.clientResults[clientId]

		switch sentimentResult.Sentiment {
		case "POSITIVE":
			clientResults.PositiveTotalRatio += sentimentResult.AverageRatio * float64(sentimentResult.TotalMovies)
			clientResults.PositiveMovies += sentimentResult.TotalMovies
			log.Printf("Client %s: Received positive sentiment stats: ratio=%.6f, movies=%d",
				clientId, sentimentResult.AverageRatio, sentimentResult.TotalMovies)
		case "NEGATIVE":
			clientResults.NegativeTotalRatio += sentimentResult.AverageRatio * float64(sentimentResult.TotalMovies)
			clientResults.NegativeMovies += sentimentResult.TotalMovies
			log.Printf("Client %s: Received negative sentiment stats: ratio=%.6f, movies=%d",
				clientId, sentimentResult.AverageRatio, sentimentResult.TotalMovies)
		default:
			log.Printf("Client %s: Unknown sentiment: %s", clientId, sentimentResult.Sentiment)
		}

		if clientResults.PositiveMovies > 0 {
			clientResults.PositiveRatio = clientResults.PositiveTotalRatio / float64(clientResults.PositiveMovies)
		}

		if clientResults.NegativeMovies > 0 {
			clientResults.NegativeRatio = clientResults.NegativeTotalRatio / float64(clientResults.NegativeMovies)
		}

		s.clientResults[clientId] = clientResults

		err = SaveSentimentSinkState(s)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}

		msg.Ack()
	}
}
