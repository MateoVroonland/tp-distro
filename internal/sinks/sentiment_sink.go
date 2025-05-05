package sinks

import (
	"encoding/json"
	"log"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type SentimentSink struct {
	sinkConsumer    *utils.ConsumerQueue
	resultsProducer *utils.ProducerQueue
	clientResults   map[string]messages.SentimentSinkResults
}

func NewSentimentSink(queue *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *SentimentSink {
	return &SentimentSink{
		sinkConsumer:    queue,
		resultsProducer: resultsProducer,
		clientResults:   make(map[string]messages.SentimentSinkResults),
	}
}

func (s *SentimentSink) SendClientResults(clientId string) {
	log.Printf("Sending sentiment results for client: %s", clientId)
	results := s.clientResults[clientId]

	row := messages.NewQ5Row(results.PositiveRatio, results.NegativeRatio)
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

	err = s.resultsProducer.Publish(resultBytes, clientId, "")
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

		if msg.Body == "FINISHED" {
			log.Printf("Received FINISHED message for client %s", clientId)
			s.SendClientResults(clientId)
			msg.Ack()
			continue
		}

		if _, ok := s.clientResults[clientId]; !ok {
			s.clientResults[clientId] = messages.SentimentSinkResults{
				PositiveRatio:      0,
				PositiveMovies:     0,
				PositiveTotalRatio: 0,
				NegativeRatio:      0,
				NegativeMovies:     0,
				NegativeTotalRatio: 0,
			}
		}

		sentimentResult, err := messages.ParseSentimentResult(msg.Body)
		if err != nil {
			log.Printf("Failed to parse sentiment result: %v", err)
			msg.Nack(false)
			continue
		}

		clientResults := s.clientResults[clientId]

		if sentimentResult.Sentiment == "POSITIVE" {
			clientResults.PositiveTotalRatio += sentimentResult.AverageRatio * float64(sentimentResult.TotalMovies)
			clientResults.PositiveMovies += sentimentResult.TotalMovies
			log.Printf("Client %s: Received positive sentiment stats: ratio=%.6f, movies=%d",
				clientId, sentimentResult.AverageRatio, sentimentResult.TotalMovies)
		} else if sentimentResult.Sentiment == "NEGATIVE" {
			clientResults.NegativeTotalRatio += sentimentResult.AverageRatio * float64(sentimentResult.TotalMovies)
			clientResults.NegativeMovies += sentimentResult.TotalMovies
			log.Printf("Client %s: Received negative sentiment stats: ratio=%.6f, movies=%d",
				clientId, sentimentResult.AverageRatio, sentimentResult.TotalMovies)
		} else {
			log.Printf("Client %s: Unknown sentiment: %s", clientId, sentimentResult.Sentiment)
		}

		if clientResults.PositiveMovies > 0 {
			clientResults.PositiveRatio = clientResults.PositiveTotalRatio / float64(clientResults.PositiveMovies)
		}

		if clientResults.NegativeMovies > 0 {
			clientResults.NegativeRatio = clientResults.NegativeTotalRatio / float64(clientResults.NegativeMovies)
		}

		s.clientResults[clientId] = clientResults
		msg.Ack()
	}
}
