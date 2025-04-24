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
}

func NewSentimentSink(queue *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *SentimentSink {
	return &SentimentSink{
		sinkConsumer:    queue,
		resultsProducer: resultsProducer,
	}
}

func (s *SentimentSink) Sink() {
	var positiveRatio float64
	var negativeRatio float64

	log.Printf("Sentiment sink started, consuming messages...")

	for msg := range s.sinkConsumer.Consume() {
		bodyStr := string(msg.Body)

		stats, err := messages.ParseSentimentStats(bodyStr)
		if err != nil {
			log.Printf("Failed to parse sentiment stats: %v", err)
			msg.Nack(false, false)
			continue
		}

		if stats.Sentiment == "POSITIVE" {
			positiveRatio = stats.AverageRatio
			log.Printf("Received positive sentiment stats: ratio=%.6f, movies=%d, total=%d",
				stats.AverageRatio, stats.TotalMovies, stats.ProcessedCount)
		} else if stats.Sentiment == "NEGATIVE" {
			negativeRatio = stats.AverageRatio
			log.Printf("Received negative sentiment stats: ratio=%.6f, movies=%d, total=%d",
				stats.AverageRatio, stats.TotalMovies, stats.ProcessedCount)
		} else {
			log.Printf("Unknown sentiment: %s", stats.Sentiment)
		}

		msg.Ack(false)
	}

	rows := []messages.Q5Row{
		*messages.NewQ5Row(positiveRatio, negativeRatio),
	}

	rowsBytes, err := json.Marshal(rows)
	if err != nil {
		log.Printf("Failed to marshal rows: %v", err)
		return
	}

	results := messages.RawResult{
		QueryID: messages.Query5Type,
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

	log.Printf("Published sentiment analysis results")
}
