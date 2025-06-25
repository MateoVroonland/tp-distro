package reducers

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type SentimentReducer struct {
	queue        *utils.ConsumerQueue
	publishQueue *utils.ProducerQueue
	ClientStats  map[string]map[string]SentimentStats
}

type SentimentStats struct {
	Sentiment    string
	TotalMovies  int
	TotalRatio   float64
	AverageRatio float64
}

func NewSentimentStats(sentiment string) SentimentStats {
	return SentimentStats{
		Sentiment:    sentiment,
		TotalMovies:  0,
		TotalRatio:   0,
		AverageRatio: 0,
	}
}

func NewSentimentReducer(queue *utils.ConsumerQueue, publishQueue *utils.ProducerQueue) *SentimentReducer {
	return &SentimentReducer{
		queue:        queue,
		publishQueue: publishQueue,
		ClientStats:  make(map[string]map[string]SentimentStats),
	}
}

func (r *SentimentReducer) Reduce() {
	processedCount := make(map[string]int)

	defer r.queue.CloseChannel()
	defer r.publishQueue.CloseChannel()

	log.Printf("Sentiment reducer started processing")

	for msg := range r.queue.ConsumeInfinite() {
		clientId := msg.ClientId

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := SaveSentimentReducerState(r)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				continue
			}

			if _, ok := r.ClientStats[clientId]; !ok {
				log.Printf("No client stats to send for client %s, skipping", clientId)
			} else {
				log.Printf("Received FINISHED message for client %s", clientId)
				r.CalculateAverages(clientId)
				r.SendResults(clientId)
				delete(r.ClientStats, clientId)

				err := SaveSentimentReducerState(r)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
			}

			msg.Ack()
			continue
		}

		if _, ok := r.ClientStats[clientId]; !ok {
			r.ClientStats[clientId] = map[string]SentimentStats{
				"POSITIVE": NewSentimentStats("POSITIVE"),
				"NEGATIVE": NewSentimentStats("NEGATIVE"),
			}
			processedCount[clientId] = 0
		}

		processedCount[clientId]++

		reader := csv.NewReader(strings.NewReader(msg.Body))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false)
			err := SaveSentimentReducerState(r)
			if err != nil {
				log.Printf("Failed to save sentiment reducer state: %v", err)
			}
			continue
		}

		var movieSentiment messages.SentimentAnalysis
		err = movieSentiment.Deserialize(record)

		if err != nil {
			msg.Nack(false)
			err := SaveSentimentReducerState(r)
			if err != nil {
				log.Printf("Failed to save sentiment reducer state: %v", err)
			}
			continue
		}

		clientStats := r.ClientStats[clientId]

		switch movieSentiment.Sentiment {
		case "POSITIVE":
			stats := clientStats["POSITIVE"]
			stats.TotalMovies++
			stats.TotalRatio += movieSentiment.Ratio
			clientStats["POSITIVE"] = stats
		case "NEGATIVE":
			stats := clientStats["NEGATIVE"]
			stats.TotalMovies++
			stats.TotalRatio += movieSentiment.Ratio
			clientStats["NEGATIVE"] = stats
		}

		r.ClientStats[clientId] = clientStats
		err = SaveSentimentReducerState(r)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}
		msg.Ack()
	}
}

func (r *SentimentReducer) CalculateAverages(clientId string) {
	clientStats := r.ClientStats[clientId]

	positiveStats := clientStats["POSITIVE"]
	if positiveStats.TotalMovies > 0 {
		positiveStats.AverageRatio = positiveStats.TotalRatio / float64(positiveStats.TotalMovies)
		clientStats["POSITIVE"] = positiveStats
	}

	negativeStats := clientStats["NEGATIVE"]
	if negativeStats.TotalMovies > 0 {
		negativeStats.AverageRatio = negativeStats.TotalRatio / float64(negativeStats.TotalMovies)
		clientStats["NEGATIVE"] = negativeStats
	}

	r.ClientStats[clientId] = clientStats

	log.Printf("Client %s: Sentiment statistics calculated - Positive avg ratio: %.2f (%d movies), Negative avg ratio: %.2f (%d movies)",
		clientId,
		positiveStats.AverageRatio, positiveStats.TotalMovies,
		negativeStats.AverageRatio, negativeStats.TotalMovies)
}

func (r *SentimentReducer) SendResults(clientId string) {
	clientStats := r.ClientStats[clientId]
	positiveStats := clientStats["POSITIVE"]
	negativeStats := clientStats["NEGATIVE"]

	positiveResult := messages.SentimentResult{
		Sentiment:    "POSITIVE",
		AverageRatio: positiveStats.AverageRatio,
		TotalMovies:  positiveStats.TotalMovies,
	}

	negativeResult := messages.SentimentResult{
		Sentiment:    "NEGATIVE",
		AverageRatio: negativeStats.AverageRatio,
		TotalMovies:  negativeStats.TotalMovies,
	}

	r.publishQueue.Publish([]byte(positiveResult.Serialize()), clientId, "")
	r.publishQueue.Publish([]byte(negativeResult.Serialize()), clientId, "")

	r.publishQueue.PublishFinished(clientId)

	log.Printf("Client %s: Sentiment analysis results published", clientId)
}
