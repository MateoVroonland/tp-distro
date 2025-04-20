package reducers

import (
	"encoding/csv"
	"fmt"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

const SENTIMENT_WORKER_AMOUNT = 5
const SENTIMENT_REDUCER_AMOUNT = 1

type SentimentReducer struct {
	queue        *utils.ConsumerQueue
	publishQueue *utils.ProducerQueue
}

type SentimentStats struct {
	Sentiment    string  `json:"sentiment"`
	TotalMovies  int     `json:"total_movies"`
	TotalRatio   float64 `json:"total_ratio"`
	AverageRatio float64 `json:"average_ratio"`
}

func NewSentimentStats(sentiment string) SentimentStats {
	return SentimentStats{
		Sentiment:    sentiment,
		TotalMovies:  0,
		TotalRatio:   0,
		AverageRatio: 0,
	}
}

func (s *SentimentStats) ToCSV() []string {
	return []string{
		s.Sentiment,
		fmt.Sprintf("%.2f", s.AverageRatio),
	}
}

func NewSentimentReducer(queue *utils.ConsumerQueue, publishQueue *utils.ProducerQueue) *SentimentReducer {
	return &SentimentReducer{queue: queue, publishQueue: publishQueue}
}

func (r *SentimentReducer) Reduce() {
	positiveStats := NewSentimentStats("POSITIVE")
	negativeStats := NewSentimentStats("NEGATIVE")

	processedCount := 0
	finishedCount := 0

	defer r.queue.CloseChannel()
	defer r.publishQueue.CloseChannel()

	log.Printf("Sentiment reducer started processing")

	for d := range r.queue.Consume() {
		stringLine := string(d.Body)

		if stringLine == "FINISHED" {
			log.Printf("Received termination message (%d/%d)", finishedCount+1, SENTIMENT_WORKER_AMOUNT)
			finishedCount++
			d.Ack(false)

			if finishedCount >= SENTIMENT_WORKER_AMOUNT {
				log.Printf("All sentiment workers have finished, proceeding to publish results")
				break
			}
			continue
		}

		processedCount++

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		log.Printf("Received message in Sentiment reducer: %s", stringLine)
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			d.Nack(false, false)
			continue
		}

		var movieSentiment messages.SentimentAnalysis
		err = movieSentiment.Deserialize(record)

		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			d.Nack(false, false)
			continue
		}

		if movieSentiment.Sentiment == "POSITIVE" {
			positiveStats.TotalMovies++
			positiveStats.TotalRatio += movieSentiment.Ratio
		} else if movieSentiment.Sentiment == "NEGATIVE" {
			negativeStats.TotalMovies++
			negativeStats.TotalRatio += movieSentiment.Ratio
		}

		d.Ack(false)
	}

	log.Printf("Sentiment reducer finished processing %d movies", processedCount)
	if positiveStats.TotalMovies > 0 {
		positiveStats.AverageRatio = positiveStats.TotalRatio / float64(positiveStats.TotalMovies)
	}
	if negativeStats.TotalMovies > 0 {
		negativeStats.AverageRatio = negativeStats.TotalRatio / float64(negativeStats.TotalMovies)
	}

	log.Printf("Publishing sentiment statistics: Positive avg ratio: %.2f (%d movies), Negative avg ratio: %.2f (%d movies)",
		positiveStats.AverageRatio, positiveStats.TotalMovies,
		negativeStats.AverageRatio, negativeStats.TotalMovies)

	positiveCSV := fmt.Sprintf("POSITIVE,%.6f,%d,%d",
		positiveStats.AverageRatio, positiveStats.TotalMovies, processedCount)
	r.publishQueue.Publish([]byte(positiveCSV))

	negativeCSV := fmt.Sprintf("NEGATIVE,%.6f,%d,%d",
		negativeStats.AverageRatio, negativeStats.TotalMovies, processedCount)
	r.publishQueue.Publish([]byte(negativeCSV))

	r.publishQueue.Publish([]byte("FINISHED"))
	log.Printf("Sentiment reducer finished processing %d movies", processedCount)
}
