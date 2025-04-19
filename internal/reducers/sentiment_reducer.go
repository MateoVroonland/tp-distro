package reducers

import (
	"encoding/csv"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

const SENTIMENT_REDUCER_AMOUNT = 1

type SentimentReducer struct {
	queue        *utils.Queue
	publishQueue *utils.Queue
}

type SentimentStats struct {
	Sentiment    string  `json:"sentiment"`
	TotalMovies  int     `json:"total_movies"`
	TotalRatio   float64 `json:"total_ratio"`
	AverageRatio float64 `json:"average_ratio"`
}

func NewSentimentReducer(queue *utils.Queue, publishQueue *utils.Queue) *SentimentReducer {
	return &SentimentReducer{queue: queue, publishQueue: publishQueue}
}

func (r *SentimentReducer) Reduce() {
	positiveStats := SentimentStats{}
	negativeStats := SentimentStats{}

	processedCount := 0
	msgs, err := r.queue.Consume()
	defer r.queue.CloseChannel()
	defer r.publishQueue.CloseChannel()

	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
		return
	}

	log.Printf("Sentiment reducer started processing")

	for d := range msgs {
		stringLine := string(d.Body)

		if stringLine == "FINISHED" {
			log.Printf("Received termination message")
			d.Ack(false)
			break
		}

		processedCount++

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			d.Nack(false, false)
			continue
		}

		if len(record) < 5 {
			log.Printf("Invalid record format: %v", record)
			d.Nack(false, false)
			continue
		}

		budgetStr := record[2]
		revenueStr := record[3]
		sentiment := record[4]

		budget, err := strconv.ParseFloat(budgetStr, 64)
		if err != nil || budget == 0 {
			log.Printf("Invalid or zero budget: %s", budgetStr)
			d.Ack(false)
			continue
		}

		revenue, err := strconv.ParseFloat(revenueStr, 64)
		if err != nil {
			log.Printf("Invalid revenue: %s", revenueStr)
			d.Ack(false)
			continue
		}

		ratio := revenue / budget

		switch sentiment {
		case "POSITIVE":
			positiveStats.TotalMovies++
			positiveStats.TotalRatio += ratio
		case "NEGATIVE":
			negativeStats.TotalMovies++
			negativeStats.TotalRatio += ratio
		default:
			log.Printf("Neutral sentiment: %s", sentiment)
		}

		d.Ack(false)
	}

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
