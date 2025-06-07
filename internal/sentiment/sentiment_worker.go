package sentiment

import (
	"encoding/csv"
	"log"
	"math/rand/v2"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type SentimentWorker struct {
	queue        *utils.ConsumerQueue
	publishQueue *utils.ProducerQueue
}

func NewSentimentWorker(queue *utils.ConsumerQueue, publishQueue *utils.ProducerQueue) *SentimentWorker {
	return &SentimentWorker{
		queue:        queue,
		publishQueue: publishQueue,
	}
}

func (w *SentimentWorker) analyzeSentiment(text string) string {
	if text == "" {
		return "NEUTRAL"
	}

	randomProbability := rand.Float64()
	if randomProbability < 0.5 {
		return "POSITIVE"
	}
	return "NEGATIVE"
}

func (w *SentimentWorker) handleMessage(msg *utils.Message) {
	if msg.Body == "FINISHED" {
		log.Printf("Received FINISHED message for client %s", msg.ClientId)
		w.publishQueue.PublishFinished(msg.ClientId)
		msg.Ack()
		return
	}

	reader := csv.NewReader(strings.NewReader(msg.Body))
	record, err := reader.Read()
	if err != nil {
		log.Printf("Failed to read record: %v", err)
		msg.Nack(false)
		return
	}

	var movieMetadata messages.MovieSentiment
	err = movieMetadata.Deserialize(record)
	if err != nil {
		log.Printf("Failed to deserialize movie: %v", err)
		msg.Nack(false)
		return
	}

	sentiment := w.analyzeSentiment(movieMetadata.Overview)

	sentimentAnalysis := &messages.SentimentAnalysis{
		MovieID:   movieMetadata.ID,
		Title:     movieMetadata.Title,
		Budget:    movieMetadata.Budget,
		Revenue:   movieMetadata.Revenue,
		Sentiment: sentiment,
		Ratio:     movieMetadata.Revenue / movieMetadata.Budget,
		RawData:   movieMetadata.RawData[:5],
	}

	serialized, err := protocol.Serialize(sentimentAnalysis)
	if err != nil {
		log.Printf("Failed to serialize sentiment analysis: %v", err)
		msg.Nack(false)
		return
	}

	w.publishQueue.Publish(serialized, msg.ClientId, sentimentAnalysis.MovieID)
	msg.Ack()
}

func (w *SentimentWorker) Start() {
	defer w.queue.CloseChannel()
	defer w.publishQueue.CloseChannel()

	log.Printf("Starting sentiment worker...")
	for msg := range w.queue.ConsumeInfinite() {
		w.handleMessage(&msg)
	}
}
