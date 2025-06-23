package sentiment

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	"github.com/cdipaolo/sentiment"
)

type SentimentWorker struct {
	queue        *utils.ConsumerQueue
	publishQueue *utils.ProducerQueue
	model        sentiment.Models
}

func NewSentimentWorker(queue *utils.ConsumerQueue, publishQueue *utils.ProducerQueue) *SentimentWorker {
	model, err := sentiment.Restore()
	if err != nil {
		log.Fatalf("Failed to initialize sentiment model: %v", err)
	}
	return &SentimentWorker{
		queue:        queue,
		publishQueue: publishQueue,
		model:        model,
	}
}

func (w *SentimentWorker) analyzeSentiment(text string) string {
	if text == "" {
		return "NEUTRAL"
	}

	analysis := w.model.SentimentAnalysis(text, sentiment.English)

	switch analysis.Score {
	case 0:
		return "NEGATIVE"
	case 1:
		return "POSITIVE"
	default:
		return "NEUTRAL"
	}
}

func (w *SentimentWorker) handleMessage(msg *utils.Message) {
	if msg.IsFinished {
		if !msg.IsLastFinished {
			err := SaveSentimentWorkerState(w)
			if err != nil {
				log.Printf("Failed to save state: %v", err)
			}
			return
		}

		log.Printf("Received FINISHED message for client %s", msg.ClientId)
		w.publishQueue.PublishFinished(msg.ClientId)
		err := SaveSentimentWorkerState(w)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}
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

	movieMetadata.AppendSentiment(sentiment)

	serialized, err := protocol.Serialize(&movieMetadata)
	if err != nil {
		log.Printf("Failed to serialize sentiment analysis: %v", err)
		msg.Nack(false)
		return
	}

	w.publishQueue.Publish(serialized, msg.ClientId, movieMetadata.ID)

	err = SaveSentimentWorkerState(w)
	if err != nil {
		log.Printf("Failed to save state: %v", err)
	}

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
