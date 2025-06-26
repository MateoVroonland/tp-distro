package receiver

import (
	"encoding/csv"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RatingsReceiver struct {
	conn            *amqp.Connection
	ratingsConsumer *utils.ConsumerQueue
	JoinerProducers map[string]*utils.ProducerQueue
}

func NewRatingsReceiver(conn *amqp.Connection, ratingsConsumer *utils.ConsumerQueue) *RatingsReceiver {
	return &RatingsReceiver{conn: conn, ratingsConsumer: ratingsConsumer, JoinerProducers: make(map[string]*utils.ProducerQueue)}
}

func (r *RatingsReceiver) ReceiveRatings() {
	ratingsConsumed := 0
	stateSaver := NewRatingsReceiverState()

	for msg := range r.ratingsConsumer.ConsumeInfinite() {
		stringLine := string(msg.Body)

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := stateSaver.SaveStateAck(&msg, r)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				continue
			}

			queue := r.JoinerProducers[msg.ClientId]
			queue.PublishFinished(msg.ClientId)

			// Save state when receiving FINISHED message
			err := stateSaver.SaveStateAck(&msg, r)
			if err != nil {
				log.Printf("Failed to save state: %v", err)
			}

			flushed, err := stateSaver.ForceFlush()
			if err != nil {
				log.Printf("Failed to flush state: %v", err)
			} else if flushed {
				log.Printf("Flushed final state for client %s", msg.ClientId)
			}
			continue
		}
		clientId := msg.ClientId
		if _, ok := r.JoinerProducers[clientId]; !ok {
			producerName := fmt.Sprintf("ratings_joiner_client_%s", clientId)
			producer, err := utils.NewProducerQueue(r.conn, producerName, env.AppEnv.RATINGS_JOINER_AMOUNT)
			if err != nil {
				log.Printf("Failed to create producer for client %s: %v", clientId, err)
				stateSaver.SaveStateNack(&msg, r, false)
				continue
			}
			r.JoinerProducers[clientId] = producer
		}
		ratingsConsumed++
		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 4
		record, err := reader.Read()
		if err != nil {
			log.Printf("Error reading record: %s", err)
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}

		rating := &messages.RawRatings{}
		if err := rating.Deserialize(record); err != nil {
			log.Printf("Error deserializing rating: %s", err)
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}
		serializedRating, err := protocol.Serialize(rating)
		if err != nil {
			log.Printf("Error serializing rating: %s", err)
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}

		err = r.JoinerProducers[clientId].Publish(serializedRating, clientId, strconv.Itoa(rating.MovieID))

		if err != nil {
			log.Printf("Error publishing rating: %s", err)
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}

		// Save state periodically (every 1000 ratings)
		err = stateSaver.SaveStateAck(&msg, r)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}

	}
	log.Printf("Ratings consumed: %d", ratingsConsumed)
}
