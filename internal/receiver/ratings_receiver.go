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
	joinerProducers map[string]*utils.ProducerQueue
}

func NewRatingsReceiver(conn *amqp.Connection, ratingsConsumer *utils.ConsumerQueue) *RatingsReceiver {
	return &RatingsReceiver{conn: conn, ratingsConsumer: ratingsConsumer, joinerProducers: make(map[string]*utils.ProducerQueue)}
}

func (r *RatingsReceiver) ReceiveRatings() {

	ratingsConsumed := 0

	for msg := range r.ratingsConsumer.ConsumeInfinite() {
		stringLine := string(msg.Body)

		if msg.Body == "FINISHED" {
			queue := r.joinerProducers[msg.ClientId]
			queue.PublishFinished(msg.ClientId)
			msg.Ack()
			continue
		}
		clientId := msg.ClientId
		if _, ok := r.joinerProducers[clientId]; !ok {
			producerName := fmt.Sprintf("ratings_joiner_client_%s", clientId)
			producer, err := utils.NewProducerQueue(r.conn, producerName, env.AppEnv.RATINGS_JOINER_AMOUNT)
			if err != nil {
				log.Printf("Failed to create producer for client %s: %v", clientId, err)
				msg.Nack(false)
				continue
			}
			r.joinerProducers[clientId] = producer
		}
		ratingsConsumed++
		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 4
		record, err := reader.Read()
		if err != nil {
			log.Printf("Error reading record: %s", err)
			msg.Nack(false)
			continue
		}

		rating := &messages.RawRatings{}
		if err := rating.Deserialize(record); err != nil {
			log.Printf("Error deserializing rating: %s", err)
			msg.Nack(false)
			continue
		}
		serializedRating, err := protocol.Serialize(rating)
		if err != nil {
			log.Printf("Error serializing rating: %s", err)
			msg.Nack(false)
			continue
		}

		err = r.joinerProducers[clientId].Publish(serializedRating, clientId, strconv.Itoa(rating.MovieID))

		if err != nil {
			log.Printf("Error publishing rating: %s", err)
			msg.Nack(false)
			continue
		}
		msg.Ack()
	}
	log.Printf("Ratings consumed: %d", ratingsConsumed)
}
