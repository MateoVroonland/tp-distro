package receiver

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RatingsReceiver struct {
	conn            *amqp.Connection
	ratingsConsumer *utils.ConsumerQueue
	joinerProducer  *utils.ProducerQueue
}

func NewRatingsReceiver(conn *amqp.Connection, ratingsConsumer *utils.ConsumerQueue, joinerProducer *utils.ProducerQueue) *RatingsReceiver {
	return &RatingsReceiver{conn: conn, ratingsConsumer: ratingsConsumer, joinerProducer: joinerProducer}
}

func (r *RatingsReceiver) ReceiveRatings() {

	for msg := range r.ratingsConsumer.Consume() {
		stringLine := string(msg.Body)
		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 4
		record, err := reader.Read()
		if err != nil {
			log.Printf("Error reading record: %s", err)
			continue
		}

		rating := &messages.RawRatings{}
		if err := rating.Deserialize(record); err != nil {
			log.Printf("Error deserializing rating: %s", err)
			continue
		}
		serializedRating, err := protocol.Serialize(rating)
		if err != nil {
			log.Printf("Error serializing rating: %s", err)
			continue
		}

		err = r.joinerProducer.Publish(serializedRating)
		if err != nil {
			log.Printf("Error publishing rating: %s", err)
			continue
		}
	}
}
