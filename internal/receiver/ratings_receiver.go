package receiver

import (
	"encoding/csv"
	"log"
	"strconv"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/constants"
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

	r.ratingsConsumer.AddFinishSubscriberWithRoutingKey(r.joinerProducer, "1")
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

		routingKey := utils.HashString(strconv.Itoa(rating.MovieID), constants.RATINGS_JOINER_AMOUNT)
		err = r.joinerProducer.PublishWithRoutingKey(serializedRating, strconv.Itoa(routingKey))
		if rating.MovieID == 259843 || rating.MovieID == 7234 || rating.MovieID == 288312 || rating.MovieID == 81022 {
			log.Printf("Published movie id %d to joiner %d", rating.MovieID, routingKey)
		}
		if err != nil {
			log.Printf("Error publishing rating: %s", err)
			continue
		}
		msg.Ack(false)
	}
}