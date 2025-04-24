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
	forever := make(chan bool)
	sendingToRoutingKey := map[int]int{}

	r.ratingsConsumer.AddFinishSubscriberWithRoutingKey(r.joinerProducer, "1")
	ratingsConsumed := 0
	for msg := range r.ratingsConsumer.Consume() {
		ratingsConsumed++
		stringLine := string(msg.Body)
		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 4
		record, err := reader.Read()
		if err != nil {
			log.Printf("Error reading record: %s", err)
			msg.Nack(false, false)
			continue
		}

		rating := &messages.RawRatings{}
		if err := rating.Deserialize(record); err != nil {
			log.Printf("Error deserializing rating: %s", err)
			msg.Nack(false, false)
			continue
		}
		serializedRating, err := protocol.Serialize(rating)
		if err != nil {
			log.Printf("Error serializing rating: %s", err)
			msg.Nack(false, false)
			continue
		}

		routingKey := utils.HashString(strconv.Itoa(rating.MovieID), constants.RATINGS_JOINER_AMOUNT)
		err = r.joinerProducer.PublishWithRoutingKey(serializedRating, strconv.Itoa(routingKey))
		if err != nil {
			log.Printf("Error publishing rating: %s", err)
			msg.Nack(false, true)
			continue
		}
		sendingToRoutingKey[routingKey]++
		msg.Ack(false)
	}
	log.Printf("Sending to routing key: %v", sendingToRoutingKey)
	log.Printf("Ratings consumed: %d", ratingsConsumed)
	<-forever
}