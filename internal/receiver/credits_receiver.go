package receiver

import (
	"encoding/csv"
	"log"
	"strconv"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/joiners"
	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CreditsReceiver struct {
	conn            *amqp.Connection
	creditsConsumer *utils.ConsumerQueue
	joinerProducer  *utils.ProducerQueue
}

func NewCreditsReceiver(conn *amqp.Connection, creditsConsumer *utils.ConsumerQueue, joinerProducer *utils.ProducerQueue) *CreditsReceiver {
	return &CreditsReceiver{conn: conn, creditsConsumer: creditsConsumer, joinerProducer: joinerProducer}
}

func (r *CreditsReceiver) ReceiveCredits() {
	msgs, err := r.creditsConsumer.Consume()
	if err != nil {
		log.Printf("Error consuming messages: %s", err)
		return
	}

	i := 0

	for msg := range msgs {

		stringLine := string(msg.Body)

		if stringLine == "FINISHED" {
			r.joinerProducer.PublishWithRoutingKey([]byte("FINISHED"), "1")
			msg.Ack(false)
			break
		}
		i++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 3
		record, err := reader.Read()
		if err != nil {
			log.Printf("Error reading record: %s", err)
			continue
		}

		credits := &messages.RawCredits{}
		if err := credits.Deserialize(record); err != nil {
			log.Printf("Error deserializing credits: %s", err)
			msg.Nack(false, false)
			continue
		}
		serializedCredits, err := protocol.Serialize(credits)
		if err != nil {
			log.Printf("Error serializing credits: %s", err)
			msg.Nack(false, false)
			continue
		}

		routingKey := utils.HashString(strconv.Itoa(credits.MovieID), joiners.CREDITS_JOINER_AMOUNT)
		err = r.joinerProducer.PublishWithRoutingKey(serializedCredits, strconv.Itoa(routingKey))
		if err != nil {
			log.Printf("Error publishing credits: %s", err)
			continue
		}
		msg.Ack(false)
	}

	log.Printf("Received %d credits", i)
}
