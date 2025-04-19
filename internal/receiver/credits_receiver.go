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

type CreditsReceiver struct {
	conn            *amqp.Connection
	creditsConsumer *utils.Queue
	joinerProducer  *utils.Queue
}

func NewCreditsReceiver(conn *amqp.Connection, creditsConsumer *utils.Queue, joinerProducer *utils.Queue) *CreditsReceiver {
	return &CreditsReceiver{conn: conn, creditsConsumer: creditsConsumer, joinerProducer: joinerProducer}
}

func (r *CreditsReceiver) ReceiveCredits() {
	msgs, err := r.creditsConsumer.Consume()
	if err != nil {
		log.Printf("Error consuming messages: %s", err)
		return
	}

	for msg := range msgs {
		stringLine := string(msg.Body)
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
			continue
		}
		serializedCredits, err := protocol.Serialize(credits)
		if err != nil {
			log.Printf("Error serializing credits: %s", err)
			continue
		}

		err = r.joinerProducer.Publish(serializedCredits)
		if err != nil {
			log.Printf("Error publishing credits: %s", err)
			continue
		}
	}
}
