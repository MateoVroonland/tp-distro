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

type CreditsReceiver struct {
	conn            *amqp.Connection
	creditsConsumer *utils.ConsumerQueue
	ClientProducers map[string]*utils.ProducerQueue
}

func NewCreditsReceiver(conn *amqp.Connection, creditsConsumer *utils.ConsumerQueue) *CreditsReceiver {
	return &CreditsReceiver{conn: conn, creditsConsumer: creditsConsumer, ClientProducers: make(map[string]*utils.ProducerQueue)}
}

func (r *CreditsReceiver) ReceiveCredits() {

	i := 0

	for msg := range r.creditsConsumer.ConsumeInfinite() {

		stringLine := string(msg.Body)

		if msg.Body == "FINISHED" {
			queue := r.ClientProducers[msg.ClientId]
			queue.PublishFinished(msg.ClientId)

			err := SaveCreditsState(r)
			if err != nil {
				log.Printf("Failed to save state: %v", err)
			}

			msg.Ack()
			continue
		}

		if _, ok := r.ClientProducers[msg.ClientId]; !ok {
			producerName := fmt.Sprintf("credits_joiner_client_%s", msg.ClientId)
			producer, err := utils.NewProducerQueue(r.conn, producerName, env.AppEnv.CREDITS_JOINER_AMOUNT)
			if err != nil {
				log.Printf("Failed to create producer for client %s: %v", msg.ClientId, err)
				msg.Nack(false)
				continue
			}
			r.ClientProducers[msg.ClientId] = producer
			log.Printf("Created producer for client %s: %s", msg.ClientId, producerName)
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
			msg.Nack(false)
			continue
		}
		serializedCredits, err := protocol.Serialize(credits)
		if err != nil {
			log.Printf("Error serializing credits: %s", err)
			msg.Nack(false)
			continue
		}

		clientProducer := r.ClientProducers[msg.ClientId]
		err = clientProducer.Publish(serializedCredits, msg.ClientId, strconv.Itoa(credits.MovieID))
		if err != nil {
			log.Printf("Error publishing credits: %s", err)
			continue
		}

		err = SaveCreditsState(r)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}

		msg.Ack()
	}

	log.Printf("Received %d credits", i)
}
