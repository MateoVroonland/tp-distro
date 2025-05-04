package receiver

import (
	"log"

	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CreditsReceiver struct {
	conn            *amqp.Connection
	creditsConsumer *utils.ConsumerQueue
	clientProducers map[string]*utils.ProducerQueue
}

func NewCreditsReceiver(conn *amqp.Connection, creditsConsumer *utils.ConsumerQueue) *CreditsReceiver {
	return &CreditsReceiver{conn: conn, creditsConsumer: creditsConsumer, clientProducers: make(map[string]*utils.ProducerQueue)}
}

func (r *CreditsReceiver) ReceiveCredits() {

	i := 0

	// // r.creditsConsumer.AddFinishSubscriberWithRoutingKey(r.joinerProducer, "1")
	// for msg := range r.creditsConsumer.ConsumeInfinite() {

	// 	stringLine := string(msg.Body)

	// 	if _, ok := r.clientProducers[msg.ClientId]; !ok {
	// 		producerName := fmt.Sprintf("credits_joiner_client_%s", msg.ClientId)
	// 		producer, err := utils.NewProducerQueue(r.conn, producerName, producerName)
	// 		if err != nil {
	// 			log.Printf("Failed to create producer for client %s: %v", msg.ClientId, err)
	// 			msg.Nack(false)
	// 			continue
	// 		}
	// 		r.creditsConsumer.AddFinishSubscriberWithRoutingKey(producer, "1") // send to the first queue in the hashed queues
	// 		r.clientProducers[msg.ClientId] = producer
	// 		log.Printf("Created producer for client %s: %s", msg.ClientId, producerName)
	// 	}

	// 	i++

	// 	reader := csv.NewReader(strings.NewReader(stringLine))
	// 	reader.FieldsPerRecord = 3
	// 	record, err := reader.Read()
	// 	if err != nil {
	// 		log.Printf("Error reading record: %s", err)
	// 		continue
	// 	}

	// 	credits := &messages.RawCredits{}
	// 	if err := credits.Deserialize(record); err != nil {
	// 		log.Printf("Error deserializing credits: %s", err)
	// 		msg.Nack(false)
	// 		continue
	// 	}
	// 	serializedCredits, err := protocol.Serialize(credits)
	// 	if err != nil {
	// 		log.Printf("Error serializing credits: %s", err)
	// 		msg.Nack(false)
	// 		continue
	// 	}

	// 	routingKey := utils.HashString(strconv.Itoa(credits.MovieID), env.AppEnv.CREDITS_JOINER_AMOUNT)

	// 	clientProducer := r.clientProducers[msg.ClientId]
	// 	err = clientProducer.PublishWithRoutingKey(serializedCredits, strconv.Itoa(routingKey), msg.ClientId)
	// 	if err != nil {
	// 		log.Printf("Error publishing credits: %s", err)
	// 		continue
	// 	}
	// 	msg.Ack()
	// }

	log.Printf("Received %d credits", i)
}
