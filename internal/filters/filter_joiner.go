package filters

import (
	"encoding/csv"
	"fmt"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FilterJoiner struct {
	conn                      *amqp.Connection
	filteredByCountryConsumer *utils.ConsumerQueue
	newClientFanout           *utils.ProducerQueue
	clientsProducers          map[string]*utils.ProducerQueue
	outputMessage             protocol.MovieToFilter
	query                     string
}

func NewFilterJoiner(filteredByCountryConsumer *utils.ConsumerQueue, outputMessage protocol.MovieToFilter, newClientFanout *utils.ProducerQueue, query string, conn *amqp.Connection) *FilterJoiner {
	return &FilterJoiner{filteredByCountryConsumer: filteredByCountryConsumer, outputMessage: outputMessage, newClientFanout: newClientFanout, clientsProducers: make(map[string]*utils.ProducerQueue), conn: conn, query: query}
}

func (f *FilterJoiner) FilterAndPublish() error {
	log.Printf("Filtering and publishing for query: %s", f.query)

	var clientId string
	var ok bool

	for msg := range f.filteredByCountryConsumer.ConsumeInfinite() {
		if clientId, ok = msg.Headers["clientId"].(string); !ok {
			log.Printf("Failed to get clientId from message headers")
			msg.Nack(false, false)
			continue
		}

		if _, ok := f.clientsProducers[clientId]; !ok && f.query != "1" {
			producerName := fmt.Sprintf("filter_q%s_client_%s", f.query, clientId)
			producer, err := utils.NewProducerQueue(f.conn, producerName, producerName)
			if err != nil {
				log.Printf("Failed to create producer for client %s: %v", clientId, err)
				msg.Nack(false, false)
				continue
			}
			f.filteredByCountryConsumer.AddFinishSubscriberWithRoutingKey(producer, "1") // send to the first queue in the hashed queues
			f.clientsProducers[clientId] = producer
			log.Printf("Created producer for client %s: %s", clientId, producerName)
			f.newClientFanout.Publish([]byte("NEW_CLIENT"), clientId)
		}

		stringLine := string(msg.Body)

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false, false)
			continue
		}
		if err := f.outputMessage.Deserialize(record); err != nil {
			log.Printf("Failed to deserialize movie: %s", string(msg.Body))
			log.Printf("Error deserializing movie: %s", err)
			msg.Nack(false, false)
			continue
		}
		if f.outputMessage.PassesFilter() {
			serializedMovie, err := protocol.Serialize(f.outputMessage)
			if err != nil {
				log.Printf("Error serializing movie: %s", err)
				msg.Nack(false, false)
				continue
			}

			routingKey := f.outputMessage.GetRoutingKey()

			queue := f.clientsProducers[clientId]

			err = queue.PublishWithRoutingKey(serializedMovie, routingKey, clientId)
			log.Printf("Published movie for client %s with routing key: %s", clientId, routingKey)

			if err != nil {
				log.Printf("Error publishing movie: %s", err)
				msg.Nack(false, false)
				continue
			}
		}
		msg.Ack(false)
	}
	return nil
}
