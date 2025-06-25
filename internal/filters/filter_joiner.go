package filters

import (
	"encoding/csv"
	"fmt"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FilterJoiner struct {
	conn                      *amqp.Connection
	filteredByCountryConsumer *utils.ConsumerQueue
	newClientFanout           *utils.ProducerFanout
	clientsProducers          map[string]*utils.ProducerQueue
	outputMessage             protocol.MovieToFilter
	query                     string
}

func NewFilterJoiner(filteredByCountryConsumer *utils.ConsumerQueue, outputMessage protocol.MovieToFilter, newClientFanout *utils.ProducerFanout, query string, conn *amqp.Connection) *FilterJoiner {
	return &FilterJoiner{filteredByCountryConsumer: filteredByCountryConsumer, outputMessage: outputMessage, newClientFanout: newClientFanout, clientsProducers: make(map[string]*utils.ProducerQueue), conn: conn, query: query}
}

func (f *FilterJoiner) RestoreClientProducers(clientsProducersState map[string]utils.ProducerQueueState) error {
	var replicas int
	switch f.query {
	case "3":
		replicas = env.AppEnv.RATINGS_JOINER_AMOUNT
	case "4":
		replicas = env.AppEnv.CREDITS_JOINER_AMOUNT
	}

	for clientId, producerState := range clientsProducersState {
		producerName := fmt.Sprintf("filter_q%s_client_%s", f.query, clientId)
		producer, err := utils.NewProducerQueue(f.conn, producerName, replicas)
		if err != nil {
			log.Printf("Failed to recreate producer for client %s: %v", clientId, err)
			return err
		}
		producer.RestoreState(producerState)
		f.clientsProducers[clientId] = producer
		log.Printf("Restored producer for client %s: %s", clientId, producerName)
	}
	return nil
}

func (f *FilterJoiner) FilterAndPublish() error {
	log.Printf("Filtering and publishing for query: %s", f.query)
	var replicas int
	switch f.query {
	case "3":
		replicas = env.AppEnv.RATINGS_JOINER_AMOUNT
	case "4":
		replicas = env.AppEnv.CREDITS_JOINER_AMOUNT
	}

	for msg := range f.filteredByCountryConsumer.ConsumeInfinite() {

		if msg.IsFinished {

			if !msg.IsLastFinished {
				err := SaveFilterJoinerState(f)
				if err != nil {
					log.Printf("Failed to save filter joiner state: %v", err)
				}
				continue
			}

			queue, ok := f.clientsProducers[msg.ClientId]
			if !ok {
				log.Printf("No producer for client %s", msg.ClientId)
				msg.Ack()
				continue
			}
			queue.PublishFinished(msg.ClientId)

			err := SaveFilterJoinerState(f)
			if err != nil {
				log.Printf("Failed to save filter joiner state: %v", err)
			}

			msg.Ack()
			continue
		}

		if _, ok := f.clientsProducers[msg.ClientId]; !ok && f.query != "1" {
			producerName := fmt.Sprintf("filter_q%s_client_%s", f.query, msg.ClientId)
			producer, err := utils.NewProducerQueue(f.conn, producerName, replicas)
			if err != nil {
				log.Printf("Failed to create producer for client %s: %v", msg.ClientId, err)
				msg.Nack(false)
				continue
			}
			f.clientsProducers[msg.ClientId] = producer
			log.Printf("Created producer for client %s: %s", msg.ClientId, producerName)
			err = f.newClientFanout.Publish([]byte("NEW_CLIENT"), msg.ClientId, replicas)
			if err != nil {
				log.Printf("FAILED TO PUBLISH NEW CLIENT: %v", err)
				msg.Nack(false)
				continue
			}
		}

		stringLine := string(msg.Body)

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false)
			continue
		}
		if err := f.outputMessage.Deserialize(record); err != nil {
			log.Printf("Failed to deserialize movie: %s", string(msg.Body))
			log.Printf("Error deserializing movie: %s", err)
			msg.Nack(false)
			continue
		}
		if f.outputMessage.PassesFilter() {
			serializedMovie, err := protocol.Serialize(f.outputMessage)
			if err != nil {
				log.Printf("Error serializing movie: %s", err)
				msg.Nack(false)
				continue
			}

			routingKey := f.outputMessage.GetMovieId()

			queue := f.clientsProducers[msg.ClientId]

			err = queue.Publish(serializedMovie, msg.ClientId, routingKey)
			log.Printf("Published movie for client %s with routing key: %s", msg.ClientId, routingKey)

			if err != nil {
				log.Printf("Error publishing movie: %s", err)
				msg.Nack(false)
				continue
			}
		}

		err = SaveFilterJoinerState(f)
		if err != nil {
			log.Printf("Failed to save filter joiner state: %v", err)
		}

		msg.Ack()
	}
	return nil
}
