package filters

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type Filter struct {
	filteredByCountryConsumer *utils.ConsumerQueue
	filteredByYearProducer    *utils.ProducerQueue
	outputMessage             protocol.MovieToFilter
}

func NewFilter(filteredByCountryConsumer *utils.ConsumerQueue, filteredByYearProducer *utils.ProducerQueue, outputMessage protocol.MovieToFilter) *Filter {
	return &Filter{filteredByCountryConsumer: filteredByCountryConsumer, filteredByYearProducer: filteredByYearProducer, outputMessage: outputMessage}
}

func (f *Filter) FilterAndPublish(query string) error {
	log.Printf("Filtering and publishing for query: %s", query)

	query = strings.ToLower(query)
	if query == "1" {
		f.filteredByCountryConsumer.AddFinishSubscriber(f.filteredByYearProducer)
	} else if query == "3" || query == "4" {
		log.Printf("Adding finish subscriber with routing key: 1")
		f.filteredByCountryConsumer.AddFinishSubscriberWithRoutingKey(f.filteredByYearProducer, "1") // send to the first queue in the hashed queues
	}

	for msg := range f.filteredByCountryConsumer.Consume() {
		log.Printf("Received message: %s", string(msg.Body))
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

			if routingKey == "" {
				err = f.filteredByYearProducer.Publish(serializedMovie)
				log.Printf("Published movie with routing key: empty")
			} else {
				err = f.filteredByYearProducer.PublishWithRoutingKey(serializedMovie, routingKey)
				log.Printf("Published movie with routing key: %s", routingKey)
			}

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
