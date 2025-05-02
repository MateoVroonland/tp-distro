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

func (f *Filter) FilterAndPublish() error {
	log.Printf("Filtering and publishing")

	f.filteredByCountryConsumer.AddFinishSubscriber(f.filteredByYearProducer)

	var clientId string
	var ok bool

	for msg := range f.filteredByCountryConsumer.ConsumeInfinite() {
		if clientId, ok = msg.Headers["clientId"].(string); !ok {
			log.Printf("Failed to get clientId from message headers")
			msg.Nack(false, false)
			continue
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

			err = f.filteredByYearProducer.Publish(serializedMovie, clientId)
			log.Printf("Published movie with routing key: empty")

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
