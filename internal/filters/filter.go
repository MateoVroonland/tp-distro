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

	for msg := range f.filteredByCountryConsumer.Consume() {

		log.Printf("Received message: %s", string(msg.Body))
		stringLine := string(msg.Body)
		if stringLine == "FINISHED" {
			f.filteredByYearProducer.Publish([]byte("FINISHED"))
			msg.Ack(false)
			break
		}
		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 6
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
			err = f.filteredByYearProducer.Publish(serializedMovie)
			if err != nil {
				log.Printf("Error publishing movie: %s", err)
				msg.Nack(false, false)
				continue
			}
			log.Printf("Published message: %s on queue %s", string(serializedMovie), f.filteredByYearProducer.QueueName)
			msg.Ack(false)
		}
	}
	return nil
}
