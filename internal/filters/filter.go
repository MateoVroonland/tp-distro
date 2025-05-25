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

	// f.filteredByCountryConsumer.AddFinishSubscriber(f.filteredByYearProducer)

	for msg := range f.filteredByCountryConsumer.ConsumeInfinite() {

		if msg.Body == "FINISHED" {
			log.Printf("Received finished message for client %s", msg.ClientId)
			f.filteredByYearProducer.PublishFinished(msg.ClientId)
			msg.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(msg.Body))
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

			err = f.filteredByYearProducer.Publish(serializedMovie, msg.ClientId, f.outputMessage.GetMovieId())

			if err != nil {
				log.Printf("Error publishing movie: %s", err)
				msg.Nack(false)
				continue
			}
		}
		msg.Ack()
	}
	return nil
}
