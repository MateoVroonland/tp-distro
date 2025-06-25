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

	for msg := range f.filteredByCountryConsumer.ConsumeInfinite() {

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := SaveFilterState(f)
				if err != nil {
					log.Printf("Failed to save filter state: %v", err)
				}
				continue
			}

			log.Printf("Received finished message for client %s", msg.ClientId)
			f.filteredByYearProducer.PublishFinished(msg.ClientId)
			err := SaveFilterState(f)
			if err != nil {
				log.Printf("Failed to save filter state: %v", err)
			}
			msg.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(msg.Body))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false)
			err := SaveFilterState(f)
			if err != nil {
				log.Printf("Failed to save filter state: %v", err)
			}
			continue
		}
		if err := f.outputMessage.Deserialize(record); err != nil {
			log.Printf("Failed to deserialize movie: %s", string(msg.Body))
			log.Printf("Error deserializing movie: %s", err)
			msg.Nack(false)
			err := SaveFilterState(f)
			if err != nil {
				log.Printf("Failed to save filter state: %v", err)
			}
			continue
		}
		if f.outputMessage.PassesFilter() {
			serializedMovie, err := protocol.Serialize(f.outputMessage)
			if err != nil {
				log.Printf("Error serializing movie: %s", err)
				msg.Nack(false)
				err := SaveFilterState(f)
				if err != nil {
					log.Printf("Failed to save filter state: %v", err)
				}
				continue
			}

			err = f.filteredByYearProducer.Publish(serializedMovie, msg.ClientId, f.outputMessage.GetMovieId())

			if err != nil {
				log.Printf("Error publishing movie: %s", err)
				msg.Nack(false)
				err := SaveFilterState(f)
				if err != nil {
					log.Printf("Failed to save filter state: %v", err)
				}
				continue
			}
		}

		err = SaveFilterState(f)
		if err != nil {
			log.Printf("Failed to save filter state: %v", err)
		}

		msg.Ack()
	}
	return nil
}
