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

	filterStateSaver := NewFilterStateSaver()

	for msg := range f.filteredByCountryConsumer.ConsumeInfinite() {

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := filterStateSaver.SaveStateAck(&msg, f)
				if err != nil {
					log.Printf("Failed to save filter state: %v", err)
				}
				continue
			}

			log.Printf("Received finished message for client %s", msg.ClientId)
			f.filteredByYearProducer.PublishFinished(msg.ClientId)
			err := filterStateSaver.SaveStateAck(&msg, f)
			if err != nil {
				log.Printf("Failed to save filter state: %v", err)
			}
			continue
		}

		reader := csv.NewReader(strings.NewReader(msg.Body))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			filterStateSaver.SaveStateNack(&msg, f, false)
			continue
		}
		if err := f.outputMessage.Deserialize(record); err != nil {

			filterStateSaver.SaveStateNack(&msg, f, false)
			continue
		}
		if f.outputMessage.PassesFilter() {
			serializedMovie, err := protocol.Serialize(f.outputMessage)
			if err != nil {
				log.Printf("Error serializing movie: %s", err)
				filterStateSaver.SaveStateNack(&msg, f, false)
				continue
			}

			err = f.filteredByYearProducer.Publish(serializedMovie, msg.ClientId, f.outputMessage.GetMovieId())

			if err != nil {
				log.Printf("Error publishing movie: %s", err)
				filterStateSaver.SaveStateNack(&msg, f, false)
				continue
			}
		}

		err = filterStateSaver.SaveStateAck(&msg, f)
		if err != nil {
			log.Printf("Failed to save filter state: %v", err)
		}
	}
	return nil
}
