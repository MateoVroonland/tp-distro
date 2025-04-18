package filters

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Filter2000s struct {
	ch *amqp.Channel
	filteredByCountryConsumer *utils.Queue
	filteredByYearProducer *utils.Queue
}

func NewFilter2000s(ch *amqp.Channel, filteredByCountryConsumer *utils.Queue, filteredByYearProducer *utils.Queue) *Filter2000s {
	return &Filter2000s{ch: ch, filteredByCountryConsumer: filteredByCountryConsumer, filteredByYearProducer: filteredByYearProducer}
}

func (f *Filter2000s) FilterAndPublish() error {
	msgs, err := f.filteredByCountryConsumer.Consume()
	if err != nil {
		log.Printf("Error consuming messages: %s", err)
		return err
	}

	for msg := range msgs {
		log.Printf("Received message: %s", string(msg.Body))
		stringLine := string(msg.Body)
		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			return err
		}
		movie := &messages.Q1Movie{}
		if err := movie.Deserialize(record); err != nil {
			log.Printf("Failed to deserialize movie: %s", string(msg.Body))
			log.Printf("Error deserializing movie: %s", err)
			return err
		}
		if movie.Is2000s() {
			serializedMovie, err := protocol.Serialize(movie)
			if err != nil {
				log.Printf("Error serializing movie: %s", err)
				return err
			}
			err = f.filteredByYearProducer.Publish(serializedMovie)
			if err != nil {
				log.Printf("Error publishing movie: %s", err)
				return err
			}
			log.Printf("Published message: %s", string(serializedMovie))
		}
	}
	return nil
}

