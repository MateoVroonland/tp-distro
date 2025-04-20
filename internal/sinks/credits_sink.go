package sinks

import (
	"encoding/csv"
	"log"
	"sort"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type CreditsSink struct {
	sinkConsumer    *utils.ConsumerQueue
	resultsProducer *utils.ProducerQueue
}

func NewCreditsSink(queue *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *CreditsSink {

	return &CreditsSink{sinkConsumer: queue, resultsProducer: resultsProducer}
}

type NameAmountTuple struct {
	Name   string
	Amount int
}

func (s *CreditsSink) Sink() {
	actors := make(map[string]int)
	msgs, err := s.sinkConsumer.Consume()
	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
	}

	i := 0
	for d := range msgs {

		stringLine := string(d.Body)

		if stringLine == "FINISHED" {
			log.Printf("Received termination message")
			d.Ack(false)
			break
		}
		i++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			d.Nack(false, false)
			continue
		}

		var credits messages.CreditsSink
		err = credits.Deserialize(record)
		if err != nil {
			log.Printf("Failed to unmarshal credits: %v", err)
			d.Nack(false, false)
			continue
		}

		for _, actor := range credits.Cast {
			actors[actor]++
		}

		d.Ack(false)
	}

	log.Printf("Processed credits: %d", i)

	topTen := []NameAmountTuple{}

	for actor, credits := range actors {
		if len(topTen) < 10 {
			topTen = append(topTen, NameAmountTuple{actor, credits})
		} else if topTen[9].Amount < credits {
			topTen[9] = NameAmountTuple{actor, credits}
		}
		sort.Slice(topTen, func(i, j int) bool {
			return topTen[i].Amount > topTen[j].Amount
		})
	}

	log.Printf("Top 10 actors by credits: %v", topTen)
	log.Printf("All actors by credits: %v", actors)

}
