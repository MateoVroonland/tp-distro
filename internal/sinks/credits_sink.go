package sinks

import (
	"log"

	"encoding/csv"
	"encoding/json"
	"sort"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/utils"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
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
	actors := make(map[string]map[string]int)

	i := 0
	for msg := range s.sinkConsumer.ConsumeInfinite() {

		stringLine := string(msg.Body)

		if stringLine == "FINISHED" {
			log.Printf("Received FINISHED message")
			s.SendClientIdResults(msg.ClientId, actors[msg.ClientId])
			msg.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false)
			continue
		}

		var credits messages.CreditsSink
		err = credits.Deserialize(record)
		if err != nil {
			log.Printf("Failed to unmarshal credits: %v", err)
			msg.Nack(false)
			continue
		}

		if _, ok := actors[msg.ClientId]; !ok {
			log.Printf("Creating new map for clientId: %s", msg.ClientId)
			actors[msg.ClientId] = make(map[string]int)
		}

		for _, actor := range credits.Cast {
			actors[msg.ClientId][actor]++
		}

		msg.Ack()
	}

	log.Printf("Processed credits: %d", i)

}

func (s *CreditsSink) SendClientIdResults(clientId string, actors map[string]int) {

	topTen := []messages.Q4Row{}

	for actor, credits := range actors {
		if len(topTen) < 10 {
			topTen = append(topTen, *messages.NewQ4Row(actor, credits))
		} else if topTen[9].MoviesCount < credits {
			topTen[9] = *messages.NewQ4Row(actor, credits)
		}
		sort.Slice(topTen, func(i, j int) bool {
			return topTen[i].MoviesCount > topTen[j].MoviesCount
		})
	}

	log.Printf("Top 10 actors by credits: %v", topTen)

	rowsBytes, err := json.Marshal(topTen)
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	results := messages.RawResult{
		QueryID: "query4",
		Results: rowsBytes,
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	err = s.resultsProducer.PublishResults(bytes, clientId, "q4")
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}
}
