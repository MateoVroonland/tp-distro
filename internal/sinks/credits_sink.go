package sinks

import (
	"encoding/csv"
	"encoding/json"
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
	actors := make(map[string]map[string]int)

	i := 0
	for msg := range s.sinkConsumer.ConsumeSink() {

		stringLine := string(msg.Body)

		var clientId string
		var ok bool
		if clientId, ok = msg.Headers["clientId"].(string); !ok {
			log.Printf("Failed to get clientId from message headers")
			msg.Nack(false, false)
			continue
		}

		if stringLine == "FINISHED" {
			log.Printf("Received FINISHED message")
			s.SendClientIdResults(clientId, actors[clientId])
			msg.Ack(false)
			continue
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false, false)
			continue
		}

		var credits messages.CreditsSink
		err = credits.Deserialize(record)
		if err != nil {
			log.Printf("Failed to unmarshal credits: %v", err)
			msg.Nack(false, false)
			continue
		}

		if _, ok := actors[clientId]; !ok {
			log.Printf("Creating new map for clientId: %s", clientId)
			actors[clientId] = make(map[string]int)
		}

		for _, actor := range credits.Cast {
			actors[clientId][actor]++
		}

		msg.Ack(false)
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

	err = s.resultsProducer.Publish(bytes, clientId)
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}
}
