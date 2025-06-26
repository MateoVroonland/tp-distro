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
	actors          map[string]map[string]int
}

func NewCreditsSink(queue *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *CreditsSink {
	return &CreditsSink{
		sinkConsumer:    queue,
		resultsProducer: resultsProducer,
		actors:          make(map[string]map[string]int),
	}
}

type NameAmountTuple struct {
	Name   string
	Amount int
}

func (s *CreditsSink) Sink() {
	i := 0
	creditsSinkStateSaver := NewCreditsSinkStateSaver()
	for msg := range s.sinkConsumer.ConsumeInfinite() {

		stringLine := string(msg.Body)

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := creditsSinkStateSaver.SaveStateAck(&msg, s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				continue
			}
			if _, ok := s.actors[msg.ClientId]; !ok {
				log.Printf("No actors to send for client %s, skipping", msg.ClientId)
			} else {
				log.Printf("Received FINISHED message for client %s", msg.ClientId)
				s.SendClientIdResults(msg.ClientId, s.actors[msg.ClientId])
				delete(s.actors, msg.ClientId)

				err := creditsSinkStateSaver.SaveStateAck(&msg, s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				creditsSinkStateSaver.ForceFlush()
			}
			continue
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			creditsSinkStateSaver.SaveStateNack(&msg, s, false)
			continue
		}

		var credits messages.CreditsSink
		err = credits.Deserialize(record)
		if err != nil {
			log.Printf("Failed to unmarshal credits: %v", err)
			creditsSinkStateSaver.SaveStateNack(&msg, s, false)
			continue
		}

		if _, ok := s.actors[msg.ClientId]; !ok {
			log.Printf("Creating new map for clientId: %s", msg.ClientId)
			s.actors[msg.ClientId] = make(map[string]int)
		}

		for _, actor := range credits.Cast {
			s.actors[msg.ClientId][actor]++
		}

		err = creditsSinkStateSaver.SaveStateAck(&msg, s)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}
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

func (s *CreditsSink) SetActors(actors map[string]map[string]int) {
	s.actors = actors
}
