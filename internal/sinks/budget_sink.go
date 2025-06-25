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

type BudgetSink struct {
	queue            *utils.ConsumerQueue
	resultsProducer  *utils.ProducerQueue
	BudgetPerCountry map[string]map[string]int // clientId -> country -> amount
}

func NewBudgetSink(queue *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *BudgetSink {

	return &BudgetSink{queue: queue, resultsProducer: resultsProducer, BudgetPerCountry: make(map[string]map[string]int)}
}

func (s *BudgetSink) Sink() {
	stateSaver := NewBudgetSinkState()
	for msg := range s.queue.ConsumeInfinite() {

		stringLine := string(msg.Body)

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := stateSaver.SaveStateAck(&msg, s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				continue
			}
			if _, ok := s.BudgetPerCountry[msg.ClientId]; !ok {
				log.Printf("No budget per country to send for client %s, skipping", msg.ClientId)
			} else {
				log.Printf("Received FINISHED message for client %s", msg.ClientId)
				s.SendResults(s.BudgetPerCountry[msg.ClientId], msg.ClientId)
				delete(s.BudgetPerCountry, msg.ClientId)

				err := stateSaver.SaveStateAck(&msg, s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}

				flushed, err := stateSaver.ForceFlush()
				if err != nil {
					log.Printf("Failed to flush state: %v", err)
				} else if flushed {
					log.Printf("Flushed final state for client %s", msg.ClientId)
				}
			}
			// msg.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			stateSaver.SaveStateNack(&msg, s, false)
			continue
		}

		var movieBudget messages.BudgetSink
		err = movieBudget.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			stateSaver.SaveStateNack(&msg, s, false)
			continue
		}

		if _, ok := s.BudgetPerCountry[msg.ClientId]; !ok {
			s.BudgetPerCountry[msg.ClientId] = make(map[string]int)
		}

		s.BudgetPerCountry[msg.ClientId][movieBudget.Country] += movieBudget.Amount

		err = stateSaver.SaveStateAck(&msg, s)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}

		// msg.Ack()
	}

}

func (s *BudgetSink) SendResults(budgetPerCountry map[string]int, clientId string) {
	budgets := messages.ParseBudgetMap(budgetPerCountry)
	sort.Slice(budgets, func(i, j int) bool {
		return budgets[i].Amount > budgets[j].Amount
	})

	top5 := make([]messages.Q2Row, 0)
	for i := 0; i < 5 && i < len(budgets); i++ {
		top5 = append(top5, *messages.NewQ2Row(budgets[i].Country, budgets[i].Amount))
	}

	rowsBytes, err := json.Marshal(top5)
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	results := messages.RawResult{
		QueryID: "query2",
		Results: rowsBytes,
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	err = s.resultsProducer.PublishResults(bytes, clientId, "q2")
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}
}
