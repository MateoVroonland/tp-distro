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
	queue           *utils.ConsumerQueue
	resultsProducer *utils.ProducerQueue
}

func NewBudgetSink(queue *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *BudgetSink {

	return &BudgetSink{queue: queue, resultsProducer: resultsProducer}
}

func (s *BudgetSink) Sink() {
	budgetPerCountry := make(map[string]map[string]int)

	for msg := range s.queue.ConsumeInfinite() {

		stringLine := string(msg.Body)

		if stringLine == "FINISHED" {
			s.SendResults(budgetPerCountry[msg.ClientId], msg.ClientId)
			delete(budgetPerCountry, msg.ClientId)
			msg.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false)
			continue
		}

		var movieBudget messages.BudgetSink
		err = movieBudget.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false)
			continue
		}

		if _, ok := budgetPerCountry[msg.ClientId]; !ok {
			budgetPerCountry[msg.ClientId] = make(map[string]int)
		}

		budgetPerCountry[msg.ClientId][movieBudget.Country] += movieBudget.Amount
		msg.Ack()
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
