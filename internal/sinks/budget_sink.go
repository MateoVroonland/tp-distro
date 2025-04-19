package sinks

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"sort"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/reducers"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type BudgetSink struct {
	queue           *utils.Queue
	resultsProducer *utils.Queue
}

func NewBudgetSink(queue *utils.Queue, resultsProducer *utils.Queue) *BudgetSink {

	return &BudgetSink{queue: queue, resultsProducer: resultsProducer}
}

func (s *BudgetSink) Sink() {
	budgetPerCountry := make(map[string]int)
	reducersMissing := reducers.BUDGET_REDUCER_AMOUNT
	msgs, err := s.queue.Consume()
	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
	}
	for d := range msgs {

		stringLine := string(d.Body)

		if stringLine == "FINISHED" {
			log.Printf("Received termination message")
			reducersMissing--
			d.Ack(false)
			if reducersMissing == 0 {
				break
			}
			continue
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			d.Nack(false, false)
			continue
		}

		var movieBudget messages.BudgetSink
		err = movieBudget.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			d.Nack(false, false)
			continue
		}

		budgetPerCountry[movieBudget.Country] += movieBudget.Amount
		d.Ack(false)
	}

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

	err = s.resultsProducer.Publish(bytes)
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}
}
