package sinks

import (
	"encoding/csv"
	"log"
	"sort"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

const REDUCER_AMOUNT = 5

type BudgetSink struct {
	queue *utils.Queue
}

func NewBudgetSink(queue *utils.Queue) *BudgetSink {

	return &BudgetSink{queue: queue}
}

func (s *BudgetSink) Sink() map[string]int {
	budgetPerCountry := make(map[string]int)
	reducersMissing := REDUCER_AMOUNT
	msgs, err := s.queue.Consume()
	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
	}
	for d := range msgs {
		stringLine := string(d.Body)
		log.Printf("Received message: %s", stringLine)

		if stringLine == "FINISHED" {
			log.Printf("Received termination message")
			reducersMissing--
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
			continue
		}

		var movieBudget messages.BudgetSink
		err = movieBudget.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			continue
		}

		budgetPerCountry[movieBudget.Country] += movieBudget.Amount

	}

	budgets := messages.ParseBudgetMap(budgetPerCountry)
	sort.Slice(budgets, func(i, j int) bool {
		return budgets[i].Amount > budgets[j].Amount
	})

	top5 := budgets[:5]

	log.Printf("Top 5 budgets: %v", top5)

	return budgetPerCountry
}
