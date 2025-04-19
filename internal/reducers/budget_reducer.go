package reducers

import (
	"encoding/csv"
	"log"
	"sort"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

const BUDGET_REDUCER_AMOUNT = 5

type BudgetReducer struct {
	queue        *utils.Queue
	publishQueue *utils.Queue
}

func NewBudgetReducer(queue *utils.Queue, publishQueue *utils.Queue) *BudgetReducer {

	return &BudgetReducer{queue: queue, publishQueue: publishQueue}
}

func (r *BudgetReducer) Reduce() map[string]int {
	budgetPerCountry := make(map[string]int)
	i := 0
	msgs, err := r.queue.Consume()
	defer r.queue.CloseChannel()
	defer r.publishQueue.CloseChannel()

	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
	}
	for d := range msgs {
		stringLine := string(d.Body)

		if stringLine == "FINISHED" {
			log.Printf("Received termination message")
			d.Ack(false)
			break
		}
		i++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 6
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			d.Nack(false, false)
			continue
		}

		var movieBudget messages.Budget
		err = movieBudget.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			d.Nack(false, false)
			continue
		}

		budgetPerCountry[movieBudget.Country] += movieBudget.Amount
		d.Ack(false)
	}

	log.Printf("Total movies processed: %d", i)

	budgets := messages.ParseBudgetMap(budgetPerCountry)
	sort.Slice(budgets, func(i, j int) bool {
		return budgets[i].Amount > budgets[j].Amount
	})

	for _, budget := range budgets {
		if budget.Amount < 1 {
			continue
		}
		serializedBudget, err := protocol.Serialize(&budget)
		if err != nil {
			log.Printf("Failed to serialize budget: %v", err)
			continue
		}
		r.publishQueue.Publish(serializedBudget)
	}
	r.publishQueue.Publish([]byte("FINISHED"))

	return budgetPerCountry
}
