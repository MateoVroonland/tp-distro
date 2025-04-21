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
	queue        *utils.ConsumerQueue
	publishQueue *utils.ProducerQueue
}

func NewBudgetReducer(queue *utils.ConsumerQueue, publishQueue *utils.ProducerQueue) *BudgetReducer {

	return &BudgetReducer{queue: queue, publishQueue: publishQueue}
}

func (r *BudgetReducer) Reduce() map[string]int {
	budgetPerCountry := make(map[string]int)
	i := 0
	defer r.queue.CloseChannel()
	defer r.publishQueue.CloseChannel()

	r.queue.AddFinishSubscriber(r.publishQueue)

	for msg := range r.queue.Consume() {
		stringLine := string(msg.Body)
		i++

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false, false)
			continue
		}

		var movieBudget messages.Budget
		err = movieBudget.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false, false)
			continue
		}

		budgetPerCountry[movieBudget.Country] += movieBudget.Amount
		msg.Ack(false)
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

	return budgetPerCountry
}
