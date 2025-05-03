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

type BudgetReducer struct {
	queue            *utils.ConsumerQueue
	publishQueue     *utils.ProducerQueue
	budgetPerCountry map[string]map[string]int
}

func NewBudgetReducer(queue *utils.ConsumerQueue, publishQueue *utils.ProducerQueue) *BudgetReducer {

	return &BudgetReducer{queue: queue, publishQueue: publishQueue, budgetPerCountry: make(map[string]map[string]int)}
}

func (r *BudgetReducer) Reduce() {
	i := 0
	defer r.queue.CloseChannel()
	defer r.publishQueue.CloseChannel()

	r.queue.AddFinishSubscriber(r.publishQueue)

	for msg := range r.queue.ConsumeSink() {
		stringLine := string(msg.Body)
		i++

		if stringLine == "FINISHED" {
			r.SendResults(msg.ClientId)
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

		var movieBudget messages.Budget
		err = movieBudget.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false)
			continue
		}

		if _, ok := r.budgetPerCountry[msg.ClientId]; !ok {
			r.budgetPerCountry[msg.ClientId] = make(map[string]int)
		}

		r.budgetPerCountry[msg.ClientId][movieBudget.Country] += movieBudget.Amount
		msg.Ack()
	}

	log.Printf("Total movies processed: %d", i)

}

func (r *BudgetReducer) SendResults(clientId string) {
	budgets := messages.ParseBudgetMap(r.budgetPerCountry[clientId])
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
		r.publishQueue.Publish(serializedBudget, clientId)
	}
}
