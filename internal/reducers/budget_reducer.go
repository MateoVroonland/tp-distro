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
	BudgetPerCountry map[string]map[string]int
}

func NewBudgetReducer(queue *utils.ConsumerQueue, publishQueue *utils.ProducerQueue) *BudgetReducer {

	return &BudgetReducer{queue: queue, publishQueue: publishQueue, BudgetPerCountry: make(map[string]map[string]int)}
}

func (r *BudgetReducer) Reduce() {
	i := 0
	defer r.queue.CloseChannel()
	defer r.publishQueue.CloseChannel()

	for msg := range r.queue.ConsumeInfinite() {

		if msg.Body == "FINISHED" {
			r.SendResults(msg.ClientId)

			delete(r.BudgetPerCountry, msg.ClientId)

			err := SaveBudgetReducerState(r)
			if err != nil {
				log.Printf("Failed to save state: %v", err)
			}

			msg.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(msg.Body))
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

		if _, ok := r.BudgetPerCountry[msg.ClientId]; !ok {
			r.BudgetPerCountry[msg.ClientId] = make(map[string]int)
		}

		r.BudgetPerCountry[msg.ClientId][movieBudget.Country] += movieBudget.Amount

		err = SaveBudgetReducerState(r)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}

		msg.Ack()
	}

	log.Printf("Total movies processed: %d", i)

}

func (r *BudgetReducer) SendResults(clientId string) {
	budgets := messages.ParseBudgetMap(r.BudgetPerCountry[clientId])
	sort.Slice(budgets, func(i, j int) bool {
		if budgets[i].Amount != budgets[j].Amount {
			return budgets[i].Amount > budgets[j].Amount
		}
		return budgets[i].Country < budgets[j].Country
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
		r.publishQueue.Publish(serializedBudget, clientId, budget.Country)
	}

	r.publishQueue.PublishFinished(clientId)
}
