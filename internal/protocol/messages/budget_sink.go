package messages

import (
	"log"
	"strconv"
)

type BudgetSink struct {
	Amount  int
	Country string
	RawData []string
}

const (
	SinkBudgetAmount = iota
	SinkBudgetCountry
)

func (m *BudgetSink) Deserialize(data []string) error {

	amount, err := strconv.Atoi(data[BudgetAmount])
	if err != nil {
		log.Printf("Failed to convert budget to int: %v", data[BudgetAmount])
		log.Printf("Error: %v", err)
		return err
	}

	m.Amount = amount
	m.Country = data[SinkBudgetCountry]

	m.RawData = make([]string, 2)
	m.RawData[SinkBudgetAmount] = data[BudgetAmount]
	m.RawData[SinkBudgetCountry] = data[BudgetCountry]
	return nil
}

func (m *BudgetSink) GetRawData() []string {
	return m.RawData
}

func ParseBudgetMap(budgetMap map[string]int) []BudgetSink {
	budgetMapString := make([]BudgetSink, 0)
	for country, amount := range budgetMap {
		budgetMapString = append(budgetMapString, BudgetSink{
			Amount:  amount,
			Country: country,
			RawData: []string{strconv.Itoa(amount), country},
		})
	}
	return budgetMapString
}
