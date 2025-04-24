package messages

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"
)

type Budget struct {
	Amount  int
	Country string
	RawData []string
}

const (
	BudgetAmount = iota
	BudgetCountry
)

func (m *Budget) Deserialize(data []string) error {

	var countries []string

	err := json.Unmarshal([]byte(data[MovieProductionCountries]), &countries)
	if err != nil {
		log.Printf("Failed to unmarshal production countries: %v", data[MovieProductionCountries])
		log.Printf("Error: %v", err)
		return err
	}

	if len(countries) != 1 {
		log.Printf("Expected 1 country, got %d", len(countries))
		return errors.New("expected 1 country, got " + strconv.Itoa(len(countries)))
	}

	amount, err := strconv.Atoi(data[MovieBudget])
	if err != nil {
		log.Printf("Failed to convert budget to int: %v", data[MovieBudget])
		log.Printf("Error: %v", err)
		return err
	}

	m.Country = countries[0]
	m.Amount = amount

	m.RawData = make([]string, 2)
	m.RawData[BudgetAmount] = data[MovieBudget]
	m.RawData[BudgetCountry] = countries[0]
	return nil
}

func (m *Budget) GetRawData() []string {
	return m.RawData
}
