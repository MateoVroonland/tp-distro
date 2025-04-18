package budget_reducer

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ReduceBudget(ch <-chan amqp.Delivery) map[string]int {
	budgetPerCountry := make(map[string]int)
	i := 0
	for d := range ch {
		stringLine := string(d.Body)

		if stringLine == "FINISHED" {
			log.Printf("Received termination message")
			break
		}
		i++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 6
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			continue
		}

		var movieBudget messages.Budget
		err = movieBudget.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			continue
		}

		budgetPerCountry[movieBudget.Country] += movieBudget.Amount

		// log.Printf("country, sum, acc %s, %d, %d", movieBudget.Country, movieBudget.Amount, budgetPerCountry[movieBudget.Country])
	}

	log.Printf("Budget per country: %v", budgetPerCountry)
	log.Printf("Total movies processed: %d", i)
	return budgetPerCountry
}
