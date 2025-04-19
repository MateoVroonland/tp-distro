package messages

import (
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strings"
)

type Movie struct {
	Countries []Country
	RawData   []string
}

type Country struct {
	Name string `json:"name"`
}

func (m *Movie) IncludesAllCountries(countries []string) bool {
	remainingCountries := len(countries)
	for _, c := range m.Countries {
		if slices.Contains(countries, c.Name) {
			remainingCountries--
		}
	}
	return remainingCountries == 0
}

const (
	MovieID = iota
	MovieTitle
	MovieReleaseDate
	MovieGenres
	MovieBudget
	MovieProductionCountries
	MovieRevenue
	MovieOverview
)

func (m *Movie) Deserialize(data []string) error {
	jsonStr := strings.ReplaceAll(data[RawMovieProductionCountries], "'", "\"")
	err := json.Unmarshal([]byte(jsonStr), &m.Countries)
	if err != nil {
		return fmt.Errorf("failed to unmarshal production countries: %v", err)
	}

	productionCountries := make([]string, len(m.Countries))
	for i, c := range m.Countries {
		productionCountries[i] = c.Name
	}

	m.RawData = make([]string, 8)
	m.RawData[MovieID] = data[RawMovieID]
	m.RawData[MovieTitle] = data[RawMovieTitle]
	m.RawData[MovieReleaseDate] = data[RawMovieReleaseDate]
	m.RawData[MovieGenres] = data[RawMovieGenres]
	m.RawData[MovieBudget] = data[RawMovieBudget]
	countriesJSON, err := json.Marshal(productionCountries)

	if err != nil {
		log.Printf("Failed to marshal production countries: %v", err)
		return err
	}

	m.RawData[MovieProductionCountries] = string(countriesJSON)
	m.RawData[MovieRevenue] = data[RawMovieRevenue]
	m.RawData[MovieOverview] = data[RawMovieOverview]
	return nil
}

func (m *Movie) GetRawData() []string {
	return m.RawData
}
