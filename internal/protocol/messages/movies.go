package messages

import (
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strconv"
)

type Movie struct {
	Countries []Country
	Revenue   float64
	Budget    float64
	RawData   []string
	MovieId   string
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

func (m *Movie) HasValidBudgetAndRevenue() bool {
	return m.Budget > 0 && m.Revenue > 0
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
	if !m.IsValid(data) {
		return fmt.Errorf("invalid data")
	}

	err := json.Unmarshal([]byte(data[RawMovieProductionCountries]), &m.Countries)
	if err != nil {
		return fmt.Errorf("failed to unmarshal production countries: %v", err)
	}

	productionCountries := make([]string, len(m.Countries))
	for i, c := range m.Countries {
		productionCountries[i] = c.Name
	}

	m.Budget, err = strconv.ParseFloat(data[RawMovieBudget], 64)
	if err != nil {
		return fmt.Errorf("failed to parse revenue: %v", err)
	}

	m.Revenue, err = strconv.ParseFloat(data[RawMovieRevenue], 64)
	if err != nil {
		return fmt.Errorf("failed to parse revenue: %v", err)
	}

	m.MovieId = data[RawMovieID]

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

func (m *Movie) IsValid(data []string) bool {
	if len(data) < 8 {
		return false
	}
	return data[RawMovieID] != "" &&
		data[RawMovieTitle] != "" &&
		data[RawMovieOverview] != "" &&
		data[RawMovieProductionCountries] != "" &&
		data[RawMovieGenres] != "" &&
		data[RawMovieReleaseDate] != "" &&
		data[RawMovieBudget] != "" &&
		data[RawMovieRevenue] != ""
}
