package messages

import (
	"fmt"
	"strconv"
)

type MovieSentiment struct {
	ID       string
	Title    string
	Budget   float64
	Revenue  float64
	Overview string
	RawData  []string
}

func (m *MovieSentiment) Deserialize(data []string) error {
	if len(data) < 8 {
		return fmt.Errorf("invalid record format, expected at least 8 fields, got %d", len(data))
	}

	m.ID = data[RawMovieID]
	m.Title = data[RawMovieTitle]
	budget, err := strconv.ParseFloat(data[RawMovieBudget], 64)
	if err != nil {
		return fmt.Errorf("failed to parse budget: %w", err)
	}
	m.Budget = budget

	revenue, err := strconv.ParseFloat(data[RawMovieRevenue], 64)
	if err != nil {
		return fmt.Errorf("failed to parse revenue: %w", err)
	}
	m.Revenue = revenue
	m.Overview = data[RawMovieOverview]
	m.RawData = make([]string, 5)
	m.RawData[RawMovieID] = data[RawMovieID]
	m.RawData[RawMovieTitle] = data[RawMovieTitle]
	m.RawData[RawMovieBudget] = data[RawMovieBudget]
	m.RawData[RawMovieRevenue] = data[RawMovieRevenue]
	m.RawData[RawMovieOverview] = data[RawMovieOverview]

	return nil
}

func (m *MovieSentiment) GetRawData() []string {
	return m.RawData
}
