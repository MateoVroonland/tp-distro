package messages

import (
	"fmt"
	"strconv"
)

const (
	SentimentWorkerMovieID = iota
	SentimentWorkerMovieTitle
	SentimentWorkerMovieBudget
	SentimentWorkerMovieRevenue
	SentimentWorkerMovieOverview
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

	m.ID = data[MovieID]
	m.Title = data[MovieTitle]
	budget, err := strconv.ParseFloat(data[MovieBudget], 64)
	if err != nil {
		return fmt.Errorf("failed to parse budget: %w", err)
	}
	m.Budget = budget

	revenue, err := strconv.ParseFloat(data[MovieRevenue], 64)
	if err != nil {
		return fmt.Errorf("failed to parse revenue: %w", err)
	}
	m.Revenue = revenue
	m.Overview = data[MovieOverview]
	m.RawData = make([]string, 5)
	m.RawData[SentimentWorkerMovieID] = data[MovieID]
	m.RawData[SentimentWorkerMovieTitle] = data[MovieTitle]
	m.RawData[SentimentWorkerMovieBudget] = data[MovieBudget]
	m.RawData[SentimentWorkerMovieRevenue] = data[MovieRevenue]
	m.RawData[SentimentWorkerMovieOverview] = data[MovieOverview]

	return nil
}

func (m *MovieSentiment) GetRawData() []string {
	return m.RawData
}
