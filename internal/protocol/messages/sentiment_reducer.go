package messages

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	SentimentMovieID = iota
	SentimentMovieTitle
	SentimentMovieBudget
	SentimentMovieRevenue
	SentimentLabel
)

type SentimentAnalysis struct {
	MovieID   string
	Title     string
	Budget    float64
	Revenue   float64
	Sentiment string
	Ratio     float64
	RawData   []string
}

type SentimentResult struct {
	Sentiment    string
	AverageRatio float64
	TotalMovies  int
}

type SentimentSinkResults struct {
	PositiveRatio      float64
	PositiveMovies     int
	PositiveTotalRatio float64
	NegativeRatio      float64
	NegativeMovies     int
	NegativeTotalRatio float64
}

func (s *SentimentResult) Serialize() string {
	return fmt.Sprintf("%s,%.6f,%d", s.Sentiment, s.AverageRatio, s.TotalMovies)
}

func ParseSentimentResult(data string) (SentimentResult, error) {
	parts := strings.Split(data, ",")
	if len(parts) < 3 {
		return SentimentResult{}, fmt.Errorf("invalid sentiment result format")
	}

	sentiment := parts[0]

	averageRatio, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return SentimentResult{}, fmt.Errorf("invalid average ratio: %v", err)
	}

	totalMovies, err := strconv.Atoi(parts[2])
	if err != nil {
		return SentimentResult{}, fmt.Errorf("invalid total movies: %v", err)
	}

	return SentimentResult{
		Sentiment:    sentiment,
		AverageRatio: averageRatio,
		TotalMovies:  totalMovies,
	}, nil
}

func (s *SentimentAnalysis) Deserialize(data []string) error {
	if len(data) < 5 {
		return fmt.Errorf("invalid record format, expected at least 5 fields, got %d", len(data))
	}

	s.MovieID = data[SentimentMovieID]
	s.Title = data[SentimentMovieTitle]

	budget, err := strconv.ParseFloat(data[SentimentMovieBudget], 64)
	if err != nil {
		return fmt.Errorf("failed to parse budget: %w", err)
	}
	s.Budget = budget

	revenue, err := strconv.ParseFloat(data[SentimentMovieRevenue], 64)
	if err != nil {
		return fmt.Errorf("failed to parse revenue: %w", err)
	}
	s.Revenue = revenue
	s.Sentiment = data[SentimentLabel]
	s.Ratio = s.Revenue / s.Budget
	s.RawData = make([]string, 5)
	s.RawData[SentimentMovieID] = data[SentimentMovieID]
	s.RawData[SentimentMovieTitle] = data[SentimentMovieTitle]
	s.RawData[SentimentMovieBudget] = data[SentimentMovieBudget]
	s.RawData[SentimentMovieRevenue] = data[SentimentMovieRevenue]
	s.RawData[SentimentLabel] = data[SentimentLabel]

	return nil
}

func (s *SentimentAnalysis) GetRawData() []string {
	return s.RawData
}
