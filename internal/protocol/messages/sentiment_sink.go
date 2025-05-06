package messages

import (
	"fmt"
	"strconv"
)

const (
	SentimentResultType = iota
	SentimentResultRatio
	SentimentResultCount
)

type SentimentResult struct {
	Sentiment    string
	AverageRatio float64
	TotalMovies  int
	RawData      []string
}

func (s *SentimentResult) Deserialize(data []string) error {
	if len(data) < 3 {
		return fmt.Errorf("invalid sentiment result format, expected at least 3 fields, got %d", len(data))
	}

	s.Sentiment = data[SentimentResultType]

	var err error
	s.AverageRatio, err = strconv.ParseFloat(data[SentimentResultRatio], 64)
	if err != nil {
		return fmt.Errorf("failed to parse average ratio: %w", err)
	}

	s.TotalMovies, err = strconv.Atoi(data[SentimentResultCount])
	if err != nil {
		return fmt.Errorf("failed to parse total movies: %w", err)
	}

	s.RawData = make([]string, len(data))
	copy(s.RawData, data)

	return nil
}

func (s *SentimentResult) GetRawData() []string {
	return s.RawData
}

func (s *SentimentResult) Serialize() string {
	return fmt.Sprintf("%s,%.6f,%d", s.Sentiment, s.AverageRatio, s.TotalMovies)
}
