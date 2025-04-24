package messages

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
)

type SentimentStats struct {
	Sentiment      string
	AverageRatio   float64
	TotalMovies    int
	ProcessedCount int
}

func (s *SentimentStats) Deserialize(record []string) error {
	if len(record) < 4 {
		return fmt.Errorf("invalid sentiment stats format, expected at least 4 fields, got %d", len(record))
	}

	s.Sentiment = record[0]

	var err error
	s.AverageRatio, err = strconv.ParseFloat(record[1], 64)
	if err != nil {
		return fmt.Errorf("failed to parse average ratio: %w", err)
	}

	s.TotalMovies, err = strconv.Atoi(record[2])
	if err != nil {
		return fmt.Errorf("failed to parse total movies: %w", err)
	}

	s.ProcessedCount, err = strconv.Atoi(record[3])
	if err != nil {
		return fmt.Errorf("failed to parse processed count: %w", err)
	}

	return nil
}

func ParseSentimentStats(csvLine string) (*SentimentStats, error) {
	reader := csv.NewReader(strings.NewReader(csvLine))
	record, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV line: %w", err)
	}

	stats := &SentimentStats{}
	if err := stats.Deserialize(record); err != nil {
		return nil, err
	}

	return stats, nil
}

type SentimentAverages struct {
	PositiveAvg float64
	NegativeAvg float64
}

func CalculateSentimentAverages(stats []SentimentStats) SentimentAverages {
	var result SentimentAverages

	for _, stat := range stats {
		if stat.Sentiment == "POSITIVE" {
			result.PositiveAvg = stat.AverageRatio
		} else if stat.Sentiment == "NEGATIVE" {
			result.NegativeAvg = stat.AverageRatio
		}
	}

	return result
}
