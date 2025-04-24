package messages

import (
	"encoding/json"
	"log"
	"time"
)

const (
	Q1SinkMovieID = iota
	Q1SinkMovieTitle
	Q1SinkMovieGenres
	Q1SinkMovieReleaseDate
)

type Q1SinkMovie struct {
	Genres      []Genre
	ID          string
	Title       string
	ReleaseDate time.Time
	RawData     []string
}

func (m *Q1SinkMovie) Deserialize(data []string) error {
	err := json.Unmarshal([]byte(data[Q1Genres]), &m.Genres)
	if err != nil {
		log.Printf("Failed to unmarshal genres: %v", data[Q1Genres])
		log.Printf("Error: %v", err)
	}

	m.ID = data[Q1MovieID]
	m.Title = data[Q1MovieTitle]
	m.ReleaseDate, err = time.Parse("2006-01-02", data[Q1ReleaseDate])
	if err != nil {
		return err
	}
	m.RawData = make([]string, 4)
	m.RawData[Q1SinkMovieID] = data[Q1MovieID]
	m.RawData[Q1SinkMovieTitle] = data[Q1MovieTitle]
	m.RawData[Q1SinkMovieGenres] = data[Q1Genres]
	m.RawData[Q1SinkMovieReleaseDate] = data[Q1ReleaseDate]
	return nil
}

func (m *Q1SinkMovie) GetRawData() []string {
	return m.RawData
}
