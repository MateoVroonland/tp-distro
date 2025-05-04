package messages

import (
	"encoding/json"
	"log"
	"time"
)

const (
	Q1MovieID = iota
	Q1MovieTitle
	Q1Genres
	Q1ReleaseDate
)

type Q1Movie struct {
	Genres      []Genre
	ID          string
	Title       string
	ReleaseDate time.Time
	RawData     []string
}

type Genre struct {
	Name string `json:"name"`
	ID   int    `json:"id"`
}

func (m *Q1Movie) Deserialize(data []string) error {
	err := json.Unmarshal([]byte(data[Q1Genres]), &m.Genres)
	if err != nil {
		log.Printf("Failed to unmarshal genres: %v", data[MovieGenres])
		log.Printf("Error: %v", err)
	}

	m.ID = data[MovieID]
	m.Title = data[MovieTitle]
	m.ReleaseDate, err = time.Parse("2006-01-02", data[MovieReleaseDate])
	if err != nil {
		return err
	}
	m.RawData = make([]string, 4)
	m.RawData[Q1MovieID] = data[MovieID]
	m.RawData[Q1MovieTitle] = data[MovieTitle]
	m.RawData[Q1Genres] = data[MovieGenres]
	m.RawData[Q1ReleaseDate] = data[MovieReleaseDate]
	return nil
}

func (m *Q1Movie) GetRawData() []string {
	return m.RawData
}

func (m *Q1Movie) PassesFilter() bool {
	return m.ReleaseDate.Year() >= 2000 && m.ReleaseDate.Year() <= 2009
}

func (m *Q1Movie) GetMovieId() string {
	return m.ID
}
