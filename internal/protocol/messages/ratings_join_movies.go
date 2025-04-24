package messages

import (
	"strconv"
)

const (
	RatingsJoinMoviesID = iota
	RatingsJoinMoviesTitle
)

type RatingsJoinMovies struct {
	ID          int
	Title       string
	RawData     []string
}

func (m *RatingsJoinMovies) Deserialize(data []string) error {
	id, err := strconv.Atoi(data[Q3MovieID])
	if err != nil {
		return err
	}

	m.ID = id
	m.Title = data[Q3MovieTitle]

	m.RawData = make([]string, 2)
	m.RawData[RatingsJoinMoviesID] = data[Q3MovieID]
	m.RawData[RatingsJoinMoviesTitle] = data[Q3MovieTitle]
	return nil
}

func (m *RatingsJoinMovies) GetRawData() []string {
	return m.RawData
}