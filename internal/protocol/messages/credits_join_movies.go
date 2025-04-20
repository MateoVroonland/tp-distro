package messages

import (
	"strconv"
	"time"
)

const (
	CreditsJoinMoviesID = iota
	CreditsJoinMoviesReleaseDate
)

type CreditsJoinMovies struct {
	ID          int
	ReleaseDate time.Time
	RawData     []string
}

func (m *CreditsJoinMovies) Deserialize(data []string) error {

	id, err := strconv.Atoi(data[Q4MovieID])
	if err != nil {
		return err
	}

	releaseDate, err := time.Parse("2006-01-02", data[Q4ReleaseDate])
	if err != nil {
		return err
	}

	m.ID = id
	m.ReleaseDate = releaseDate

	m.RawData = make([]string, 2)
	m.RawData[CreditsJoinMoviesID] = data[Q4MovieID]
	m.RawData[CreditsJoinMoviesReleaseDate] = data[Q4ReleaseDate]
	return nil
}

func (m *CreditsJoinMovies) GetRawData() []string {
	return m.RawData
}
