package messages

import (
	"strconv"
	"time"
)

const (
	Q4MovieID = iota
	Q4ReleaseDate
)

type Q4Movie struct {
	ID          int
	ReleaseDate time.Time
	RawData     []string
}

func (m *Q4Movie) Deserialize(data []string) error {

	id, err := strconv.Atoi(data[MovieID])
	if err != nil {
		return err
	}

	releaseDate, err := time.Parse("2006-01-02", data[MovieReleaseDate])
	if err != nil {
		return err
	}

	m.ID = id
	m.ReleaseDate = releaseDate

	m.RawData = make([]string, 2)
	m.RawData[Q4MovieID] = data[MovieID]
	m.RawData[Q4ReleaseDate] = data[MovieReleaseDate]
	return nil
}

func (m *Q4Movie) GetRawData() []string {
	return m.RawData
}

func (m *Q4Movie) Is2000s() bool {
	return m.ReleaseDate.Year() >= 2000 && m.ReleaseDate.Year() <= 2009
}
