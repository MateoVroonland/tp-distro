package messages

import (
	"strconv"
	"time"
)

const (
	Q3MovieID = iota
	Q3MovieTitle
)

type Q3Movie struct {
	ID          int
	Title       string
	ReleaseDate time.Time
	RawData     []string
}

func (m *Q3Movie) Deserialize(data []string) error {

	id, err := strconv.Atoi(data[MovieID])
	if err != nil {
		return err
	}

	m.ID = id
	m.Title = data[MovieTitle]
	m.ReleaseDate, err = time.Parse("2006-01-02", data[MovieReleaseDate])
	if err != nil {
		return err
	}

	m.RawData = make([]string, 2)
	m.RawData[Q3MovieID] = data[MovieID]
	m.RawData[Q3MovieTitle] = data[MovieTitle]
	return nil
}

func (m *Q3Movie) GetRawData() []string {
	return m.RawData
}

func (m *Q3Movie) PassesFilter() bool {
	return m.ReleaseDate.Year() >= 2000
}

func (m *Q3Movie) GetRoutingKey() string {
	return ""
}