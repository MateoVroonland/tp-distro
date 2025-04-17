package messages

import (
	"encoding/json"
	"log"
	"slices"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
)

type Movie struct {
	protocol.Protocol
	Countries []Country
	RawData   []string
}

type Country struct {
	Name string `json:"name"`
}

func (m *Movie) IncludesAllCountries(countries []string) bool {
	remainingCountries := len(countries)
	for _, c := range m.Countries {
		if slices.Contains(countries, c.Name) {
			remainingCountries--
		}
	}
	return remainingCountries == 0
}

const (
	MovieID = iota
	MovieTitle
	MovieReleaseDate
	MovieGenres
)

func (m *Movie) Deserialize(data []string) {
	jsonStr := strings.ReplaceAll(data[RawMovieProductionCountries], "'", "\"")
	err := json.Unmarshal([]byte(jsonStr), &m.Countries)
	if err != nil {
		log.Printf("Failed to unmarshal production countries: %v", jsonStr)
		log.Printf("Error: %v", err)
	}

	m.RawData = make([]string, 4)
	m.RawData[MovieID] = data[RawMovieID]
	m.RawData[MovieTitle] = data[RawMovieTitle]
	m.RawData[MovieReleaseDate] = data[RawMovieReleaseDate]
	m.RawData[MovieGenres] = data[RawMovieGenres]
}

func (m *Movie) GetRawData() []string {
	return m.RawData
}
