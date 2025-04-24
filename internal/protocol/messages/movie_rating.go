package messages

import (
	"fmt"
	"strconv"
)

type MovieRating struct {
	MovieID int 
	Title string 
	Rating  float64 
}

func (m *MovieRating) Deserialize(data []string) error {
	movieID, err := strconv.Atoi(data[0])
	if err != nil {
		return fmt.Errorf("failed to parse movie ID: %w", err)
	}
	m.MovieID = movieID
	m.Title = data[1]
	rating, err := strconv.ParseFloat(data[2], 64)
	if err != nil {
		return fmt.Errorf("failed to parse rating: %w", err)
	}
	m.Rating = rating

	return nil
}