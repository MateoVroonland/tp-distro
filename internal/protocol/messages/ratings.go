package messages

import (
	"strconv"
)

type Ratings struct {
	MovieID int
	Rating  float64
	RawData []string
}

const (
	RatingsMovieIDIndex = 0
	RatingsRatingIndex  = 1
)

func (r *Ratings) Deserialize(data []string) error {
	var err error
	r.MovieID, err = strconv.Atoi(data[0])
	if err != nil {
		return err
	}
	rating, err := strconv.ParseFloat(data[1], 64)
	if err != nil {
		return err
	}
	r.Rating = rating

	r.RawData = data
	return nil
}

func (r *Ratings) GetRawData() []string {
	return r.RawData
}
