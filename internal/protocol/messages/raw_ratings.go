package messages

import (
	"fmt"
	"strconv"
)

type RawRatings struct {
	MovieID int
	Rating  float64
	RawData []string
}

const (
	RawRatingMovieIDIndex = 1
	RawRatingRatingIndex  = 2
)

func (r *RawRatings) Deserialize(data []string) error {
	var err error
	r.MovieID, err = strconv.Atoi(data[RawRatingMovieIDIndex])
	if err != nil {
		fmt.Println("data[1]", data[1])
		return err
	}
	r.Rating, err = strconv.ParseFloat(data[RawRatingRatingIndex], 64)
	if err != nil {
		return err
	}
	rawData := make([]string, 2)
	rawData[0] = data[RawRatingMovieIDIndex]
	rawData[1] = data[RawRatingRatingIndex]

	r.RawData = rawData
	return nil
}

func (r *RawRatings) GetRawData() []string {
	return r.RawData
}
