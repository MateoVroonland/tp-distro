package messages

import (
	"strconv"
)

type RawRatings struct {
	MovieID int 
	Rating  float64 
	RawData []string
}

func (r *RawRatings) Deserialize(data []string) error {
	var err error
	r.MovieID, err = strconv.Atoi(data[1])
	if err != nil {
		return err
	}
	r.Rating, err = strconv.ParseFloat(data[2], 64)
	if err != nil {
		return err
	}
	rawData := make([]string, 2)
	rawData[0] = data[1]
	rawData[1] = data[2]

	r.RawData = rawData
	return nil
}

func (r *RawRatings) GetRawData() []string {
	return r.RawData
}

