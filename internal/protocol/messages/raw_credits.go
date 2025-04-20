package messages

import (
	"strconv"
)

type RawCredits struct {
	MovieID int
	RawData []string
}

const (
	RawCreditsMovieIDIndex = 0
	RawCreditsCastIndex    = 1
)

func (r *RawCredits) Deserialize(data []string) error {
	var err error
	r.MovieID, err = strconv.Atoi(data[2])
	if err != nil {
		return err
	}
	rawData := make([]string, 2)
	rawData[0] = data[2]
	rawData[1] = data[0]
	r.RawData = rawData
	return nil
}

func (r *RawCredits) GetRawData() []string {
	return r.RawData
}
