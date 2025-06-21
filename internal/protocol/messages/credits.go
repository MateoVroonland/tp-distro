package messages

import (
	"strconv"
)

type Credit struct {
	MovieID int
	Cast    string
	RawData []string
}

const (
	CreditsMovieIDIndex = 0
	CreditsCastIndex    = 1
)

func (c *Credit) Deserialize(data []string) error {
	var err error
	c.MovieID, err = strconv.Atoi(data[RawCreditsMovieIDIndex])
	if err != nil {
		return err
	}

	c.Cast = data[RawCreditsCastIndex]

	c.RawData = make([]string, 2)
	c.RawData[CreditsCastIndex] = data[RawCreditsCastIndex]
	c.RawData[CreditsMovieIDIndex] = strconv.Itoa(c.MovieID)
	return nil
}

func (c *Credit) GetRawData() []string {
	return c.RawData
}
