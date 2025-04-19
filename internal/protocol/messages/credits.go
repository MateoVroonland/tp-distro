package messages

import (
	"strconv"
	"strings"
)

type Credits struct {
	MovieID int
	Cast    []string
	RawData []string
}

const (
	CreditsMovieIDIndex = 0
	CreditsCastIndex    = 1
)

func (c *Credits) Deserialize(data []string) error {
	var err error
	c.MovieID, err = strconv.Atoi(data[0])
	if err != nil {
		return err
	}
	cast := strings.Split(data[1], ",") // TODO: JSON PARSE THIS
	c.Cast = cast

	c.RawData = data
	return nil
}

func (c *Credits) GetRawData() []string {
	return c.RawData
}
