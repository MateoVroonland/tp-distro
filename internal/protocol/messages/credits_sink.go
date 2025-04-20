package messages

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
)

type CreditsSink struct {
	MovieID int
	Cast    []string
	RawData []string
}

const (
	CreditsSinkMovieIDIndex = 0
	CreditsSinkCastIndex    = 1
)

type ParsedCreditJSON struct {
	Name string `json:"name"`
}

func (c *CreditsSink) Deserialize(data []string) error {
	var err error
	c.MovieID, err = strconv.Atoi(data[CreditsMovieIDIndex])
	if err != nil {
		return err
	}
	parsedCredits := []ParsedCreditJSON{}

	jsonStr := strings.ReplaceAll(data[CreditsSinkCastIndex], "'", "\"")
	notNone := strings.ReplaceAll(jsonStr, "None", "null")
	err = json.Unmarshal([]byte(notNone), &parsedCredits)
	if err != nil {
		log.Printf("Error unmarshalling JSON: %v", data[CreditsSinkCastIndex])
		return err
	}

	var castNames []string

	for _, credit := range parsedCredits {
		castNames = append(castNames, credit.Name)
	}

	c.Cast = castNames

	stringArr, err := json.Marshal(castNames)
	if err != nil {
		return err
	}

	c.RawData = make([]string, 2)
	c.RawData[CreditsCastIndex] = string(stringArr)
	c.RawData[CreditsMovieIDIndex] = strconv.Itoa(c.MovieID)
	c.RawData = data
	return nil
}

func (c *CreditsSink) GetRawData() []string {
	return c.RawData
}
