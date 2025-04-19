package messages

import (
	"encoding/json"
	"fmt"
)

type Results struct {
	Query1 *QueryResult[Q1Row] `json:"query1,omitempty"`
}

type RawResult struct {
	QueryID string `json:"query_id"`
	Results []byte `json:"results"`
}

const (
	Query1Type = "query1"
)

func (aq *Results) UnmarshalJSON(data []byte) error {
	var raw RawResult
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	switch raw.QueryID {
	case Query1Type:
		var results []Q1Row
		if err := json.Unmarshal(raw.Results, &results); err != nil {
			return err
		}
		aq.Query1 = &QueryResult[Q1Row]{
			QueryID: raw.QueryID,
			Results: results,
		}
	default:
		return fmt.Errorf("unknown query type: %s", raw.QueryID)
	}

	return nil
}

type QueryResult[T any] struct {
	QueryID string `json:"query_id"`
	Results []T    `json:"results"`
}

func NewQuery1Result(rows []Q1Row) *QueryResult[Q1Row] {
	return &QueryResult[Q1Row]{
		QueryID: "query1",
		Results: rows,
	}
}

type Q1Row struct {
	MovieID string  `json:"movie_id"`
	Title   string  `json:"title"`
	Genres  []Genre `json:"genres"`
}

func NewQ1Row(movieID string, title string, genres []Genre) *Q1Row {
	return &Q1Row{
		MovieID: movieID,
		Title:   title,
		Genres:  genres,
	}
}
