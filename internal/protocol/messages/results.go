package messages

import (
	"encoding/json"
	"fmt"
)

type Results struct {
	Query1 *QueryResult[Q1Row] `json:"query1,omitempty"`
	Query2 *QueryResult[Q2Row] `json:"query2,omitempty"`
	Query3 *QueryResult[Q3Row] `json:"query3,omitempty"`
}

type RawResult struct {
	QueryID string `json:"query_id"`
	Results []byte `json:"results"`
}

const (
	Query1Type = "query1"
	Query2Type = "query2"
	Query3Type = "query3"
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
	case Query2Type:
		var results []Q2Row
		if err := json.Unmarshal(raw.Results, &results); err != nil {
			return err
		}
		aq.Query2 = &QueryResult[Q2Row]{
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
		QueryID: Query1Type,
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

type Q2Row struct {
	Country string `json:"country"`
	Amount  int    `json:"amount"`
}

func NewQ2Row(country string, amount int) *Q2Row {
	return &Q2Row{
		Country: country,
		Amount:  amount,
	}
}

func NewQ2Result(rows []Q2Row) *QueryResult[Q2Row] {
	return &QueryResult[Q2Row]{
		QueryID: Query2Type,
		Results: rows,
	}
}

type Q3Row struct {
	MovieID string `json:"movie_id"`
	Title   string `json:"title"`
	Rating  float64 `json:"rating"`
}

func NewQ3Row(movieID string, title string, rating float64) *Q3Row {
	return &Q3Row{
		MovieID: movieID,
		Title:   title,
		Rating:  rating,
	}
}

func NewQ3Result(rows []Q3Row) *QueryResult[Q3Row] {
	return &QueryResult[Q3Row]{
		QueryID: Query3Type,
		Results: rows,
	}
}

