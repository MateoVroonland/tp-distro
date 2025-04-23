package messages

import (
	"encoding/json"
	"fmt"
)

type Results struct {
	Query1 *QueryResult[Q1Row] `json:"query1,omitempty"`
	Query2 *QueryResult[Q2Row] `json:"query2,omitempty"`
	Query4 *QueryResult[Q4Row] `json:"query4,omitempty"`
	Query5 *QueryResult[Q5Row] `json:"query5,omitempty"`
}

type RawResult struct {
	QueryID string `json:"query_id"`
	Results []byte `json:"results"`
}

const (
	Query1Type = "query1"
	Query2Type = "query2"
	Query4Type = "query4"
	Query5Type = "query5"
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
	case Query4Type:
		var results []Q4Row
		if err := json.Unmarshal(raw.Results, &results); err != nil {
			return err
		}
		aq.Query4 = &QueryResult[Q4Row]{
			QueryID: raw.QueryID,
			Results: results,
		}
	case Query5Type:
		var results []Q5Row
		if err := json.Unmarshal(raw.Results, &results); err != nil {
			return err
		}
		aq.Query5 = &QueryResult[Q5Row]{
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

type Q4Row struct {
	Actor       string `json:"actor"`
	MoviesCount int    `json:"movies_count"`
}

func NewQ4Row(actor string, moviesCount int) *Q4Row {
	return &Q4Row{
		Actor:       actor,
		MoviesCount: moviesCount,
	}
}

type Q5Row struct {
	PositiveRatio float64 `json:"positive_ratio"`
	NegativeRatio float64 `json:"negative_ratio"`
}

func NewQ5Result(rows []Q5Row) *QueryResult[Q5Row] {
	return &QueryResult[Q5Row]{
		QueryID: Query5Type,
		Results: rows,
	}
}

func NewQ5Row(positiveRatio float64, negativeRatio float64) *Q5Row {
	return &Q5Row{
		PositiveRatio: positiveRatio,
		NegativeRatio: negativeRatio,
	}
}
