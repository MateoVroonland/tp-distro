package sinks

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)


type Q3Sink struct {
	SinkConsumer *utils.ConsumerQueue
	ResultsProducer *utils.ProducerQueue
}

func NewQ3Sink(sinkConsumer *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *Q3Sink {
	return &Q3Sink{
		SinkConsumer: sinkConsumer,
		ResultsProducer: resultsProducer,
	}
}

func (s *Q3Sink) GetMaxAndMinMovies() {
	msgs, err := s.SinkConsumer.Consume()
	if err != nil {
		log.Printf("Failed to consume messages: %v", err)
		return
	}

	maxMovie := messages.MovieRating{}
	minMovie := messages.MovieRating{}

	for msg := range msgs {
		if string(msg.Body) == "FINISHED" {
			msg.Ack(false)
			break
		}
		var movie messages.MovieRating
		stringLine := string(msg.Body)
		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false, false)
			continue
		}
		err = movie.Deserialize(record)	
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false, false)
			continue
		}

		if movie.Rating > maxMovie.Rating {
			maxMovie = movie
		}
		if movie.Rating < minMovie.Rating || minMovie.Rating == 0 {
			minMovie = movie
		}
		msg.Ack(false)
	}

	results := []messages.Q3Row{
		{
			MovieID: string(maxMovie.MovieID),
			Title:   maxMovie.Title,
			Rating:  maxMovie.Rating,
		},
		{
			MovieID: string(minMovie.MovieID),
			Title:   minMovie.Title,
			Rating:  minMovie.Rating,
		},
	}
	log.Printf("Results after getting max and min movies: %v", results)
	resultsBytes, err := json.Marshal(results)
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	rawResult := messages.RawResult{
		QueryID: "query3",
		Results: resultsBytes,
	}

	bytes, err := json.Marshal(rawResult)
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	err = s.ResultsProducer.Publish(bytes)
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}	
	



}
