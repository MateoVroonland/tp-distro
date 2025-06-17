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
	SinkConsumer    *utils.ConsumerQueue
	ResultsProducer *utils.ProducerQueue
	clientsResults  map[string]MinAndMaxMovie
}

type MinAndMaxMovie struct {
	MinMovie messages.MovieRating
	MaxMovie messages.MovieRating
}

func NewQ3Sink(sinkConsumer *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *Q3Sink {
	return &Q3Sink{
		SinkConsumer:    sinkConsumer,
		ResultsProducer: resultsProducer,
		clientsResults:  make(map[string]MinAndMaxMovie),
	}
}

func (s *Q3Sink) GetMaxAndMinMovies() {
	for msg := range s.SinkConsumer.ConsumeInfinite() {

		stringLine := string(msg.Body)

		if stringLine == "FINISHED" {
			if _, ok := s.clientsResults[msg.ClientId]; !ok {
				log.Printf("No client results to send for client %s, skipping", msg.ClientId)
			} else {
				log.Printf("Received FINISHED message for client %s", msg.ClientId)
				s.SendClientIdResults(msg.ClientId, s.clientsResults[msg.ClientId])
				delete(s.clientsResults, msg.ClientId)

				err := SaveQ3SinkState(s, s.clientsResults)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
			}
			msg.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			msg.Nack(false)
			continue
		}

		var movie messages.MovieRating
		err = movie.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false)
			continue
		}
		minAndMaxMovie, ok := s.clientsResults[msg.ClientId]
		if !ok {
			minAndMaxMovie = MinAndMaxMovie{}
		}

		if !ok || movie.Rating > minAndMaxMovie.MaxMovie.Rating {
			minAndMaxMovie.MaxMovie = movie
		}
		if !ok || movie.Rating < minAndMaxMovie.MinMovie.Rating || minAndMaxMovie.MinMovie.Rating == 0 {
			minAndMaxMovie.MinMovie = movie
		}
		s.clientsResults[msg.ClientId] = minAndMaxMovie
		log.Printf("Clients results: %v", s.clientsResults)

		err = SaveQ3SinkState(s, s.clientsResults)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}

		msg.Ack()
	}
}

func (s *Q3Sink) SendClientIdResults(clientId string, minAndMaxMovie MinAndMaxMovie) {
	minMovie := minAndMaxMovie.MinMovie
	maxMovie := minAndMaxMovie.MaxMovie
	log.Printf("Getting results")
	results := []messages.Q3Row{
		{
			MovieID: maxMovie.MovieID,
			Title:   maxMovie.Title,
			Rating:  maxMovie.Rating,
		},
		{
			MovieID: minMovie.MovieID,
			Title:   minMovie.Title,
			Rating:  minMovie.Rating,
		},
	}
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

	log.Printf("Publishing results")
	err = s.ResultsProducer.PublishResults(bytes, clientId, "q3")
	if err != nil {
		log.Printf("Failed to publish results: %v", err)
		return
	}
}

func (s *Q3Sink) SetClientsResults(results map[string]MinAndMaxMovie) {
	s.clientsResults = results
}
