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
	clientsResults  map[string]map[int]MovieRatingCumulative // clientId -> movieId -> MovieRatingCumulative
}

type MovieRatingCumulative struct {
	MovieID     int
	Title       string
	TotalRating float64
	Count       int
}

func NewQ3Sink(sinkConsumer *utils.ConsumerQueue, resultsProducer *utils.ProducerQueue) *Q3Sink {
	return &Q3Sink{
		SinkConsumer:    sinkConsumer,
		ResultsProducer: resultsProducer,
		clientsResults:  make(map[string]map[int]MovieRatingCumulative),
	}
}

func (s *Q3Sink) GetMaxAndMinMovies() {
	q3SinkStateSaver := NewQ3SinkStateSaver()

	for msg := range s.SinkConsumer.ConsumeInfinite() {

		stringLine := string(msg.Body)

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := q3SinkStateSaver.SaveStateAck(&msg, s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				continue
			}
			if _, ok := s.clientsResults[msg.ClientId]; !ok {
				log.Printf("No client results to send for client %s, skipping", msg.ClientId)
			} else {
				log.Printf("Received FINISHED message for client %s", msg.ClientId)

				s.SendClientIdResults(msg.ClientId, s.clientsResults[msg.ClientId])
				delete(s.clientsResults, msg.ClientId)

				err := q3SinkStateSaver.SaveStateAck(&msg, s)
				if err != nil {
					log.Printf("Failed to save state: %v", err)
				}
				q3SinkStateSaver.ForceFlush()
			}
			continue
		}

		var movie messages.MovieRating
		reader := csv.NewReader(strings.NewReader(stringLine))
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			q3SinkStateSaver.SaveStateNack(&msg, s, false)
			continue
		}

		err = movie.Deserialize(record)
		if err != nil {
			q3SinkStateSaver.SaveStateNack(&msg, s, false)
			continue
		}

		if _, ok := s.clientsResults[msg.ClientId]; !ok {
			s.clientsResults[msg.ClientId] = make(map[int]MovieRatingCumulative)
		}

		if _, ok := s.clientsResults[msg.ClientId][movie.MovieID]; !ok {
			s.clientsResults[msg.ClientId][movie.MovieID] = MovieRatingCumulative{
				MovieID:     movie.MovieID,
				Title:       movie.Title,
				TotalRating: movie.Rating,
				Count:       1,
			}
		} else {
			current := s.clientsResults[msg.ClientId][movie.MovieID]
			current.TotalRating += movie.Rating
			current.Count++
			s.clientsResults[msg.ClientId][movie.MovieID] = current
		}

		err = q3SinkStateSaver.SaveStateAck(&msg, s)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}
	}
}

func (s *Q3Sink) SetClientsResults(clientsResults map[string]map[int]MovieRatingCumulative) {
	s.clientsResults = clientsResults
}

func (s *Q3Sink) FindMinAndMaxAverageRatingMovies(clientResults map[int]MovieRatingCumulative) (MovieRatingCumulative, MovieRatingCumulative) {
	var minMovie MovieRatingCumulative
	var maxMovie MovieRatingCumulative
	first := true
	for _, movie := range clientResults {
		if first {
			minMovie = movie
			maxMovie = movie
			first = false
			continue
		}
		if movie.Count == 0 {
			continue
		}
		avgRating := movie.TotalRating / float64(movie.Count)
		if avgRating < minMovie.TotalRating/float64(minMovie.Count) {
			minMovie = movie
		}
		if avgRating > maxMovie.TotalRating/float64(maxMovie.Count) {
			maxMovie = movie
		}
	}

	return minMovie, maxMovie
}

func (s *Q3Sink) SendClientIdResults(clientId string, clientResults map[int]MovieRatingCumulative) {
	minMovie, maxMovie := s.FindMinAndMaxAverageRatingMovies(clientResults)
	log.Printf("Getting results")
	results := []messages.Q3Row{
		{
			MovieID: maxMovie.MovieID,
			Title:   maxMovie.Title,
			Rating:  maxMovie.TotalRating / float64(maxMovie.Count),
		},
		{
			MovieID: minMovie.MovieID,
			Title:   minMovie.Title,
			Rating:  minMovie.TotalRating / float64(minMovie.Count),
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
