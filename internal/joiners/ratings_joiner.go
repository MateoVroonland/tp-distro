package joiners

import (
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RatingsJoiner struct {
	conn           *amqp.Connection
	newClientQueue *utils.ConsumerFanout
	waitGroup      *sync.WaitGroup
	clientsLock    *sync.RWMutex
	ClientsJoiners map[string]bool
}

func NewRatingsJoiner(conn *amqp.Connection, newClientQueue *utils.ConsumerFanout) *RatingsJoiner {
	return &RatingsJoiner{
		conn:           conn,
		newClientQueue: newClientQueue,
		waitGroup:      &sync.WaitGroup{},
		clientsLock:    &sync.RWMutex{},
		ClientsJoiners: make(map[string]bool),
	}
}

func (r *RatingsJoiner) removeClient(clientId string) {
	r.clientsLock.Lock()
	defer r.clientsLock.Unlock()

	delete(r.ClientsJoiners, clientId)
}

func (r *RatingsJoiner) JoinRatings(routingKey string) error {

	defer r.newClientQueue.Close()

	for clientId := range r.ClientsJoiners {
		clientRatingsJoiner, err := NewRatingsJoinerClient(r, clientId)
		if err != nil {
			log.Printf("Failed to create ratings joiner client for client %s: %v", clientId, err)
			continue
		}
		r.waitGroup.Add(1)
		log.Printf("Restored joiner for client %s", clientId)
		go clientRatingsJoiner.JoinRatingsForClient()
	}

	for msg := range r.newClientQueue.Consume() {

		log.Printf("Received new client %s", msg.ClientId)

		r.clientsLock.Lock()
		if _, ok := r.ClientsJoiners[msg.ClientId]; !ok {
			log.Printf("Creating ratings joiner client for client %s", msg.ClientId)
			clientRatingsJoiner, err := NewRatingsJoinerClient(r, msg.ClientId)
			if err != nil {
				log.Printf("Failed to create ratings joiner client for client %s: %v", msg.ClientId, err)
				msg.Nack(false)
				r.clientsLock.Unlock()
				continue
			}

			r.ClientsJoiners[msg.ClientId] = true

			r.waitGroup.Add(1)
			go clientRatingsJoiner.JoinRatingsForClient()

			err = SaveRatingsJoinerState(r)
			if err != nil {
				log.Printf("Failed to save ratings joiner state: %v", err)
			} else {
				log.Printf("Saved ratings joiner state for client %s", msg.ClientId)
			}

		}

		r.clientsLock.Unlock()

		msg.Ack()

	}

	r.waitGroup.Wait()
	return nil
}

type RatingsJoinerClient struct {
	ClientId               string
	MoviesConsumer         *utils.ConsumerQueue
	RatingsConsumer        *utils.ConsumerQueue
	SinkProducer           *utils.ProducerQueue
	MoviesIds              map[int]string
	ratingsJoiner          *RatingsJoiner
	FinishedFetchingMovies bool
}

func NewRatingsJoinerClient(ratingsJoiner *RatingsJoiner, clientId string) (*RatingsJoinerClient, error) {
	finishedFetchingMovies := false
	moviesConsumer, err := utils.NewConsumerQueue(ratingsJoiner.conn, "filter_q3_client_"+clientId, "filter_q3_client_"+clientId, env.AppEnv.MOVIES_RECEIVER_AMOUNT)
	if err != nil {
		log.Printf("Failed to create movies consumer for client %s: %v", clientId, err)
		return nil, err
	}

	ratingsConsumer, err := utils.NewConsumerQueue(ratingsJoiner.conn, "ratings_joiner_client_"+clientId, "ratings_joiner_client_"+clientId, env.AppEnv.RATINGS_RECEIVER_AMOUNT)
	if err != nil {
		log.Printf("Failed to create ratings consumer for client %s: %v", clientId, err)
		return nil, err
	}

	sinkProducer, err := utils.NewProducerQueue(ratingsJoiner.conn, "q3_sink", 1)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	stateFile, fileErr := os.Open("data/ratings_joiner_state_" + clientId + ".gob")
	var state RatingsJoinerClientState
	var moviesIds map[int]string

	if os.IsNotExist(fileErr) {
		log.Printf("State file of ratings joiner for client %s does not exist, starting from scratch", clientId)
		moviesIds = make(map[int]string)
	} else if fileErr != nil {
		log.Printf("Failed to open state file of ratings joiner for client %s: %v", clientId, fileErr)
		if moviesConsumer != nil {
			moviesConsumer.CloseChannel()
		}
		return nil, fileErr
	} else {
		defer stateFile.Close()
		dec := gob.NewDecoder(stateFile)
		err := dec.Decode(&state)
		if err != nil {
			log.Printf("Failed to decode state file of ratings joiner for client %s: %v", clientId, err)
			if moviesConsumer != nil {
				moviesConsumer.CloseChannel()
			}
			return nil, err
		}

		defer stateFile.Close()

		sinkProducer.RestoreState(state.SinkProducer)
		ratingsConsumer.RestoreState(state.RatingsConsumer)
		moviesIds = state.MoviesIds
		finishedFetchingMovies = state.FinishedFetchingMovies
		moviesConsumer.RestoreState(state.MoviesConsumer)

		if finishedFetchingMovies {
			log.Printf("Finished fetching movies for client %s upon restart", clientId)
			moviesConsumer.CloseChannel()
			moviesConsumer.DeleteQueue()

		}
	}

	return &RatingsJoinerClient{
		ratingsJoiner:          ratingsJoiner,
		ClientId:               clientId,
		MoviesConsumer:         moviesConsumer,
		RatingsConsumer:        ratingsConsumer,
		SinkProducer:           sinkProducer,
		MoviesIds:              moviesIds,
		FinishedFetchingMovies: finishedFetchingMovies,
	}, nil
}

func (r *RatingsJoinerClient) JoinRatingsForClient() error {
	log.Printf("Joining ratings for client %s", r.ClientId)

	defer r.SinkProducer.CloseChannel()
	defer r.ratingsJoiner.waitGroup.Done()

	defer r.RatingsConsumer.CloseChannel()

	if !r.FinishedFetchingMovies {
		r.fetchMovies()
	}

	log.Printf("Received %d movies for client %s", len(r.MoviesIds), r.ClientId)

	r.fetchRatings()

	return nil
}

func (r *RatingsJoinerClient) fetchMovies() {
	stateSaver := NewRatingsJoinerPerClientState()

	for msg := range r.MoviesConsumer.ConsumeInfinite() {
		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := stateSaver.SaveStateAck(&msg, r)
				if err != nil {
					log.Printf("Failed to save ratings joiner state: %v", err)
				}
				continue
			}

			r.FinishedFetchingMovies = true
			err := stateSaver.SaveStateAck(&msg, r)
			if err != nil {
				log.Printf("Failed to save ratings joiner state: %v", err)
			}
			stateSaver.ForceFlush()
			r.MoviesConsumer.DeleteQueue()
			break
		}

		stringLine := string(msg.Body)
		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Movie: %s", stringLine)
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}

		var movie messages.RatingsJoinMovies
		err = movie.Deserialize(record)
		if err != nil {
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}

		r.MoviesIds[movie.ID] = movie.Title

		err = SaveRatingsJoinerPerClientState(r)
		if err != nil {
			log.Printf("Failed to save ratings joiner state: %v", err)
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}

		err = stateSaver.SaveStateAck(&msg, r)
		if err != nil {
			log.Printf("Failed to save ratings joiner state: %v", err)
		}
	}
}

func (r *RatingsJoinerClient) fetchRatings() {
	stateSaver := NewRatingsJoinerPerClientState()

	for msg := range r.RatingsConsumer.ConsumeInfinite() {

		if msg.IsFinished {

			if !msg.IsLastFinished {
				err := stateSaver.SaveStateAck(&msg, r)
				if err != nil {
					log.Printf("Failed to save ratings joiner state: %v", err)
				}
				continue
			}

			r.ratingsJoiner.removeClient(r.ClientId)
			r.SinkProducer.PublishFinished(r.ClientId)

			err := stateSaver.SaveStateAck(&msg, r)
			if err != nil {
				log.Printf("Failed to save ratings joiner state: %v", err)
			}
			stateSaver.ForceFlush()
			r.RatingsConsumer.DeleteQueue()

			break

		}

		stringLine := string(msg.Body)

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Rating: %s", stringLine)
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}

		var rating messages.Ratings
		err = rating.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize ratings: %v", err)
			stateSaver.SaveStateNack(&msg, r, false)
			continue
		}

		if movieTitle, ok := r.MoviesIds[rating.MovieID]; ok {
			res := fmt.Sprintf("%d,%s,%f", rating.MovieID, movieTitle, rating.Rating)
			r.SinkProducer.Publish([]byte(res), r.ClientId, "")
		}

		err = stateSaver.SaveStateAck(&msg, r)
		if err != nil {
			log.Printf("Failed to save ratings joiner state: %v", err)
		}
	}

	log.Printf("Finished joining ratings for client %s", r.ClientId)
}
