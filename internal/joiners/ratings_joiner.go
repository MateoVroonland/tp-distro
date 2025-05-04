package joiners

import (
	"encoding/csv"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RatingsJoiner struct {
	conn *amqp.Connection
	newClientQueue *utils.ConsumerQueue
	waitGroup *sync.WaitGroup
	clientsLock *sync.Mutex
	moviesConsumers map[string]*utils.ConsumerQueue
	ratingsConsumers map[string]*utils.ConsumerQueue
}

func NewRatingsJoiner(conn *amqp.Connection, newClientQueue *utils.ConsumerQueue) *RatingsJoiner {
	return &RatingsJoiner{conn: conn, newClientQueue: newClientQueue, waitGroup: &sync.WaitGroup{}, clientsLock: &sync.Mutex{}, moviesConsumers: make(map[string]*utils.ConsumerQueue), ratingsConsumers: make(map[string]*utils.ConsumerQueue)}
}

func (r *RatingsJoiner) JoinRatings(routingKey string) error {

	defer r.newClientQueue.CloseChannel()

	for msg := range r.newClientQueue.ConsumeInfinite() {
		var clientId string
		var ok bool
		if clientId, ok = msg.Headers["clientId"].(string); !ok {
			log.Printf("Failed to get clientId from message headers")
			msg.Nack(false, false)
			continue
		}

		log.Printf("Received new client %s", clientId)

		r.clientsLock.Lock()
		if _, ok := r.moviesConsumers[clientId]; !ok {
			moviesConsumer, err := utils.NewConsumerQueueWithRoutingKey(r.conn, "filter_q3	_client_"+clientId, "filter_q3_client_"+clientId, routingKey, "internal_filter_q3_client_"+clientId)
			if err != nil {
				log.Printf("Failed to create movies consumer for client %s: %v", clientId, err)
				msg.Nack(false, false)
				r.clientsLock.Unlock()
				continue
			}
			r.moviesConsumers[clientId] = moviesConsumer
		}

		if _, ok := r.ratingsConsumers[clientId]; !ok {
			ratingsConsumer, err := utils.NewConsumerQueueWithRoutingKey(r.conn, "ratings_joiner_client_"+clientId, "ratings_joiner_client_"+clientId, routingKey, "internal_ratings_joiner_client_"+clientId)
			if err != nil {
				log.Printf("Failed to create ratings consumer for client %s: %v", clientId, err)
				msg.Nack(false, false)
				r.clientsLock.Unlock()
				delete(r.moviesConsumers, clientId)
				continue
			}
			r.ratingsConsumers[clientId] = ratingsConsumer
			r.waitGroup.Add(1)
			go r.JoinRatingsForClient(clientId)
		}

		r.clientsLock.Unlock()

	}

	r.waitGroup.Wait()
	return nil
}

func (r *RatingsJoiner) JoinRatingsForClient(clientId string) error {
	log.Printf("Joining ratings for client %s", clientId)

	sinkProducer, err := utils.NewProducerQueue(r.conn, "q3_sink", "q3_sink")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	

	defer sinkProducer.CloseChannel()
	defer r.waitGroup.Done()

	moviesConsumer := r.moviesConsumers[clientId]
	ratingsConsumer := r.ratingsConsumers[clientId]

	defer moviesConsumer.CloseChannel()
	defer ratingsConsumer.CloseChannel()

	moviesIds := make(map[int]string)

	i := 0
	for msg := range moviesConsumer.Consume() {

		stringLine := string(msg.Body)

		i++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Movie: %s", stringLine)
			msg.Nack(false)
			continue
		}

		var movie messages.RatingsJoinMovies
		err = movie.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false)
			continue
		}

		moviesIds[movie.ID] = movie.Title
		msg.Ack()

	}

	ratings := make(map[int]float64)
	ratingsCount := make(map[int]int)
	j := 0
	ratingsConsumer.AddFinishSubscriber(sinkProducer)
	for msg := range ratingsConsumer.Consume() {
		stringLine := string(msg.Body)
		j++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Rating: %s", stringLine)
			msg.Nack(false)
			continue
		}

		var rating messages.Ratings
		err = rating.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize ratings: %v", err)
			msg.Nack(false)
			continue
		}

		if _, ok := moviesIds[rating.MovieID]; !ok {
			msg.Ack()
			continue
		}

		currentRatings, ok := ratings[rating.MovieID]

		if !ok {
			ratings[rating.MovieID] = rating.Rating
			ratingsCount[rating.MovieID] = 1
		} else {
			ratings[rating.MovieID] = currentRatings + rating.Rating
			ratingsCount[rating.MovieID]++
		}

		msg.Ack()
	}

	log.Printf("Ratings: %v", ratings)
	log.Printf("RatingsCount: %v", ratingsCount)
	log.Printf("MoviesIds: %v", moviesIds)

	for movieId, rating := range ratings {
		count := ratingsCount[movieId]
		res := fmt.Sprintf("%d,%s,%f", movieId, moviesIds[movieId], rating/float64(count))
		log.Printf("Res: %s", res)
		sinkProducer.Publish([]byte(res), clientId)
	}

	log.Printf("Finished joining ratings for client %s", clientId)

	r.clientsLock.Lock()
	delete(r.moviesConsumers, clientId)
	delete(r.ratingsConsumers, clientId)
	r.clientsLock.Unlock()

	return nil
}
