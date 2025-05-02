package joiners

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type RatingsJoiner struct {
	ratingsJoinerConsumer *utils.ConsumerQueue
	moviesJoinerConsumer  *utils.ConsumerQueue
	sinkProducer          *utils.ProducerQueue
}

func NewRatingsJoiner(ratingsJoinerConsumer *utils.ConsumerQueue, moviesJoinerConsumer *utils.ConsumerQueue, sinkProducer *utils.ProducerQueue) *RatingsJoiner {
	return &RatingsJoiner{ratingsJoinerConsumer: ratingsJoinerConsumer, moviesJoinerConsumer: moviesJoinerConsumer, sinkProducer: sinkProducer}
}

func (r *RatingsJoiner) JoinRatings() error {
	defer r.ratingsJoinerConsumer.CloseChannel()
	defer r.moviesJoinerConsumer.CloseChannel()
	defer r.sinkProducer.CloseChannel()

	moviesIds := make(map[int]string)

	i := 0
	for msg := range r.moviesJoinerConsumer.ConsumeInfinite() {

		stringLine := string(msg.Body)

		i++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Movie: %s", stringLine)
			msg.Nack(false, false)
			continue
		}

		var movie messages.RatingsJoinMovies
		err = movie.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false, false)
			continue
		}

		moviesIds[movie.ID] = movie.Title
		msg.Ack(false)

	}

	ratings := make(map[int]float64)
	ratingsCount := make(map[int]int)
	j := 0
	r.ratingsJoinerConsumer.AddFinishSubscriber(r.sinkProducer)
	for msg := range r.ratingsJoinerConsumer.ConsumeInfinite() {
		stringLine := string(msg.Body)
		j++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Rating: %s", stringLine)
			msg.Nack(false, false)
			continue
		}

		var rating messages.Ratings
		err = rating.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize ratings: %v", err)
			msg.Nack(false, false)
			continue
		}

		if _, ok := moviesIds[rating.MovieID]; !ok {
			msg.Ack(false)
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

		msg.Ack(false)
	}

	log.Printf("Ratings: %v", ratings)
	log.Printf("RatingsCount: %v", ratingsCount)
	log.Printf("MoviesIds: %v", moviesIds)

	// for movieId, rating := range ratings {
	// 	count := ratingsCount[movieId]
	// 	res := fmt.Sprintf("%d,%s,%f", movieId, moviesIds[movieId], rating/float64(count))
	// 	r.sinkProducer.Publish([]byte(res))
	// }
	return nil
}
