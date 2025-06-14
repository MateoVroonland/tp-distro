package receiver

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MoviesReceiver struct {
	conn           *amqp.Connection
	MoviesConsumer *utils.ConsumerQueue
	Q1Producer     *utils.ProducerQueue
	Q2Producer     *utils.ProducerQueue
	Q3Producer     *utils.ProducerQueue
	Q4Producer     *utils.ProducerQueue
	Q5Producer     *utils.ProducerQueue
}

func NewMoviesReceiver(conn *amqp.Connection, moviesConsumer *utils.ConsumerQueue, q1Producer *utils.ProducerQueue, q2Producer *utils.ProducerQueue, q3Producer *utils.ProducerQueue, q4Producer *utils.ProducerQueue, q5Producer *utils.ProducerQueue) *MoviesReceiver {
	return &MoviesReceiver{conn: conn, MoviesConsumer: moviesConsumer, Q1Producer: q1Producer, Q2Producer: q2Producer, Q3Producer: q3Producer, Q4Producer: q4Producer, Q5Producer: q5Producer}
}

func (r *MoviesReceiver) ReceiveMovies() {

	i := map[string]int{}
	for d := range r.MoviesConsumer.ConsumeInfinite() {
		i[d.ClientId]++
		if i[d.ClientId]%1000 == 0 {
			log.Printf("Received %d movies for client %s", i[d.ClientId], d.ClientId)
		}

		// aca que hacemos?????
		if d.Body == "FINISHED" {
			log.Printf("Received %d movies for client %s", i[d.ClientId], d.ClientId)
			r.Q1Producer.PublishFinished(d.ClientId)
			r.Q2Producer.PublishFinished(d.ClientId)
			r.Q3Producer.PublishFinished(d.ClientId)
			r.Q4Producer.PublishFinished(d.ClientId)
			r.Q5Producer.PublishFinished(d.ClientId)

			err := SaveState(r)
			if err != nil {
				log.Printf("Failed to save state: %v", err)
			}

			d.Ack()
			continue
		}

		reader := csv.NewReader(strings.NewReader(d.Body))
		reader.FieldsPerRecord = 24
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			d.Nack(false)
			continue
		}

		movie := &messages.Movie{}
		if err := movie.Deserialize(record); err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			d.Nack(false)
			continue
		}
		serializedMovie, err := protocol.Serialize(movie)
		if err != nil {
			log.Printf("Failed to serialize movie: %v", err)
			d.Nack(false)
			continue
		}

		if movie.IncludesAllCountries([]string{"Argentina", "Spain"}) {
			err = r.Q1Producer.Publish(serializedMovie, d.ClientId, movie.MovieId)
			if err != nil {
				log.Printf("Failed to publish to queue 1: %v", err)

			}
		}

		if len(movie.Countries) == 1 {
			err = r.Q2Producer.Publish(serializedMovie, d.ClientId, movie.MovieId)
			if err != nil {
				log.Printf("Failed to publish to queue 2: %v", err)
			}
		}

		if movie.IncludesAllCountries([]string{"Argentina"}) {
			err = r.Q3Producer.Publish(serializedMovie, d.ClientId, movie.MovieId)
			if err != nil {
				log.Printf("Failed to publish to queue 3: %v", err)
			}

			err = r.Q4Producer.Publish(serializedMovie, d.ClientId, movie.MovieId)
			if err != nil {
				log.Printf("Failed to publish to queue 4: %v", err)
			}
		}

		if movie.HasValidBudgetAndRevenue() {
			err = r.Q5Producer.Publish(serializedMovie, d.ClientId, movie.MovieId)
			if err != nil {
				log.Printf("Failed to publish to queue 5: %v", err)
			}
		}

		err = SaveState(r)
		if err != nil {
			log.Printf("Failed to save state: %v", err)
		}

		d.Ack()
	}
}
