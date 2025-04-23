package main

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	totalReceivedMessages := 0
	q, err := utils.NewConsumerQueue(conn, "movies_metadata", "movies_metadata", "movies_receiver_internal")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	q1, err := utils.NewProducerQueue(conn, "movies_metadata_q1", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q1.CloseChannel()

	q2, err := utils.NewProducerQueue(conn, "movies_metadata_q2", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q2.CloseChannel()

	q3, err := utils.NewProducerQueue(conn, "movies_metadata_q3", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q3.CloseChannel()

	q4, err := utils.NewProducerQueue(conn, "movies_metadata_q4", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q4.CloseChannel()

	q5, err := utils.NewProducerQueue(conn, "movies_metadata_q5", "movies")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	defer q5.CloseChannel()

	q.AddFinishSubscriber(q1)
	q.AddFinishSubscriber(q2)
	q.AddFinishSubscriber(q3)
	q.AddFinishSubscriber(q4)
	q.AddFinishSubscriber(q5)

	for d := range q.Consume() {
		totalReceivedMessages++
		stringLine := string(d.Body)

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 24
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			d.Nack(false, false)
			continue
		}

		movie := &messages.Movie{}
		if err := movie.Deserialize(record); err != nil {
			//log.Printf("Failed to deserialize movie: %v", err)
			d.Nack(false, false)
			continue
		}
		serializedMovie, err := protocol.Serialize(movie)
		if err != nil {
			log.Printf("Failed to serialize movie: %v", err)
			d.Nack(false, false)
			continue
		}

		if movie.IncludesAllCountries([]string{"Argentina", "Spain"}) {
			err = q1.Publish(serializedMovie)
			if err != nil {
				log.Printf("Failed to publish to queue 1: %v", err)

			}
		}

		if len(movie.Countries) == 1 {
			err = q2.Publish(serializedMovie)
			if err != nil {
				log.Printf("Failed to publish to queue 2: %v", err)
			}
		}

		if movie.IncludesAllCountries([]string{"Argentina"}) {
			err = q3.Publish(serializedMovie)
			if err != nil {
				log.Printf("Failed to publish to queue 3: %v", err)
			}
			err = q4.Publish(serializedMovie)
			if err != nil {
				log.Printf("Failed to publish to queue 4: %v", err)
			}
		}

		if !movie.HasValidBudgetAndRevenue() {
			d.Nack(false, false)
			continue
		}

		err = q5.Publish(serializedMovie)
		if err != nil {
			log.Printf("Failed to publish to queue 5: %v", err)
		}

		d.Ack(false)
	}

	log.Printf("Total received messages: %d", totalReceivedMessages)
	defer conn.Close()
}
