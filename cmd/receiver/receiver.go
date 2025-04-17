package main

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := utils.NewQueue(ch, "movies_metadata", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	msgs, err := q.Consume()
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	var forever chan struct{}

	go func() {

		q1, err := utils.NewQueue(ch, "movies_metadata_q1", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("Failed to declare a queue: %v", err)
		}

		q2, err := utils.NewQueue(ch, "movies_metadata_q2", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("Failed to declare a queue: %v", err)
		}

		q3, err := utils.NewQueue(ch, "movies_metadata_q3", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("Failed to declare a queue: %v", err)
		}

		q4, err := utils.NewQueue(ch, "movies_metadata_q4", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("Failed to declare a queue: %v", err)
		}

		q5, err := utils.NewQueue(ch, "movies_metadata_q5", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("Failed to declare a queue: %v", err)
		}

		for d := range msgs {
			stringLine := string(d.Body)
			reader := csv.NewReader(strings.NewReader(stringLine))
			reader.FieldsPerRecord = -1
			record, err := reader.Read()
			if err != nil {
				log.Fatalf("Failed to read record: %v", err)
			}

			movie := messages.Movie{}
			movie.Deserialize(record)

			if movie.IncludesAllCountries([]string{"Spain", "Argentina"}) {
				err = q1.Publish(d.Body)
				if err != nil {
					log.Fatalf("Failed to publish to queue 1: %v", err)
				}
			}

			if len(movie.Countries) == 1 {
				err = q2.Publish(d.Body)
				if err != nil {
					log.Fatalf("Failed to publish to queue 2: %v", err)
				}
			}

			if movie.IncludesAllCountries([]string{"Argentina"}) {
				err = q3.Publish(d.Body)
				if err != nil {
					log.Fatalf("Failed to publish to queue 3: %v", err)
				}
				err = q4.Publish(d.Body)
				if err != nil {
					log.Fatalf("Failed to publish to queue 4: %v", err)
				}
			}

			err = q5.Publish(d.Body)
			if err != nil {
				log.Fatalf("Failed to publish to queue 5: %v", err)
			}

			log.Printf("%v", movie)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
