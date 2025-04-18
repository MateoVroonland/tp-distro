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

			if stringLine == "FINISHED" {
				log.Printf("Received message: %s", stringLine)
				q1.Publish([]byte("FINISHED"))
				q2.Publish([]byte("FINISHED"))
				q3.Publish([]byte("FINISHED"))
				q4.Publish([]byte("FINISHED"))
				q5.Publish([]byte("FINISHED"))
				break
			}
			reader := csv.NewReader(strings.NewReader(stringLine))
			reader.FieldsPerRecord = 24
			record, err := reader.Read()
			if err != nil {
				log.Printf("Failed to read record: %v", err)
				continue
			}

			movie := &messages.Movie{}
			if err := movie.Deserialize(record); err != nil {
				log.Printf("Failed to deserialize movie: %v", err)
				continue
			}
			serializedMovie, err := protocol.Serialize(movie)
			if err != nil {
				log.Printf("Failed to serialize movie: %v", err)
				continue
			}

			if movie.IncludesAllCountries([]string{"Argentina", "Spain"}) {
				err = q1.Publish(serializedMovie)
				if err != nil {
					log.Fatalf("Failed to publish to queue 1: %v", err)
				}
			}

			if len(movie.Countries) == 1 {
				err = q2.Publish(serializedMovie)
				if err != nil {
					log.Fatalf("Failed to publish to queue 2: %v", err)
				}
			}

			if movie.IncludesAllCountries([]string{"Argentina"}) {
				err = q3.Publish(serializedMovie)
				if err != nil {
					log.Fatalf("Failed to publish to queue 3: %v", err)
				}
				err = q4.Publish(serializedMovie)
				if err != nil {
					log.Fatalf("Failed to publish to queue 4: %v", err)
				}
			}

			err = q5.Publish(serializedMovie)
			if err != nil {
				log.Fatalf("Failed to publish to queue 5: %v", err)
			}

		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
