package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

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

	wg := sync.WaitGroup{}
	wg.Add(1)
	go publishFile("ratings", ch, &wg)
	go publishFile("credits", ch, &wg)
	go publishFile("movies_metadata", ch, &wg)

	wg.Wait()
}

func publishFile(filename string, ch *amqp.Channel, wg *sync.WaitGroup) error {
	log.Printf("Publishing file: %s", filename)
	defer wg.Done()

	file, err := os.Open(fmt.Sprintf("docs/%s.csv", filename))
	if err != nil {
		return err
	}
	defer file.Close()

	q, err := ch.QueueDeclare(
		filename,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	csvReader := csv.NewReader(file)
	csvReader.FieldsPerRecord = 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(strings.Join(record, ",")),
		})
		if err != nil {
			return err
		}
	}

	return nil
}
