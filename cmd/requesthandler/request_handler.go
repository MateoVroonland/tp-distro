package main

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
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
	q, err := ch.QueueDeclare(
		"ratings", 
		false,   
		false,  
		false,  
		false,  
		nil,     
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	
	file, err := os.Open("docs/ratings.csv")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()
	csvReader := csv.NewReader(file)
	csvReader.FieldsPerRecord = 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(strings.Join(record, ",")),
		})
		if err != nil {
			log.Fatalf("Failed to publish a message: %v", err)
		}
	}
	

}