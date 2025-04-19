package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

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

	wg := sync.WaitGroup{}
	wg.Add(2)
	// go publishFile("ratings", ch, &wg)
	// go publishFile("credits", ch, &wg)
	go publishFile("movies_metadata", ch, &wg)

	go func() {
		var results messages.Results
		resultsConsumer, err := utils.NewQueue(ch, "results", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("Failed to declare a queue: %v", err)
		}

		msgs, err := resultsConsumer.Consume()
		if err != nil {
			log.Fatalf("Failed to register a consumer: %v", err)
		}
		queries := 5

		for d := range msgs {
			err = json.Unmarshal(d.Body, &results)
			if err != nil {
				log.Printf("Failed to unmarshal results: %v", err)
				continue
			}
			queries--
			if queries == 0 {
				break
			}

			jsonBytes, err := json.Marshal(results.Query1)
			if err != nil {
				fmt.Printf("Error al convertir a JSON: %v\n", err)
				return
			}
			fmt.Println(string(jsonBytes))
		}

		
	}()

	wg.Wait()
}

func publishFile(filename string, ch *amqp.Channel, wg *sync.WaitGroup) error {
	defer wg.Done()

	log.Printf("Publishing file: %s", filename)

	file, err := os.Open(fmt.Sprintf("docs/%s.csv", filename))
	if err != nil {
		return err
	}
	defer file.Close()

	q, err := utils.NewQueue(ch, filename, false, false, false, false, nil)
	if err != nil {
		return err
	}

	lineReader := bufio.NewReader(file)
	lineReader.ReadString('\n')
	for {
		line, err := lineReader.ReadString('\n')
		if err == io.EOF {
			q.Publish([]byte("FINISHED"))
			break
		} else if err != nil {
			return err
		}

		err = q.Publish([]byte(line))
		if err != nil {
			return err
		}
	}

	return nil
}
