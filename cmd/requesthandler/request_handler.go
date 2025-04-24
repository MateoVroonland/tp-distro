package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

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

	time.Sleep(5 * time.Second)

	wg := sync.WaitGroup{}
	wg.Add(4)
	go publishFile("ratings", conn, &wg)
	go publishFile("credits", conn, &wg)
	go publishFile("movies_metadata", conn, &wg)

	go listenForResults(conn, &wg)

	wg.Wait()
}

func publishFile(filename string, conn *amqp.Connection, wg *sync.WaitGroup) error {
	defer wg.Done()

	log.Printf("Publishing file: %s", filename)

	file, err := os.Open(fmt.Sprintf("docs/%s.csv", filename))
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
		return err
	}
	defer file.Close()

	q, err := utils.NewProducerQueue(conn, filename, filename)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
		return err
	}

	lineReader := bufio.NewReader(file)
	lineReader.ReadString('\n')
	i := 0
	j := 0
	for {
		i++
		line, err := lineReader.ReadString('\n')
		if err == io.EOF {
			q.Publish([]byte("FINISHED"))
			break
		} else if err != nil {
			return err
		}

		err = q.Publish([]byte(line))
		if err != nil {
			log.Printf("Failed to publish line: %v", err)
			continue
		}
		j++

	}

	log.Printf("Processed lines: %d", i)
	log.Printf("Published lines: %d", j)

	return nil
}

func listenForResults(conn *amqp.Connection, wg *sync.WaitGroup) {
	defer wg.Done()
	var results messages.Results
	resultsConsumer, err := utils.NewConsumerQueue(conn, "results", "results", "")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	queries := 5

	for d := range resultsConsumer.Consume() {
		err = json.Unmarshal(d.Body, &results)
		if err != nil {
			// log.Printf("Failed to unmarshal results: %v", err)
			d.Nack(false, false)
			continue
		}
		queries--
		if queries == 0 {
			d.Ack(false)
			break
		}

		jsonQ1Bytes, err := json.Marshal(results.Query1)
		if err != nil {
			log.Printf("Error al convertir a JSON: %v\n", err)
			d.Nack(false, false)
			continue
		}
		log.Printf("Query 1: %s", string(jsonQ1Bytes))

		jsonQ2Bytes, err := json.Marshal(results.Query2)
		if err != nil {
			log.Printf("Error al convertir a JSON: %v\n", err)
			d.Nack(false, false)
			continue
		}
		log.Printf("Query 2: %s", string(jsonQ2Bytes))

		jsonQ3Bytes, err := json.Marshal(results.Query3)
		if err != nil {
			log.Printf("Error al convertir a JSON: %v\n", err)
			d.Nack(false, false)
			continue
		}
		log.Printf("Query 3: %s", string(jsonQ3Bytes))

		jsonQ4Bytes, err := json.Marshal(results.Query4)
		if err != nil {
			log.Printf("Error al convertir a JSON: %v\n", err)
			d.Nack(false, false)
			continue
		}
		log.Printf("Query 4: %s", string(jsonQ4Bytes))
		d.Ack(false)
	}

}
