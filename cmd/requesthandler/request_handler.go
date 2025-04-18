package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	// go publishFile("ratings", ch, &wg)
	// go publishFile("credits", ch, &wg)
	go publishFile("movies_metadata", conn, &wg)

	wg.Wait()
}

func publishFile(filename string, conn *amqp.Connection, wg *sync.WaitGroup) error {
	defer wg.Done()

	log.Printf("Publishing file: %s", filename)

	file, err := os.Open(fmt.Sprintf("docs/%s.csv", filename))
	if err != nil {
		return err
	}
	defer file.Close()

	q, err := utils.NewQueue(conn, filename, false, false, false, false, nil)
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
