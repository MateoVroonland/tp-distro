package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	PORT = "8888"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	startServer()
	time.Sleep(15 * time.Second)

	log.Printf("Distributing files")

	wg := sync.WaitGroup{}
	wg.Add(4)
	go publishFile("ratings", conn, &wg)
	go publishFile("credits", conn, &wg)
	go publishFile("movies", conn, &wg)

	go listenForResults(conn, &wg)

	wg.Wait()
}

func publishFile(filename string, conn *amqp.Connection, wg *sync.WaitGroup) error {
	defer wg.Done()

	log.Printf("Publishing file: %s", filename)

	file, err := os.Open(fmt.Sprintf("/out/%s.csv", filename))
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

		jsonQ5Bytes, err := json.Marshal(results.Query5)
		if err != nil {
			log.Printf("Error al convertir a JSON: %v\n", err)
			d.Nack(false, false)
			continue
		}
		log.Printf("Query 5: %s", string(jsonQ5Bytes))
		d.Ack(false)
	}

}

func startServer() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", PORT))
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	filesRemaining := 3

	movies := ""
	credits := ""
	ratings := ""

	for filesRemaining > 0 {

		var file string

		switch filesRemaining {
		case 3:
			file = "movies"
		case 2:
			file = "credits"
		case 1:
			file = "ratings"
		}
		log.Printf("Waiting for %s", file)

		conn, err := listener.Accept()
		log.Printf("Accepted connection")
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		message, err := handleConnection(conn)
		if err != nil {
			log.Printf("Failed to handle connection: %v", err)
			continue
		}

		switch file {
		case "movies":
			movies += string(message)
		case "credits":
			credits += string(message)
		case "ratings":
			ratings += string(message)
		}

		filesRemaining--
	}

	outMoviesFile, err := os.Create("/out/movies.csv")
	if err != nil {
		log.Printf("Failed to create out/movies.csv: %v", err)
		return
	}
	defer outMoviesFile.Close()

	outCreditsFile, err := os.Create("/out/credits.csv")
	if err != nil {
		log.Printf("Failed to create out/credits.csv: %v", err)
		return
	}
	defer outCreditsFile.Close()

	outRatingsFile, err := os.Create("/out/ratings.csv")
	if err != nil {
		log.Printf("Failed to create out/ratings.csv: %v", err)
		return
	}
	defer outRatingsFile.Close()

	outMoviesFile.WriteString(movies)
	outCreditsFile.WriteString(credits)
	outRatingsFile.WriteString(ratings)

	log.Printf("Finished writing files")
}

func handleConnection(conn net.Conn) (string, error) {
	defer conn.Close()

	buffer := ""

	i := 0
	for {
		i++
		message, err := MessageFromSocket(&conn)
		if err != nil {
			log.Printf("Failed to read message: %v", err)
			return "", err
		}

		if string(message) == "FINISHED_FILE" {
			log.Printf("Finished file")
			return buffer, nil
		}

		if i%100_000 == 0 {
			log.Printf("Processed %d messages", i)
		}

		buffer += string(message)
	}

}

func MessageFromSocket(socket *net.Conn) ([]byte, error) {
	reader := bufio.NewReader(*socket)
	u8Buffer := make([]byte, 4)
	_, err := io.ReadFull(reader, u8Buffer)
	if err != nil {
		log.Printf("Failed to read message length: %v", err)
		log.Printf("String: %s", string(u8Buffer))
		log.Printf("Bytes: %v", u8Buffer)
		return nil, err
	}

	messageLength := binary.BigEndian.Uint32(u8Buffer)
	payload := make([]byte, messageLength)
	_, err = io.ReadFull(reader, payload)
	if err != nil {
		log.Printf("Failed to read message: %v", err)
		return nil, err
	}

	return payload, nil
}
