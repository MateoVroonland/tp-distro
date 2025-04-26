package main

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	PORT = "8888"
)

type Server struct {
	results     map[string]messages.Results
	resultsLock sync.Mutex
	conn        *amqp.Connection
	wg          sync.WaitGroup
	connections map[string]net.Conn
	moviesQueue *utils.ProducerQueue
	creditsQueue *utils.ProducerQueue
	ratingsQueue *utils.ProducerQueue
}

func NewServer(conn *amqp.Connection) *Server {
	moviesQueue, err := utils.NewProducerQueue(conn, MOVIES.GetQueueName(), MOVIES.GetQueueName())
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err) // TODO: handle this
	}
	creditsQueue, err := utils.NewProducerQueue(conn, CREDITS.GetQueueName(), CREDITS.GetQueueName())
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err) // TODO: handle this
	}
	ratingsQueue, err := utils.NewProducerQueue(conn, RATINGS.GetQueueName(), RATINGS.GetQueueName())
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	
	return &Server{
		results:     make(map[string]messages.Results),
		resultsLock: sync.Mutex{},
		conn:        conn,
		wg:          sync.WaitGroup{},
		connections: make(map[string]net.Conn),
		moviesQueue: moviesQueue,
		creditsQueue: creditsQueue,
		ratingsQueue: ratingsQueue,
	}
}

type FileType string

const (
	RATINGS FileType = "ratings"
	CREDITS FileType = "credits"
	MOVIES  FileType = "movies"
)

func (f FileType) GetQueueName() string {
	return string(f)
}

func fileTypeFromMessage(message string) FileType {
	switch message {
	case "ratings":
		return RATINGS
	case "credits":
		return CREDITS
	case "movies":
		return MOVIES
	default:
		panic("Invalid file type: " + message) // TODO: handle this
	}
}

func (s *Server) Start() {
	defer s.conn.Close()	
	s.wg.Add(1)
	go s.startServer()
	s.wg.Wait()
}


// func (s *Server) listenForResults() {
// 	defer s.wg.Done()

// 	var results messages.Results
// 	resultsConsumer, err := utils.NewConsumerQueue(s.conn, "results", "results", "")
// 	if err != nil {
// 		log.Fatalf("Failed to declare a queue: %v", err)
// 	}

// 	queries := 1

// 	for d := range resultsConsumer.Consume() {
// 		err = json.Unmarshal(d.Body, &results)
// 		if err != nil {
// 			d.Nack(false, false)
// 			continue
// 		}
// 		queries--
// 		if queries == 0 {
// 			d.Ack(false)
// 			break
// 		}

// 		jsonQ1Bytes, err := json.Marshal(results.Query1)
// 		if err != nil {
// 			log.Printf("Error al convertir a JSON: %v\n", err)
// 			d.Nack(false, false)
// 			continue
// 		}
// 		log.Printf("Query 1: %s", string(jsonQ1Bytes))

// 		jsonQ2Bytes, err := json.Marshal(results.Query2)
// 		if err != nil {
// 			log.Printf("Error al convertir a JSON: %v\n", err)
// 			d.Nack(false, false)
// 			continue
// 		}
// 		log.Printf("Query 2: %s", string(jsonQ2Bytes))

// 		jsonQ3Bytes, err := json.Marshal(results.Query3)
// 		if err != nil {
// 			log.Printf("Error al convertir a JSON: %v\n", err)
// 			d.Nack(false, false)
// 			continue
// 		}
// 		log.Printf("Query 3: %s", string(jsonQ3Bytes))

// 		jsonQ4Bytes, err := json.Marshal(results.Query4)
// 		if err != nil {
// 			log.Printf("Error al convertir a JSON: %v\n", err)
// 			d.Nack(false, false)
// 			continue
// 		}
// 		log.Printf("Query 4: %s", string(jsonQ4Bytes))

// 		jsonQ5Bytes, err := json.Marshal(results.Query5)
// 		if err != nil {
// 			log.Printf("Error al convertir a JSON: %v\n", err)
// 			d.Nack(false, false)
// 			continue
// 		}
// 		log.Printf("Query 5: %s", string(jsonQ5Bytes))

// 		d.Ack(false)
// 	}

// 	s.resultsLock.Lock()
// 	s.results = &results
// 	s.resultsLock.Unlock()

// }

func (s *Server) startServer() {
	defer s.wg.Done()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", PORT))
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	log.Printf("Listening for connections")

	for {
		conn, err := listener.Accept()
		log.Printf("Accepted connection")

		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

			message, err := MessageFromSocket(&conn)
			if err != nil {
			log.Printf("Failed to read message: %v", err)
			continue
		}

		messageString := string(message)
		log.Printf("Message: %s", messageString)
		connId := utils.GenerateRandomID()
		log.Printf("Conn ID: %s", connId)

		if messageString != "INITIALIZE_CONNECTION" {
			panic("Invalid message: " + messageString)
		}

		s.connections[connId] = conn

		written, err := sendMessage(conn, []byte("ACK:" + connId)) 
		if err != nil {
			log.Printf("Failed to write ACK: %v", err)
		}
		log.Printf("Sent ACK: %d bytes", written)
		s.handleConnection(connId)
	}
}

func (s *Server) handleConnection(connId string) {
	// defer s.wg.Done()

	conn := s.connections[connId]
	if conn == nil {
		log.Printf("Connection not found")
		return
	}

	filesRemaining := 3

	for filesRemaining > 0 {
		message, err := MessageFromSocket(&conn)
		if err != nil {
			log.Printf("Failed to read message: %v", err)
			continue
		}

		messageString := string(message)
		
		if strings.HasPrefix(messageString, "FILE_BATCH:") {

			messageString = strings.TrimPrefix(messageString, "FILE_BATCH:")
			split := strings.Split(messageString, ":")
			if len(split) != 2 {
				log.Printf("Invalid message: %s", messageString)
				continue
			}
			filename := split[0]
			batch := split[1]
			fileType := fileTypeFromMessage(filename)
			s.publishBatch(fileType, batch)

		} else if strings.HasPrefix(messageString, "FINISHED_FILE:") {

			messageString = strings.TrimPrefix(messageString, "FINISHED_FILE:")
			filename := messageString
			fileType := fileTypeFromMessage(filename)
			queue := s.getQueue(fileType)
			queue.Publish([]byte("FINISHED"))

			filesRemaining--
		}

	}
}

func (s *Server) getQueue(filename FileType) *utils.ProducerQueue {
	switch filename {
	case RATINGS:
		return s.ratingsQueue
	case CREDITS:
		return s.creditsQueue
	case MOVIES:
		return s.moviesQueue
	default:
		panic("Invalid file type: " + filename) // TODO: handle this
	}
		
}
func (s *Server) publishBatch(filename FileType, batch string) {
	log.Printf("Publishing batch for %s", filename)
	queue := s.getQueue(filename)
	reader := csv.NewReader(strings.NewReader(batch))

	for {
		line, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read line: %v", err)
			continue
		}

		queue.Publish([]byte(utils.EncodeArrayToCsv(line)))
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

func sendMessage(conn net.Conn, message []byte) (int, error) {

		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, uint32(len(message)))

		messageToSend := append(lengthBytes, message...)
	
	written := 0

	for written < len(messageToSend) {
		written, err := conn.Write(messageToSend[written:])
		if err != nil {
			log.Printf("Failed to write message: %v", err)
			return written, err
		}
		written += written
	}
	return written, nil
}

// func (s *Server) respondResults() error {
// 	defer s.wg.Done()
// 	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", PORT))
// 	if err != nil {
// 		log.Fatalf("Failed to start server: %v", err)
// 	}
// 	defer listener.Close()

// 	for {
// 		log.Printf("Waiting for connection")
// 		conn, err := listener.Accept()
// 		log.Printf("Accepted connection")

// 		if err != nil {
// 			log.Printf("Failed to accept connection: %v", err)
// 			continue
// 		}

// 		s.resultsLock.Lock()
// 		if s.results == nil {
// 			conn.Write([]byte("NO_RESULTS"))
// 			s.resultsLock.Unlock()
// 			continue
// 		}

// 		resultsBytes, err := json.Marshal(s.results)
// 		if err != nil {
// 			log.Printf("Error al convertir a JSON: %v\n", err)
// 			s.resultsLock.Unlock()
// 			continue
// 		}

// 		lengthBytes := make([]byte, 4)
// 		binary.BigEndian.PutUint32(lengthBytes, uint32(len(resultsBytes)))

// 		messageToSend := append(lengthBytes, resultsBytes...)
// 		conn.Write(messageToSend)

// 		log.Printf("Sent results %s", resultsBytes)

// 		s.resultsLock.Unlock()
// 		return nil
// 	}

// }

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	server := NewServer(conn)
	server.Start()
}
