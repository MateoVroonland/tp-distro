package requesthandler

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	PORT = "8888"
)

type Server struct {
	conn               *amqp.Connection
	wg                 sync.WaitGroup
	producers          map[string]*utils.ProducerQueue
	isListening        bool
	listener           net.Listener
	shuttingDown       bool
	shutdownChannel    chan struct{}
	clientsLock        sync.Mutex
	clients            map[string]bool
	clientsResultsChan map[string]chan messages.Results
	resultsChanLock    sync.RWMutex
	partialResults     map[string]messages.Results
	partialResultsLock sync.RWMutex
}

func NewServer(conn *amqp.Connection) *Server {
	return &Server{
		conn:               conn,
		wg:                 sync.WaitGroup{},
		producers:          make(map[string]*utils.ProducerQueue),
		isListening:        false,
		shuttingDown:       false,
		shutdownChannel:    make(chan struct{}),
		clientsLock:        sync.Mutex{},
		clients:            make(map[string]bool),
		clientsResultsChan: make(map[string]chan messages.Results),
		partialResults:     make(map[string]messages.Results),
		partialResultsLock: sync.RWMutex{},
	}
}

func (s *Server) Start() {
	defer s.conn.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		<-sigChan
		s.Shutdown()
	}()

	s.initializeProducers()
	s.wg.Add(1)
	go s.listenForResults()

	if err := s.startTCPServer(); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}

	s.wg.Wait()
}

type FileType struct {
	Name     string
	Replicas int
}

func (s *Server) initializeProducers() error {
	fileTypes := []FileType{
		{Name: "movies", Replicas: env.AppEnv.MOVIES_RECEIVER_AMOUNT},
		{Name: "credits", Replicas: env.AppEnv.CREDITS_RECEIVER_AMOUNT},
		{Name: "ratings", Replicas: env.AppEnv.RATINGS_RECEIVER_AMOUNT},
	}
	for _, fileType := range fileTypes {
		producer, err := utils.NewProducerQueue(s.conn, fileType.Name, fileType.Replicas)
		if err != nil {
			return fmt.Errorf("error creating producer for %s: %v", fileType.Name, err)
		}
		s.producers[fileType.Name] = producer
		log.Printf("Producer for %s initialized", fileType.Name)
	}

	return nil
}

func (s *Server) startTCPServer() error {
	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%s", PORT))
	if err != nil {
		return fmt.Errorf("failed to start TCP server: %v", err)
	}

	s.isListening = true
	log.Printf("Server listening on port %s", PORT)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.listener.Close()

		for !s.shuttingDown {
			conn, err := s.listener.Accept()

			if err != nil {
				if s.shuttingDown {
					return
				}
				log.Printf("Error accepting connection: %v", err)
				continue
			}

			s.wg.Add(1)
			go s.handleClientConnection(conn)
		}
	}()

	return nil
}

func (s *Server) handleClientConnection(conn net.Conn) {

	defer s.wg.Done()

	resultsSent := false

	var clientId string
	reader := bufio.NewReader(conn)

	for !resultsSent && !s.shuttingDown {

		message, err := utils.MessageFromSocket(reader)
		if err != nil {
			log.Printf("Error reading message: %v, reading message: %v", err, string(message))

			return
		}

		msgContent := string(message)

		switch {
		case msgContent == "CLIENT_ID_REQUEST":
			clientId = s.handleClientIDRequest(conn)
			log.Printf("New client connected: %s", clientId)
		case strings.Contains(msgContent, "WAITING_FOR_RESULTS:"):
			clientId = strings.Split(msgContent, ":")[1]
			resultsSent = s.handleResultRequest(conn, clientId)
			if resultsSent {
				log.Printf("Results sent to client: %s", clientId)
			}
		case msgContent == "STARTING_FILE":
			s.handleDataStream(conn, clientId)
		}
	}
	log.Printf("Client %s disconnected", clientId)
}

func (s *Server) handleClientIDRequest(conn net.Conn) string {
	clientID := utils.GenerateRandomID()
	s.clientsLock.Lock()
	s.clients[clientID] = true
	s.clientsLock.Unlock()
	utils.SendMessage(conn, []byte(clientID))
	return clientID
}

func (s *Server) handleDataStream(conn net.Conn, clientId string) {
	log.Println("Processing data stream...")

	filesRemaining := 3
	fileType := ""
	var index int

	reader := bufio.NewReader(conn)
	for filesRemaining > 0 && !s.shuttingDown {
		switch filesRemaining {
		case 3:
			fileType = "movies"
			index = messages.RawMovieID
		case 2:
			fileType = "credits"
			index = messages.RawCreditsMovieIDIndex
		case 1:
			fileType = "ratings"
			index = messages.RawRatingMovieIDIndex
		}

		message, err := utils.MessageFromSocket(reader)
		if err != nil {
			if err == io.EOF {
				log.Printf("End of stream reached for %s", fileType)
			} else {
				log.Printf("Error reading message: %v", err)
				log.Printf("Message: %v", string(message))
			}
			return
		}

		msgContent := string(message)

		if msgContent == "FINISHED_FILE" {
			log.Printf("Received FINISHED_FILE signal %s, clientId: %s", fileType, clientId)

			producer, exists := s.producers[fileType]
			if !exists {
				log.Printf("No producer found for %s clientId: %s", fileType, clientId)
				return
			}

			producer.PublishFinished(clientId)

			filesRemaining--

			continue
		}

		producer, exists := s.producers[fileType]
		if !exists {
			log.Printf("No producer found for %s clientId: %s", fileType, clientId)
			return
		}
		reader := csv.NewReader(strings.NewReader(msgContent))
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading CSV line: %v", err)
				log.Printf("Record: %v", record)
				continue
			}

			buffer := &bytes.Buffer{}
			writer := csv.NewWriter(buffer)
			err = writer.Write(record)
			if err != nil {
				log.Printf("Error writing CSV line: %v", err)
				continue
			}
			writer.Flush()
			err = producer.Publish(buffer.Bytes(), clientId, record[index])
			if err != nil {
				log.Printf("Error publishing line to %s: %v", fileType, err)
			}
		}
	}
}

func (s *Server) handleResultRequest(conn net.Conn, clientId string) bool {
	log.Printf("Client requested results, clientId: %s", clientId)

	s.resultsChanLock.RLock()
	channel, exists := s.clientsResultsChan[clientId]
	s.resultsChanLock.RUnlock()

	if !exists {
		s.resultsChanLock.Lock()
		s.clientsResultsChan[clientId] = make(chan messages.Results)
		channel = s.clientsResultsChan[clientId]
		s.resultsChanLock.Unlock()
	}

	select {
	case clientResults := <-channel:
		if !clientResults.IsComplete() {
			utils.SendMessage(conn, []byte("NO_RESULTS"))
			return false
		}

		resultsBytes, err := json.Marshal(clientResults)
		if err != nil {
			log.Printf("Error marshalling results: %v", err)
			return true
		}

		utils.SendMessage(conn, resultsBytes)
		return true
	case <-s.shutdownChannel:
		log.Printf("Shutdown in progress, aborting result wait for client %s", clientId)
		utils.SendMessage(conn, []byte("SERVER_SHUTDOWN"))
		return true
	}
}

func (s *Server) listenForResults() {
	defer s.wg.Done()

	resultsConsumer, err := utils.NewConsumerQueue(s.conn, "results", "results", 1)
	if err != nil {
		log.Fatalf("Error creating consumer for results: %v", err)
	}

	for d := range resultsConsumer.ConsumeInfinite() {
		log.Println("Received results from client:", d.ClientId)

		s.partialResultsLock.RLock()
		clientResults := s.partialResults[d.ClientId]
		s.partialResultsLock.RUnlock()

		err = json.Unmarshal([]byte(d.Body), &clientResults)
		if err != nil {
			log.Printf("Error unmarshalling results: %v", err)
			d.Nack(false)
			continue
		}

		s.logResults(clientResults)

		if clientResults.IsComplete() {
			s.resultsChanLock.Lock()
			channel, exists := s.clientsResultsChan[d.ClientId]
			if !exists {
				channel = make(chan messages.Results)
				s.clientsResultsChan[d.ClientId] = channel
			}
			s.resultsChanLock.Unlock()

			channel <- clientResults

			s.partialResultsLock.Lock()
			delete(s.partialResults, d.ClientId)
			s.partialResultsLock.Unlock()
		} else {
			s.partialResultsLock.Lock()
			s.partialResults[d.ClientId] = clientResults
			s.partialResultsLock.Unlock()
		}

		d.Ack()
	}
}

func (s *Server) logResults(results messages.Results) {

	if jsonBytes, err := json.Marshal(results.Query1); err == nil {
		log.Printf("Query 1: %s", string(jsonBytes))
	}

	if jsonBytes, err := json.Marshal(results.Query2); err == nil {
		log.Printf("Query 2: %s", string(jsonBytes))
	}

	if jsonBytes, err := json.Marshal(results.Query3); err == nil {
		log.Printf("Query 3: %s", string(jsonBytes))
	}

	if jsonBytes, err := json.Marshal(results.Query4); err == nil {
		log.Printf("Query 4: %s", string(jsonBytes))
	}

	if jsonBytes, err := json.Marshal(results.Query5); err == nil {
		log.Printf("Query 5: %s", string(jsonBytes))
	}
}

func (s *Server) Shutdown() {
	s.shuttingDown = true
	close(s.shutdownChannel)

	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}
