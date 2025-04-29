package requesthandler

import (
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
	"time"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	PORT = "8888"
)

type Server struct {
	conn            *amqp.Connection
	results         *messages.Results
	resultsLock     sync.Mutex
	wg              sync.WaitGroup
	producers       map[string]*utils.ProducerQueue
	isListening     bool
	listener        net.Listener
	shuttingDown    bool
	shutdownChannel chan struct{}
}

func NewServer(conn *amqp.Connection) *Server {
	return &Server{
		conn:            conn,
		results:         nil,
		resultsLock:     sync.Mutex{},
		wg:              sync.WaitGroup{},
		producers:       make(map[string]*utils.ProducerQueue),
		isListening:     false,
		shuttingDown:    false,
		shutdownChannel: make(chan struct{}),
	}
}

func (s *Server) Start() {
	defer s.conn.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Printf("Received SIGTERM signal, closing connection")
		s.Shutdown()
	}()

	s.initializeProducers()
	time.Sleep(15 * time.Second)
	s.wg.Add(1)
	go s.listenForResults()

	if err := s.startTCPServer(); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}

	s.wg.Wait()
}

func (s *Server) initializeProducers() error {
	fileTypes := []string{"movies", "credits", "ratings"}
	for _, fileType := range fileTypes {
		producer, err := utils.NewProducerQueue(s.conn, fileType, fileType)
		if err != nil {
			return fmt.Errorf("Error creating producer for %s: %v", fileType, err)
		}
		s.producers[fileType] = producer
		log.Printf("Producer for %s initialized", fileType)
	}

	return nil
}

func (s *Server) startTCPServer() error {
	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%s", PORT))
	if err != nil {
		return fmt.Errorf("Failed to start TCP server: %v", err)
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
	defer conn.Close()
	defer s.wg.Done()
	ch := make(chan os.Signal, 1)

	signal.Notify(ch, syscall.SIGTERM)

	go func() {
		<-ch
		conn.Close()
	}()

	log.Printf("New client connected: %s", conn.RemoteAddr())

	message, err := utils.MessageFromSocket(&conn)
	if err != nil {
		log.Printf("Error reading message: %v", err)
		return
	}

	msgContent := string(message)

	switch {
	case msgContent == "WAITING_FOR_RESULTS":
		s.handleResultRequest(conn)
	case msgContent == "STARTING_FILE":
		s.handleDataStream(conn)
	}
}

func (s *Server) handleDataStream(conn net.Conn) {
	log.Println("Processing data stream...")

	filesRemaining := 3
	fileType := ""

	for !s.shuttingDown {
		switch filesRemaining {
		case 3:
			fileType = "movies"
		case 2:
			fileType = "credits"
		case 1:
			fileType = "ratings"
		}

		message, err := utils.MessageFromSocket(&conn)
		if err != nil {
			if err == io.EOF {
				log.Printf("End of stream reached for %s", fileType)
			} else {
				log.Printf("Error reading message: %v", err)
			}
			return
		}

		msgContent := string(message)

		if msgContent == "FINISHED_FILE" {
			log.Printf("Received FINISHED_FILE signal for %s", fileType)

			producer, exists := s.producers[fileType]
			if !exists {
				log.Printf("No producer found for %s", fileType)
				return
			}

			producer.Publish([]byte("FINISHED"))

			filesRemaining--

			continue
		}

		producer, exists := s.producers[fileType]
		if !exists {
			log.Printf("No producer found for %s", fileType)
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
			err = producer.Publish(buffer.Bytes())
			if err != nil {
				log.Printf("Error publishing line to %s: %v", fileType, err)
			}
		}
	}
}

func (s *Server) handleResultRequest(conn net.Conn) {
	log.Printf("Client requested results")

	s.resultsLock.Lock()
	defer s.resultsLock.Unlock()

	if !s.results.IsComplete() {
		utils.SendMessage(conn, []byte("NO_RESULTS"))
		return
	}

	resultsBytes, err := json.Marshal(s.results)
	if err != nil {
		log.Printf("Error marshalling results: %v", err)
		return
	}

	utils.SendMessage(conn, resultsBytes)
}

func (s *Server) listenForResults() {
	defer s.wg.Done()

	var results messages.Results
	resultsConsumer, err := utils.NewConsumerQueue(s.conn, "results", "results", "")
	if err != nil {
		log.Fatalf("Error creating consumer for results: %v", err)
	}

	for d := range resultsConsumer.Consume() {
		err = json.Unmarshal(d.Body, &results)
		if err != nil {
			log.Printf("Error unmarshalling results: %v", err)
			d.Nack(false, false)
			continue
		}

		s.logResults(&results)

		s.resultsLock.Lock()
		s.results = &results
		s.resultsLock.Unlock()

		d.Ack(false)
	}
}

func (s *Server) logResults(results *messages.Results) {
	if results == nil {
		return
	}

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
