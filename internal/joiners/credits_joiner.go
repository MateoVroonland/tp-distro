package joiners

import (
	"encoding/csv"
	"encoding/gob"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/MateoVroonland/tp-distro/internal/env"
	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CreditsJoiner struct {
	conn           *amqp.Connection
	newClientQueue *utils.ConsumerFanout
	clientsJoiners map[string]bool
	waitGroup      *sync.WaitGroup
	clientsLock    *sync.RWMutex
}

func NewCreditsJoiner(conn *amqp.Connection, newClientQueue *utils.ConsumerFanout) *CreditsJoiner {
	return &CreditsJoiner{conn: conn, newClientQueue: newClientQueue, waitGroup: &sync.WaitGroup{}, clientsLock: &sync.RWMutex{}, clientsJoiners: make(map[string]bool)}
}

func (c *CreditsJoiner) GetCurrentClients() []string {
	c.clientsLock.RLock()
	defer c.clientsLock.RUnlock()

	clients := make([]string, 0, len(c.clientsJoiners))
	for clientId := range c.clientsJoiners {
		clients = append(clients, clientId)
	}

	return clients
}

func (c *CreditsJoiner) removeClient(clientId string) {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()

	delete(c.clientsJoiners, clientId)
}

func (c *CreditsJoiner) JoinCredits(routingKey int) error {

	stateFile, err := os.Open("data/credits_joiner_state.gob")

	var currentClients []string
	if os.IsNotExist(err) {
		log.Printf("State file does not exist, starting from scratch")
		currentClients = make([]string, 0)

	} else if err != nil {
		log.Printf("Failed to open state file: %v", err)
		return err
	} else {
		var state CreditsJoinerState
		dec := gob.NewDecoder(stateFile)
		err = dec.Decode(&state)
		if err != nil {
			log.Printf("Failed to decode state file: %v", err)
			return err
		}

		currentClients = state.CurrentClients
		defer stateFile.Close()

	}

	defer c.newClientQueue.Close()

	for _, clientId := range currentClients {
		clientCreditsJoiner, err := NewCreditsJoinerClient(c, clientId)
		if err != nil {
			log.Printf("Failed to create credits joiner client for client %s: %v", clientId, err)
			continue
		}
		c.waitGroup.Add(1)
		go clientCreditsJoiner.JoinCreditsForClient()
	}

	for msg := range c.newClientQueue.Consume() {

		log.Printf("Received new client %s", msg.ClientId)

		c.clientsLock.Lock()
		if _, ok := c.clientsJoiners[msg.ClientId]; !ok {

			clientCreditsJoiner, err := NewCreditsJoinerClient(c, msg.ClientId)
			if err != nil {
				log.Printf("Failed to create credits joiner client for client %s: %v", msg.ClientId, err)
				msg.Nack(false)
				c.clientsLock.Unlock()
				continue
			}

			c.clientsJoiners[msg.ClientId] = true

			c.waitGroup.Add(1)
			go clientCreditsJoiner.JoinCreditsForClient()

			SaveCreditsJoinerState(c)
		}

		c.clientsLock.Unlock()

		msg.Ack()

	}

	c.waitGroup.Wait()
	return nil
}

type CreditsJoinerClient struct {
	ClientId               string
	MoviesConsumer         *utils.ConsumerQueue
	CreditsConsumer        *utils.ConsumerQueue
	SinkProducer           *utils.ProducerQueue
	MoviesIds              map[int]bool
	creditsJoiner          *CreditsJoiner
	FinishedFetchingMovies bool
}

func NewCreditsJoinerClient(creditsJoiner *CreditsJoiner, clientId string) (*CreditsJoinerClient, error) {
	finishedFetchingMovies := false
	moviesConsumer, err := utils.NewConsumerQueue(creditsJoiner.conn, "filter_q4_client_"+clientId, "filter_q4_client_"+clientId, env.AppEnv.MOVIES_RECEIVER_AMOUNT)
	if err != nil {
		log.Printf("Failed to create movies consumer for client %s: %v", clientId, err)
		return nil, err
	}

	creditsConsumer, err := utils.NewConsumerQueue(creditsJoiner.conn, "credits_joiner_client_"+clientId, "credits_joiner_client_"+clientId, env.AppEnv.CREDITS_RECEIVER_AMOUNT)
	if err != nil {
		log.Printf("Failed to create credits consumer for client %s: %v", clientId, err)
		return nil, err
	}

	sinkProducer, err := utils.NewProducerQueue(creditsJoiner.conn, "sink_q4", env.AppEnv.CREDITS_SINK_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	stateFile, fileErr := os.Open("data/credits_joiner_state_" + clientId + ".gob")
	var state CreditsJoinerClientState
	var moviesIds map[int]bool

	if os.IsNotExist(fileErr) {
		log.Printf("State file of credits joiner for client %s does not exist, starting from scratch", clientId)
		moviesIds = make(map[int]bool)
	} else if fileErr != nil {
		log.Printf("Failed to open state file of credits joiner for client %s: %v", clientId, fileErr)
		if moviesConsumer != nil {
			moviesConsumer.CloseChannel()
		}
		return nil, fileErr
	} else {
		defer stateFile.Close()
		dec := gob.NewDecoder(stateFile)
		err := dec.Decode(&state)
		if err != nil {
			log.Printf("Failed to decode state file of credits joiner for client %s: %v", clientId, err)
			if moviesConsumer != nil {
				moviesConsumer.CloseChannel()
			}
			return nil, err
		}

		defer stateFile.Close()

		sinkProducer.RestoreState(state.SinkProducer)
		creditsConsumer.RestoreState(state.CreditsConsumer)
		moviesIds = state.MoviesIds
		finishedFetchingMovies = state.FinishedFetchingMovies
	}
	if moviesConsumer != nil {
		if fileErr != nil {
			moviesConsumer.RestoreState(state.MoviesConsumer)
		}
	}

	return &CreditsJoinerClient{
		creditsJoiner:          creditsJoiner,
		ClientId:               clientId,
		MoviesConsumer:         moviesConsumer,
		CreditsConsumer:        creditsConsumer,
		SinkProducer:           sinkProducer,
		MoviesIds:              moviesIds,
		FinishedFetchingMovies: finishedFetchingMovies,
	}, nil
}

func (c *CreditsJoinerClient) JoinCreditsForClient() error {
	log.Printf("Joining credits for client %s", c.ClientId)

	defer c.SinkProducer.CloseChannel()
	defer c.creditsJoiner.waitGroup.Done()

	defer c.CreditsConsumer.CloseChannel()

	if !c.FinishedFetchingMovies {
		c.fetchMovies()
	}

	log.Printf("Received %d movies for client %s", len(c.MoviesIds), c.ClientId)

	c.fetchCredits()

	return nil
}

func (c *CreditsJoinerClient) fetchMovies() {
	for msg := range c.MoviesConsumer.ConsumeInfinite() {
		stringLine := string(msg.Body)

		if stringLine == "FINISHED" {
			c.FinishedFetchingMovies = true
			err := SaveCreditsJoinerPerClientState(c)
			if err != nil {
				log.Printf("Failed to save credits joiner state: %v", err)
			}
			msg.Ack()
			c.MoviesConsumer.DeleteQueue() // TODO: implement garbage collection
			break
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Movie: %s", stringLine)
			msg.Nack(false)
			continue
		}

		var movie messages.CreditsJoinMovies
		err = movie.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false)
			continue
		}

		c.MoviesIds[movie.ID] = true

		err = SaveCreditsJoinerPerClientState(c)
		if err != nil {
			log.Printf("Failed to save credits joiner state: %v", err)
			msg.Nack(false)
			continue
		}

		msg.Ack()
	}

}

func (c *CreditsJoinerClient) fetchCredits() {

	for msg := range c.CreditsConsumer.ConsumeInfinite() {

		stringLine := string(msg.Body)

		if stringLine == "FINISHED" {
			c.creditsJoiner.removeClient(c.ClientId)
			c.SinkProducer.PublishFinished(c.ClientId)

			err := SaveCreditsJoinerPerClientState(c)
			if err != nil {
				log.Printf("Failed to save credits joiner state: %v", err)
			}
			msg.Ack()
			c.CreditsConsumer.DeleteQueue()

			break
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Credit: %s", stringLine)
			msg.Nack(false)
			continue
		}

		var credit messages.Credit
		err = credit.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize credits: %v", err)
			msg.Nack(false)
			continue
		}

		if !c.MoviesIds[credit.MovieID] {
			msg.Ack()
			continue
		}

		payload, err := protocol.Serialize(&credit)
		if err != nil {
			log.Printf("Failed to serialize credits: %v", record)
			log.Printf("json.Marshal: %v", err)
			msg.Nack(false)
			continue
		}
		c.SinkProducer.Publish(payload, c.ClientId, "")

		err = SaveCreditsJoinerPerClientState(c)
		if err != nil {
			log.Printf("Failed to save credits joiner state: %v", err)
			msg.Nack(false)
			continue
		}

		msg.Ack()
	}
}
