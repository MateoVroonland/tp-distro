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
	conn             *amqp.Connection
	newClientQueue   *utils.ConsumerFanout
	MoviesConsumers  map[string]*utils.ConsumerQueue
	CreditsConsumers map[string]*utils.ConsumerQueue
	waitGroup        *sync.WaitGroup
	clientsLock      *sync.RWMutex
}

func NewCreditsJoiner(conn *amqp.Connection, newClientQueue *utils.ConsumerFanout) *CreditsJoiner {
	return &CreditsJoiner{conn: conn, newClientQueue: newClientQueue, waitGroup: &sync.WaitGroup{}, clientsLock: &sync.RWMutex{}, MoviesConsumers: make(map[string]*utils.ConsumerQueue), CreditsConsumers: make(map[string]*utils.ConsumerQueue)}
}

func (c *CreditsJoiner) getCurrentClientsUnsafe() []string {

	clients := make([]string, 0, len(c.MoviesConsumers))
	for clientId := range c.MoviesConsumers {
		clients = append(clients, clientId)
	}

	for clientId := range c.CreditsConsumers {
		if _, ok := c.MoviesConsumers[clientId]; !ok {
			clients = append(clients, clientId)
		}
	}

	return clients

}

func (c *CreditsJoiner) GetCurrentClients() []string {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()

	return c.getCurrentClientsUnsafe()
}

func (c *CreditsJoiner) JoinCredits(routingKey int) error {

	defer c.newClientQueue.Close()

	currentClients := c.GetCurrentClients()

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
		if _, ok := c.MoviesConsumers[msg.ClientId]; !ok {
			moviesConsumer, err := utils.NewConsumerQueue(c.conn, "filter_q4_client_"+msg.ClientId, "filter_q4_client_"+msg.ClientId, env.AppEnv.MOVIES_RECEIVER_AMOUNT)
			if err != nil {
				log.Printf("Failed to create movies consumer for client %s: %v", msg.ClientId, err)
				msg.Nack(false)
				c.clientsLock.Unlock()
				continue
			}
			c.MoviesConsumers[msg.ClientId] = moviesConsumer
		}

		if _, ok := c.CreditsConsumers[msg.ClientId]; !ok {
			creditsConsumer, err := utils.NewConsumerQueue(c.conn, "credits_joiner_client_"+msg.ClientId, "credits_joiner_client_"+msg.ClientId, env.AppEnv.CREDITS_RECEIVER_AMOUNT)
			if err != nil {
				log.Printf("Failed to create credits consumer for client %s: %v", msg.ClientId, err)
				msg.Nack(false)
				c.clientsLock.Unlock()
				delete(c.MoviesConsumers, msg.ClientId)
				continue
			}
			c.CreditsConsumers[msg.ClientId] = creditsConsumer
			c.waitGroup.Add(1)
			clientCreditsJoiner, err := NewCreditsJoinerClient(c, msg.ClientId)
			if err != nil {
				log.Printf("Failed to create credits joiner client for client %s: %v", msg.ClientId, err)
				msg.Nack(false)
				c.clientsLock.Unlock()
				delete(c.MoviesConsumers, msg.ClientId)
				delete(c.CreditsConsumers, msg.ClientId)
				continue
			}

			currentClients := c.getCurrentClientsUnsafe()
			SaveCreditsJoinerState(currentClients)

			go clientCreditsJoiner.JoinCreditsForClient()
		}

		c.clientsLock.Unlock()

		msg.Ack()

	}

	c.waitGroup.Wait()
	return nil
}

type CreditsJoinerClient struct {
	ClientId        string
	MoviesConsumer  *utils.ConsumerQueue
	CreditsConsumer *utils.ConsumerQueue
	SinkProducer    *utils.ProducerQueue
	MoviesIds       map[int]bool
	creditsJoiner   *CreditsJoiner
}

func NewCreditsJoinerClient(creditsJoiner *CreditsJoiner, clientId string) (*CreditsJoinerClient, error) {
	sinkProducer, err := utils.NewProducerQueue(creditsJoiner.conn, "sink_q4", env.AppEnv.CREDITS_SINK_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	creditsJoiner.clientsLock.RLock()
	moviesConsumer := creditsJoiner.MoviesConsumers[clientId]
	creditsConsumer := creditsJoiner.CreditsConsumers[clientId]
	creditsJoiner.clientsLock.RUnlock()

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
	}
	if moviesConsumer != nil {
		if fileErr != nil {
			moviesConsumer.RestoreState(state.MoviesConsumer)
		}
	}

	return &CreditsJoinerClient{
		creditsJoiner:   creditsJoiner,
		ClientId:        clientId,
		MoviesConsumer:  moviesConsumer,
		CreditsConsumer: creditsConsumer,
		SinkProducer:    sinkProducer,
		MoviesIds:       moviesIds,
	}, nil
}

func (c *CreditsJoinerClient) JoinCreditsForClient() error {
	log.Printf("Joining credits for client %s", c.ClientId)

	defer c.SinkProducer.CloseChannel()
	defer c.creditsJoiner.waitGroup.Done()

	defer c.CreditsConsumer.CloseChannel()

	if c.MoviesConsumer != nil {
		c.fetchMovies()
	}

	c.creditsJoiner.clientsLock.Lock()
	delete(c.creditsJoiner.MoviesConsumers, c.ClientId)
	c.creditsJoiner.clientsLock.Unlock()

	log.Printf("Received %d movies for client %s", len(c.MoviesIds), c.ClientId)

	var credits []messages.Credits

	j := 0
	for msg := range c.CreditsConsumer.Consume() {

		stringLine := string(msg.Body)
		j++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Credit: %s", stringLine)
			msg.Nack(false)
			continue
		}

		var credit messages.Credits
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

		credits = append(credits, credit)
		payload, err := protocol.Serialize(&credit)
		if err != nil {
			log.Printf("Failed to serialize credits: %v", record)
			log.Printf("json.Marshal: %v", err)
			msg.Nack(false)
			continue
		}
		c.SinkProducer.Publish(payload, c.ClientId, "")

		// err = SaveCreditsJoinerPerClientState(moviesConsumer.GetState(), creditsConsumer.GetState(), moviesIds, clientId)
		if err != nil {
			log.Printf("Failed to save credits joiner state: %v", err)
			msg.Nack(false)
			continue
		}

		msg.Ack()
	}
	c.CreditsConsumer.DeleteQueue()
	log.Printf("Saved %d credits for client %s", len(credits), c.ClientId)
	c.SinkProducer.PublishFinished(c.ClientId)

	c.creditsJoiner.clientsLock.Lock()
	delete(c.creditsJoiner.CreditsConsumers, c.ClientId)
	c.creditsJoiner.clientsLock.Unlock()

	return nil
}

func (c *CreditsJoinerClient) fetchMovies() {
	for msg := range c.MoviesConsumer.Consume() {
		stringLine := string(msg.Body)

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

		// err = SaveCreditsJoinerPerClientState(moviesConsumer.GetState(), creditsConsumer.GetState(), moviesIds, clientId)
		if err != nil {
			log.Printf("Failed to save credits joiner state: %v", err)
			msg.Nack(false)
			continue
		}

		msg.Ack()
	}
	c.MoviesConsumer.DeleteQueue()
}
