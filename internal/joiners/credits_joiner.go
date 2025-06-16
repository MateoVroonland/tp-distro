package joiners

import (
	"encoding/csv"
	"log"
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
	clientsLock      *sync.Mutex
}

func NewCreditsJoiner(conn *amqp.Connection, newClientQueue *utils.ConsumerFanout) *CreditsJoiner {
	return &CreditsJoiner{conn: conn, newClientQueue: newClientQueue, waitGroup: &sync.WaitGroup{}, clientsLock: &sync.Mutex{}, MoviesConsumers: make(map[string]*utils.ConsumerQueue), CreditsConsumers: make(map[string]*utils.ConsumerQueue)}
}

func (c *CreditsJoiner) GetCurrentClients() ([]string, error) {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()

	clients := make([]string, 0, len(c.MoviesConsumers))
	for clientId := range c.MoviesConsumers {
		clients = append(clients, clientId)
	}

	for clientId := range c.CreditsConsumers {
		if _, ok := c.MoviesConsumers[clientId]; !ok {
			clients = append(clients, clientId)
		}
	}

	return clients, nil
}

func (c *CreditsJoiner) JoinCredits(routingKey int) error {

	defer c.newClientQueue.Close()

	currentClients, err := c.GetCurrentClients()

	if err != nil {
		return err
	}

	for _, clientId := range currentClients {
		c.waitGroup.Add(1)
		go c.JoinCreditsForClient(clientId)
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
			go c.JoinCreditsForClient(msg.ClientId)
		}

		c.clientsLock.Unlock()

		msg.Ack()

	}

	c.waitGroup.Wait()
	return nil
}

func (c *CreditsJoiner) JoinCreditsForClient(clientId string) error {
	log.Printf("Joining credits for client %s", clientId)

	sinkProducer, err := utils.NewProducerQueue(c.conn, "sink_q4", env.AppEnv.CREDITS_SINK_AMOUNT)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	defer sinkProducer.CloseChannel()
	defer c.waitGroup.Done()

	moviesConsumer := c.MoviesConsumers[clientId]
	creditsConsumer := c.CreditsConsumers[clientId]

	defer creditsConsumer.CloseChannel()

	moviesIds := make(map[int]bool)

	i := 0
	if moviesConsumer != nil {
		c.fetchMovies(moviesConsumer, moviesIds, &i)
		defer moviesConsumer.CloseChannel()
	}

	c.clientsLock.Lock()
	delete(c.MoviesConsumers, clientId)
	c.clientsLock.Unlock()

	log.Printf("Received %d movies for client %s", i, clientId)

	var credits []messages.Credits

	j := 0
	for msg := range creditsConsumer.Consume() {

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

		if !moviesIds[credit.MovieID] {
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
		sinkProducer.Publish(payload, clientId, "")

		err = SaveCreditsJoinerState(c)
		if err != nil {
			log.Printf("Failed to save credits joiner state: %v", err)
			msg.Nack(false)
			continue
		}

		msg.Ack()
	}
	creditsConsumer.DeleteQueue()
	log.Printf("Saved %d credits for client %s", len(credits), clientId)
	sinkProducer.PublishFinished(clientId)

	c.clientsLock.Lock()
	delete(c.CreditsConsumers, clientId)
	c.clientsLock.Unlock()

	return nil
}

func (c *CreditsJoiner) fetchMovies(moviesConsumer *utils.ConsumerQueue, moviesIds map[int]bool, i *int) {
	for msg := range moviesConsumer.Consume() {
		stringLine := string(msg.Body)
		*i++

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

		moviesIds[movie.ID] = true

		err = SaveCreditsJoinerState(c)
		if err != nil {
			log.Printf("Failed to save credits joiner state: %v", err)
			msg.Nack(false)
			continue
		}

		msg.Ack()
	}
	moviesConsumer.DeleteQueue()
}
