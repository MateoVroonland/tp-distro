package joiners

import (
	"encoding/csv"
	"log"
	"strings"
	"sync"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CreditsJoiner struct {
	conn             *amqp.Connection
	newClientQueue   *utils.ConsumerQueue
	moviesConsumers  map[string]*utils.ConsumerQueue
	creditsConsumers map[string]*utils.ConsumerQueue
	waitGroup        *sync.WaitGroup
	clientsLock      *sync.Mutex
}

func NewCreditsJoiner(conn *amqp.Connection, newClientQueue *utils.ConsumerQueue) *CreditsJoiner {
	return &CreditsJoiner{conn: conn, newClientQueue: newClientQueue, waitGroup: &sync.WaitGroup{}, clientsLock: &sync.Mutex{}, moviesConsumers: make(map[string]*utils.ConsumerQueue), creditsConsumers: make(map[string]*utils.ConsumerQueue)}
}

func (c *CreditsJoiner) JoinCredits(routingKey string) error {

	defer c.newClientQueue.CloseChannel()

	for msg := range c.newClientQueue.ConsumeInfinite() {
		var clientId string
		var ok bool
		if clientId, ok = msg.Headers["clientId"].(string); !ok {
			log.Printf("Failed to get clientId from message headers")
			msg.Nack(false, false)
			continue
		}

		log.Printf("Received new client %s", clientId)

		c.clientsLock.Lock()
		if _, ok := c.moviesConsumers[clientId]; !ok {
			moviesConsumer, err := utils.NewConsumerQueueWithRoutingKey(c.conn, "filter_q4_client_"+clientId, "filter_q4_client_"+clientId, routingKey, "internal_filter_q4_client_"+clientId)
			if err != nil {
				log.Printf("Failed to create movies consumer for client %s: %v", clientId, err)
				msg.Nack(false, false)
				c.clientsLock.Unlock()
				continue
			}
			c.moviesConsumers[clientId] = moviesConsumer
		}

		if _, ok := c.creditsConsumers[clientId]; !ok {
			creditsConsumer, err := utils.NewConsumerQueueWithRoutingKey(c.conn, "credits_joiner_client_"+clientId, "credits_joiner_client_"+clientId, routingKey, "internal_credits_joiner_client_"+clientId)
			if err != nil {
				log.Printf("Failed to create credits consumer for client %s: %v", clientId, err)
				msg.Nack(false, false)
				c.clientsLock.Unlock()
				delete(c.moviesConsumers, clientId)
				continue
			}
			c.creditsConsumers[clientId] = creditsConsumer
			c.waitGroup.Add(1)
			go c.JoinCreditsForClient(clientId)
		}

		c.clientsLock.Unlock()

	}

	c.waitGroup.Wait()
	return nil
}

func (c *CreditsJoiner) JoinCreditsForClient(clientId string) error {
	log.Printf("Joining credits for client %s", clientId)

	sinkProducer, err := utils.NewProducerQueue(c.conn, "sink_q4", "sink_q4")
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	defer sinkProducer.CloseChannel()
	defer c.waitGroup.Done()

	moviesConsumer := c.moviesConsumers[clientId]
	creditsConsumer := c.creditsConsumers[clientId]

	defer moviesConsumer.CloseChannel()
	defer creditsConsumer.CloseChannel()

	moviesIds := make(map[int]bool)

	i := 0
	for msg := range moviesConsumer.Consume() {
		stringLine := string(msg.Body)
		i++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Movie: %s", stringLine)
			msg.Nack(false, false)
			continue
		}

		var movie messages.CreditsJoinMovies
		err = movie.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize movie: %v", err)
			msg.Nack(false, false)
			continue
		}

		moviesIds[movie.ID] = true
		msg.Ack(false)
	}

	log.Printf("Received %d movies for client %s", i, clientId)

	var credits []messages.Credits

	j := 0
	creditsConsumer.AddFinishSubscriber(sinkProducer)
	for msg := range creditsConsumer.Consume() {

		stringLine := string(msg.Body)
		j++

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Credit: %s", stringLine)
			msg.Nack(false, false)
			continue
		}

		var credit messages.Credits
		err = credit.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize credits: %v", err)
			msg.Nack(false, false)
			continue
		}

		if !moviesIds[credit.MovieID] {
			msg.Ack(false)
			continue
		}

		credits = append(credits, credit)
		payload, err := protocol.Serialize(&credit)
		if err != nil {
			log.Printf("Failed to serialize credits: %v", record)
			log.Printf("json.Marshal: %v", err)
			msg.Nack(false, false)
			continue
		}
		sinkProducer.Publish(payload, clientId)

		msg.Ack(false)
	}

	log.Printf("Saved %d credits for client %s", len(credits), clientId)

	c.clientsLock.Lock()
	delete(c.moviesConsumers, clientId)
	delete(c.creditsConsumers, clientId)
	c.clientsLock.Unlock()

	return nil
}
