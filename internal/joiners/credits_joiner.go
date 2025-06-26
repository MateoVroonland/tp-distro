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
	ClientsJoiners map[string]bool
	waitGroup      *sync.WaitGroup
	clientsLock    *sync.RWMutex
}

func NewCreditsJoiner(conn *amqp.Connection, newClientQueue *utils.ConsumerFanout) *CreditsJoiner {
	return &CreditsJoiner{conn: conn, newClientQueue: newClientQueue, waitGroup: &sync.WaitGroup{}, clientsLock: &sync.RWMutex{}, ClientsJoiners: make(map[string]bool)}
}

func (c *CreditsJoiner) removeClient(clientId string) {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()

	delete(c.ClientsJoiners, clientId)
	err := SaveCreditsJoinerState(c)
	if err != nil {
		log.Printf("Failed to save credits joiner state: %v", err)
	}
}

func (c *CreditsJoiner) JoinCredits(routingKey int) error {

	defer c.newClientQueue.Close()

	for clientId := range c.ClientsJoiners {
		clientCreditsJoiner, err := NewCreditsJoinerClient(c, clientId)
		if err != nil {
			log.Printf("Failed to create credits joiner client for client %s: %v", clientId, err)
			continue
		}
		c.waitGroup.Add(1)
		log.Printf("Restored joiner for client %s", clientId)
		go clientCreditsJoiner.JoinCreditsForClient()
	}

	for msg := range c.newClientQueue.Consume() {

		log.Printf("Received new client %s", msg.ClientId)

		c.clientsLock.Lock()
		if _, ok := c.ClientsJoiners[msg.ClientId]; !ok {
			log.Printf("Creating credits joiner client for client %s", msg.ClientId)
			clientCreditsJoiner, err := NewCreditsJoinerClient(c, msg.ClientId)
			if err != nil {
				log.Printf("Failed to create credits joiner client for client %s: %v", msg.ClientId, err)
				msg.Nack(false)
				c.clientsLock.Unlock()
				continue
			}

			c.ClientsJoiners[msg.ClientId] = true

			c.waitGroup.Add(1)
			go clientCreditsJoiner.JoinCreditsForClient()

			err = SaveCreditsJoinerState(c)
			if err != nil {
				log.Printf("Failed to save credits joiner state: %v", err)
			} else {
				log.Printf("Saved credits joiner state for client %s", msg.ClientId)
			}

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
		return nil, fileErr
	} else {
		defer stateFile.Close()
		dec := gob.NewDecoder(stateFile)
		err := dec.Decode(&state)
		if err != nil {
			log.Printf("Failed to decode state file of credits joiner for client %s: %v", clientId, err)
			return nil, err
		}

		defer stateFile.Close()

		log.Printf("Restored state: %v", state)

		sinkProducer.RestoreState(state.SinkProducer)
		creditsConsumer.RestoreState(state.CreditsConsumer)
		moviesIds = state.MoviesIds
		finishedFetchingMovies = state.FinishedFetchingMovies
		moviesConsumer.RestoreState(state.MoviesConsumer)

		if finishedFetchingMovies {
			log.Printf("Finished fetching movies for client %s upon restart", clientId)
			moviesConsumer.DeleteQueue()
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
	stateSaver := NewCreditsJoinerPerClientState()

	for msg := range c.MoviesConsumer.ConsumeInfinite() {
		stringLine := string(msg.Body)

		if msg.IsFinished {
			if !msg.IsLastFinished {
				err := stateSaver.SaveStateAck(&msg, c)
				if err != nil {
					log.Printf("Failed to save credits joiner state: %v", err)
				}
				continue
			}
			c.FinishedFetchingMovies = true
			err := stateSaver.SaveStateAck(&msg, c)
			if err != nil {
				log.Printf("Failed to save credits joiner state: %v", err)
			}
			stateSaver.ForceFlush()

			c.MoviesConsumer.DeleteQueue()
			break
		}

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Movie: %s", stringLine)
			stateSaver.SaveStateNack(&msg, c, false)
			continue
		}

		var movie messages.CreditsJoinMovies
		err = movie.Deserialize(record)
		if err != nil {
			stateSaver.SaveStateNack(&msg, c, false)
			continue
		}

		c.MoviesIds[movie.ID] = true

		err = SaveCreditsJoinerPerClientState(c)
		if err != nil {
			log.Printf("Failed to save credits joiner state: %v", err)
			stateSaver.SaveStateNack(&msg, c, false)
			continue
		}

	}

}

func (c *CreditsJoinerClient) fetchCredits() {
	stateSaver := NewCreditsJoinerPerClientState()

	for msg := range c.CreditsConsumer.ConsumeInfinite() {

		if msg.IsFinished {

			if !msg.IsLastFinished {
				err := stateSaver.SaveStateAck(&msg, c)
				if err != nil {
					log.Printf("Failed to save credits joiner state: %v", err)
				}
				continue
			}

			c.SinkProducer.PublishFinished(c.ClientId)
			c.creditsJoiner.removeClient(c.ClientId)

			err := stateSaver.SaveStateAck(&msg, c)
			if err != nil {
				log.Printf("Failed to save credits joiner state: %v", err)
			}
			stateSaver.ForceFlush()
			c.CreditsConsumer.DeleteQueue()

			break

		}

		stringLine := string(msg.Body)

		reader := csv.NewReader(strings.NewReader(stringLine))
		reader.FieldsPerRecord = 2
		record, err := reader.Read()
		if err != nil {
			log.Printf("Failed to read record: %v", err)
			log.Printf("Credit: %s", stringLine)

			continue
		}

		var credit messages.Credit
		err = credit.Deserialize(record)
		if err != nil {
			log.Printf("Failed to deserialize credits: %v", err)
			stateSaver.SaveStateNack(&msg, c, false)

			continue
		}

		if !c.MoviesIds[credit.MovieID] {
			err = stateSaver.SaveStateAck(&msg, c)
			if err != nil {
				log.Printf("Failed to save credits joiner state: %v", err)
			}
			continue
		}

		payload, err := protocol.Serialize(&credit)
		if err != nil {
			log.Printf("Failed to serialize credits: %v", record)
			log.Printf("json.Marshal: %v", err)
			stateSaver.SaveStateNack(&msg, c, false)
			continue
		}

		c.SinkProducer.Publish(payload, c.ClientId, "")

		err = stateSaver.SaveStateAck(&msg, c)
		if err != nil {
			log.Printf("Failed to save credits joiner state: %v", err)
		}

	}
}
