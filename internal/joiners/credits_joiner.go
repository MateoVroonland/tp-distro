package joiners

import (
	"encoding/csv"
	"log"
	"strings"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

const CREDITS_JOINER_AMOUNT = 1

type CreditsJoiner struct {
	creditsJoinerConsumer *utils.ConsumerQueue
	moviesJoinerConsumer  *utils.ConsumerQueue
	sinkProducer          *utils.ProducerQueue
}

func NewCreditsJoiner(creditsJoinerConsumer *utils.ConsumerQueue, moviesJoinerConsumer *utils.ConsumerQueue, sinkProducer *utils.ProducerQueue) *CreditsJoiner {
	return &CreditsJoiner{creditsJoinerConsumer: creditsJoinerConsumer, moviesJoinerConsumer: moviesJoinerConsumer, sinkProducer: sinkProducer}
}

func (c *CreditsJoiner) JoinCredits() error {
	moviesMsgs, err := c.moviesJoinerConsumer.Consume()
	defer c.creditsJoinerConsumer.CloseChannel()
	defer c.moviesJoinerConsumer.CloseChannel()
	defer c.sinkProducer.CloseChannel()

	moviesIds := make(map[int]bool)

	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
	}

	i := 0
	for msg := range moviesMsgs {
		stringLine := string(msg.Body)

		if stringLine == "FINISHED" {
			c.sinkProducer.Publish([]byte("FINISHED"))
			msg.Ack(false)
			break
		}
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
	}


	creditsMsgs, err := c.creditsJoinerConsumer.Consume()
	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
	}

	credits := make(map[int]messages.Credits)

	j := 0
	for msg := range creditsMsgs {

		stringLine := string(msg.Body)
		if stringLine == "FINISHED" {
			c.sinkProducer.Publish([]byte("FINISHED"))
			msg.Ack(false)
			break
		}
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

		currentCredits, ok := credits[credit.MovieID]

		if !ok {
			credits[credit.MovieID] = credit
		} else {
			log.Printf("APPENDING CREDITS")
			currentCredits.Cast = append(currentCredits.Cast, credit.Cast...)
			credits[credit.MovieID] = currentCredits
		}

		msg.Ack(false)
	}

	log.Printf("Received %d credits", j)

	log.Printf("Credits: %v", credits)

	return nil
}
