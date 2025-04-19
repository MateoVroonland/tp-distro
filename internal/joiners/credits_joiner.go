package joiners

import (
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

const CREDITS_JOINER_AMOUNT = 5

type CreditsJoiner struct {
	creditsJoinerConsumer *utils.ConsumerQueue
	moviesJoinerConsumer  *utils.ConsumerQueue
	sinkProducer          *utils.ProducerQueue
}

func NewCreditsJoiner(creditsJoinerConsumer *utils.ConsumerQueue, moviesJoinerConsumer *utils.ConsumerQueue, sinkProducer *utils.ProducerQueue) *CreditsJoiner {
	return &CreditsJoiner{creditsJoinerConsumer: creditsJoinerConsumer, moviesJoinerConsumer: moviesJoinerConsumer, sinkProducer: sinkProducer}
}

func (c *CreditsJoiner) JoinCredits() error {
	return nil
}
