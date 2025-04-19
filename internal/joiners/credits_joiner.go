package joiners

import (
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type CreditsJoiner struct {
	creditsJoinerConsumer *utils.Queue
	moviesJoinerConsumer  *utils.Queue
	sinkProducer          *utils.Queue
}

func NewCreditsJoiner(creditsJoinerConsumer *utils.Queue, moviesJoinerConsumer *utils.Queue, sinkProducer *utils.Queue) *CreditsJoiner {
	return &CreditsJoiner{creditsJoinerConsumer: creditsJoinerConsumer, moviesJoinerConsumer: moviesJoinerConsumer, sinkProducer: sinkProducer}
}

func (c *CreditsJoiner) JoinCredits() error {
	return nil
}
