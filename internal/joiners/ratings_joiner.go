package joiners

import (
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type RatingsJoiner struct {
	ratingsJoinerConsumer *utils.ConsumerQueue
	moviesJoinerConsumer  *utils.ConsumerQueue
	sinkProducer          *utils.ProducerQueue
}

func NewRatingsJoiner(ratingsJoinerConsumer *utils.ConsumerQueue, moviesJoinerConsumer *utils.ConsumerQueue, sinkProducer *utils.ProducerQueue) *RatingsJoiner {
	return &RatingsJoiner{ratingsJoinerConsumer: ratingsJoinerConsumer, moviesJoinerConsumer: moviesJoinerConsumer, sinkProducer: sinkProducer}
}

func (r *RatingsJoiner) JoinRatings() error {
	return nil
}
