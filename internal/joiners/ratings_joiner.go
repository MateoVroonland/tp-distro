package joiners

import (
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type RatingsJoiner struct {
	ratingsJoinerConsumer *utils.Queue
	moviesJoinerConsumer  *utils.Queue
	sinkProducer          *utils.Queue
}

func NewRatingsJoiner(ratingsJoinerConsumer *utils.Queue, moviesJoinerConsumer *utils.Queue, sinkProducer *utils.Queue) *RatingsJoiner {
	return &RatingsJoiner{ratingsJoinerConsumer: ratingsJoinerConsumer, moviesJoinerConsumer: moviesJoinerConsumer, sinkProducer: sinkProducer}
}

func (r *RatingsJoiner) JoinRatings() error {
	return nil
}
