package joiners

import (
	"github.com/MateoVroonland/tp-distro/internal/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)


type RatingsJoiner struct {
	ch *amqp.Channel
	ratingsJoinerConsumer *utils.Queue
	moviesJoinerConsumer *utils.Queue
	sinkProducer *utils.Queue
}

func NewRatingsJoiner(ch *amqp.Channel, ratingsJoinerConsumer *utils.Queue, moviesJoinerConsumer *utils.Queue, sinkProducer *utils.Queue) *RatingsJoiner {
	return &RatingsJoiner{ch: ch, ratingsJoinerConsumer: ratingsJoinerConsumer, moviesJoinerConsumer: moviesJoinerConsumer, sinkProducer: sinkProducer}
}

func (r *RatingsJoiner) JoinRatings() error {
	return nil
}
