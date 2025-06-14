package receiver

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type RatingsReceiverState struct {
	RatingsConsumer utils.ConsumerQueueState
	JoinerProducers map[string]utils.ProducerQueueState
}

func SaveRatingsState(receiver *RatingsReceiver) error {
	state := RatingsReceiverState{
		RatingsConsumer: receiver.ratingsConsumer.GetState(),
		JoinerProducers: make(map[string]utils.ProducerQueueState),
	}

	// Save state for each joiner producer
	for clientId, producer := range receiver.JoinerProducers {
		state.JoinerProducers[clientId] = producer.GetState()
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/ratings_receiver_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
