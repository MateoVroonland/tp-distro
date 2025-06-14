package receiver

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type CreditsReceiverState struct {
	CreditsConsumer utils.ConsumerQueueState
	ClientProducers map[string]utils.ProducerQueueState
}

func SaveCreditsState(receiver *CreditsReceiver) error {
	state := CreditsReceiverState{
		CreditsConsumer: receiver.creditsConsumer.GetState(),
		ClientProducers: make(map[string]utils.ProducerQueueState),
	}

	// Save state for each client producer
	for clientId, producer := range receiver.clientProducers {
		state.ClientProducers[clientId] = producer.GetState()
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/credits_receiver_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
