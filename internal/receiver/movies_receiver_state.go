package receiver

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type MoviesReceiverState struct {
	MoviesConsumer utils.ConsumerQueueState
	Q1Producer     utils.ProducerQueueState
	Q2Producer     utils.ProducerQueueState
	Q3Producer     utils.ProducerQueueState
	Q4Producer     utils.ProducerQueueState
	Q5Producer     utils.ProducerQueueState
}

func NewMoviesReceiverState() *state_saver.StateSaver[*MoviesReceiver] {
	return state_saver.NewStateSaver(SaveMoviesState)
}

func SaveMoviesState(receiver *MoviesReceiver) error {
	state := MoviesReceiverState{
		MoviesConsumer: receiver.MoviesConsumer.GetState(),
		Q1Producer:     receiver.Q1Producer.GetState(),
		Q2Producer:     receiver.Q2Producer.GetState(),
		Q3Producer:     receiver.Q3Producer.GetState(),
		Q4Producer:     receiver.Q4Producer.GetState(),
		Q5Producer:     receiver.Q5Producer.GetState(),
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/movies_receiver_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
