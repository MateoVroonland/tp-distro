package joiners

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type CreditsJoinerClientState struct {
	MoviesConsumer  utils.ConsumerQueueState
	CreditsConsumer utils.ConsumerQueueState
	SinkProducer    utils.ProducerQueueState
	MoviesIds       map[int]bool
	ClientId        string
}

func SaveCreditsJoinerPerClientState(
	moviesConsumer utils.ConsumerQueueState,
	creditsConsumer utils.ConsumerQueueState,
	sinkProducer utils.ProducerQueueState,
	moviesIds map[int]bool,
	clientId string,
) error {
	state := CreditsJoinerClientState{
		MoviesConsumer:  moviesConsumer,
		CreditsConsumer: creditsConsumer,
		SinkProducer:    sinkProducer,
		MoviesIds:       moviesIds,
		ClientId:        clientId,
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/credits_joiner_state_"+clientId+".gob", buff.Bytes())
	if err != nil {
		return err
	}
	return nil
}

type CreditsJoinerState struct {
	CurrentClients []string
}

func SaveCreditsJoinerState(currentClients []string) error {
	state := CreditsJoinerState{
		CurrentClients: currentClients,
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/credits_joiner_state.gob", buff.Bytes())
	if err != nil {
		return err
	}
	return nil
}
