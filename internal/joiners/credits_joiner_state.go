package joiners

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type CreditsJoinerClientState struct {
	MoviesConsumer         utils.ConsumerQueueState
	CreditsConsumer        utils.ConsumerQueueState
	SinkProducer           utils.ProducerQueueState
	MoviesIds              map[int]bool
	ClientId               string
	FinishedFetchingMovies bool
}

func SaveCreditsJoinerPerClientState(
	c *CreditsJoinerClient,
) error {
	state := CreditsJoinerClientState{
		MoviesConsumer:         c.MoviesConsumer.GetState(),
		CreditsConsumer:        c.CreditsConsumer.GetState(),
		SinkProducer:           c.SinkProducer.GetState(),
		MoviesIds:              c.MoviesIds,
		ClientId:               c.ClientId,
		FinishedFetchingMovies: c.FinishedFetchingMovies,
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/credits_joiner_state_"+c.ClientId+".gob", buff.Bytes())
	if err != nil {
		return err
	}
	return nil
}

type CreditsJoinerState struct {
	CurrentClients map[string]bool
}

func SaveCreditsJoinerState(c *CreditsJoiner) error {

	state := CreditsJoinerState{
		CurrentClients: c.ClientsJoiners,
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
