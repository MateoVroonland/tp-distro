package joiners

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type CreditsJoinerState struct {
	MoviesConsumers  map[string]utils.ConsumerQueueState
	CreditsConsumers map[string]utils.ConsumerQueueState
}

func SaveCreditsJoinerState(c *CreditsJoiner) error {
	state := CreditsJoinerState{
		MoviesConsumers:  make(map[string]utils.ConsumerQueueState),
		CreditsConsumers: make(map[string]utils.ConsumerQueueState),
	}

	for clientId, moviesConsumer := range c.MoviesConsumers {
		state.MoviesConsumers[clientId] = moviesConsumer.GetState()
	}

	for clientId, creditsConsumer := range c.CreditsConsumers {
		state.CreditsConsumers[clientId] = creditsConsumer.GetState()
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
