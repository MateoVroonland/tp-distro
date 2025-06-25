package filters

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type FilterJoinerState struct {
	FilteredByCountryConsumer utils.ConsumerQueueState
	ClientsProducers          map[string]utils.ProducerQueueState
	Query                     string
}

func SaveFilterJoinerState(f *FilterJoiner) error {
	clientsProducersState := make(map[string]utils.ProducerQueueState)
	for clientId, producer := range f.clientsProducers {
		clientsProducersState[clientId] = producer.GetState()
	}

	state := FilterJoinerState{
		FilteredByCountryConsumer: f.filteredByCountryConsumer.GetState(),
		ClientsProducers:          clientsProducersState,
		Query:                     f.query,
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/filter_joiner_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
