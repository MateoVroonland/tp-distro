package filters

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/protocol"
	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type FilterState struct {
	FilteredByCountryConsumer utils.ConsumerQueueState
	FilteredByYearProducer    utils.ProducerQueueState
	outputMessage             protocol.MovieToFilter
}

func NewFilterStateSaver() *state_saver.StateSaver[*Filter] {
	return state_saver.NewStateSaver(SaveFilterState)
}

func SaveFilterState(filter *Filter) error {
	state := FilterState{
		FilteredByCountryConsumer: filter.filteredByCountryConsumer.GetState(),
		FilteredByYearProducer:    filter.filteredByYearProducer.GetState(),
		outputMessage:             filter.outputMessage,
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/filter_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
