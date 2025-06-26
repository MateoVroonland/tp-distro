package sinks

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/protocol/messages"
	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type Q1SinkState struct {
	ClientResults          map[string][]messages.Q1Row
	FilteredByYearConsumer utils.ConsumerQueueState
	ResultsProducer        utils.ProducerQueueState
}

func NewQ1SinkStateSaver() *state_saver.StateSaver[*Q1Sink] {
	return state_saver.NewStateSaver(SaveQ1SinkState)
}

func SaveQ1SinkState(s *Q1Sink) error {
	state := Q1SinkState{
		ClientResults:          s.clientResults,
		FilteredByYearConsumer: s.filteredByYearConsumer.GetState(),
		ResultsProducer:        s.resultsProducer.GetState(),
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/q1_sink_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
