package sinks

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type Q3SinkState struct {
	ClientsResults  map[string]map[int]MovieRatingCumulative
	SinkConsumer    utils.ConsumerQueueState
	ResultsProducer utils.ProducerQueueState
}

func NewQ3SinkStateSaver() *state_saver.StateSaver[*Q3Sink] {
	return state_saver.NewStateSaver(SaveQ3SinkState)
}

func SaveQ3SinkState(s *Q3Sink) error {
	state := Q3SinkState{
		ClientsResults:  s.clientsResults,
		SinkConsumer:    s.SinkConsumer.GetState(),
		ResultsProducer: s.ResultsProducer.GetState(),
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/q3_sink_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
