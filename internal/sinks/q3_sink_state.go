package sinks

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type Q3SinkState struct {
	ClientsResults  map[string]MinAndMaxMovie
	SinkConsumer    utils.ConsumerQueueState
	ResultsProducer utils.ProducerQueueState
}

func SaveQ3SinkState(s *Q3Sink, clientsResults map[string]MinAndMaxMovie) error {
	state := Q3SinkState{
		ClientsResults:  clientsResults,
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
