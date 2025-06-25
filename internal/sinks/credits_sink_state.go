package sinks

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type CreditsSinkState struct {
	Actors          map[string]map[string]int
	SinkConsumer    utils.ConsumerQueueState
	ResultsProducer utils.ProducerQueueState
}

func SaveCreditsSinkState(s *CreditsSink, actors map[string]map[string]int) error {
	state := CreditsSinkState{
		Actors:          actors,
		SinkConsumer:    s.sinkConsumer.GetState(),
		ResultsProducer: s.resultsProducer.GetState(),
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/credits_sink_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
