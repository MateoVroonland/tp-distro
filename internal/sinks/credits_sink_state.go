package sinks

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type CreditsSinkState struct {
	Actors          map[string]map[string]int
	SinkConsumer    utils.ConsumerQueueState
	ResultsProducer utils.ProducerQueueState
}

func NewCreditsSinkStateSaver() *state_saver.StateSaver[*CreditsSink] {
	return state_saver.NewStateSaver(SaveCreditsSinkState)
}

func SaveCreditsSinkState(s *CreditsSink) error {
	state := CreditsSinkState{
		Actors:          s.actors,
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
