package sinks

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type BudgetSinkState struct {
	BudgetPerCountry map[string]map[string]int // clientId -> country -> amount
	Queue            utils.ConsumerQueueState
	ResultsProducer  utils.ProducerQueueState
}

func SaveBudgetSinkState(s *BudgetSink) error {

	state := BudgetSinkState{
		BudgetPerCountry: s.BudgetPerCountry,
		Queue:            s.queue.GetState(),
		ResultsProducer:  s.resultsProducer.GetState(),
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/budget_sink_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil

}
