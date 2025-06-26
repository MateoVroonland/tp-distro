package sinks

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type SentimentSinkState struct {
	ClientResults   map[string]SentimentSinkResults
	Queue           utils.ConsumerQueueState
	ResultsProducer utils.ProducerQueueState
}

func NewSentimentSinkStateSaver() *state_saver.StateSaver[*SentimentSink] {
	return state_saver.NewStateSaver(SaveSentimentSinkState)
}

func SaveSentimentSinkState(s *SentimentSink) error {
	state := SentimentSinkState{
		ClientResults:   s.clientResults,
		Queue:           s.sinkConsumer.GetState(),
		ResultsProducer: s.resultsProducer.GetState(),
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/sentiment_sink_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
