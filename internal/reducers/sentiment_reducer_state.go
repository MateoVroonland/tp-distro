package reducers

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type SentimentReducerState struct {
	Queue        utils.ConsumerQueueState
	PublishQueue utils.ProducerQueueState
	ClientStats  map[string]map[string]SentimentStats
}

func NewSentimentReducerStateSaver() *state_saver.StateSaver[*SentimentReducer] {
	return state_saver.NewStateSaver(SaveSentimentReducerState)
}

func SaveSentimentReducerState(reducer *SentimentReducer) error {

	state := SentimentReducerState{
		Queue:        reducer.queue.GetState(),
		PublishQueue: reducer.publishQueue.GetState(),
		ClientStats:  reducer.ClientStats,
	}

	var buff bytes.Buffer

	err := gob.NewEncoder(&buff).Encode(state)
	if err != nil {
		return err
	}

	return utils.AtomicallyWriteFile("data/sentiment_reducer_state.gob", buff.Bytes())

}
