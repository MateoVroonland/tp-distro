package sentiment

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type SentimentWorkerState struct {
	InputQueue   utils.ConsumerQueueState
	PublishQueue utils.ProducerQueueState
}

func SaveSentimentWorkerState(w *SentimentWorker) error {
	state := SentimentWorkerState{
		InputQueue:   w.queue.GetState(),
		PublishQueue: w.publishQueue.GetState(),
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/sentiment_worker_state.gob", buff.Bytes())
	if err != nil {
		return err
	}

	return nil
}
