package reducers

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type BudgetReducerState struct {
	Queue            utils.ConsumerQueueState
	PublishQueue     utils.ProducerQueueState
	BudgetPerCountry map[string]map[string]int
}

func NewBudgetReducerState() *state_saver.StateSaver[*BudgetReducer] {
	return state_saver.NewStateSaver(SaveBudgetReducerState)
}

func SaveBudgetReducerState(reducer *BudgetReducer) error {

	state := BudgetReducerState{
		Queue:            reducer.queue.GetState(),
		PublishQueue:     reducer.publishQueue.GetState(),
		BudgetPerCountry: reducer.BudgetPerCountry,
	}

	var buff bytes.Buffer

	err := gob.NewEncoder(&buff).Encode(state)
	if err != nil {
		return err
	}

	return utils.AtomicallyWriteFile("data/budget_reducer_state.gob", buff.Bytes())

}
