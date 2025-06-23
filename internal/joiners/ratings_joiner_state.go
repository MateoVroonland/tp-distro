package joiners

import (
	"bytes"
	"encoding/gob"

	"github.com/MateoVroonland/tp-distro/internal/state_saver"
	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type RatingsJoinerClientState struct {
	MoviesConsumer         utils.ConsumerQueueState
	RatingsConsumer        utils.ConsumerQueueState
	SinkProducer           utils.ProducerQueueState
	MoviesIds              map[int]string
	Ratings                map[int]float64
	RatingsCount           map[int]int
	ClientId               string
	FinishedFetchingMovies bool
}

func NewRatingsJoinerPerClientState() *state_saver.StateSaver[*RatingsJoinerClient] {
	return state_saver.NewStateSaver(SaveRatingsJoinerPerClientState)
}

func SaveRatingsJoinerPerClientState(
	c *RatingsJoinerClient,
) error {
	state := RatingsJoinerClientState{
		MoviesConsumer:         c.MoviesConsumer.GetState(),
		RatingsConsumer:        c.RatingsConsumer.GetState(),
		SinkProducer:           c.SinkProducer.GetState(),
		MoviesIds:              c.MoviesIds,
		Ratings:                c.Ratings,
		RatingsCount:           c.RatingsCount,
		ClientId:               c.ClientId,
		FinishedFetchingMovies: c.FinishedFetchingMovies,
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/ratings_joiner_state_"+c.ClientId+".gob", buff.Bytes())
	if err != nil {
		return err
	}
	return nil
}

type RatingsJoinerState struct {
	CurrentClients map[string]bool
}

func SaveRatingsJoinerState(r *RatingsJoiner) error {

	state := RatingsJoinerState{
		CurrentClients: r.ClientsJoiners,
	}

	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(state)
	if err != nil {
		return err
	}

	err = utils.AtomicallyWriteFile("data/ratings_joiner_state.gob", buff.Bytes())
	if err != nil {
		return err
	}
	return nil
}
