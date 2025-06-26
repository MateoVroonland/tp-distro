package state_saver

import (
	"log"
	"sync"
	"time"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

type StateSaver[T any] struct {
	pendingAcks            []func()
	pendingNacks           []func()
	seqNumToAck            map[string]map[string]int // producerId -> clientId -> sequence number
	messagesSinceLastFlush int
	lastFlushedTime        time.Time
	flushMutex             sync.Mutex
	flushFunc              func(T) error
	currentState           T
}

func (s *StateSaver[T]) flushInterval() {
	for {

		if !s.lastFlushedTime.Before(time.Now().Add(-1 * time.Minute)) {
			time.Sleep(1 * time.Minute)
		}

		s.flushMutex.Lock()
		err := s.flush()
		if err != nil {
			log.Printf("Failed to flush state: %v", err)
		}
		s.flushMutex.Unlock()
	}

}

func NewStateSaver[T any](flushFunc func(T) error) *StateSaver[T] {
	stateSaver := &StateSaver[T]{
		pendingAcks:            make([]func(), 0),
		pendingNacks:           make([]func(), 0),
		seqNumToAck:            make(map[string]map[string]int),
		messagesSinceLastFlush: 0,
		lastFlushedTime:        time.Now(),
		flushFunc:              flushFunc,
	}
	go stateSaver.flushInterval()
	return stateSaver
}

func (s *StateSaver[T]) SaveStateAck(msg *utils.Message, currentState T) error {

	if _, ok := s.seqNumToAck[msg.ProducerId]; !ok {
		s.seqNumToAck[msg.ProducerId] = make(map[string]int)
	}

	if msg.SequenceNumber <= s.seqNumToAck[msg.ProducerId][msg.ClientId] {
		log.Printf("Sequence number %d for client %s already acknowledged, current seqNumToAck: %+v", msg.SequenceNumber, msg.ClientId, s.seqNumToAck)
		msg.Ack()
		return nil
	}

	s.currentState = currentState
	s.seqNumToAck[msg.ProducerId][msg.ClientId] = msg.SequenceNumber
	s.pendingAcks = append(s.pendingAcks, msg.Ack)

	if s.messagesSinceLastFlush > 4700 {
		s.flushMutex.Lock()
		err := s.flush()
		if err != nil {
			return err
		}
		s.flushMutex.Unlock()
	}

	s.messagesSinceLastFlush++

	return nil
}
func (s *StateSaver[T]) SaveStateNack(msg *utils.Message, currentState T, requeue bool) {

	if _, ok := s.seqNumToAck[msg.ProducerId]; !ok {
		s.seqNumToAck[msg.ProducerId] = make(map[string]int)
	}

	if msg.SequenceNumber <= s.seqNumToAck[msg.ProducerId][msg.ClientId] {
		log.Printf("Sequence number %d already acknowledged", msg.SequenceNumber)
		return
	}

	s.currentState = currentState
	s.seqNumToAck[msg.ProducerId][msg.ClientId] = msg.SequenceNumber
	s.pendingNacks = append(s.pendingNacks, func() { msg.Nack(requeue) })

	if s.lastFlushedTime.Before(time.Now().Add(-2*time.Second)) || s.messagesSinceLastFlush > 4700 {
		s.flushMutex.Lock()
		err := s.flush()
		if err != nil {
			log.Printf("Failed to flush state: %v", err)
			return
		}
		s.flushMutex.Unlock()
	}

	s.messagesSinceLastFlush++
}

func (s *StateSaver[T]) ForceFlush() (bool, error) {
	if s.messagesSinceLastFlush > 0 {
		s.flushMutex.Lock()
		err := s.flush()
		s.flushMutex.Unlock()
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (s *StateSaver[T]) flush() error {

	err := s.flushFunc(s.currentState)
	if err != nil {
		return err
	}

	for _, ack := range s.pendingAcks {
		ack()
	}
	s.pendingAcks = make([]func(), 0)

	for _, nack := range s.pendingNacks {
		nack()
	}
	s.pendingNacks = make([]func(), 0)

	s.messagesSinceLastFlush = 0
	s.lastFlushedTime = time.Now()

	return nil
}
