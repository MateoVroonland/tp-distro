package utils

import (
	"fmt"
	"iter"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/MateoVroonland/tp-distro/internal/env"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerQueue struct {
	ch               *amqp.Channel
	queueName        string
	deliveryChannel  <-chan amqp.Delivery
	signalChan       chan os.Signal
	previousReplicas int
	finishedReceived map[string]map[string]bool
	sequenceNumbers  map[string]map[string]int // clientId -> producerId -> sequenceNumber
	finishedClients  map[string]bool
}

type ConsumerQueueState struct {
	FinishedReceived map[string]map[string]bool
	SequenceNumbers  map[string]map[string]int // clientId -> producerId -> sequenceNumber
	FinishedClients  map[string]bool
}

func (q *ConsumerQueue) GetState() ConsumerQueueState {
	return ConsumerQueueState{
		FinishedReceived: q.finishedReceived,
		SequenceNumbers:  q.sequenceNumbers,
		FinishedClients:  q.finishedClients,
	}
}

func NewConsumerQueue(conn *amqp.Connection, queueName string, exchangeName string, previousReplicas int) (*ConsumerQueue, error) {
	id := env.AppEnv.ID
	uniqueQueueName := fmt.Sprintf("%s_%s", queueName, strconv.Itoa(id))

	return newConsumerQueueWithRoutingKey(conn, uniqueQueueName, exchangeName, strconv.Itoa(id), previousReplicas)
}

func (q *ConsumerQueue) RestoreState(state ConsumerQueueState) {
	q.finishedReceived = state.FinishedReceived
	q.sequenceNumbers = state.SequenceNumbers
	q.finishedClients = state.FinishedClients
}

// func NewConsumerQueueFromState(conn *amqp.Connection, queueName string, exchangeName string, previousReplicas int, state ConsumerQueueState) (*ConsumerQueue, error) {
// 	consumerQueue, err := NewConsumerQueue(conn, queueName, exchangeName, previousReplicas)
// 	if err != nil {
// 		return nil, err
// 	}
// 	consumerQueue.finishedReceived = state.FinishedReceived
// 	consumerQueue.sequenceNumbers = state.SequenceNumbers
// 	return consumerQueue, nil
// }

func newConsumerQueueWithRoutingKey(conn *amqp.Connection, queueName string, exchangeName string, routingKey string, previousReplicas int) (*ConsumerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type.
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	ch.Qos(
		5000,  // prefetch count
		0,     // prefetch size
		false, // global
	)

	deliveryChannel, err := ch.Consume(
		queueName, // queue name - use the unique queue name
		"",        // id
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return nil, err
	}

	return &ConsumerQueue{
		ch:               ch,
		queueName:        queueName,
		deliveryChannel:  deliveryChannel,
		signalChan:       signalChan,
		finishedReceived: make(map[string]map[string]bool),
		previousReplicas: previousReplicas,
		sequenceNumbers:  make(map[string]map[string]int),
		finishedClients:  make(map[string]bool),
	}, nil
}

type Message struct {
	redeliveryCount int
	Body            string
	ClientId        string
	delivery        amqp.Delivery
	SequenceNumber  int
	ProducerId      string
	Redelivered     bool
	IsFinished      bool
	IsLastFinished  bool
}

func (m *Message) Ack() {
	m.delivery.Ack(false)
}

func (m *Message) Nack(requeue bool) {
	m.delivery.Headers["redeliveryCount"] = m.redeliveryCount + 1
	m.delivery.Nack(false, requeue)
}

func MessageFromDelivery(delivery amqp.Delivery) (*Message, error) {
	clientId, ok := delivery.Headers["clientId"].(string)
	if !ok {
		return nil, fmt.Errorf("clientId not found in headers")
	}
	sequenceNumberRaw, ok := delivery.Headers["sequenceNumber"]
	if !ok {
		return nil, fmt.Errorf("sequenceNumber not found in headers, headers: %s", delivery.Headers)
	}

	sequenceNumber, ok := sequenceNumberRaw.(int32)
	if !ok {
		return nil, fmt.Errorf("sequenceNumber of invalid type, sequenceNumberRaw: %s", sequenceNumberRaw)
	}

	producerIdRaw, ok := delivery.Headers["producerId"]
	if !ok {
		return nil, fmt.Errorf("producerId not found in headers, headers: %s", delivery.Headers)
	}

	producerId, ok := producerIdRaw.(string)
	if !ok {
		return nil, fmt.Errorf("producerId of invalid type, producerIdRaw: %s", producerIdRaw)
	}

	redeliveryCountRaw := delivery.Headers["redeliveryCount"]
	redeliveryCount, ok := redeliveryCountRaw.(int32)
	if !ok {
		redeliveryCount = 0
	} else {
		log.Printf("redeliveryCount: %d", redeliveryCount)
	}

	return &Message{
		redeliveryCount: int(redeliveryCount),
		Body:            string(delivery.Body),
		ClientId:        clientId,
		delivery:        delivery,
		SequenceNumber:  int(sequenceNumber),
		ProducerId:      producerId,
		Redelivered:     delivery.Redelivered,
		IsFinished:      strings.HasPrefix(string(delivery.Body), "FINISHED:"),
	}, nil
}

func (q *ConsumerQueue) ConsumeInfinite() iter.Seq[Message] {
	return func(yield func(Message) bool) {

		for {
			select {
			case <-q.signalChan:
				log.Printf("Received SIGTERM signal, closing connection")
				return
			case delivery := <-q.deliveryChannel:
				message, err := MessageFromDelivery(delivery)
				if err != nil {
					log.Printf("Failed to parse message in delivery channel: %v", err)
					log.Printf("delivery: %v", delivery)
					delivery.Nack(false, false)
					continue
				}

				if q.finishedClients[message.ClientId] {
					log.Printf("Finished client %s, ignoring message", message.ClientId)
					message.Ack()
					continue
				}

				if _, ok := q.sequenceNumbers[message.ClientId]; !ok {
					q.sequenceNumbers[message.ClientId] = make(map[string]int)
				}

				expectedSequenceNumber := q.sequenceNumbers[message.ClientId][message.ProducerId] + 1

				if message.SequenceNumber < expectedSequenceNumber {
					log.Printf("Duplicate message for client %s on queue %s, sequence number %d, expected %d, producer %s, ignoring...", message.ClientId, q.queueName, message.SequenceNumber, expectedSequenceNumber, message.ProducerId)
					message.Nack(false)
					continue
				}

				if message.SequenceNumber > expectedSequenceNumber {
					log.Printf("Out of order message for client %s on queue %s, sequence number %d, expected %d, producer %s, requeuing...", message.ClientId, q.queueName, message.SequenceNumber, expectedSequenceNumber, message.ProducerId)
					message.Nack(true)
					continue
				}

				q.sequenceNumbers[message.ClientId][message.ProducerId]++

				if _, ok := q.finishedReceived[message.ClientId]; !ok {
					q.finishedReceived[message.ClientId] = make(map[string]bool)
				}
				if message.IsFinished {
					q.finishedReceived[message.ClientId][message.Body] = true
					if len(q.finishedReceived[message.ClientId]) == q.previousReplicas {
						log.Printf("Received all messages for client %s", message.ClientId)
						delete(q.finishedReceived, message.ClientId)
						delete(q.sequenceNumbers, message.ClientId) // TODO: check if no data will be lost, maybe do later or garbage collect?
						q.finishedClients[message.ClientId] = true
						finishedMessage := message
						finishedMessage.Body = "FINISHED"
						message.IsLastFinished = true
					}
				}

				if !yield(*message) {
					log.Printf("Exiting early consumer loop")
					return
				}
			}
		}
	}
}

func (q *ConsumerQueue) CloseChannel() error {
	return q.ch.Close()
}

type ProducerQueue struct {
	ch              *amqp.Channel
	boundQueues     map[string]bool
	exchangeName    string
	isFanout        bool
	nextReplicas    int
	sequenceNumbers map[string]map[string]int // clientId -> routingKey -> sequenceNumber
	sequenceMutex   sync.RWMutex
}

type ProducerQueueState struct {
	SequenceNumbers map[string]map[string]int // clientId -> routingKey -> sequenceNumber
}

func (q *ProducerQueue) GetState() ProducerQueueState {
	q.sequenceMutex.RLock()
	defer q.sequenceMutex.RUnlock()
	return ProducerQueueState{
		SequenceNumbers: q.sequenceNumbers,
	}
}

func (q *ProducerQueue) RestoreState(state ProducerQueueState) {
	q.sequenceMutex.Lock()
	defer q.sequenceMutex.Unlock()
	q.sequenceNumbers = state.SequenceNumbers
}

// func NewProducerQueueFromState(conn *amqp.Connection, exchangeName string, nextReplicas int, state ProducerQueueState) (*ProducerQueue, error) {
// 	producerQueue, err := NewProducerQueue(conn, exchangeName, nextReplicas)
// 	if err != nil {
// 		return nil, err
// 	}
// 	producerQueue.sequenceNumbers = state.SequenceNumbers
// 	return producerQueue, nil
// }

func NewProducerQueue(conn *amqp.Connection, exchangeName string, nextReplicas int) (*ProducerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type.
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	return &ProducerQueue{
		ch:              ch,
		exchangeName:    exchangeName,
		boundQueues:     make(map[string]bool),
		isFanout:        false,
		nextReplicas:    nextReplicas,
		sequenceNumbers: make(map[string]map[string]int),
		sequenceMutex:   sync.RWMutex{},
	}, nil
}

func (q *ProducerQueue) Publish(body []byte, clientId string, routingKey string) error {
	hashedRoutingKey := strconv.Itoa(HashString(routingKey, q.nextReplicas))
	return q.publishWithParams(body, hashedRoutingKey, clientId, strconv.Itoa(env.AppEnv.ID))
}

func (q *ProducerQueue) PublishResults(body []byte, clientId string, producerId string) error {
	hashedRoutingKey := strconv.Itoa(HashString(producerId, q.nextReplicas))
	return q.publishWithParams(body, hashedRoutingKey, clientId, producerId)
}

func (q *ProducerQueue) PublishFinished(clientId string) error {
	for i := range q.nextReplicas {
		q.publishWithParams([]byte("FINISHED:"+strconv.Itoa(env.AppEnv.ID)), strconv.Itoa(i+1), clientId, strconv.Itoa(env.AppEnv.ID))
	}
	q.sequenceMutex.Lock()
	delete(q.sequenceNumbers, clientId) // TODO: check if no data will be lost, maybe do later or garbage collect?
	q.sequenceMutex.Unlock()
	return nil
}

func (q *ProducerQueue) publishWithParams(body []byte, routingKey string, clientId string, producerId string) error {
	if !q.boundQueues[routingKey] && !q.isFanout {
		name := fmt.Sprintf("%s_%s", q.exchangeName, routingKey)
		_, err := q.ch.QueueDeclare(name, false, false, false, false, nil)
		if err != nil {
			return err
		}
		err = q.ch.QueueBind(
			name,
			routingKey,
			q.exchangeName,
			false,
			nil,
		)
		if err != nil {
			return err
		}
		q.boundQueues[routingKey] = true
	}

	q.sequenceMutex.Lock()
	if _, ok := q.sequenceNumbers[clientId]; !ok {
		q.sequenceNumbers[clientId] = make(map[string]int)
	}

	q.sequenceNumbers[clientId][routingKey]++
	sequenceNumber := q.sequenceNumbers[clientId][routingKey]
	q.sequenceMutex.Unlock()

	headers := amqp.Table{
		"clientId":       clientId,
		"sequenceNumber": sequenceNumber,
		"producerId":     producerId,
	}

	err := q.ch.Publish(
		q.exchangeName, // exchange
		routingKey,     // routing key
		true,           // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
			Headers:     headers,
		})
	if err != nil {
		return err
	}

	return nil
}

func (q *ProducerQueue) CloseChannel() error {
	return q.ch.Close()
}

func (q *ConsumerQueue) DeleteQueue() error {
	_, err := q.ch.QueueDelete(q.queueName, false, false, false)
	log.Printf("Deleted queue %s", q.queueName)
	return err
}
